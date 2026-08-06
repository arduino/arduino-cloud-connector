// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package downloader

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"path/filepath"
	"time"

	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
)

// Discard implements Downloader.
func (d *downloader) Discard(destPath string) {
	removeQuietly(partPath(destPath), resumePath(destPath))
}

// Download implements Downloader.
func (d *downloader) Download(ctx context.Context, req Request, onProgress ProgressFunc) error {
	if req.DestPath == "" {
		return storageapi.Terminal(ErrOpenFile, "no destination path given")
	}
	if onProgress == nil {
		onProgress = func(int64, int64) {}
	}
	digestHex := hexDigest(req.ExpectedSHA256)

	// One deadline for the whole transfer, retries and back-off included.
	ctx, cancel := context.WithTimeout(ctx, d.cfg.DownloadTimeout)
	defer cancel()

	t, err := d.openTransfer(req, digestHex)
	if err != nil {
		return err
	}
	defer t.close()

	if t.written > 0 {
		slog.Info("downloader: resuming interrupted download",
			"dest", req.DestPath, "resume_at", t.written, "total", t.total)
	}

	if err := d.run(ctx, t, req, onProgress); err != nil {
		return d.abandon(t, req.DestPath, err)
	}

	// Digest check over the bytes actually written — the gate between "downloaded"
	// and "usable".
	if got := t.digest(); got != req.ExpectedSHA256 {
		return d.abandon(t, req.DestPath,
			storageapi.Terminal(ErrDigestMismatch, "got %x, want %x", got, req.ExpectedSHA256))
	}

	if err := t.finish(); err != nil {
		return err
	}
	slog.Info("downloader: download complete and verified",
		"dest", req.DestPath, "bytes", t.written)
	return nil
}

// abandon decides the fate of the partial file after a failed download, and is the
// single place that decision is made.
//
// Most failures leave the bytes on disk perfectly valid — the retry budget ran out
// on a flaky link, the deadline expired, the caller cancelled. Those keep their
// partial, which is the entire reason the resume record exists: the next attempt
// continues instead of re-fetching gigabytes.
//
// A minority mean the opposite: the bytes describe something other than the
// artefact being asked for. Those must not survive, or the next attempt would
// resume from poisoned bytes and only discover it at the final digest check, having
// re-downloaded the remainder for nothing. This is enforced here rather than left to
// the caller so the invariant belongs to the package that owns the files.
func (d *downloader) abandon(t *transfer, dest string, err error) error {
	if !poisonsPartial(err) {
		return err
	}
	// Close before unlinking: an open file cannot be removed on every platform, and
	// silently failing to delete it would leave bytes behind that can never verify.
	t.close()
	d.Discard(dest)
	return err
}

// poisonsPartial reports whether err means the bytes already written can no longer
// be trusted to belong to the requested artefact.
func poisonsPartial(err error) bool {
	return errors.Is(err, ErrDigestMismatch) || errors.Is(err, ErrBadSize)
}

// isRetryable reports whether the download loop may try err again. The answer is
// carried by storageapi.Failure, which both this package and the storage client use
// to raise their failures, so one classification covers the whole loop.
func isRetryable(err error) bool {
	var f *storageapi.Failure
	return errors.As(err, &f) && f.Retry
}

// run drives the chunk loop with its retry budget.
func (d *downloader) run(ctx context.Context, t *transfer, req Request, onProgress ProgressFunc) error {
	var (
		failures    int
		streakStart time.Time
	)
	for t.total == 0 || t.written < t.total {
		startedAt := t.written

		err := d.fetchChunk(ctx, t, req, onProgress)
		if err == nil {
			failures, streakStart = 0, time.Time{}
			continue
		}

		// Distinguish "our deadline/shutdown" from "the transfer failed": the context
		// error wins, because retrying under a dead context is pointless.
		if ctxErr := ctx.Err(); ctxErr != nil {
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return storageapi.Terminal(ErrTimeout, "exceeded %s at %d/%d bytes",
					d.cfg.DownloadTimeout, t.written, t.total)
			}
			return ctxErr // cancelled by the caller: not a failure
		}
		if !isRetryable(err) {
			return err
		}

		// Any forward progress means the link is working, just unreliable — start
		// the budget over rather than giving up on a large artefact.
		if t.written > startedAt {
			failures, streakStart = 0, time.Time{}
		}
		if streakStart.IsZero() {
			streakStart = d.now()
		}
		failures++
		elapsed := d.now().Sub(streakStart)
		if failures >= maxChunkAttempts || elapsed >= retryWindow {
			return storageapi.Terminal(ErrTransfer,
				"failed after %d consecutive attempts in %s at %d/%d bytes: %v",
				failures, elapsed.Round(time.Second), t.written, t.total, err)
		}

		delay := backoffDelay(failures)
		slog.Warn("downloader: chunk failed, retrying",
			"dest", req.DestPath, "attempt", failures, "max_attempts", maxChunkAttempts,
			"retry_in", delay, "written", t.written, "total", t.total, "error", err)
		if err := d.sleep(ctx, delay); err != nil {
			return err
		}
	}
	return nil
}

// fetchChunk asks the storage client for the next range and streams it to disk.
// Returns nil when the chunk (or, on a server that ignores Range, the whole body)
// has been consumed.
func (d *downloader) fetchChunk(ctx context.Context, t *transfer, req Request, onProgress ProgressFunc) error {
	start, end := d.rangeFor(t)
	resp, err := d.client.FetchAppRelease(ctx, req.URL, start, end)
	if err != nil {
		return err
	}
	defer func() {
		// Drain-free close: the body may hold tens of MiB we no longer want.
		_ = resp.Body.Close()
	}()

	if err := t.setTotal(resp.Total); err != nil {
		return err
	}

	if !resp.Ranged {
		// Splicing a from-byte-0 body onto a partial file would silently corrupt the
		// artefact, so restart the transfer instead.
		if t.written > 0 {
			slog.Warn("downloader: Range request ignored; restarting from the beginning",
				"dest", req.DestPath, "discarded_bytes", t.written,
				"requested_range", fmt.Sprintf("bytes=%d-%d", start, end))
			if err := t.reset(d.now()); err != nil {
				return err
			}
		}
		// Must be set before streaming: it tells stream to consume the whole body in
		// one pass. Stopping at a chunk boundary and asking for the next range would
		// get the whole object again from byte 0 — forever.
		t.noRange = true
	}

	if err := d.checkCapacity(t); err != nil {
		return err
	}
	return d.stream(t, resp.Body, onProgress)
}

// rangeFor is the next chunk to ask for. Before the total is known the first
// request still asks for a bounded range, so a server that honours Range never
// starts a multi-gigabyte response we would immediately want to cut short.
func (d *downloader) rangeFor(t *transfer) (start, end int64) {
	start = t.written
	end = start + d.chunkSize - 1
	if t.total > 0 && end > t.total-1 {
		end = t.total - 1
	}
	return start, end
}

// checkCapacity refuses an artefact that is too large to accept or too large to
// store, before a single byte of it is written.
func (d *downloader) checkCapacity(t *transfer) error {
	if t.total > d.cfg.MaxBundleSize {
		return storageapi.Terminal(ErrTooLarge,
			"artefact is %d bytes, limit is %d (ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE)",
			t.total, d.cfg.MaxBundleSize)
	}
	if t.capacityChecked {
		return nil
	}
	t.capacityChecked = true

	dir := filepath.Dir(t.dest)
	free, err := d.diskFree(dir)
	if err != nil {
		// Not knowing the free space is not a reason to refuse the download; the
		// write itself will fail with ENOSPC if it really does not fit.
		slog.Warn("downloader: could not determine free space, skipping pre-flight check",
			"dir", dir, "error", err)
		return nil
	}
	need := t.total - t.written
	if free < need {
		return storageapi.Terminal(ErrNoSpace, "need %d more bytes in %s, only %d free", need, dir, free)
	}
	slog.Debug("downloader: free space check passed", "dir", dir, "need", need, "free", free)
	return nil
}

// stream copies the response body to disk, hashing as it goes, checkpointing every
// checkpointInterval bytes and reporting progress every progressInterval. Returns
// nil when the body is exhausted or the chunk is full.
func (d *downloader) stream(t *transfer, body io.Reader, onProgress ProgressFunc) error {
	buf := make([]byte, readBufSize)
	// Where this pass should stop. On a Range-capable server that is the end of the
	// requested chunk; on one that ignored Range there is only ever one pass,
	// covering the whole object.
	chunkEnd := t.written + d.chunkSize
	if t.noRange {
		chunkEnd = t.total
	}
	if t.total > 0 && chunkEnd > t.total {
		chunkEnd = t.total
	}
	lastCheckpoint := t.written
	lastProgress := d.now()

	for {
		n, readErr := body.Read(buf)
		if n > 0 {
			if err := t.append(buf[:n]); err != nil {
				return err
			}
			if t.written-lastCheckpoint >= d.checkpointInterval {
				if err := t.commit(d.now()); err != nil {
					return err
				}
				lastCheckpoint = t.written
			}
			if now := d.now(); now.Sub(lastProgress) >= d.progressInterval {
				onProgress(t.written, t.total)
				lastProgress = now
			}
		}

		switch {
		case readErr == nil:
			if t.written >= chunkEnd {
				// Chunk complete. Persist it, then let the loop request the next range
				// on a fresh connection.
				return t.commit(d.now())
			}
		case errors.Is(readErr, io.EOF):
			if err := t.commit(d.now()); err != nil {
				return err
			}
			if t.written < chunkEnd {
				return storageapi.Retryable(ErrTransfer,
					"body ended early at %d, expected %d", t.written, chunkEnd)
			}
			return nil
		default:
			// Persist what did arrive before backing off — those bytes are not
			// re-fetched on the retry.
			if err := t.commit(d.now()); err != nil {
				return err
			}
			return storageapi.Retryable(ErrTransfer, "read body at offset %d", t.written).Wrap(readErr)
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// backoffDelay is exponential back-off with jitter for the failures'th consecutive
// failure (1-based), capped at retryBackoffMax. Jitter keeps a fleet of boards from
// retrying a struggling storage endpoint in lockstep.
func backoffDelay(failures int) time.Duration {
	delay := retryBackoffBase
	for i := 1; i < failures && delay < retryBackoffMax; i++ {
		delay *= 2
	}
	if delay > retryBackoffMax {
		delay = retryBackoffMax
	}
	// crypto/rand mirrors the jitter helper in internal/cloud.
	n, err := rand.Int(rand.Reader, big.NewInt(1000))
	if err != nil {
		return delay
	}
	frac := (float64(n.Int64())/1000)*2 - 1 // [-1, 1)
	return delay + time.Duration(float64(delay)*retryBackoffJitter*frac)
}

// sleepCtx sleeps for d unless ctx is cancelled first.
func sleepCtx(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// hexDigest renders a digest as lowercase hex — the form stored in the resume
// record and used as the artefact's identity.
func hexDigest(dg [32]byte) string { return hex.EncodeToString(dg[:]) }
