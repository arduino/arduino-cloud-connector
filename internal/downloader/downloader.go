// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package downloader fetches a large artefact onto local disk, resumably, driving
// an internal/storage-api client for the bytes.
//
// The split of responsibilities is the point of the package: storage-api knows how
// to ask for a byte range over mTLS and nothing else, while everything that makes a
// multi-gigabyte transfer survivable lives here — the partial file, the resumable
// SHA-256, checkpointing, chunk sizing, the retry budget, the free-space
// pre-flight, and the verification that gates "downloaded" from "usable".
//
// It has no notion of an App, a deploy job, a board or the OTA protocol. Callers
// hand it a URL, a destination and an expected digest; it hands back bytes on disk
// or a typed error. Mapping those errors onto anything user-visible (for the
// App-deploy flow: the OTAProgressCmd wire codes) is the caller's job — see
// internal/ota.
//
// Resume is invisible from the outside: the sidecar files live next to the
// destination and a transfer is picked back up automatically when Download is
// called again for the same destination and digest. Callers never see a partial
// file, and never need to know how one is tracked.
package downloader

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
	"github.com/arduino/arduino-cloud-connector/internal/system"
)

// Transfer tuning.
//
// # Why the transfer is cut into ranged chunks
//
// An artefact can exceed 2 GB, so one HTTP response would have to stay healthy for
// tens of minutes. Every hop in between has an opinion about that: CDNs and reverse
// proxies cap response duration, and a storage URL can expire while its own body is
// still streaming. The C++ ArduinoIoTCloud library splits the transfer for the same
// reason (its ChunkDownload policy, OTAInterfaceDefault::requestOta), so this
// implementation does too: each chunk is a separate short-lived ranged request, and
// a failure only ever costs the chunk in flight — never the gigabytes already on
// disk.
//
// The artefact is never held in memory: bytes go straight from the response body to
// the file in readBufSize slices, hashed on the way past.
const (
	// chunkSize is how much one ranged request asks for.
	chunkSize = int64(64) << 20 // 64 MiB
	// readBufSize is the response-body read granularity.
	readBufSize = 256 << 10 // 256 KiB
	// checkpointInterval is how many bytes may be written before the resume record
	// is updated. It is the upper bound on transfer redone after a crash.
	checkpointInterval = int64(8) << 20 // 8 MiB
	// progressInterval is the onProgress cadence. 10 s matches the MCU firmware-OTA
	// cadence (OTAInterfaceDefault::parseOta), which is what the Arduino Cloud UI
	// already expects for a deploy (RFC-14 §5.3).
	progressInterval = 10 * time.Second
)

// Chunk retry policy.
//
// A failed chunk is retried with exponential back-off, bounded two ways:
// maxChunkAttempts consecutive failures, or retryWindow of wall-clock time in the
// same failure streak — whichever trips first ends the download.
//
// The budget counts CONSECUTIVE failures and resets as soon as an attempt transfers
// bytes. That matters at this size: on a flaky link a 2 GB download can legitimately
// be interrupted far more than five times while still making steady forward
// progress, and killing it would be wrong — the failure worth reporting is "stuck",
// not "slow". The overall bound on a download that never finishes is
// config.DownloadTimeout, applied to the whole call.
const (
	maxChunkAttempts   = 5
	retryWindow        = 3 * time.Minute
	retryBackoffBase   = 2 * time.Second
	retryBackoffMax    = 60 * time.Second
	retryBackoffJitter = 0.20 // ±20%
)

// Downloader fetches artefacts onto local disk.
type Downloader interface {
	// Download fetches req.URL into req.DestPath and verifies it against
	// req.ExpectedSHA256. On success DestPath holds the complete, verified
	// artefact; on failure it does not exist.
	//
	// Verification hashes the FILE on disk, not the byte stream that produced it, so
	// what the caller is handed is what was checked. A stream that arrived intact and
	// a file that did not keep it are distinguished in the log.
	//
	// A transfer interrupted by a crash, a restart or a network failure is resumed
	// automatically on the next call for the same DestPath and digest — including
	// across process restarts, so a multi-gigabyte artefact is never re-fetched
	// from byte zero because the daemon was restarted.
	//
	// A failure that invalidates the bytes already on disk (a digest that does not
	// match, a size that changed mid-transfer, a server that oversent) drops the
	// partial before returning, so a later call cannot resume from poisoned bytes.
	// A failure that leaves them valid (the retry budget running out, a deadline, a
	// cancellation) keeps the partial — that is what resume is for.
	//
	// onProgress, if non-nil, is called periodically with the bytes written so far
	// and the total size (0 while still unknown). It must not block.
	//
	// Errors match one of the Err* sentinels below, or one of storage-api's, via
	// errors.Is — except a context cancellation, which is returned as-is so callers
	// can tell "asked to stop" apart from "failed".
	Download(ctx context.Context, req Request, onProgress ProgressFunc) error

	// Discard removes the partial file and resume state for destPath, abandoning
	// any interrupted transfer. Call it once a failure has been acted on, so a
	// later Download for the same destination starts clean instead of resuming a
	// transfer nobody is waiting for. Safe to call when nothing is in progress.
	Discard(destPath string)
}

// Request describes one artefact to fetch.
type Request struct {
	// URL is the artefact's location, as sent by the cloud. Must be https; it is
	// otherwise used verbatim, host included.
	URL string
	// ExpectedSHA256 is the digest the fetched bytes must hash to. It is also the
	// artefact's identity for resume purposes: a resume record with a different
	// digest describes different bytes and is discarded rather than continued.
	ExpectedSHA256 [32]byte
	// DestPath is the absolute path the verified artefact is written to. The
	// sidecar files live alongside it.
	DestPath string
}

// ProgressFunc reports transfer progress. total is 0 until the size is known.
type ProgressFunc func(written, total int64)

// Failure sentinels for everything this package decides. Network-level outcomes
// keep storage-api's sentinels (storageapi.ErrConnect, ErrBadResponse,
// ErrBadHeaders, ErrURLInvalid) and travel through unchanged, so a caller
// classifies both layers with errors.Is.
var (
	// ErrTooLarge: the artefact is larger than the configured limit.
	ErrTooLarge = errors.New("downloader: artefact exceeds the size limit")
	// ErrNoSpace: not enough free space to store the artefact.
	ErrNoSpace = errors.New("downloader: not enough free space")
	// ErrBadSize: the size storage reported cannot be reconciled with the transfer
	// — non-positive, changed mid-flight, or exceeded by the bytes actually sent.
	// Every case means the bytes on disk describe a different artefact, so the
	// partial is dropped.
	ErrBadSize = errors.New("downloader: artefact size is inconsistent")
	// ErrTransfer: the transfer failed or was truncated and the retry budget ran
	// out.
	ErrTransfer = errors.New("downloader: transfer failed")
	// ErrDigestMismatch: the artefact on disk does not hash to ExpectedSHA256. Raised
	// from the verification pass that reads the finished file back, so it covers both
	// an artefact that never matched and one that did not survive being stored.
	ErrDigestMismatch = errors.New("downloader: digest mismatch")
	// ErrTimeout: the download did not finish within the configured timeout.
	ErrTimeout = errors.New("downloader: download exceeded its deadline")
	// ErrOpenFile: the destination or its resume state could not be opened.
	ErrOpenFile = errors.New("downloader: cannot open the destination")
	// ErrWriteFile: writing to the destination failed.
	ErrWriteFile = errors.New("downloader: cannot write the destination")
)

// downloader is the real implementation.
type downloader struct {
	cfg    config.Config
	client storageapi.Client

	// Transfer granularity. Defaulted from the constants above; overridable so
	// tests can exercise the chunk, checkpoint and progress paths without moving
	// tens of megabytes.
	chunkSize          int64
	checkpointInterval int64
	progressInterval   time.Duration

	// Injection seams for tests: wall-clock time, sleeping and free-space
	// reporting are what a unit test cannot afford to do for real. diskFree is
	// system.DiskFreeBytes in production — the platform syscall lives in
	// internal/system, next to the other OS lookups.
	now      func() time.Time
	sleep    func(context.Context, time.Duration) error
	diskFree func(dir string) (int64, error)
}

// New returns a Downloader that fetches through client.
func New(cfg config.Config, client storageapi.Client) Downloader {
	slog.Info("downloader: ready",
		"max_size", cfg.MaxBundleSize, "timeout", cfg.DownloadTimeout)
	return newDownloader(cfg, client)
}

func newDownloader(cfg config.Config, client storageapi.Client) *downloader {
	return &downloader{
		cfg:                cfg,
		client:             client,
		chunkSize:          chunkSize,
		checkpointInterval: checkpointInterval,
		progressInterval:   progressInterval,
		now:                time.Now,
		sleep:              sleepCtx,
		diskFree:           system.DiskFreeBytes,
	}
}
