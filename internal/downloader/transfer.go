// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package downloader

import (
	"hash"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
)

// transfer is the live state of one download: the open partial file, the running
// SHA-256, and the resume record that makes both survivable across a restart.
// Owned by a single goroutine (the one inside Download), so it needs no locking.
type transfer struct {
	dest   string
	resume *resumeState
	f      *os.File
	h      hash.Hash

	// written is the number of bytes appended so far. It tracks the file position;
	// the resume record's Written lags it by up to one checkpoint interval.
	written int64
	// total is the full size once a response has advertised it, else 0.
	total int64

	// capacityChecked makes the free-space pre-flight run once per download rather
	// than once per chunk.
	capacityChecked bool

	// noRange records that storage answered a ranged request with a full 200
	// response, so this transfer cannot be chunked — the body has to be consumed in
	// one pass. Set by fetchChunk.
	noRange bool

	closed bool
}

// openTransfer prepares a download, resuming from a usable record when one exists.
func (d *downloader) openTransfer(req Request, digestHex string) (*transfer, error) {
	dest := req.DestPath
	if err := os.MkdirAll(filepath.Dir(dest), dirMode); err != nil {
		return nil, storageapi.Terminal(ErrOpenFile, "create directory for %s", dest).Wrap(err)
	}

	state := loadResumeState(dest, req.URL, digestHex)
	if state == nil {
		// Nothing usable to continue: drop any stale partial so the fresh record and
		// the file agree from byte zero.
		removeQuietly(partPath(dest), resumePath(dest))
		var err error
		if state, err = newResumeState(req.URL, digestHex, d.now()); err != nil {
			return nil, storageapi.Terminal(ErrOpenFile, "initialise resume state").Wrap(err)
		}
		if err := state.save(dest, d.now()); err != nil {
			return nil, storageapi.Terminal(ErrOpenFile, "write resume state").Wrap(err)
		}
	}

	h, err := state.hasher()
	if err != nil {
		return nil, storageapi.Terminal(ErrOpenFile, "restore digest state").Wrap(err)
	}

	f, err := os.OpenFile(partPath(dest), os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return nil, storageapi.Terminal(ErrOpenFile, "open partial file for %s", dest).Wrap(err)
	}

	t := &transfer{dest: dest, resume: state, f: f, h: h, written: state.Written, total: state.TotalSize}

	// The invariant that makes resume safe: the record is authoritative. The partial
	// file may be longer than Written (bytes fsynced but not yet recorded), never
	// shorter, so cutting it back to Written realigns the bytes on disk with the
	// hasher state that describes them.
	if err := t.truncateTo(state.Written); err != nil {
		t.close()
		return nil, err
	}
	return t, nil
}

// truncateTo cuts the partial file to n bytes and positions the write offset there.
func (t *transfer) truncateTo(n int64) error {
	if err := t.f.Truncate(n); err != nil {
		return storageapi.Terminal(ErrWriteFile, "truncate partial file to %d", n).Wrap(err)
	}
	if _, err := t.f.Seek(n, io.SeekStart); err != nil {
		return storageapi.Terminal(ErrWriteFile, "seek partial file to %d", n).Wrap(err)
	}
	return nil
}

// setTotal records the size learned from a response. Once known it must never
// change: a different size means the artefact behind the URL was replaced
// mid-transfer, and the bytes already on disk belong to the old one — which is why
// this is ErrBadSize and drops the partial rather than ending a single attempt.
func (t *transfer) setTotal(total int64) error {
	if total <= 0 {
		return storageapi.Terminal(ErrBadSize, "storage advertised a non-positive size %d", total)
	}
	if t.total == 0 {
		t.total = total
		return nil
	}
	if t.total != total {
		return storageapi.Terminal(ErrBadSize, "size changed mid-transfer: was %d, now %d", t.total, total)
	}
	return nil
}

// append writes p to the partial file and folds it into the digest.
func (t *transfer) append(p []byte) error {
	if t.total > 0 && t.written+int64(len(p)) > t.total {
		return storageapi.Terminal(ErrBadSize, "storage sent more than the advertised %d bytes", t.total)
	}
	if _, err := t.f.Write(p); err != nil {
		return storageapi.Terminal(ErrWriteFile, "write at offset %d", t.written).Wrap(err)
	}
	// hash.Hash.Write never returns an error (documented), so the digest cannot
	// silently fall out of step with the file.
	t.h.Write(p)
	t.written += int64(len(p))
	return nil
}

// commit makes the current offset durable and records it. Ordering matters: the
// bytes are fsynced BEFORE the record names them, so a crash can only ever leave
// the record behind the file, never ahead of it.
func (t *transfer) commit(now time.Time) error {
	if t.written == t.resume.Written && t.total == t.resume.TotalSize {
		return nil
	}
	if err := t.f.Sync(); err != nil {
		return storageapi.Terminal(ErrWriteFile, "fsync partial file").Wrap(err)
	}
	t.resume.TotalSize = t.total
	if err := t.resume.checkpoint(t.dest, t.written, t.h, now); err != nil {
		return storageapi.Terminal(ErrOpenFile, "checkpoint resume state").Wrap(err)
	}
	return nil
}

// reset discards everything downloaded so far and restarts from byte 0. Used when
// the server ignores our ranged request and replies with the whole object.
func (t *transfer) reset(now time.Time) error {
	if err := t.truncateTo(0); err != nil {
		return err
	}
	fresh, err := newResumeState(t.resume.URL, t.resume.Digest, now)
	if err != nil {
		return storageapi.Terminal(ErrOpenFile, "reset digest state").Wrap(err)
	}
	if t.h, err = fresh.hasher(); err != nil {
		return storageapi.Terminal(ErrOpenFile, "reset digest state").Wrap(err)
	}
	t.written = 0
	if err := t.resume.checkpoint(t.dest, 0, t.h, now); err != nil {
		return storageapi.Terminal(ErrOpenFile, "checkpoint resume state after reset").Wrap(err)
	}
	return nil
}

// digest returns the SHA-256 of everything appended so far. Sum does not consume
// the hasher, so the transfer stays usable afterwards.
func (t *transfer) digest() [32]byte {
	var out [32]byte
	copy(out[:], t.h.Sum(nil))
	return out
}

// finish promotes the verified partial file to the destination and drops the resume
// record. The rename is what publishes the artefact: until it happens no consumer
// can mistake an incomplete download for a complete one.
func (t *transfer) finish() error {
	if err := t.f.Sync(); err != nil {
		return storageapi.Terminal(ErrWriteFile, "fsync before rename").Wrap(err)
	}
	if err := t.closeFile(); err != nil {
		return storageapi.Terminal(ErrWriteFile, "close partial file").Wrap(err)
	}
	if err := os.Rename(partPath(t.dest), t.dest); err != nil {
		return storageapi.Terminal(ErrWriteFile, "rename %s to %s", partPath(t.dest), t.dest).Wrap(err)
	}
	removeQuietly(resumePath(t.dest))
	return nil
}

// close releases the partial file. Safe to call more than once, so Download can
// defer it while finish also closes on the success path.
func (t *transfer) close() {
	if err := t.closeFile(); err != nil {
		slog.Warn("downloader: closing partial file failed", "dest", t.dest, "error", err)
	}
}

func (t *transfer) closeFile() error {
	if t.closed {
		return nil
	}
	t.closed = true
	return t.f.Close()
}
