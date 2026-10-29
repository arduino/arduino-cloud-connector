// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package downloader

import (
	"crypto/sha256"
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

// A download in progress owns two sidecar files next to its destination:
//
//	<dest>.part    — the bytes fetched so far
//	<dest>.resume  — how many of those bytes are trustworthy
//
// Both are internal to this package. A caller only ever sees <dest>, which appears
// atomically once the transfer is complete and verified.
//
// # Why a resume record at all
//
// An artefact can exceed 2 GB, so a transfer can easily outlive the process (a
// crash, a package upgrade, a reboot). Re-fetching from byte 0 would throw away the
// whole transfer, so the record holds the byte offset already on disk and the next
// attempt continues with a ranged request.
//
// # The consistency problem, and the invariant that solves it
//
// Resuming needs the SHA-256 of the bytes already written, but SHA-256 is streaming
// state, not a property of a byte range that can be re-derived cheaply — rehashing
// 2 GB from disk on every restart is exactly the cost this record exists to avoid.
// So it stores the *marshalled hasher state* (crypto/sha256's digest implements
// encoding.BinaryMarshaler) alongside the offset it belongs to.
//
// Two files therefore have to agree, and no filesystem gives us an atomic write
// across both. The order of operations makes the record authoritative:
//
//  1. append the chunk to <dest>.part
//  2. fsync <dest>.part          — the bytes are durable
//  3. write <dest>.resume atomically (temp + rename) with Written = the new offset
//     and HashState = the hasher folded over exactly those Written bytes
//
// A crash between 2 and 3 leaves .part LONGER than Written but never shorter, so
// load truncates .part back to Written. Written bytes and hasher state are then
// consistent by construction, and at most one checkpoint interval of transfer is
// redone.
const (
	partSuffix   = ".part"
	resumeSuffix = ".resume"

	// Artefacts are fetched 0600 into a 0700 directory: an in-flight download is
	// never world-readable.
	fileMode = 0o600
	dirMode  = 0o700
)

// resumeState is the on-disk record for one in-flight transfer.
type resumeState struct {
	// URL and Digest identify the artefact. A record whose either field differs
	// from the current request describes different bytes, so it is discarded rather
	// than continued.
	URL    string `json:"url"`
	Digest string `json:"digest"`
	// TotalSize is the full length once a response has advertised it (0 = not yet
	// known).
	TotalSize int64 `json:"total_size"`
	// Written is the number of bytes durably in <dest>.part AND folded into
	// HashState. This field, not the size of .part, defines the resume point.
	Written int64 `json:"written"`
	// HashState is the marshalled crypto/sha256 digest after exactly Written bytes.
	HashState []byte    `json:"hash_state"`
	UpdatedAt time.Time `json:"updated_at"`
}

func partPath(dest string) string   { return dest + partSuffix }
func resumePath(dest string) string { return dest + resumeSuffix }

// newResumeState starts a fresh record. The hasher state is that of an empty
// stream, so Written == 0 is a valid, consistent starting point.
func newResumeState(url, digest string, now time.Time) (*resumeState, error) {
	state, err := marshalHash(sha256.New())
	if err != nil {
		return nil, err
	}
	return &resumeState{URL: url, Digest: digest, HashState: state, UpdatedAt: now}, nil
}

// loadResumeState reads the record for dest. Returns nil (no error) when there is
// nothing usable to resume from — no record, a corrupt one, or one describing a
// different artefact — because in every one of those cases the correct action is the
// same: start over.
func loadResumeState(dest, url, digest string) *resumeState {
	data, err := os.ReadFile(resumePath(dest))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		slog.Warn("downloader: cannot read resume state, starting over", "dest", dest, "error", err)
		return nil
	}

	var s resumeState
	if err := json.Unmarshal(data, &s); err != nil {
		slog.Warn("downloader: unusable resume state, starting over", "dest", dest, "error", err)
		return nil
	}
	if s.Written < 0 {
		slog.Warn("downloader: resume state has a negative offset, starting over", "dest", dest, "written", s.Written)
		return nil
	}
	if s.URL != url || s.Digest != digest {
		slog.Warn("downloader: resume state describes a different artefact, starting over", "dest", dest)
		return nil
	}
	// Without a restorable hasher we cannot continue the digest, and trusting the
	// bytes without it would let a corrupt resume pass verification.
	if _, err := unmarshalHash(s.HashState); err != nil {
		slog.Warn("downloader: resume state digest is unusable, starting over", "dest", dest, "error", err)
		return nil
	}
	return &s
}

// hasher rebuilds the SHA-256 hasher positioned exactly after Written bytes.
func (s *resumeState) hasher() (hash.Hash, error) { return unmarshalHash(s.HashState) }

// save writes the record atomically: a temp file in the same directory, then a
// rename over the target. The rename is atomic on POSIX, so a crash mid-write
// leaves either the previous record or the complete new one — never a torn file
// that would strand the whole .part.
func (s *resumeState) save(dest string, now time.Time) error {
	s.UpdatedAt = now
	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("encode resume state: %w", err)
	}
	return atomicWrite(resumePath(dest), data, fileMode)
}

// checkpoint records that the stream is consistent at offset written with the given
// hasher, and persists it. Call only after the corresponding bytes have been
// fsynced (see the file-level comment).
func (s *resumeState) checkpoint(dest string, written int64, h hash.Hash, now time.Time) error {
	state, err := marshalHash(h)
	if err != nil {
		return err
	}
	s.Written = written
	s.HashState = state
	return s.save(dest, now)
}

// ── hasher (de)serialisation ─────────────────────────────────────────────────

var errHashState = errors.New("unusable sha256 state")

func marshalHash(h hash.Hash) ([]byte, error) {
	m, ok := h.(encoding.BinaryMarshaler)
	if !ok {
		// Unreachable with crypto/sha256, which has implemented BinaryMarshaler for
		// many releases; guarded so a future stdlib change fails loudly here instead
		// of silently disabling resume.
		return nil, fmt.Errorf("%w: hasher is not a BinaryMarshaler", errHashState)
	}
	state, err := m.MarshalBinary()
	if err != nil {
		return nil, fmt.Errorf("%w: marshal: %w", errHashState, err)
	}
	return state, nil
}

func unmarshalHash(state []byte) (hash.Hash, error) {
	if len(state) == 0 {
		return nil, fmt.Errorf("%w: empty state", errHashState)
	}
	h := sha256.New()
	u, ok := h.(encoding.BinaryUnmarshaler)
	if !ok {
		return nil, fmt.Errorf("%w: hasher is not a BinaryUnmarshaler", errHashState)
	}
	if err := u.UnmarshalBinary(state); err != nil {
		return nil, fmt.Errorf("%w: unmarshal: %w", errHashState, err)
	}
	return h, nil
}

// ── file helpers ─────────────────────────────────────────────────────────────

// atomicWrite writes data to path via a temp file in the same directory followed by
// a rename, with mode set explicitly (CreateTemp ignores umask but defaults to
// 0600). Same pattern as keystore.writeFile.
func atomicWrite(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("temp file for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write %s: %w", tmpName, err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod %s: %w", tmpName, err)
	}
	// Flush before the rename: the rename being atomic only guarantees which name
	// the inode has, not that its data reached disk.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename %s: %w", path, err)
	}
	return nil
}

// removeQuietly deletes paths, logging anything other than "already gone". Cleanup
// must never turn into a hard failure: a leftover file is recoverable, an aborted
// cleanup path is not.
func removeQuietly(paths ...string) {
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("downloader: could not remove file", "file", p, "error", err)
		}
	}
}
