// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package downloader

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

const testURL = "https://storage.example/a/b.zip"

func testDigest(body []byte) string { return hexDigest(sha256.Sum256(body)) }

func seedRecord(t *testing.T, dest string, body []byte, written int64) *resumeState {
	t.Helper()
	s, err := newResumeState(testURL, testDigest(body), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h, err := s.hasher()
	if err != nil {
		t.Fatal(err)
	}
	h.Write(body[:written])
	if err := s.checkpoint(dest, written, h, time.Now()); err != nil {
		t.Fatal(err)
	}
	return s
}

// ── the record ───────────────────────────────────────────────────────────────

func TestResumeStateSaveAndLoadRoundTrip(t *testing.T) {
	body := content(2000)
	dp := destIn(t.TempDir())
	seedRecord(t, dp, body, 800)

	got := loadResumeState(dp, testURL, testDigest(body))
	if got == nil {
		t.Fatal("a record just written did not load back")
	}
	if got.Written != 800 {
		t.Errorf("Written: got %d want 800", got.Written)
	}
	if got.URL != testURL {
		t.Errorf("URL: got %q want %q", got.URL, testURL)
	}
	if len(got.HashState) == 0 {
		t.Error("the hasher state was not persisted")
	}
	if got.UpdatedAt.IsZero() {
		t.Error("UpdatedAt was not stamped")
	}
}

// Every rejection below returns nil rather than an error, because in all of these
// cases the correct action is identical: start over. Distinguishing them would only
// give the caller a decision it cannot act on differently.
func TestLoadResumeStateStartsOverWhenTheRecordIsUnusable(t *testing.T) {
	body := content(2000)

	t.Run("no file at all", func(t *testing.T) {
		if got := loadResumeState(destIn(t.TempDir()), testURL, testDigest(body)); got != nil {
			t.Errorf("expected nil for a missing record, got %+v", got)
		}
	})

	t.Run("a different URL", func(t *testing.T) {
		dp := destIn(t.TempDir())
		seedRecord(t, dp, body, 800)
		if got := loadResumeState(dp, "https://storage.example/other.zip", testDigest(body)); got != nil {
			t.Error("a record for another URL was accepted")
		}
	})

	t.Run("a different digest", func(t *testing.T) {
		dp := destIn(t.TempDir())
		seedRecord(t, dp, body, 800)
		if got := loadResumeState(dp, testURL, testDigest([]byte("other"))); got != nil {
			t.Error("a record for another artefact was accepted")
		}
	})

	t.Run("corrupt JSON", func(t *testing.T) {
		dp := destIn(t.TempDir())
		if err := os.WriteFile(resumePath(dp), []byte("{not json"), fileMode); err != nil {
			t.Fatal(err)
		}
		if got := loadResumeState(dp, testURL, testDigest(body)); got != nil {
			t.Error("a corrupt record was accepted")
		}
	})

	t.Run("a negative offset", func(t *testing.T) {
		dp := destIn(t.TempDir())
		s := seedRecord(t, dp, body, 800)
		s.Written = -1
		if err := s.save(dp, time.Now()); err != nil {
			t.Fatal(err)
		}
		if got := loadResumeState(dp, testURL, testDigest(body)); got != nil {
			t.Error("a negative offset was accepted")
		}
	})

	t.Run("an unusable hasher state", func(t *testing.T) {
		// Trusting the offset without a restorable hasher would let a corrupt resume
		// pass the final verification.
		dp := destIn(t.TempDir())
		s := seedRecord(t, dp, body, 800)
		s.HashState = []byte("not a sha256 state")
		if err := s.save(dp, time.Now()); err != nil {
			t.Fatal(err)
		}
		if got := loadResumeState(dp, testURL, testDigest(body)); got != nil {
			t.Error("an unusable hasher state was accepted")
		}
	})
}

// TestHasherStateContinuesTheDigest is the reason the record stores marshalled
// hasher state at all: a resumed transfer must finish with the digest of the WHOLE
// artefact without re-reading the gigabytes already on disk.
func TestHasherStateContinuesTheDigest(t *testing.T) {
	body := content(1500)

	first := sha256.New()
	first.Write(body[:1000])
	state, err := marshalHash(first)
	if err != nil {
		t.Fatal(err)
	}

	restored, err := unmarshalHash(state)
	if err != nil {
		t.Fatal(err)
	}
	restored.Write(body[1000:])

	var got [32]byte
	copy(got[:], restored.Sum(nil))
	if want := sha256.Sum256(body); got != want {
		t.Errorf("resumed digest %x does not match the one-shot digest %x", got, want)
	}
}

func TestUnmarshalHashRejectsGarbage(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state []byte
	}{
		{"empty", nil},
		{"not a hash state", []byte("xxxxxxxxxxxx")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := unmarshalHash(tc.state); err == nil {
				t.Error("expected an error")
			}
		})
	}
}

func TestCheckpointAdvancesWrittenAndHashState(t *testing.T) {
	body := content(2000)
	dp := destIn(t.TempDir())
	s := seedRecord(t, dp, body, 500)
	firstState := append([]byte(nil), s.HashState...)

	h, err := s.hasher()
	if err != nil {
		t.Fatal(err)
	}
	h.Write(body[500:1200])
	if err := s.checkpoint(dp, 1200, h, time.Now()); err != nil {
		t.Fatal(err)
	}

	reloaded := loadResumeState(dp, testURL, testDigest(body))
	if reloaded == nil {
		t.Fatal("the checkpointed record did not load back")
	}
	if reloaded.Written != 1200 {
		t.Errorf("Written: got %d want 1200", reloaded.Written)
	}
	if string(reloaded.HashState) == string(firstState) {
		t.Error("the hasher state did not advance with the offset")
	}
}

// ── the crash invariant ──────────────────────────────────────────────────────

// TestOpenTransferTruncatesThePartFileBackToWritten covers the case the whole
// ordering rule exists for: a crash between fsyncing bytes and recording them
// leaves .part LONGER than Written. The record is authoritative, so the extra bytes
// — which no hasher state describes — must be cut off.
func TestOpenTransferTruncatesThePartFileBackToWritten(t *testing.T) {
	body := content(2000)
	dp := destIn(t.TempDir())

	// 1000 bytes durable on disk, but only 600 of them recorded and hashed.
	if err := os.WriteFile(partPath(dp), body[:1000], fileMode); err != nil {
		t.Fatal(err)
	}
	seedRecord(t, dp, body, 600)

	d := newDownloader(config.Config{}, nil)
	tr, err := d.openTransfer(Request{URL: testURL, DestPath: dp}, testDigest(body))
	if err != nil {
		t.Fatalf("openTransfer: %v", err)
	}
	defer tr.close()

	if tr.written != 600 {
		t.Errorf("written: got %d want 600", tr.written)
	}
	fi, err := os.Stat(partPath(dp))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 600 {
		t.Errorf("partial file size: got %d want 600 (unrecorded bytes were kept)", fi.Size())
	}

	// And the truncated state still produces the right digest for the whole artefact.
	if err := tr.append(body[600:]); err != nil {
		t.Fatal(err)
	}
	if got, want := tr.digest(), sha256.Sum256(body); got != want {
		t.Errorf("digest after truncate+append: got %x want %x", got, want)
	}
}

func TestOpenTransferStartsCleanWhenThereIsNothingToResume(t *testing.T) {
	body := content(500)
	dp := destIn(t.TempDir())
	// A stale partial with no usable record must not be adopted.
	if err := os.WriteFile(partPath(dp), []byte("stale bytes"), fileMode); err != nil {
		t.Fatal(err)
	}

	d := newDownloader(config.Config{}, nil)
	tr, err := d.openTransfer(Request{URL: testURL, DestPath: dp}, testDigest(body))
	if err != nil {
		t.Fatalf("openTransfer: %v", err)
	}
	defer tr.close()

	if tr.written != 0 {
		t.Errorf("written: got %d want 0", tr.written)
	}
	fi, err := os.Stat(partPath(dp))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != 0 {
		t.Errorf("the stale partial was not discarded: %d bytes left", fi.Size())
	}
}

func TestOpenTransferCreatesTheDestinationDirectory(t *testing.T) {
	dp := filepath.Join(t.TempDir(), "nested", "deeper", "artefact.bin")
	d := newDownloader(config.Config{}, nil)
	tr, err := d.openTransfer(Request{URL: testURL, DestPath: dp}, testDigest(nil))
	if err != nil {
		t.Fatalf("openTransfer: %v", err)
	}
	defer tr.close()

	fi, err := os.Stat(filepath.Dir(dp))
	if err != nil {
		t.Fatalf("the download directory was not created: %v", err)
	}
	if !fi.IsDir() {
		t.Error("expected a directory")
	}
}

// ── file helpers ─────────────────────────────────────────────────────────────

func TestAtomicWriteReplacesTheTargetAndLeavesNoTemp(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "record.json")
	if err := os.WriteFile(path, []byte("old"), fileMode); err != nil {
		t.Fatal(err)
	}
	if err := atomicWrite(path, []byte("new"), fileMode); err != nil {
		t.Fatalf("atomicWrite: %v", err)
	}

	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "new" {
		t.Errorf("content: got %q want %q", got, "new")
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("expected only the target file, found %d entries (a temp file leaked)", len(entries))
	}
}

func TestRemoveQuietlyIgnoresMissingFiles(t *testing.T) {
	dir := t.TempDir()
	present := filepath.Join(dir, "here")
	if err := os.WriteFile(present, []byte("x"), fileMode); err != nil {
		t.Fatal(err)
	}
	// Must not panic or block on the absent one, and must still remove the present
	// one: cleanup that aborts halfway leaves state nobody will collect.
	removeQuietly(filepath.Join(dir, "absent"), present)
	if _, err := os.Stat(present); !os.IsNotExist(err) {
		t.Error("the existing file was not removed")
	}
}
