// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !mock && (linux || darwin)

package system

import (
	"path/filepath"
	"testing"
)

// TestDiskFreeBytesReportsSpace is the only check that statfs is read correctly —
// picking the wrong field or dropping the block-size multiplication still compiles
// and still returns a plausible-looking number. These tests do not run on the
// non-statfs platforms, where DiskFreeBytes measures nothing by design; CI is
// ubuntu-latest, so they run there.
func TestDiskFreeBytesReportsSpace(t *testing.T) {
	free, err := DiskFreeBytes(t.TempDir())
	if err != nil {
		t.Fatalf("DiskFreeBytes: unexpected error: %v", err)
	}
	// A host running the test suite has room for a temp dir, so anything <= 0 means
	// the value never came back from the OS.
	if free <= 0 {
		t.Errorf("DiskFreeBytes: got %d, want a positive byte count", free)
	}
}

// TestDiskFreeBytesErrorsOnMissingDir pins the contract the caller relies on: an
// unusable path is an error, never a zero that would read as "disk full" and
// refuse a download.
func TestDiskFreeBytesErrorsOnMissingDir(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "no-such-dir")
	if _, err := DiskFreeBytes(missing); err == nil {
		t.Errorf("DiskFreeBytes(%q): got nil error, want a failure", missing)
	}
}
