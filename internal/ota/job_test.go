// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package ota

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"testing"
	"time"
)

// jobKeeper builds the minimal FSM the record methods need: they read the download
// dir and the clock and nothing else, which is the whole reason they are methods
// rather than functions taking a directory — there is one job and one place to keep
// it, and no signature that suggests otherwise.
func jobKeeper(dir string) *OTAFSM {
	return &OTAFSM{downloadDir: dir, now: time.Now}
}

// seedJob writes a record as an interrupted run would have left it. Used by the FSM
// tests to stage a resume.
func seedJob(t *testing.T, dir string, job Job) {
	t.Helper()
	k := jobKeeper(dir)
	k.job = &job
	if err := k.saveJob(); err != nil {
		t.Fatalf("seed the job record: %v", err)
	}
}

func TestJobRecordRoundTrip(t *testing.T) {
	f := jobKeeper(t.TempDir())

	if got := f.loadJob(); got != nil {
		t.Fatalf("expected no record in an empty dir, got %+v", got)
	}

	want := Job{ID: [16]byte{1, 2, 3}, URL: testBundleURL, SHA256: sha256.Sum256([]byte("bundle"))}
	f.job = &want
	if err := f.saveJob(); err != nil {
		t.Fatalf("saveJob: %v", err)
	}

	got := f.loadJob()
	if got == nil {
		t.Fatal("expected a record after saveJob")
	}
	if *got != want {
		t.Errorf("round trip: got %+v want %+v", *got, want)
	}

	f.forgetJob()
	if got := f.loadJob(); got != nil {
		t.Errorf("expected no record after forgetJob, got %+v", got)
	}
}

func TestSaveJobRefusesWithNothingInFlight(t *testing.T) {
	// Nothing to record is a programming error, not a disk problem: it must not write
	// an empty record that a later Resume would then try to act on.
	f := jobKeeper(t.TempDir())
	if err := f.saveJob(); err == nil {
		t.Error("saveJob succeeded with no job in flight")
	}
	if _, err := os.Stat(f.jobPath()); !os.IsNotExist(err) {
		t.Errorf("saveJob created a record with no job in flight (%v)", err)
	}
}

func TestSaveJobCreatesItsDirectory(t *testing.T) {
	// The download dir may not exist yet on a fresh board: accepting a job must not
	// fail because of that.
	dir := t.TempDir() + string(os.PathSeparator) + "not-created-yet"
	job := Job{ID: [16]byte{4}, URL: testBundleURL, SHA256: sha256.Sum256([]byte("x"))}

	f := jobKeeper(dir)
	f.job = &job
	if err := f.saveJob(); err != nil {
		t.Fatalf("saveJob into a missing directory: %v", err)
	}
	if got := f.loadJob(); got == nil || *got != job {
		t.Errorf("round trip through a created directory failed: %+v", got)
	}
}

// A record that cannot be acted on must never block future deploys: loadJob reports
// "nothing to resume" and removes it, rather than returning an error the FSM would
// have to interpret on every start.
func TestLoadJobDiscardsUnusableRecords(t *testing.T) {
	valid := Job{ID: [16]byte{5}, URL: testBundleURL, SHA256: sha256.Sum256([]byte("y"))}

	cases := map[string]persistedJob{
		"unparsable job id": {JobID: "not-hex", URL: testBundleURL, SHA256: hexOf(valid.SHA256)},
		"short job id":      {JobID: "abcd", URL: testBundleURL, SHA256: hexOf(valid.SHA256)},
		"unparsable digest": {JobID: valid.IDHex(), URL: testBundleURL, SHA256: "zz"},
		"missing url":       {JobID: valid.IDHex(), URL: "", SHA256: hexOf(valid.SHA256)},
	}
	for name, rec := range cases {
		t.Run(name, func(t *testing.T) {
			f := jobKeeper(t.TempDir())
			data, err := json.Marshal(rec)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.jobPath(), data, 0o600); err != nil {
				t.Fatal(err)
			}

			if got := f.loadJob(); got != nil {
				t.Fatalf("expected nil for %s, got %+v", name, got)
			}
			if _, err := os.Stat(f.jobPath()); !os.IsNotExist(err) {
				t.Errorf("the unusable record was not removed (%v)", err)
			}
		})
	}
}

func TestLoadJobDiscardsGarbledRecord(t *testing.T) {
	f := jobKeeper(t.TempDir())
	if err := os.WriteFile(f.jobPath(), []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := f.loadJob(); got != nil {
		t.Fatalf("expected nil for a garbled record, got %+v", got)
	}
	if _, err := os.Stat(f.jobPath()); !os.IsNotExist(err) {
		t.Error("the garbled record was not removed")
	}
}

func TestJobRecordLivesInTheDownloadDir(t *testing.T) {
	// The record sits next to the bundle, under the one configured directory, and not
	// in DataDir: a deploy's whole on-disk footprint is reclaimable by clearing one
	// place.
	dir := t.TempDir()
	f := jobKeeper(dir)
	if got, want := f.jobPath(), dir+string(os.PathSeparator)+fileDeployJob; got != want {
		t.Errorf("jobPath: got %q want %q", got, want)
	}
}
