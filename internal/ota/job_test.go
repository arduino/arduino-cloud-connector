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

// seedRawJob writes a record verbatim, including one saveJob would never produce.
// That is the point: the records worth testing against are the damaged ones a crash
// mid-write or an older format can leave behind.
func seedRawJob(t *testing.T, dir string, rec persistedJob) {
	t.Helper()
	data, err := json.Marshal(rec)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jobKeeper(dir).jobPath(), data, fileMode); err != nil {
		t.Fatalf("seed a raw job record: %v", err)
	}
}

func TestJobRecordRoundTrip(t *testing.T) {
	f := jobKeeper(t.TempDir())

	if got, lost := f.loadJob(); got != nil || lost != nil {
		t.Fatalf("expected no record in an empty dir, got %+v / lost %+v", got, lost)
	}

	want := Job{ID: [16]byte{1, 2, 3}, URL: testBundleURL, SHA256: sha256.Sum256([]byte("bundle"))}
	f.job = &want
	if err := f.saveJob(); err != nil {
		t.Fatalf("saveJob: %v", err)
	}

	got, lost := f.loadJob()
	if got == nil {
		t.Fatal("expected a record after saveJob")
	}
	if lost != nil {
		t.Errorf("a usable record was reported as lost: %+v", lost)
	}
	if *got != want {
		t.Errorf("round trip: got %+v want %+v", *got, want)
	}

	f.forgetJob()
	if got, lost := f.loadJob(); got != nil || lost != nil {
		t.Errorf("expected no record after forgetJob, got %+v / lost %+v", got, lost)
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
	if got, _ := f.loadJob(); got == nil || *got != job {
		t.Errorf("round trip through a created directory failed: %+v", got)
	}
}

// A record that cannot be acted on must never block future deploys: loadJob resumes
// nothing and removes it, rather than returning an error the FSM would have to
// interpret on every start.
//
// What it does report is whether the record at least named a job. That is the whole
// difference between closing the Cloud job with ErrDeployInterrupted and leaving it to
// time out in silence, because every OTA message is keyed by the job id: with one there
// is something to send, without one there is not.
func TestLoadJobDiscardsUnusableRecords(t *testing.T) {
	valid := Job{ID: [16]byte{5}, URL: testBundleURL, SHA256: sha256.Sum256([]byte("y"))}

	cases := map[string]struct {
		rec persistedJob
		// wantLost is the id the caller can report the failure against, empty when the
		// record does not yield one.
		wantLost string
	}{
		"unparsable job id": {persistedJob{JobID: "not-hex", URL: testBundleURL, SHA256: hexOf(valid.SHA256)}, ""},
		"short job id":      {persistedJob{JobID: "abcd", URL: testBundleURL, SHA256: hexOf(valid.SHA256)}, ""},
		"unparsable digest": {persistedJob{JobID: valid.IDHex(), URL: testBundleURL, SHA256: "zz"}, valid.IDHex()},
		"missing url":       {persistedJob{JobID: valid.IDHex(), URL: "", SHA256: hexOf(valid.SHA256)}, valid.IDHex()},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := jobKeeper(t.TempDir())
			data, err := json.Marshal(tc.rec)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(f.jobPath(), data, 0o600); err != nil {
				t.Fatal(err)
			}

			got, lost := f.loadJob()
			if got != nil {
				t.Fatalf("expected nothing to resume for %s, got %+v", name, got)
			}
			switch {
			case tc.wantLost == "" && lost != nil:
				t.Errorf("reported a lost job with no usable id: %+v", lost)
			case tc.wantLost != "" && lost == nil:
				t.Errorf("the record named job %s but no failure can be reported against it", tc.wantLost)
			case tc.wantLost != "" && lost.IDHex() != tc.wantLost:
				t.Errorf("lost job id: got %s want %s", lost.IDHex(), tc.wantLost)
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
	// Nothing parsed, so there is no job id and nothing that could be reported.
	got, lost := f.loadJob()
	if got != nil || lost != nil {
		t.Fatalf("expected nothing for a garbled record, got %+v / lost %+v", got, lost)
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
