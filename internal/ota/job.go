// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package ota

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
)

// Job is one execution of the App-deploy process: the unit of work the Cloud opens
// with OTAUpdateCmd and closes when the device confirms the install or reports a
// failure.
//
// The Cloud owns it and assigns the id — which is why the wire, the logs and the
// Arduino Cloud UI all call it a job — and the device services exactly one at a
// time. That singularity is not a hope, it is the shape of the code: the FSM holds
// one *Job and keeps one record of it, in one place.
type Job struct {
	// ID is the OTA job id assigned by the Cloud; it correlates every progress
	// message with the job and names the bundle on disk.
	ID [16]byte
	// URL is the storage URL of the bundle.
	URL string
	// SHA256 is the digest the downloaded bytes must hash to. It is also what a
	// successful deploy announces back to the Cloud to close the job.
	SHA256 [32]byte
}

// IDHex returns the job id as lowercase hex, the form used in logs and file names.
func (j Job) IDHex() string { return hex.EncodeToString(j.ID[:]) }

// hexOf renders a digest as lowercase hex — the form it takes both in the record and
// in the handover to arduino-app-cli.
func hexOf(d [32]byte) string { return hex.EncodeToString(d[:]) }

// installRequestFor builds the handover payload for a downloaded job: where the
// verified archive is, and what it must hash to.
func installRequestFor(job Job, bundlePath string) appinstaller.Request {
	return appinstaller.Request{
		BundlePath: bundlePath,
		SHA256:     hexOf(job.SHA256),
	}
}

// ── the job on disk ──────────────────────────────────────────────────────────

// fileDeployJob names the record of the job currently being executed.
//
// It exists because a deploy can outlive the daemon: an App bundle can exceed 2 GB,
// so a crash, a package upgrade or a reboot can easily land in the middle of one. On
// the next start the FSM's Resume state reads this file and knows both that a job was
// interrupted and which job it was — the id to report progress against, and the URL
// and digest to carry on with.
//
// It deliberately holds only what the OTA protocol needs, and it is the ONLY place
// the download URL survives a restart: the Cloud does not resend OTAUpdateCmd after a
// crash, so a job whose URL was lost could never be continued. The byte-level state of
// the transfer (how far it got, the partial file, the streaming digest) belongs to
// internal/downloader, which resumes it from its own sidecar without being told
// anything.
//
// Same division, and the same shape, as provisioning: internal/provisioning owns its
// `provisioning_inflight` marker as methods on the service that derives its state
// from it, while internal/provisioning-api knows nothing about it.
const fileDeployJob = "deploy_job.json"

// persistedJob is the on-disk form of an accepted, unfinished job.
type persistedJob struct {
	// JobID is the hex-encoded 16-byte OTA job id. Every progress and outcome
	// message is keyed by it, so without it a resumed deploy could not be reported.
	JobID string `json:"job_id"`
	// URL is where the bundle is fetched from.
	URL string `json:"url"`
	// SHA256 is the hex digest the bundle must hash to. It is needed twice, at
	// opposite ends of the deploy: to verify the download, and — after the install —
	// as the payload of the OTABeginCmd that closes the job. It arrives with the job
	// and is consumed at the very end, so a restart in between would leave the process
	// with an installed App and nothing to confirm it with. Hence it lives here, next
	// to the URL, rather than only in memory.
	SHA256 string `json:"sha256"`
	// StartedAt is when the job was accepted. Informational: it makes a stray record
	// self-describing when debugging a stuck deploy.
	StartedAt time.Time `json:"started_at"`
}

// jobPath is where the record lives. Derived from the download dir, like the bundle
// itself, so the FSM's whole on-disk footprint sits under one configured directory.
func (f *OTAFSM) jobPath() string { return filepath.Join(f.downloadDir, fileDeployJob) }

// saveJob records the job in flight so a crash mid-deploy can pick it back up.
// Written atomically: a crash mid-write must not leave a half-parsed record, which
// would strand every future deploy.
//
// It takes no arguments on purpose. There is exactly one job in flight and exactly
// one place it is kept, so there is no second job to pass and nowhere else to put it.
func (f *OTAFSM) saveJob() error {
	if f.job == nil {
		return errors.New("ota: no job in flight to record")
	}
	if err := os.MkdirAll(f.downloadDir, dirMode); err != nil {
		return fmt.Errorf("ota: create deploy dir: %w", err)
	}
	data, err := json.Marshal(persistedJob{
		JobID:     f.job.IDHex(),
		URL:       f.job.URL,
		SHA256:    hexOf(f.job.SHA256),
		StartedAt: f.now(),
	})
	if err != nil {
		return fmt.Errorf("ota: encode the deploy job record: %w", err)
	}
	return atomicWrite(f.jobPath(), data, fileMode)
}

// loadJob reports what a previous run left behind. At most one return is non-nil:
//
//   - job — the interrupted deploy, complete enough to carry on with.
//   - lost — a deploy that cannot be carried on with, carrying only the job id that
//     was recovered from the record. The Cloud is still holding that job open, so the
//     caller closes it with ErrDeployInterrupted rather than letting it time out.
//   - neither — there is nothing to resume, or the record was too damaged to even name
//     a job. Without an id there is no message to send: every OTA report is keyed by
//     one, which is also why the C++ reference returns early with no context.
//
// Every unusable record is removed on the way out, whichever of the two it turns out
// to be: a record that cannot be acted on must never block future deploys.
func (f *OTAFSM) loadJob() (job, lost *Job) {
	path := f.jobPath()

	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		slog.Warn("ota: cannot read the deploy job record, ignoring it", "file", path, "error", err)
		f.forgetJob()
		return nil, nil
	}

	var rec persistedJob
	if err := json.Unmarshal(data, &rec); err != nil {
		slog.Warn("ota: unusable deploy job record, discarding", "file", path, "error", err)
		f.forgetJob()
		return nil, nil
	}

	id, err := parseJobID(rec.JobID)
	if err != nil {
		slog.Warn("ota: deploy job record has an unparsable job id, discarding", "job_id", rec.JobID)
		f.forgetJob()
		return nil, nil
	}

	// From here the job has a name, so a record that fails the remaining checks can be
	// reported rather than only dropped.
	digest, err := parseDigest(rec.SHA256)
	if err != nil {
		slog.Warn("ota: deploy job record has an unparsable digest, discarding", "job_id", rec.JobID)
		f.forgetJob()
		return nil, &Job{ID: id}
	}
	if rec.URL == "" {
		slog.Warn("ota: deploy job record has no URL, discarding", "job_id", rec.JobID)
		f.forgetJob()
		return nil, &Job{ID: id}
	}

	return &Job{ID: id, URL: rec.URL, SHA256: digest}, nil
}

// forgetJob drops the record. Called when a job finishes, whether it succeeded or
// failed: the Cloud job is closed by then, so a record left behind would make the next
// Resume re-fetch a bundle nobody is waiting for.
func (f *OTAFSM) forgetJob() {
	path := f.jobPath()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		slog.Warn("ota: could not remove the deploy job record", "file", path, "error", err)
	}
}

// parseJobID decodes the 32-hex-char job id back into the 16-byte id carried on the
// wire.
func parseJobID(s string) ([16]byte, error) {
	var id [16]byte
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, fmt.Errorf("ota: job id %q: %w", s, err)
	}
	if len(b) != len(id) {
		return id, fmt.Errorf("ota: job id %q: got %d bytes, want %d", s, len(b), len(id))
	}
	copy(id[:], b)
	return id, nil
}

// parseDigest decodes a 64-hex-char SHA-256 digest.
func parseDigest(s string) ([32]byte, error) {
	var d [32]byte
	b, err := hex.DecodeString(s)
	if err != nil {
		return d, fmt.Errorf("ota: digest %q: %w", s, err)
	}
	if len(b) != len(d) {
		return d, fmt.Errorf("ota: digest %q: got %d bytes, want %d", s, len(b), len(d))
	}
	copy(d[:], b)
	return d, nil
}

// Permissions for the record. It names a storage URL the operator's deploy came from,
// so it is not world-readable.
const (
	fileMode = 0o600
	dirMode  = 0o700
)

// atomicWrite writes data to path via a temp file in the same directory followed by a
// rename, with mode set explicitly (CreateTemp ignores umask but defaults to 0600).
// Same pattern as keystore.writeFile.
//
// internal/downloader carries an identical helper for its own sidecars, on purpose:
// neither package imports the other, and a shared package for these few lines would
// couple them for no gain.
func atomicWrite(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("ota: temp file for %s: %w", path, err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once the rename succeeds

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("ota: write %s: %w", tmpName, err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("ota: chmod %s: %w", tmpName, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("ota: sync %s: %w", tmpName, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("ota: close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("ota: rename %s: %w", path, err)
	}
	return nil
}
