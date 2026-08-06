// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package ota

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/downloader"
	"github.com/arduino/arduino-cloud-connector/internal/downloader/downloadertest"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
)

// ── test doubles ─────────────────────────────────────────────────────────────

// fakePublisher records every uplink command the process publishes, which is the
// entire contract with Arduino IoT Cloud.
type fakePublisher struct {
	mu sync.Mutex
	// attempts counts every call, including failed ones, so a test can detect a
	// retry loop even when nothing is recorded.
	attempts int
	sent     []command.Cmd
	err      error
}

func (f *fakePublisher) PublishCommand(_ string, cmd command.Cmd) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts++
	if f.err != nil {
		return f.err
	}
	f.sent = append(f.sent, cmd)
	return nil
}

func (f *fakePublisher) attemptCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts
}

func (f *fakePublisher) all() []command.Cmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]command.Cmd(nil), f.sent...)
}

// progressStates returns the (state, state_data) pairs of every OTAProgressCmd
// published, in order.
func (f *fakePublisher) progressStates() [][2]int64 {
	var out [][2]int64
	for _, c := range f.all() {
		if pc, ok := c.Inner().(command.OTAProgressCmd); ok {
			out = append(out, [2]int64{int64(pc.State), int64(pc.StateData)})
		}
	}
	return out
}

// begins returns the digest of every OTABeginCmd published, in order.
func (f *fakePublisher) begins() [][32]byte {
	var out [][32]byte
	for _, c := range f.all() {
		if b, ok := c.Inner().(command.OTABeginCmd); ok {
			out = append(out, b.SHA256)
		}
	}
	return out
}

// hasFailCode reports whether a Fail(8) report with the given code was published.
func (f *fakePublisher) hasFailCode(code Error) bool {
	for _, s := range f.progressStates() {
		if s[0] == int64(StateFail) && s[1] == int64(code) {
			return true
		}
	}
	return false
}

// hasDigest reports whether any published OTABeginCmd carried digest.
func (f *fakePublisher) hasDigest(digest [32]byte) bool {
	for _, d := range f.begins() {
		if d == digest {
			return true
		}
	}
	return false
}

// waitFor polls cond until it holds or the deadline passes. The process runs on
// its own goroutine, so assertions have to wait for it rather than assume ordering.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// newTestFSM wires an OTAFSM to a fake publisher and a scriptable storage
// client. No HTTP server is involved: the transfer's own edge cases live in
// internal/storage-api's tests, so these exercise the state machine only.
func newTestFSM(t *testing.T, storage downloader.Downloader, inst appinstaller.Installer) (*OTAFSM, *fakePublisher, config.Config) {
	t.Helper()
	cfg := config.Config{
		DataDir:         t.TempDir(),
		AppDownloadDir:  t.TempDir(),
		MaxBundleSize:   1 << 20,
		DownloadTimeout: 30 * time.Second,
		InstallTimeout:  5 * time.Second,
	}
	pub := &fakePublisher{}
	return New(cfg, "device-1", pub, storage, inst), pub, cfg
}

const testBundleURL = "https://api2.arduino.cc/apps/owner/app-uuid/bundle.zip"

func updateCmd(final [32]byte) command.OTAUpdateCmd {
	return command.OTAUpdateCmd{ID: [16]byte{9, 9, 9}, URL: testBundleURL, FinalSHA: final}
}

// waitForIdle waits until the process is parked waiting for a job.
//
// It is the "the process is up" synchronisation point. It used to be the initial
// OTABeginCmd, but that message no longer exists: the digest is published once after
// an install and never at startup, so reaching Idle is what "ready" means now.
func waitForIdle(t *testing.T, f *OTAFSM) {
	t.Helper()
	waitFor(t, "the process to reach Idle", func() bool { return f.Snapshot().State == StateIdle })
}

// runFSM starts f and returns a stop function that cancels it and waits.
func runFSM(t *testing.T, f *OTAFSM) func() {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	go f.Run(ctx)
	return func() {
		cancel()
		select {
		case <-f.Done():
		case <-time.After(5 * time.Second):
			t.Error("process did not stop within 5s")
		}
	}
}

// blockingInstaller returns an appinstaller.Installer that signals on started and then waits
// for finish, so a test can inspect the process while the install is underway.
func blockingInstaller(started, finish chan struct{}) appinstaller.Installer {
	return appinstaller.Func(func(ctx context.Context, _ appinstaller.Request, onProgress appinstaller.ProgressFunc) error {
		close(started)
		onProgress(50)
		select {
		case <-finish:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestOTAFSMHappyPathReportsPhasesAndAnnouncesNewDigest(t *testing.T) {
	content := []byte("an app bundle")
	final := sha256.Sum256(content)

	installed := make(chan appinstaller.Request, 1)
	inst := appinstaller.Func(func(_ context.Context, req appinstaller.Request, onProgress appinstaller.ProgressFunc) error {
		onProgress(50)
		onProgress(100)
		installed <- req
		return nil
	})

	fake := &downloadertest.FakeDownloader{Content: content}
	f, pub, cfg := newTestFSM(t, fake, inst)
	stop := runFSM(t, f)
	defer stop()

	f.SetConnected(true)
	waitForIdle(t, f)
	// Nothing is announced just for being up: the Cloud hears from this process only
	// when there is a job.
	if n := len(pub.all()); n != 0 {
		t.Errorf("published %d messages before any job arrived, want 0", n)
	}

	f.Deliver(updateCmd(final))

	var req appinstaller.Request
	select {
	case req = <-installed:
	case <-time.After(5 * time.Second):
		t.Fatalf("install was never invoked; published: %v", pub.progressStates())
	}

	// The storage client got exactly the request the job describes.
	reqs := fake.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 download request, got %d", len(reqs))
	}
	if reqs[0].URL != testBundleURL {
		t.Errorf("download URL: got %q", reqs[0].URL)
	}
	if reqs[0].ExpectedSHA256 != final {
		t.Errorf("expected digest: got %x want %x", reqs[0].ExpectedSHA256, final)
	}
	if req.BundlePath != reqs[0].DestPath {
		t.Errorf("handover path %q does not match the download destination %q",
			req.BundlePath, reqs[0].DestPath)
	}

	// Success is signalled by publishing the installed digest — once, and it is the
	// only OTABegin of the whole deploy.
	waitFor(t, "the success OTABegin", func() bool { return len(pub.begins()) == 1 })
	if got := pub.begins()[0]; got != final {
		t.Errorf("success OTABegin digest: got %x want %x", got, final)
	}

	// The phases the Cloud UI renders, in order: accepted(3), starting(4), then the
	// install percentages(6). Fetch(5) is only reported on the progress interval, so
	// a fake that completes instantly may not produce one.
	states := pub.progressStates()
	wantPrefix := [][2]int64{{3, 0}, {4, 0}}
	for i, want := range wantPrefix {
		if i >= len(states) || states[i] != want {
			t.Fatalf("progress sequence %v does not start with %v", states, wantPrefix)
		}
	}
	var sawInstall50, sawInstall100 bool
	for _, s := range states {
		if s == [2]int64{6, 50} {
			sawInstall50 = true
		}
		if s == [2]int64{6, 100} {
			sawInstall100 = true
		}
		if s[0] == 8 {
			t.Errorf("unexpected Fail report in a successful deploy: %v", states)
		}
	}
	if !sawInstall50 || !sawInstall100 {
		t.Errorf("install progress not forwarded: %v", states)
	}

	// Nothing about the installed App is written to disk: a Linux board can run
	// several Apps, so this process keeps no record of "the deployed one" to
	// re-announce later.
	if entries, err := os.ReadDir(cfg.DataDir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		names := make([]string, 0, len(entries))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		t.Errorf("a successful deploy left state in DataDir: %v", names)
	}
	// The multi-GB bundle does not stay on disk (RFC-14 §5.9)…
	if _, err := os.Stat(req.BundlePath); !os.IsNotExist(err) {
		t.Errorf("bundle was not removed after a successful install: %v", err)
	}
	// …and the job record is gone, so the next start does not re-run the job.
	if job := f.loadJob(); job != nil {
		t.Errorf("job record survived a completed deploy: %+v", job)
	}
}

// TestOTAFSMConfirmsNewDigestOnlyAfterInstallCompletes pins the ordering that the
// Cloud's success semantics rest on.
//
// OTABeginCmd carrying the new digest IS the "deploy succeeded" signal: the Cloud
// matches it against the digest it asked for and closes the job. So it must not
// leave the device until the App is genuinely installed.
func TestOTAFSMConfirmsNewDigestOnlyAfterInstallCompletes(t *testing.T) {
	content := []byte("bundle v2")
	final := sha256.Sum256(content)

	started, finish := make(chan struct{}), make(chan struct{})
	f, pub, _ := newTestFSM(t,
		&downloadertest.FakeDownloader{Content: content}, blockingInstaller(started, finish))
	stop := runFSM(t, f)
	defer stop()

	f.SetConnected(true)
	waitForIdle(t, f)

	f.Deliver(updateCmd(final))

	// The download has completed and been digest-verified; the install is running.
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("install never started; published: %v", pub.progressStates())
	}

	// Nothing may claim success yet. Sleep past a couple of FSM passes so a
	// premature publish would have had time to happen.
	time.Sleep(100 * time.Millisecond)
	if pub.hasDigest(final) {
		t.Error("the new digest was confirmed to the Cloud before the install finished")
	}
	if state := f.Snapshot().State; state != StateFlashOTA {
		t.Errorf("state during install: got %s want %s", state, StateFlashOTA)
	}

	close(finish)

	waitFor(t, "the success confirmation", func() bool { return pub.hasDigest(final) })

	// Exactly one OTABegin for the whole deploy: the confirmation, and nothing else.
	time.Sleep(100 * time.Millisecond)
	if n := len(pub.begins()); n != 1 {
		t.Errorf("expected exactly 1 OTABegin message (the confirmation), got %d", n)
	}
	// The confirmation must be the LAST thing published — nothing follows a closed
	// job, in particular no further progress report.
	all := pub.all()
	if _, ok := all[len(all)-1].Inner().(command.OTABeginCmd); !ok {
		t.Errorf("the last published message is %T, expected the OTABegin confirmation",
			all[len(all)-1].Inner())
	}
}

// TestOTAFSMDoesNotConfirmOnReconnectMidDeploy covers the interleaving that could
// leak a premature confirmation: a broker flap during the install. Nothing about a
// connection coming up may publish a digest — only a finished install does.
func TestOTAFSMDoesNotConfirmOnReconnectMidDeploy(t *testing.T) {
	content := []byte("bundle v3")
	final := sha256.Sum256(content)

	started, finish := make(chan struct{}), make(chan struct{})
	f, pub, _ := newTestFSM(t,
		&downloadertest.FakeDownloader{Content: content}, blockingInstaller(started, finish))
	stop := runFSM(t, f)
	defer stop()

	f.SetConnected(true)
	waitForIdle(t, f)

	f.Deliver(updateCmd(final))
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("install never started")
	}

	beginsBefore := len(pub.begins())
	f.SetConnected(false)
	f.SetConnected(true)
	time.Sleep(100 * time.Millisecond)

	if n := len(pub.begins()); n != beginsBefore {
		t.Errorf("a reconnect mid-deploy published %d extra OTABegin message(s)", n-beginsBefore)
	}
	if pub.hasDigest(final) {
		t.Error("the new digest was confirmed by a mid-deploy reconnect")
	}

	close(finish)

	waitFor(t, "the success confirmation", func() bool { return pub.hasDigest(final) })
	time.Sleep(100 * time.Millisecond)
	if n := len(pub.begins()); n != beginsBefore+1 {
		t.Errorf("expected exactly one confirmation after the deploy, got %d extra", n-beginsBefore)
	}
}

// TestOTAFSMNeverConfirmsAFailedDeploy is the mirror image: a deploy that fails
// must leave the Cloud's view of the board untouched.
func TestOTAFSMNeverConfirmsAFailedDeploy(t *testing.T) {
	content := []byte("bundle v4")
	final := sha256.Sum256(content)

	inst := appinstaller.Func(func(context.Context, appinstaller.Request, appinstaller.ProgressFunc) error {
		return fmt.Errorf("%w: unpack failed", appinstaller.ErrFailed)
	})
	f, pub, _ := newTestFSM(t, &downloadertest.FakeDownloader{Content: content}, inst)
	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)
	waitForIdle(t, f)

	f.Deliver(updateCmd(final))

	waitFor(t, "the Fail report", func() bool { return pub.hasFailCode(ErrInstallFailed) })
	waitFor(t, "a return to Idle", func() bool { return f.Snapshot().State == StateIdle })

	time.Sleep(100 * time.Millisecond)
	if pub.hasDigest(final) {
		t.Error("a failed deploy confirmed the new digest to the Cloud")
	}
	if n := len(pub.begins()); n != 0 {
		t.Errorf("a failed deploy published %d OTABegin message(s), want none", n)
	}
	// A reported failure closes the Cloud job, so nothing must be left for Resume
	// to pick back up.
	if job := f.loadJob(); job != nil {
		t.Errorf("job record survived a failed deploy: %+v", job)
	}
}

func TestOTAFSMReportsUnavailableInstaller(t *testing.T) {
	// The wiring shipped today: the bundle downloads, then the deploy fails honestly
	// because arduino-app-cli has no deploy endpoint yet.
	content := []byte("bundle v5")
	f, pub, _ := newTestFSM(t, &downloadertest.FakeDownloader{Content: content}, appinstaller.Unavailable())
	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)
	waitForIdle(t, f)

	f.Deliver(updateCmd(sha256.Sum256(content)))
	waitFor(t, "the installer-unavailable Fail report",
		func() bool { return pub.hasFailCode(ErrInstallerUnavailable) })
}

// TestOTAFSMMapsStorageFailuresToWireCodes is the integration counterpart of
// TestDecodeError: the failure the storage client reports must reach the Cloud as the
// matching OTAProgressCmd code.
func TestOTAFSMMapsStorageFailuresToWireCodes(t *testing.T) {
	cases := map[string]struct {
		storageErr error
		want       Error
	}{
		// Both layers are represented on purpose: a network sentinel from the storage
		// client and disk/transfer sentinels from the downloader must all reach the
		// Cloud as the right code.
		"bad url":         {storageapi.ErrURLInvalid, ErrURLParse},
		"unreachable":     {storageapi.ErrConnect, ErrServerConnect},
		"digest mismatch": {downloader.ErrDigestMismatch, ErrDigestMismatch},
		"too large":       {downloader.ErrTooLarge, ErrBundleTooLarge},
		"no space":        {downloader.ErrNoSpace, ErrNoOtaStorage},
		"transfer failed": {downloader.ErrTransfer, ErrDownload},
		"timeout":         {downloader.ErrTimeout, ErrDownloadTimeout},
		"bad size":        {downloader.ErrBadSize, ErrHTTPHeader},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f, pub, _ := newTestFSM(t,
				&downloadertest.FakeDownloader{Err: tc.storageErr}, appinstaller.Unavailable())
			stop := runFSM(t, f)
			defer stop()
			f.SetConnected(true)
			waitForIdle(t, f)

			f.Deliver(updateCmd([32]byte{1}))
			waitFor(t, "the mapped Fail report", func() bool { return pub.hasFailCode(tc.want) })
		})
	}
}

// TestOTAFSMPublishesNothingUntilAJobArrives pins the decision that the digest is
// never announced for its own sake.
//
// The MCU flow publishes OTABeginCmd on every connection, and this process used to
// copy it. It must not: a Linux board can hold and run several Apps, some installed
// by hand from App Lab, so "the deployed App" is not a thing this process can name.
// Announcing one at startup or on every reconnect would tell the Cloud something
// false. Only a finished install publishes, and only the bundle it just installed.
func TestOTAFSMPublishesNothingUntilAJobArrives(t *testing.T) {
	f, pub, cfg := newTestFSM(t, &downloadertest.FakeDownloader{}, appinstaller.Unavailable())
	stop := runFSM(t, f)
	defer stop()

	// Disconnected: nothing to publish, and nowhere to publish it.
	time.Sleep(50 * time.Millisecond)
	if n := len(pub.all()); n != 0 {
		t.Fatalf("published %d messages before the broker was connected", n)
	}

	// Connected, still no job: silence.
	f.SetConnected(true)
	waitForIdle(t, f)
	time.Sleep(100 * time.Millisecond)
	if n := len(pub.all()); n != 0 {
		t.Errorf("published %d messages on connecting, want 0: %v", n, pub.all())
	}

	// And a reconnect is not an occasion to announce anything either.
	f.SetConnected(false)
	f.SetConnected(true)
	time.Sleep(100 * time.Millisecond)
	if n := len(pub.all()); n != 0 {
		t.Errorf("published %d messages on reconnecting, want 0: %v", n, pub.all())
	}
	// Nothing was written to disk to be announced later, either.
	if entries, err := os.ReadDir(cfg.DataDir); err != nil {
		t.Fatal(err)
	} else if len(entries) != 0 {
		t.Errorf("the process created state in DataDir with no job: %d entries", len(entries))
	}
}

func TestOTAFSMResumesInterruptedDeployOnStart(t *testing.T) {
	// A job record left by a previous daemon run must be picked up on start,
	// without waiting for the Cloud to resend the job.
	content := []byte("interrupted bundle")
	final := sha256.Sum256(content)

	installed := make(chan appinstaller.Request, 1)
	inst := appinstaller.Func(func(_ context.Context, req appinstaller.Request, _ appinstaller.ProgressFunc) error {
		installed <- req
		return nil
	})
	fake := &downloadertest.FakeDownloader{Content: content}
	f, pub, cfg := newTestFSM(t, fake, inst)

	// Seed the record as an interrupted run would have left it.
	job := Job{ID: [16]byte{7}, URL: testBundleURL, SHA256: final}
	seedJob(t, cfg.AppDownloadDir, job)

	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)

	select {
	case <-installed:
	case <-time.After(5 * time.Second):
		t.Fatalf("resumed deploy never reached the installer; published: %v", pub.progressStates())
	}

	// The resumed job kept its identity: same id (so progress is attributable) and
	// same digest and URL.
	reqs := fake.Requests()
	if len(reqs) != 1 {
		t.Fatalf("expected 1 download request, got %d", len(reqs))
	}
	if reqs[0].ExpectedSHA256 != final || reqs[0].URL != testBundleURL {
		t.Errorf("resumed request does not match the record: %+v", reqs[0])
	}
	for _, s := range pub.progressStates() {
		if s[0] == int64(StateFail) {
			t.Errorf("the resumed deploy failed: %v", pub.progressStates())
		}
	}

	// And the confirmation carries the digest recovered from the record. Nothing else
	// holds it after a restart: the digest is announced at the END of the deploy but
	// arrives with the job at the START, so it has to survive the gap in the job
	// record — exactly like the URL. Losing it would mean finishing an install and
	// then having nothing to close the job with.
	waitFor(t, "the confirmation of the resumed deploy",
		func() bool { return len(pub.begins()) == 1 })
	if got := pub.begins()[0]; got != final {
		t.Errorf("confirmation digest after a resume: got %x want %x (the record's)", got, final)
	}
}

func TestOTAFSMRemovesOrphanBundlesWithNothingInFlight(t *testing.T) {
	// A bundle with no deploy in flight belongs to a job whose install never
	// completed. Nobody is waiting for it and at multiple GB it must not accumulate.
	f, _, cfg := newTestFSM(t, &downloadertest.FakeDownloader{}, appinstaller.Unavailable())

	orphan := cfg.AppDownloadDir + string(os.PathSeparator) + "deadbeef.zip"
	if err := os.WriteFile(orphan, []byte("stale gigabytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)
	waitForIdle(t, f)

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphan bundle was not removed: %v", err)
	}
}

// TestOTAFSMRemovesOrphanPartialsWithNothingInFlight is the case the "*.zip" glob
// used to miss entirely. A crash leaves a partial and a resume record but no
// finished bundle; if the job record is also gone — which is exactly when this
// cleanup runs — nothing else will ever reclaim them, and a partial is the multi-
// gigabyte file of the pair.
func TestOTAFSMRemovesOrphanPartialsWithNothingInFlight(t *testing.T) {
	fake := &downloadertest.FakeDownloader{}
	f, _, cfg := newTestFSM(t, fake, appinstaller.Unavailable())

	bundle := filepath.Join(cfg.AppDownloadDir, "deadbeef.zip")
	orphans := []string{
		bundle + ".part",   // the gigabytes
		bundle + ".resume", // the record describing them
		// A temp file from an atomic write that never finished. Named with a leading
		// dot, so it must be recognised despite the prefix.
		filepath.Join(cfg.AppDownloadDir, ".deadbeef.zip.resume.tmp-42"),
	}
	for _, path := range orphans {
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)
	waitForIdle(t, f)

	for _, path := range orphans {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("%s was not reclaimed: %v", filepath.Base(path), err)
		}
	}
	// The downloader is also asked to discard, through its own API, in case it keeps
	// state that is not named after the bundle.
	if got := fake.Discarded(); len(got) != 1 || got[0] != bundle {
		t.Errorf("Discard calls: got %v want [%s]", got, bundle)
	}
}

func TestBundleBase(t *testing.T) {
	dir := filepath.Join("srv", "bundles")
	bundle := filepath.Join(dir, "abcd.zip")
	cases := []struct {
		in   string
		want string
		ok   bool
	}{
		{filepath.Join(dir, "abcd.zip"), bundle, true},
		{filepath.Join(dir, "abcd.zip.part"), bundle, true},
		{filepath.Join(dir, "abcd.zip.resume"), bundle, true},
		{filepath.Join(dir, ".abcd.zip.resume.tmp-42"), bundle, true},
		// Not ours: no bundle suffix to key on.
		{filepath.Join(dir, "notes.txt"), "", false},
		{filepath.Join(dir, "abcd.zipped"), "", false},
	}
	for _, tc := range cases {
		t.Run(filepath.Base(tc.in), func(t *testing.T) {
			got, ok := bundleBase(tc.in)
			if ok != tc.ok {
				t.Fatalf("ok: got %v want %v", ok, tc.ok)
			}
			if ok && got != tc.want {
				t.Errorf("got %q want %q", got, tc.want)
			}
		})
	}
}

func TestOTAFSMInstallStallTimeout(t *testing.T) {
	// An install that goes silent must not hold the Cloud job open forever
	// (RFC-14 §5.5).
	content := []byte("bundle v6")
	inst := appinstaller.Func(func(ctx context.Context, _ appinstaller.Request, _ appinstaller.ProgressFunc) error {
		<-ctx.Done() // never reports progress, never returns on its own
		return ctx.Err()
	})

	f, pub, _ := newTestFSM(t, &downloadertest.FakeDownloader{Content: content}, inst)
	f.installTimeout = 30 * time.Millisecond
	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)
	waitForIdle(t, f)

	f.Deliver(updateCmd(sha256.Sum256(content)))
	waitFor(t, "the install-timeout Fail report",
		func() bool { return pub.hasFailCode(ErrInstallTimeout) })
}

func TestOTAFSMInstallProgressResetsStallTimer(t *testing.T) {
	// The watchdog times out on silence, not on duration: an install that keeps
	// reporting must be allowed to take longer than installTimeout.
	content := []byte("bundle v7")
	final := sha256.Sum256(content)

	inst := appinstaller.Func(func(ctx context.Context, _ appinstaller.Request, onProgress appinstaller.ProgressFunc) error {
		for i := 1; i <= 6; i++ {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(15 * time.Millisecond):
			}
			onProgress(int32(i * 15))
		}
		return nil
	})

	f, pub, _ := newTestFSM(t, &downloadertest.FakeDownloader{Content: content}, inst)
	f.installTimeout = 40 * time.Millisecond // < the 90ms total, > each 15ms gap
	stop := runFSM(t, f)
	defer stop()
	f.SetConnected(true)
	waitForIdle(t, f)

	f.Deliver(updateCmd(final))

	waitFor(t, "the success OTABegin", func() bool { return pub.hasDigest(final) })
	for _, s := range pub.progressStates() {
		if s[0] == 8 {
			t.Errorf("a steadily-reporting install was failed: %v", pub.progressStates())
		}
	}
}

func TestProgressDataClampsToInt32WireLimit(t *testing.T) {
	// The known protocol limit for >2 GiB bundles: OTAProgressCmd.StateData is
	// int32, so the byte count saturates rather than wrapping to a negative number,
	// which the Cloud would read as an error code.
	f := &OTAFSM{}
	cases := []struct {
		written int64
		want    int32
	}{
		{0, 0},
		{1024, 1024},
		{math.MaxInt32, math.MaxInt32},
		{math.MaxInt32 + 1, math.MaxInt32},
		{4 << 30, math.MaxInt32}, // a 4 GiB bundle
	}
	for _, tc := range cases {
		got := f.progressData(tc.written)
		if got != tc.want {
			t.Errorf("progressData(%d): got %d want %d", tc.written, got, tc.want)
		}
		if got < 0 {
			t.Errorf("progressData(%d) went negative (%d) — the Cloud would read that as a failure",
				tc.written, got)
		}
	}
}

func TestReportTimestampStrictlyIncreasesWithinASecond(t *testing.T) {
	// Several reports can land in the same second; the Cloud orders them by
	// timestamp, so they must not collide.
	fixed := time.Unix(1_700_000_000, 0)
	f := &OTAFSM{now: func() time.Time { return fixed }}

	prev := uint64(0)
	for i := 0; i < 5; i++ {
		ts := f.reportTimestamp()
		if ts <= prev {
			t.Fatalf("timestamp %d did not increase past %d", ts, prev)
		}
		prev = ts
	}
	fixed = fixed.Add(time.Second)
	if ts := f.reportTimestamp(); ts <= prev {
		t.Errorf("timestamp %d in the next second did not increase past %d", ts, prev)
	}
}

func TestReportSkippedWithoutAJob(t *testing.T) {
	// OTAProgressCmd is keyed by job id; without a job there is nothing for the
	// Cloud to correlate it with.
	pub := &fakePublisher{}
	f := &OTAFSM{pub: pub, now: time.Now}
	f.connected.Store(true)
	f.report(StateFetch, 100)
	if n := len(pub.all()); n != 0 {
		t.Errorf("published %d messages without a job in flight", n)
	}
}

func TestPublishRejectsNonOTACommand(t *testing.T) {
	f := &OTAFSM{pub: &fakePublisher{}}
	if err := f.publish(command.ThingBeginCmd{}); err == nil {
		t.Error("expected a non-OTA uplink command to be rejected")
	}
}

// TestPublishFailureDoesNotAbortTheProcess covers a broker hiccup during a deploy:
// every publish fails, and the process must still walk the job to its end and return
// to Idle rather than wedging or spinning.
//
// The lost confirmation is NOT retried — nothing remembers the digest afterwards, so
// the Cloud job simply stays open. That is the accepted cost of never announcing a
// digest this process cannot vouch for; see TestOTAFSMPublishesNothingUntilAJobArrives.
func TestPublishFailureDoesNotAbortTheProcess(t *testing.T) {
	content := []byte("bundle v7")
	final := sha256.Sum256(content)
	installed := make(chan struct{}, 1)
	inst := appinstaller.Func(func(_ context.Context, _ appinstaller.Request, _ appinstaller.ProgressFunc) error {
		installed <- struct{}{}
		return nil
	})

	f, pub, _ := newTestFSM(t, &downloadertest.FakeDownloader{Content: content}, inst)
	pub.err = errors.New("broker unavailable")
	stop := runFSM(t, f)
	defer stop()

	f.SetConnected(true)
	waitForIdle(t, f)
	f.Deliver(updateCmd(final))

	select {
	case <-installed:
	case <-time.After(5 * time.Second):
		t.Fatal("the install never ran while publishes were failing")
	}

	waitFor(t, "a return to Idle despite the publish failures", func() bool {
		return f.Snapshot().State == StateIdle
	})

	// Nothing was recorded (every publish failed), and nothing keeps retrying.
	if n := len(pub.begins()); n != 0 {
		t.Errorf("recorded %d OTABegin messages, but every publish was failing", n)
	}
	settled := pub.attemptCount()
	time.Sleep(100 * time.Millisecond)
	if got := pub.attemptCount(); got != settled {
		t.Errorf("publish retried %d extra times in 100ms — something is spinning", got-settled)
	}
}

func TestInstallHandoverMapsPathAndDigest(t *testing.T) {
	// What this package contributes to the handover: the destination the bundle was
	// downloaded to, and the digest it was verified against. The body's wire shape is
	// app-installer's own contract and is pinned there.
	//
	// The handover deliberately carries no App identity: the daemon has none to give
	// (the OTA command channel delivers only id/url/digests) and must not invent one
	// by parsing the storage URL's key layout.
	digest := sha256.Sum256([]byte("bundle"))
	job := Job{URL: testBundleURL, SHA256: digest}

	req := installRequestFor(job, "/var/lib/x/job.zip")
	if req.BundlePath != "/var/lib/x/job.zip" {
		t.Errorf("bundle_path: got %q", req.BundlePath)
	}
	if req.SHA256 != hexOf(digest) {
		t.Errorf("sha256: got %q want %q", req.SHA256, hexOf(digest))
	}
}
