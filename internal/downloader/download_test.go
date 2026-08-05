// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package downloader

import (
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
	"github.com/arduino/arduino-cloud-connector/internal/storage-api/storageapitest"
)

// These tests drive the transfer machinery against a scripted storage client. No
// server, no TLS: what the HTTP layer does with a request is internal/storage-api's
// business and is tested there. That split is what makes it cheap to script the
// conditions that matter here — a body that dies at byte N, a server that ignores
// Range, a size that changes mid-transfer.

// ── harness ──────────────────────────────────────────────────────────────────

// testDownloader wires a downloader to fake with granularities small enough to
// exercise chunking, checkpointing and progress on a few KiB.
func testDownloader(t *testing.T, fake *storageapitest.FakeClient) *downloader {
	t.Helper()
	d := newDownloader(config.Config{
		MaxBundleSize:   1 << 20,
		DownloadTimeout: 30 * time.Second,
	}, fake)
	d.chunkSize = 1024
	d.checkpointInterval = 256
	d.progressInterval = time.Hour // off unless a test dials it down
	// No real sleeping: the retry back-off is asserted by call count, not by making
	// the suite wait 2+4+8 seconds.
	d.sleep = func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	// The host filesystem's free space is irrelevant to these tests.
	d.diskFree = func(string) (int64, error) { return 1 << 40, nil }
	return d
}

func destIn(dir string) string { return filepath.Join(dir, "artefact.bin") }

func testReq(destPath string, body []byte) Request {
	return Request{URL: "https://storage.example/a/b.zip", DestPath: destPath, ExpectedSHA256: sha256.Sum256(body)}
}

func content(n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte((i*31 + 7) % 251)
	}
	return out
}

func noProgress(int64, int64) {}

// ── the happy path and chunking ──────────────────────────────────────────────

func TestDownloadFetchesInChunks(t *testing.T) {
	body := content(5000) // ~5 chunks at chunkSize=1024
	fake := &storageapitest.FakeClient{Content: body}
	dp := destIn(t.TempDir())

	if err := testDownloader(t, fake).Download(context.Background(), testReq(dp, body), noProgress); err != nil {
		t.Fatalf("Download: %v", err)
	}

	got, err := os.ReadFile(dp)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatalf("content mismatch: got %d bytes, want %d", len(got), len(body))
	}

	ranges := fake.Ranges()
	if len(ranges) != 5 {
		t.Errorf("expected 5 chunked requests, got %d (%v)", len(ranges), ranges)
	}
	if ranges[0] != "bytes=0-1023" {
		t.Errorf("first range: got %q want bytes=0-1023", ranges[0])
	}
	if last := ranges[len(ranges)-1]; last != "bytes=4096-4999" {
		t.Errorf("last range: got %q want bytes=4096-4999", last)
	}

	// A finished download leaves no sidecar behind: the caller sees only DestPath.
	for _, p := range []string{partPath(dp), resumePath(dp)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s survived a completed download", filepath.Base(p))
		}
	}
}

func TestDownloadReportsProgress(t *testing.T) {
	body := content(4000)
	d := testDownloader(t, &storageapitest.FakeClient{Content: body})
	d.progressInterval = 0 // report on every read

	var calls int
	var lastW, lastTot int64
	err := d.Download(context.Background(), testReq(destIn(t.TempDir()), body),
		func(written, total int64) {
			calls++
			if written < lastW {
				t.Errorf("progress went backwards: %d after %d", written, lastW)
			}
			lastW, lastTot = written, total
		})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if calls == 0 {
		t.Fatal("progress was never reported")
	}
	if lastTot != int64(len(body)) {
		t.Errorf("reported total: got %d want %d", lastTot, len(body))
	}
}

func TestDownloadAcceptsANilProgressFunc(t *testing.T) {
	body := content(1200)
	d := testDownloader(t, &storageapitest.FakeClient{Content: body})
	d.progressInterval = 0
	if err := d.Download(context.Background(), testReq(destIn(t.TempDir()), body), nil); err != nil {
		t.Fatalf("Download with a nil ProgressFunc: %v", err)
	}
}

func TestDownloadRejectsAnEmptyDestination(t *testing.T) {
	err := testDownloader(t, &storageapitest.FakeClient{}).Download(
		context.Background(), Request{URL: "https://h/x.zip"}, noProgress)
	if !errors.Is(err, ErrOpenFile) {
		t.Errorf("expected ErrOpenFile, got %v", err)
	}
}

// ── resume ───────────────────────────────────────────────────────────────────

func TestDownloadResumesAcrossProcessRestart(t *testing.T) {
	// A process that dies mid-download must not re-fetch what it already has.
	body := content(8000)
	fake := &storageapitest.FakeClient{Content: body}
	dp := destIn(t.TempDir())
	req := testReq(dp, body)

	// First run: cancel the context partway, as a crash would.
	ctx, cancel := context.WithCancel(context.Background())
	d1 := testDownloader(t, fake)
	d1.progressInterval = 0 // report on every read so we can cancel promptly
	var downloaded int64
	err := d1.Download(ctx, req, func(written, _ int64) {
		downloaded = written
		if written >= 2048 {
			cancel()
		}
	})
	if err == nil {
		t.Fatal("expected the interrupted download to fail")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if downloaded < 2048 {
		t.Fatalf("expected at least 2048 bytes before the interruption, got %d", downloaded)
	}

	// The resume record must have captured the progress.
	state := loadResumeState(dp, req.URL, hexDigest(req.ExpectedSHA256))
	if state == nil {
		t.Fatal("no resume state after the interruption")
	}
	if state.Written == 0 {
		t.Fatal("resume state recorded no progress")
	}
	resumeAt := state.Written
	servedBefore := fake.Served()

	// Second run: a brand-new downloader, as a restarted daemon would build.
	d2 := testDownloader(t, fake)
	if err := d2.Download(context.Background(), req, noProgress); err != nil {
		t.Fatalf("resumed Download: %v", err)
	}

	got, err := os.ReadFile(dp)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Fatal("the resumed artefact does not match the original")
	}
	// The point of the exercise: the second run fetched only the remainder, and the
	// digest still verified — so the hasher state was restored, not recomputed from
	// byte zero.
	served := fake.Served() - servedBefore
	if served > int64(len(body))-resumeAt+int64(d2.checkpointInterval) {
		t.Errorf("resumed run fetched %d bytes; expected roughly the %d remaining",
			served, int64(len(body))-resumeAt)
	}
	if fake.Ranges()[len(fake.Ranges())-1] == "bytes=0-1023" {
		t.Error("the resumed run restarted from byte 0")
	}
}

func TestDownloadIgnoresResumeStateForADifferentArtefact(t *testing.T) {
	// A record whose digest or URL differs describes different bytes; continuing it
	// would produce a file that can never verify.
	body := content(1500)
	dp := destIn(t.TempDir())

	stale, err := newResumeState("https://storage.example/other.zip",
		hexDigest(sha256.Sum256([]byte("other"))), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(partPath(dp), content(700), 0o600); err != nil {
		t.Fatal(err)
	}
	h, _ := stale.hasher()
	if err := stale.checkpoint(dp, 700, h, time.Now()); err != nil {
		t.Fatal(err)
	}

	fake := &storageapitest.FakeClient{Content: body}
	if err := testDownloader(t, fake).Download(context.Background(), testReq(dp, body), noProgress); err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := os.ReadFile(dp)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Error("stale resume state corrupted the download")
	}
}

// ── retry budget ─────────────────────────────────────────────────────────────

func TestDownloadRetriesTransientFailures(t *testing.T) {
	body := content(2000)
	fake := &storageapitest.FakeClient{Content: body, FailNext: 3}

	if err := testDownloader(t, fake).Download(context.Background(),
		testReq(destIn(t.TempDir()), body), noProgress); err != nil {
		t.Fatalf("expected the retries to succeed, got %v", err)
	}
	if fake.Calls() < 4 {
		t.Errorf("expected at least 4 calls (3 failures + success), got %d", fake.Calls())
	}
}

func TestDownloadGivesUpAfterMaxAttempts(t *testing.T) {
	body := content(2000)
	fake := &storageapitest.FakeClient{Content: body, FailNext: 1000}

	err := testDownloader(t, fake).Download(context.Background(),
		testReq(destIn(t.TempDir()), body), noProgress)
	if !errors.Is(err, ErrTransfer) {
		t.Errorf("expected ErrTransfer, got %v", err)
	}
	if fake.Calls() != maxChunkAttempts {
		t.Errorf("expected exactly %d attempts, got %d", maxChunkAttempts, fake.Calls())
	}
}

func TestDownloadRetryBudgetResetsOnProgress(t *testing.T) {
	// A large artefact over a flaky link may be interrupted many more than
	// maxChunkAttempts times while still making progress; only a stuck transfer
	// should fail. Truncating every response mid-chunk simulates exactly that.
	body := content(4000)
	fake := &storageapitest.FakeClient{Content: body, TruncateAt: 300}

	if err := testDownloader(t, fake).Download(context.Background(),
		testReq(destIn(t.TempDir()), body), noProgress); err != nil {
		t.Fatalf("expected the transfer to complete despite repeated truncation, got %v", err)
	}
	if fake.Calls() <= maxChunkAttempts {
		t.Errorf("expected more than %d attempts (budget should reset on progress), got %d",
			maxChunkAttempts, fake.Calls())
	}
}

func TestDownloadDoesNotRetryATerminalFailure(t *testing.T) {
	// A 403 means the download grant is gone; retrying only delays the failure. The
	// decision is storage-api's, carried on the Failure — this asserts the loop
	// honours it.
	body := content(100)
	fake := &storageapitest.FakeClient{
		Content: body,
		Err:     storageapi.Terminal(storageapi.ErrBadResponse, "storage returned 403 Forbidden"),
	}

	err := testDownloader(t, fake).Download(context.Background(),
		testReq(destIn(t.TempDir()), body), noProgress)
	if !errors.Is(err, storageapi.ErrBadResponse) {
		t.Errorf("expected storageapi.ErrBadResponse, got %v", err)
	}
	if fake.Calls() != 1 {
		t.Errorf("expected a single attempt for a terminal failure, got %d", fake.Calls())
	}
}

func TestDownloadTimesOut(t *testing.T) {
	body := content(2000)
	fake := &storageapitest.FakeClient{Content: body, FailNext: 1000}
	d := testDownloader(t, fake)
	d.cfg.DownloadTimeout = time.Nanosecond

	err := d.Download(context.Background(), testReq(destIn(t.TempDir()), body), noProgress)
	if !errors.Is(err, ErrTimeout) {
		t.Errorf("expected ErrTimeout, got %v", err)
	}
}

func TestBackoffDelayGrowsAndCaps(t *testing.T) {
	prev := time.Duration(0)
	for attempt := 1; attempt <= 8; attempt++ {
		d := backoffDelay(attempt)
		if d <= 0 {
			t.Fatalf("attempt %d: non-positive delay %s", attempt, d)
		}
		upper := time.Duration(float64(retryBackoffMax) * (1 + retryBackoffJitter))
		if d > upper {
			t.Errorf("attempt %d: delay %s exceeds the cap+jitter %s", attempt, d, upper)
		}
		if attempt <= 4 && d < prev {
			t.Errorf("attempt %d: delay %s shrank from %s", attempt, d, prev)
		}
		prev = d
	}
	// The first four back-offs must fit inside the retry window, so the budget is
	// exhausted by attempt count rather than by the clock on a fast link.
	var total time.Duration
	for attempt := 1; attempt < maxChunkAttempts; attempt++ {
		total += backoffDelay(attempt)
	}
	if total >= retryWindow {
		t.Errorf("back-off total %s must stay under the %s retry window", total, retryWindow)
	}
}

// ── a server that ignores Range ──────────────────────────────────────────────

func TestDownloadRestartsWhenTheServerIgnoresRange(t *testing.T) {
	// Splicing a from-byte-0 body onto a partial file would corrupt the artefact,
	// so the transfer must start over instead — and still verify.
	body := content(3000)
	dp := destIn(t.TempDir())
	req := testReq(dp, body)

	// Seed a record claiming progress, as an interrupted run would leave.
	state, err := newResumeState(req.URL, hexDigest(req.ExpectedSHA256), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	h, _ := state.hasher()
	h.Write(body[:500])
	if err := os.WriteFile(partPath(dp), body[:500], 0o600); err != nil {
		t.Fatal(err)
	}
	if err := state.checkpoint(dp, 500, h, time.Now()); err != nil {
		t.Fatal(err)
	}

	fake := &storageapitest.FakeClient{Content: body, IgnoreRange: true}
	if err := testDownloader(t, fake).Download(context.Background(), req, noProgress); err != nil {
		t.Fatalf("Download: %v", err)
	}
	got, err := os.ReadFile(dp)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(body) {
		t.Errorf("content mismatch after a Range-ignoring restart: got %d bytes", len(got))
	}
}

// TestDownloadDoesNotLoopWhenTheServerIgnoresRange is a regression test for a real
// bug: without the noRange flag, stream stops at a chunk boundary, the loop asks for
// the next range, the server answers 200 from byte 0 again, the transfer resets —
// forever. The transfer must instead consume the whole body in ONE pass.
//
// ReadSize is what makes this test able to fail. Without it the body arrives in a
// single Read, the transfer sails past the chunk boundary and completes in one pass
// whether noRange is set or not — the test would pass over the bug it exists to
// catch. Capping the read size to well under chunkSize is what forces the boundary
// to be reached, and with it the reset/re-request loop.
func TestDownloadDoesNotLoopWhenTheServerIgnoresRange(t *testing.T) {
	body := content(5000) // 5 chunks' worth, deliberately more than one
	fake := &storageapitest.FakeClient{Content: body, IgnoreRange: true, ReadSize: 100}
	dp := destIn(t.TempDir())
	// A regression re-requests forever. Cut it off after a couple of calls so the
	// failure surfaces in milliseconds instead of hanging until the suite timeout.
	fake.Hook = func(call int) {
		if call > 2 {
			fake.SetErr(storageapi.Terminal(storageapi.ErrBadResponse, "fake: called %d times", call))
		}
	}

	if err := testDownloader(t, fake).Download(context.Background(), testReq(dp, body), noProgress); err != nil {
		t.Fatalf("Download: %v", err)
	}
	// One call is all it may take: the whole object arrives in a single response.
	if fake.Calls() != 1 {
		t.Errorf("expected exactly 1 call for an unranged transfer, got %d — the "+
			"reset/re-request loop is back", fake.Calls())
	}
	if served := fake.Served(); served != int64(len(body)) {
		t.Errorf("served %d bytes for a %d-byte artefact: the body was fetched more than once",
			served, len(body))
	}
}

// ── capacity ─────────────────────────────────────────────────────────────────

func TestDownloadRejectsAnOversizedArtefact(t *testing.T) {
	body := content(5000)
	d := testDownloader(t, &storageapitest.FakeClient{Content: body})
	d.cfg.MaxBundleSize = 1000

	err := d.Download(context.Background(), testReq(destIn(t.TempDir()), body), noProgress)
	if !errors.Is(err, ErrTooLarge) {
		t.Errorf("expected ErrTooLarge, got %v", err)
	}
}

func TestDownloadRejectsInsufficientDiskSpace(t *testing.T) {
	body := content(5000)
	d := testDownloader(t, &storageapitest.FakeClient{Content: body})
	d.diskFree = func(string) (int64, error) { return 100, nil }

	err := d.Download(context.Background(), testReq(destIn(t.TempDir()), body), noProgress)
	if !errors.Is(err, ErrNoSpace) {
		t.Errorf("expected ErrNoSpace, got %v", err)
	}
}

func TestDownloadProceedsWhenFreeSpaceIsUnknown(t *testing.T) {
	// On a platform without statfs the check is skipped, not treated as a failure:
	// refusing every download because we cannot measure the disk would be worse than
	// letting the write fail with ENOSPC.
	body := content(1200)
	d := testDownloader(t, &storageapitest.FakeClient{Content: body})
	d.diskFree = func(string) (int64, error) { return 0, errors.New("unsupported") }

	if err := d.Download(context.Background(), testReq(destIn(t.TempDir()), body), noProgress); err != nil {
		t.Fatalf("Download: %v", err)
	}
}

// ── what happens to the partial file ─────────────────────────────────────────

func TestDownloadRejectsADigestMismatch(t *testing.T) {
	body := content(1500)
	dp := destIn(t.TempDir())
	req := testReq(dp, body)
	req.ExpectedSHA256 = sha256.Sum256([]byte("a different artefact"))

	err := testDownloader(t, &storageapitest.FakeClient{Content: body}).
		Download(context.Background(), req, noProgress)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch, got %v", err)
	}
	// Nothing verifiable is left behind for a later call to resume from, and the
	// destination was never published. This also pins the ordering the sidecars are
	// removed in: the partial file is closed before it is unlinked, which Windows
	// requires — a regression there shows up here as a surviving .part.
	for _, p := range []string{dp, partPath(dp), resumePath(dp)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s survived a digest mismatch", filepath.Base(p))
		}
	}
}

// A size that changes mid-transfer means the artefact behind the URL was replaced,
// so the bytes on disk belong to the old one and must not survive to be resumed.
func TestDownloadDropsThePartialWhenTheSizeChangesMidTransfer(t *testing.T) {
	body := content(5000)
	fake := &storageapitest.FakeClient{Content: body}
	// On the second chunk, claim a different total.
	fake.Hook = func(call int) {
		if call == 2 {
			fake.SetTotal(9999)
		}
	}
	dp := destIn(t.TempDir())

	err := testDownloader(t, fake).Download(context.Background(), testReq(dp, body), noProgress)
	if !errors.Is(err, ErrBadSize) {
		t.Fatalf("expected ErrBadSize, got %v", err)
	}
	for _, p := range []string{partPath(dp), resumePath(dp)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s survived a mid-transfer size change", filepath.Base(p))
		}
	}
}

// The other side of the rule: an honest interruption keeps its partial, because
// re-fetching gigabytes is exactly what the resume record exists to avoid.
func TestDownloadKeepsThePartialWhenTheRetryBudgetRunsOut(t *testing.T) {
	body := content(5000)
	fake := &storageapitest.FakeClient{Content: body}
	// Serve the first chunk, then fail forever.
	fake.Hook = func(call int) {
		if call == 2 {
			fake.FailNext = 1000
		}
	}
	dp := destIn(t.TempDir())

	err := testDownloader(t, fake).Download(context.Background(), testReq(dp, body), noProgress)
	if !errors.Is(err, ErrTransfer) {
		t.Fatalf("expected ErrTransfer, got %v", err)
	}
	state := loadResumeState(dp, testReq(dp, body).URL, hexDigest(sha256.Sum256(body)))
	if state == nil {
		t.Fatal("the resume record was dropped after a recoverable failure")
	}
	if state.Written == 0 {
		t.Error("the resume record kept no progress")
	}
	if _, err := os.Stat(partPath(dp)); err != nil {
		t.Errorf("the partial file was dropped after a recoverable failure: %v", err)
	}
}

func TestDiscardRemovesPartialAndResumeStateOnly(t *testing.T) {
	dp := destIn(t.TempDir())
	for _, p := range []string{partPath(dp), resumePath(dp)} {
		if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A finished artefact at DestPath belongs to the caller, not to us.
	if err := os.WriteFile(dp, []byte("finished"), 0o600); err != nil {
		t.Fatal(err)
	}

	newDownloader(config.Config{}, nil).Discard(dp)

	for _, p := range []string{partPath(dp), resumePath(dp)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s was not removed", filepath.Base(p))
		}
	}
	if _, err := os.Stat(dp); err != nil {
		t.Errorf("Discard removed the destination file: %v", err)
	}
}
