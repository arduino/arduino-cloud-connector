// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package downloader

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

// TestDownloadVerifiesWhenTheServerOverrunsTheRange covers the one response shape an
// exact range slice cannot produce and that a real CDN or object store readily does:
// a 206 that honours the requested START but ignores its END, streaming on to the end
// of the object.
//
// It matters because stream() does not bound its reads to the requested range — it
// stops only once the chunk boundary has been *crossed* — so every chunk but the last
// overshoots by up to one read. This test is what says the overshoot is safe, rather
// than an argument that it is: the surplus bytes belong at the offsets they land on,
// and t.written carries the real position into the next range request, so the
// assembled artefact still verifies. Nothing else in the suite exercises it, since
// the multi-chunk happy path is served exact slices — and it is exactly the class of
// bug a bundle over one chunk in size would be blamed for.
func TestDownloadVerifiesWhenTheServerOverrunsTheRange(t *testing.T) {
	body := content(5000) // ~5 chunks at chunkSize=1024
	// ReadSize well under chunkSize is what makes the overshoot happen at all: with
	// one giant Read the whole object would arrive in a single pass and the boundary
	// handling would never be reached.
	fake := &storageapitest.FakeClient{Content: body, ServeToEnd: true, ReadSize: 300}
	dp := destIn(t.TempDir())

	// Download verifies the digest itself, so returning nil already proves the
	// assembled bytes hash to ExpectedSHA256.
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

	// The requested starts must advance strictly. A repeated or rewound start would
	// mean a span was fetched twice, which is how this could go wrong while still
	// writing a plausible number of bytes.
	prev := int64(-1)
	for _, r := range fake.Ranges() {
		spec, _ := strings.CutPrefix(r, "bytes=")
		startSpec, _, _ := strings.Cut(spec, "-")
		start, err := strconv.ParseInt(startSpec, 10, 64)
		if err != nil {
			t.Fatalf("unparsable recorded range %q: %v", r, err)
		}
		if start <= prev {
			t.Errorf("range starts did not advance: %d after %d (%v)", start, prev, fake.Ranges())
		}
		prev = start
	}
}

// ── reassembly fidelity over chunk boundaries ────────────────────────────────

// This test answers one question the byte-level tests above cannot: is the archive that
// comes out of a chunked transfer the same FILE that went in?
//
// The artefact is shaped like a real deploy bundle, because its shape is what makes the
// failure mode visible. scripts/make-test-bundle.py builds one block of numbered lines
// and rewrites that same block until the payload is long enough, so the payload repeats
// with a fixed period — which means a span reassembled a few bytes out of position is
// INVISIBLE inside it: the text still reads correctly, only with different line numbers
// than belong at that offset. What gives it away is the ZIP trailer: it appears exactly
// once, at the very end, so any misplacement pushes it off the end of the file. That is
// how a 70 MiB bundle came down with the right byte count, a plausible payload, and no
// central directory.
//
// Everything here is sized in kilobytes. The script's real period is 1 MiB and the
// production chunk is 64 MiB, but neither number is what is under test: the chunk
// arithmetic is size-independent (int64 throughout, no width to overflow), so a boundary
// at 1 kB exercises exactly the code a boundary at 64 MiB does. What has to be
// reproduced is the *property* — a periodic payload with a unique trailer — not the
// constants. Keeping it small costs the suite milliseconds and a few kB of temp space.
const payloadPeriod = 1 << 10

// payloadBlock reproduces the script's text_block: 57-byte numbered lines, cut to
// exactly n bytes (the cut is why a block ends mid-line, as the real payload does).
func payloadBlock(n int) []byte {
	var out []byte
	for line := 1; len(out) < n; line++ {
		out = append(out, fmt.Sprintf("arduino-cloud-connector test payload - line %012d\n", line)...)
	}
	return out[:n]
}

// storedZip builds an archive shaped like a deploy bundle: one STORED member whose
// content is payloadBlock repeated to payloadSize, then the central directory and EOCD.
// Stored, not deflated, for the same reason the script defaults to it — deflate would
// crush repetitive text and leave nothing to chunk.
func storedZip(t *testing.T, payloadSize int) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := zip.NewWriter(&buf)
	w, err := zw.CreateHeader(&zip.FileHeader{Name: "payload.txt", Method: zip.Store})
	if err != nil {
		t.Fatal(err)
	}
	block := payloadBlock(min(payloadSize, payloadPeriod))
	for written := 0; written < payloadSize; {
		n := min(len(block), payloadSize-written)
		if _, err := w.Write(block[:n]); err != nil {
			t.Fatal(err)
		}
		written += n
	}
	if err := zw.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

// firstDiff reports the offset of the first differing byte, or -1 when equal. It is
// what `cmp` prints, and it is the number that localises a reassembly fault: an offset
// that lands exactly on a multiple of the chunk size names the boundary that lost the
// bytes.
func firstDiff(got, want []byte) int {
	for i := 0; i < len(got) && i < len(want); i++ {
		if got[i] != want[i] {
			return i
		}
	}
	if len(got) != len(want) {
		return min(len(got), len(want))
	}
	return -1
}

// reassembles downloads body through a fake serving exact ranges, and asserts the file on
// disk is byte-identical to what went in.
func reassembles(t *testing.T, body []byte, chunk int64, wantRequests int) {
	t.Helper()
	// ReadSize caps each Read well under both the chunk size and the downloader's own
	// read buffer, so a chunk boundary is actually reached mid-body instead of the whole
	// chunk arriving in one Read — without it a boundary bug can hide.
	fake := &storageapitest.FakeClient{Content: body, ReadSize: 100}
	dp := destIn(t.TempDir())

	d := testDownloader(t, fake)
	d.chunkSize = chunk

	if err := d.Download(context.Background(), testReq(dp, body), noProgress); err != nil {
		t.Fatalf("Download: %v", err)
	}

	got, err := os.ReadFile(dp)
	if err != nil {
		t.Fatal(err)
	}
	if at := firstDiff(got, body); at >= 0 {
		t.Errorf("reassembled archive differs at byte %d (got %d bytes, want %d); "+
			"chunk size %d, so the boundaries are at multiples of it",
			at, len(got), len(body), chunk)
	}
	// Without this the table would quietly degenerate: a case whose chunk size turns out
	// to cover the whole archive proves nothing about boundaries, and would still pass.
	if n := fake.Calls(); n != wantRequests {
		t.Errorf("expected %d ranged requests at chunk size %d, got %d (%v) — the case "+
			"did not exercise the boundary it was written for",
			wantRequests, chunk, n, fake.Ranges())
	}
}

func TestDownloadReassemblesAnArchiveAcrossChunkBoundaries(t *testing.T) {
	// Four full periods, so the payload repeats and a misplaced span cannot be spotted by
	// reading it — only the trailer can betray one.
	body := storedZip(t, 4*payloadPeriod)
	total := int64(len(body))

	cases := []struct {
		name         string
		chunk        int64
		wantRequests int
	}{
		// The control: no boundary at all. If this one ever fails, the fault is not in
		// the chunking.
		{"one request for the whole archive", total + 1, 1},
		// An exact divisor: the boundary falls between two chunks with nothing left
		// over, so an off-by-one at the seam has nowhere to hide.
		{"two equal chunks", total / 2, 2},
		// A partial last chunk, the ordinary case in production. The +1 is what makes it
		// one: total/3 happens to divide exactly, which would have made this a second
		// copy of the case above.
		{"three chunks, last one partial", total/3 + 1, 3},
		// The boundary lands on the payload's own repetition period — the alignment at
		// which a misplaced chunk is least visible in the payload text.
		{"boundary aligned to the payload period", payloadPeriod, 5},
		// The trailer, alone, in its own chunk. This is the shape of the real failure:
		// everything up to the last few bytes arrives, and the central directory is
		// whatever the final short request returns.
		{"trailer alone in the last chunk", total - 128, 2},
		// The extreme of the same idea.
		{"final chunk of one byte", total - 1, 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reassembles(t, body, tc.chunk, tc.wantRequests)
		})
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

// TestDownloadVerifiesTheFileOnDiskNotTheByteStream pins the verification onto the
// file. The streaming hasher describes the bytes that came off the network and
// necessarily agrees with itself, so on its own it can never notice that the artefact
// as stored is wrong — and the artefact as stored is what gets installed.
//
// The corruption is applied through a second file handle, to a region the transfer has
// already written and moved past, so the stream digest still matches the request
// perfectly and only a check that reads the file back can fail. Before verify() hashed
// the file, this download succeeded and published a corrupted bundle.
func TestDownloadVerifiesTheFileOnDiskNotTheByteStream(t *testing.T) {
	body := content(5000) // several chunks, so there is a written region to go back and spoil
	dp := destIn(t.TempDir())
	fake := &storageapitest.FakeClient{Content: body}

	// By the second chunk request, byte 10 is long since written and fsynced.
	fake.Hook = func(call int) {
		if call != 2 {
			return
		}
		f, err := os.OpenFile(partPath(dp), os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatalf("open partial to corrupt it: %v", err)
		}
		defer func() { _ = f.Close() }()
		if _, err := f.WriteAt([]byte{^body[10]}, 10); err != nil {
			t.Fatalf("corrupt partial: %v", err)
		}
	}

	err := testDownloader(t, fake).Download(context.Background(), testReq(dp, body), noProgress)
	if !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expected ErrDigestMismatch for a corrupted file, got %v", err)
	}
	// Same disposal as any other poisoned partial: nothing published, nothing left to
	// resume from.
	for _, p := range []string{dp, partPath(dp), resumePath(dp)} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Errorf("%s survived a corrupted download", filepath.Base(p))
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
