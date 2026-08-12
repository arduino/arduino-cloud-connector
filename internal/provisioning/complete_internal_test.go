// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioning

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning-api/provisioningapitest"
)

// These tests cover what happens when provision/complete does not succeed — the case
// that had no coverage at all, which is how a comment claiming "the cert already works
// for MQTT" survived. It does not: an un-activated certificate is valid but inactive
// and the broker refuses the connection, so a failed complete has to fail the whole
// attempt.
//
// They live in the internal test package because they need to shorten
// provisioningWindow and the back-off, and to plant an on-disk state directly.

// testUHWIDInternal is an arbitrary well-formed 64-hex-char UHWID; the offline client
// does not validate it.
const testUHWIDInternal = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"

// newTestService builds a Service over a fresh data dir and a scriptable offline
// client, and returns the client's call counters.
//
// The skip is capability-based rather than a GOOS check like the external tests use:
// the keystore refuses private keys that are not 0600, which Windows cannot express,
// but a `-tags mock` build makes that check a no-op. Asking whether the keystore can
// be built runs the test everywhere it can run instead of everywhere we predicted.
func newTestService(t *testing.T, opts ...provisioningapitest.Option) (*Service, *keystore.Keystore, provisioningapitest.Counters) {
	t.Helper()

	cfg := config.Config{DataDir: t.TempDir()}
	ks, err := keystore.New(cfg)
	if err != nil {
		t.Skipf("keystore unavailable on this host (%v); run on Linux/macOS or with -tags mock", err)
	}
	client := provisioningapitest.NewMockClient(cfg, opts...)
	counters, ok := client.(provisioningapitest.Counters)
	if !ok {
		t.Fatal("mock client does not expose call counters")
	}
	return NewWithClient(cfg, identity.NewWithUHWID(testUHWIDInternal, ks), ks, client), ks, counters
}

// shortenRetries makes the window and back-off small enough for a test to watch an
// attempt exhaust itself, and restores them afterwards.
func shortenRetries(t *testing.T, window, initial, max time.Duration) {
	t.Helper()
	oldWindow, oldInitial, oldMax := provisioningWindow, retryInitialBackoff, retryMaxBackoff
	provisioningWindow, retryInitialBackoff, retryMaxBackoff = window, initial, max
	t.Cleanup(func() {
		provisioningWindow, retryInitialBackoff, retryMaxBackoff = oldWindow, oldInitial, oldMax
	})
}

// TestCompleteFailureDiscardsTheCertificateAndReportsError is the core of this change.
// Before it, a complete that never succeeded was logged at Warn and the attempt
// returned success, so the board reported Provisioned and then failed every broker
// handshake forever with nothing to explain why.
func TestCompleteFailureDiscardsTheCertificateAndReportsError(t *testing.T) {
	shortenRetries(t, 60*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond)
	svc, ks, counters := newTestService(t, provisioningapitest.WithCompleteFailures(-1))

	err := svc.Start(context.Background(), "")
	if err == nil {
		t.Fatal("Start: expected an error when provision/complete never succeeds, got nil")
	}

	// The CSR half worked, so this is specifically the complete half failing.
	if counters.CSRCalls() != 1 {
		t.Errorf("CSRCalls = %d, want 1", counters.CSRCalls())
	}
	// More than one proves the window was actually spent retrying rather than giving
	// up on the first refusal.
	if counters.CompleteCalls() < 2 {
		t.Errorf("CompleteCalls = %d, want at least 2 (the retry budget was not used)", counters.CompleteCalls())
	}

	if got := svc.State(); got != StateError {
		t.Errorf("State = %q, want %q", got, StateError)
	}
	// The certificate cannot connect to the broker, so it must not be left behind for
	// a later reader of IsProvisioned to mistake for a working one.
	if ks.IsProvisioned() {
		t.Error("credentials survived a complete that never succeeded")
	}
	if svc.InFlight() {
		t.Error("in-flight marker survived a terminal failure; it should be cleared")
	}
}

// TestCompleteFailureStateIsInMemoryOnly pins the trade-off accepted with the design:
// the Error state is remembered in memory only, so a restart downgrades it to
// Unprovisioned. The daemon behaves the same either way (idle in Provisioning, waiting
// for a fresh /start) — only the string the operator sees degrades.
func TestCompleteFailureStateIsInMemoryOnly(t *testing.T) {
	shortenRetries(t, 60*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond)
	svc, ks, _ := newTestService(t, provisioningapitest.WithCompleteFailures(-1))

	if err := svc.Start(context.Background(), ""); err == nil {
		t.Fatal("Start: expected an error, got nil")
	}
	if got := svc.State(); got != StateError {
		t.Fatalf("State = %q, want %q", got, StateError)
	}

	// A new Service over the same disk is what a daemon restart looks like.
	restarted := NewWithClient(svc.cfg, svc.id, ks, svc.client)
	if got := restarted.State(); got != StateUnprovisioned {
		t.Errorf("State after restart = %q, want %q", got, StateUnprovisioned)
	}
}

// TestCompleteSucceedsAfterRetries covers the ordinary transient case: the endpoint
// refuses twice and then works. Nothing should be discarded.
func TestCompleteSucceedsAfterRetries(t *testing.T) {
	shortenRetries(t, time.Minute, 5*time.Millisecond, 10*time.Millisecond)
	svc, ks, counters := newTestService(t, provisioningapitest.WithCompleteFailures(2))

	if err := svc.Start(context.Background(), ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if counters.CompleteCalls() != 3 {
		t.Errorf("CompleteCalls = %d, want 3 (two refusals then success)", counters.CompleteCalls())
	}
	if got := svc.State(); got != StateProvisioned {
		t.Errorf("State = %q, want %q", got, StateProvisioned)
	}
	if !ks.IsProvisioned() {
		t.Error("credentials missing after a successful attempt")
	}
	// Cleared only now: the marker is what says "the attempt is not finished", and it
	// is finished only once the certificate is active.
	if svc.InFlight() {
		t.Error("in-flight marker survived a successful attempt")
	}
}

// TestResumeAfterRestartDoesNotRepeatTheCSR covers the state the marker's new lifetime
// creates: a certificate on disk under a live marker, i.e. issued but never activated.
// The attempt must continue from provision/complete. Repeating the CSR would spend a
// second certificate — and, on the real API, create a second device — for nothing.
//
// The on-disk state is planted rather than provoked: a successful attempt, then the
// marker written back, is exactly what a crash between StoreDeviceID and clearMarker
// leaves behind.
func TestResumeAfterRestartDoesNotRepeatTheCSR(t *testing.T) {
	shortenRetries(t, time.Minute, 5*time.Millisecond, 10*time.Millisecond)
	svc, ks, counters := newTestService(t)

	if err := svc.Start(context.Background(), ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	deviceID, err := svc.DeviceID()
	if err != nil {
		t.Fatalf("DeviceID: %v", err)
	}

	if err := svc.writeMarker(time.Now()); err != nil {
		t.Fatalf("writeMarker: %v", err)
	}
	// Credentials plus a live marker read as an attempt still in flight, not as a
	// provisioned board.
	if got := svc.State(); got != StateProvisioning {
		t.Fatalf("State with credentials under a live marker = %q, want %q", got, StateProvisioning)
	}

	csrBefore, completeBefore := counters.CSRCalls(), counters.CompleteCalls()
	if err := svc.RunAttempt(context.Background()); err != nil {
		t.Fatalf("RunAttempt (resume): %v", err)
	}

	if got := counters.CSRCalls(); got != csrBefore {
		t.Errorf("CSRCalls = %d, want %d: the resumed attempt re-issued the certificate", got, csrBefore)
	}
	if got := counters.CompleteCalls(); got != completeBefore+1 {
		t.Errorf("CompleteCalls = %d, want %d: the resumed attempt did not call complete", got, completeBefore+1)
	}
	if got := svc.State(); got != StateProvisioned {
		t.Errorf("State = %q, want %q", got, StateProvisioned)
	}
	if got, _ := svc.DeviceID(); got != deviceID {
		t.Errorf("device_id changed across the resume: %q → %q", deviceID, got)
	}
	if !ks.IsProvisioned() {
		t.Error("credentials missing after a resumed attempt")
	}
}

// TestCompleteInterruptedByShutdownKeepsMarkerAndCredentials is the exception that
// makes the design safe. On shutdown neither the marker nor the credentials may be
// touched: clearing the marker here would leave credentials with no marker, which reads
// as Provisioned, and the board would come back up announcing itself ready with a
// certificate the broker rejects — precisely the bug this change removes.
func TestCompleteInterruptedByShutdownKeepsMarkerAndCredentials(t *testing.T) {
	shortenRetries(t, time.Minute, 5*time.Millisecond, 10*time.Millisecond)
	svc, ks, counters := newTestService(t)

	if err := svc.Start(context.Background(), ""); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := svc.writeMarker(time.Now()); err != nil {
		t.Fatalf("writeMarker: %v", err)
	}

	// A cancelled context makes the offline client return ctx.Err() from Complete,
	// which is what a shutdown mid-activation looks like.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	csrBefore := counters.CSRCalls()
	if err := svc.RunAttempt(ctx); err == nil {
		t.Fatal("RunAttempt: expected the context error, got nil")
	}

	if got := counters.CSRCalls(); got != csrBefore {
		t.Errorf("CSRCalls = %d, want %d: a shutdown must not re-issue the certificate", got, csrBefore)
	}
	if !svc.InFlight() {
		t.Error("in-flight marker was cleared on shutdown: the board would restart claiming to be provisioned")
	}
	if !ks.IsProvisioned() {
		t.Error("credentials were discarded on shutdown; they are still valid and the attempt resumes")
	}
	if got := svc.State(); got != StateProvisioning {
		t.Errorf("State = %q, want %q", got, StateProvisioning)
	}
	// Nothing was remembered as a failure: this attempt is not over.
	if svc.completeFailure() != nil {
		t.Errorf("a shutdown was recorded as a terminal complete failure: %v", svc.completeFailure())
	}
}

// TestOnlyAnExpiredWindowAuthorisesDiscardingTheCertificate pins the discriminator that
// makes the discard safe: completePhase destroys work only on errRetryWindowExpired.
//
// It is a test about error classification rather than about behaviour, deliberately.
// The alternative — synthesising a third kind of failure to prove the conservative
// branch is conservative — cannot be written, because the retry loop only ever returns
// nil, ctx.Err(), or the wrapped verdict. So the thing worth pinning is that those two
// non-nil returns are distinguishable, since the whole safety property rests on it: if
// a future edit made a cancelled context satisfy errors.Is(err, errRetryWindowExpired),
// every shutdown would start discarding certificates and only this test would notice.
func TestOnlyAnExpiredWindowAuthorisesDiscardingTheCertificate(t *testing.T) {
	shortenRetries(t, 40*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond)
	svc, _, _ := newTestService(t, provisioningapitest.WithCompleteFailures(-1))

	boardToken, err := svc.boardToken()
	if err != nil {
		t.Fatalf("boardToken: %v", err)
	}

	// A window that has already closed: the verdict, which authorises the discard.
	expired := svc.completeWithRetry(context.Background(), boardToken, time.Now().Add(-time.Second))
	if !errors.Is(expired, errRetryWindowExpired) {
		t.Errorf("window expiry: errors.Is(err, errRetryWindowExpired) = false, got %v — "+
			"completePhase would no longer discard an un-activated certificate", expired)
	}

	// A cancelled context: NOT the verdict, so the conservative branch must take it.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cancelled := svc.completeWithRetry(ctx, boardToken, time.Now().Add(time.Minute))
	if cancelled == nil {
		t.Fatal("cancelled context: got nil error")
	}
	if errors.Is(cancelled, errRetryWindowExpired) {
		t.Errorf("cancelled context: classified as an expired window (%v) — a shutdown "+
			"would discard a certificate that is still activatable", cancelled)
	}
}

// TestNewStartClearsARememberedFailure: a fresh /start supersedes an earlier verdict.
// Leaving it set would keep the status at Error while an attempt was visibly running.
func TestNewStartClearsARememberedFailure(t *testing.T) {
	shortenRetries(t, 60*time.Millisecond, 5*time.Millisecond, 10*time.Millisecond)
	svc, _, _ := newTestService(t, provisioningapitest.WithCompleteFailures(-1))

	if err := svc.Start(context.Background(), ""); err == nil {
		t.Fatal("Start: expected an error, got nil")
	}
	if got := svc.State(); got != StateError {
		t.Fatalf("State = %q, want %q", got, StateError)
	}

	if err := svc.BeginProvisioning(""); err != nil {
		t.Fatalf("BeginProvisioning: %v", err)
	}
	if svc.completeFailure() != nil {
		t.Error("BeginProvisioning did not forget the previous failure")
	}
	if got := svc.State(); got != StateProvisioning {
		t.Errorf("State after a fresh start = %q, want %q", got, StateProvisioning)
	}
}
