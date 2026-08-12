// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package provisioning implements the Provisioning 2.0 client-side flow.
//
// The HTTP transport against the Arduino Provisioning API lives in a separate
// package — internal/provisioning-api — and is consumed here through the
// provisioningapi.Client interface. That separation keeps this package free
// of HTTP/JSON concerns and lets a test client be injected.
//
// # State is derived from disk, with one in-memory exception
//
// There is no dedicated provisioning-state file. The state is almost a pure
// function of two on-disk facts:
//
//   - the credentials (device cert + device_id), via keystore.IsProvisioned, and
//   - an "in-flight" marker file written while a (re)provisioning attempt runs.
//
// The marker takes precedence over the credentials while an attempt is running,
// which is what makes crash recovery deterministic — see BeginProvisioning /
// RunAttempt.
//
// The exception is a provision/complete that ran out of retries: that clears the
// marker and wipes the credentials, so disk alone would read "unprovisioned",
// which understates what happened. The failure is therefore remembered in memory
// (completeFailed) for as long as the process lives, so GET /v1/status can say
// "error" and not merely "nothing here". A restart loses it and the state reads
// Unprovisioned — accepted deliberately: the daemon behaves identically in both
// cases (idle in Provisioning, waiting for /v1/provisioning/start), only the
// string shown to the operator degrades.
//
// # Why provision/complete is not best-effort
//
// A certificate that has been issued but never completed is VALID BUT NOT ACTIVE:
// the broker refuses the connection. Verified on a real board while the
// Provisioning API's complete endpoint was failing. So a deploy that stores the
// certificate and skips complete produces a board that reports itself provisioned
// and can never connect — which is why complete failing fails the whole attempt.
package provisioning

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	provisioningapi "github.com/arduino/arduino-cloud-connector/internal/provisioning-api"
)

// State represents the board provisioning lifecycle, derived from disk.
type State string

const (
	StateUnprovisioned State = "unprovisioned" // no credentials, no attempt in flight
	StateProvisioning  State = "provisioning"  // an attempt is in flight, within the retry window
	StateProvisioned   State = "provisioned"   // device certificate + device_id on disk, complete done
	StateError         State = "error"         // an attempt ran out of its retry window
)

// markerFile records that a (re)provisioning attempt is in flight. Its content is
// the RFC3339 start timestamp. It is written BEFORE the old credentials are wiped
// (so a crash mid-wipe is still recoverable) and removed only once the attempt has
// finished — which now means AFTER provision/complete has succeeded, not after the
// certificate has been stored.
//
// Its presence therefore no longer implies "credentials, if any, are stale". The
// marker means "the attempt is not finished", and there are two ways it can be
// unfinished, told apart by whether the credentials are on disk:
//
//   - marker, no credentials → the CSR has not been issued yet: resume from there.
//   - marker + credentials   → the certificate is stored but not activated: resume
//     from provision/complete, and do NOT redo the CSR.
//
// That second combination is new, and it is why the old invariant "marker present
// ⇔ credentials absent" no longer holds.
const markerFile = "provisioning_inflight"

// provisioningWindow caps the total wall-clock time — measured from the marker
// timestamp, so it survives restarts — spent on one attempt before it is abandoned
// (State becomes StateError). A var, not a const, so tests can shorten it.
//
// It covers the WHOLE attempt: the CSR and provision/complete share it, and
// complete gets whatever the CSR left. That is deliberate — the number the operator
// waits on is the total, and giving each phase its own budget would double the worst
// case they experience. A CSR that consumes nearly all of it leaves complete a
// couple of attempts, and then the attempt fails; that is the intended trade.
var provisioningWindow = 10 * time.Minute

// Back-off between attempts, shared by both retry loops. Vars, not consts, for the
// same reason provisioningWindow is one: a test that has to observe several attempts
// cannot afford to wait seconds for each.
var (
	retryInitialBackoff = 2 * time.Second
	retryMaxBackoff     = 30 * time.Second
)

// errRetryWindowExpired marks the one outcome that authorises destroying work: a retry
// loop that used up provisioningWindow and gave up.
//
// It exists so that completePhase can require POSITIVE PROOF before discarding an
// issued certificate, instead of inferring the intent from the context. Inferring it
// was fragile in a way that would have failed silently: "ctx cancelled" happens to mean
// "we are being stopped" only because nothing on this path wraps the attempt in a
// deadline today, and the first person to add a context.WithTimeout around it — an
// obvious thing to do — would have turned every timeout into a shutdown and quietly
// disabled the discard. With this sentinel, anything unclassified is conservative by
// construction.
var errRetryWindowExpired = errors.New("provisioning: retry window expired")

// Service manages the provisioning flow and exposes the derived state.
type Service struct {
	cfg    config.Config
	id     *identity.Service
	ks     *keystore.Keystore
	client provisioningapi.Client

	// mu guards completeFailed, which is written by the goroutine running an attempt
	// (the daemon FSM's) and read by the REST handler's on every GET /v1/status.
	mu sync.RWMutex
	// completeFailed remembers that provision/complete ran out of retries. See the
	// package comment: that outcome leaves nothing on disk to derive a state from, so
	// without this the status would read Unprovisioned and lose the distinction
	// between "never started" and "tried and failed". Cleared by BeginProvisioning.
	completeFailed error
}

// New creates a provisioning Service wired to the real Provisioning API client.
func New(cfg config.Config, id *identity.Service, ks *keystore.Keystore) *Service {
	return NewWithClient(cfg, id, ks, provisioningapi.NewClient(cfg))
}

// NewWithClient is like New but takes an explicit provisioningapi.Client, so
// tests can inject the offline client from provisioningapitest.
func NewWithClient(cfg config.Config, id *identity.Service, ks *keystore.Keystore, client provisioningapi.Client) *Service {
	svc := &Service{cfg: cfg, id: id, ks: ks, client: client}
	slog.Info("Provisioning state", "state", svc.State())
	return svc
}

// State reports the current provisioning state, in this order of precedence:
//
//  1. a provision/complete that ran out of retries → Error. Checked first because
//     that outcome deliberately leaves nothing on disk (marker cleared, credentials
//     wiped), so every later rule would answer Unprovisioned.
//  2. an in-flight marker → Provisioning within the window, Error past it. This
//     covers both phases; whether the credentials are already on disk decides which
//     one a resumed attempt continues from, not what the state is called.
//  3. credentials with no marker → Provisioned. With the marker now surviving until
//     complete has succeeded, this means fully provisioned AND activated.
//  4. otherwise Unprovisioned.
func (s *Service) State() State {
	if s.completeFailure() != nil {
		return StateError
	}
	if ts, ok := s.markerTimestamp(); ok {
		if time.Now().Before(ts.Add(provisioningWindow)) {
			return StateProvisioning
		}
		return StateError
	}
	if s.ks.IsProvisioned() {
		return StateProvisioned
	}
	return StateUnprovisioned
}

// completeFailure returns the remembered provision/complete failure, if any.
func (s *Service) completeFailure() error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.completeFailed
}

// setCompleteFailure records (or, with nil, forgets) a terminal provision/complete
// failure.
func (s *Service) setCompleteFailure(err error) {
	s.mu.Lock()
	s.completeFailed = err
	s.mu.Unlock()
}

// IsProvisioned reports whether a device cert and device_id are present.
func (s *Service) IsProvisioned() bool { return s.ks.IsProvisioned() }

// InFlight reports whether a provisioning attempt marker is present (regardless
// of whether its retry window has expired). Used by the daemon FSM on restart
// to decide whether to resume an interrupted attempt.
func (s *Service) InFlight() bool {
	_, ok := s.markerTimestamp()
	return ok
}

// DeviceID returns the device UUID, or an error if not yet provisioned.
func (s *Service) DeviceID() (string, error) { return s.ks.DeviceID() }

// OrganizationID returns the organization the device was provisioned into, or an
// error if none was supplied at provisioning time (the field is optional).
func (s *Service) OrganizationID() (string, error) { return s.ks.OrganizationID() }

// BeginProvisioning is the synchronous prelude of a (re)provisioning request:
// it commits the daemon to reprovisioning by writing the in-flight marker,
// wiping the old credentials and, if organizationID is non-empty, persisting the
// new organization id. It is fast (a few local filesystem operations) so its
// result can be surfaced to the REST caller — a failure is known immediately
// instead of being swallowed in a background goroutine.
//
// organizationID is optional: when empty no organization_id file is written (and
// the wipe above already cleared any stale one), so the board is provisioned
// without an associated organization.
//
// Order matters: the marker is written BEFORE the wipe, so a crash after the
// wipe still leaves a marker (→ the attempt is resumed on restart). If a step
// after the marker fails the marker is rolled back, so a failed prelude leaves the
// board exactly as it was rather than half-committed.
//
// A remembered provision/complete failure from an earlier attempt is forgotten here:
// a fresh start supersedes it, and leaving it set would keep the status at Error
// while an attempt was visibly running.
func (s *Service) BeginProvisioning(organizationID string) error {
	s.setCompleteFailure(nil)

	if err := s.writeMarker(time.Now()); err != nil {
		return fmt.Errorf("provisioning: write in-flight marker: %w", err)
	}
	if err := s.ks.WipeCloudCredentials(); err != nil {
		_ = s.clearMarker() // rollback: do not leave a marker for an attempt that never began
		return fmt.Errorf("provisioning: wipe credentials: %w", err)
	}
	if organizationID != "" {
		if err := s.ks.StoreOrganizationID(organizationID); err != nil {
			_ = s.clearMarker() // rollback the marker (the wiped credentials are already gone)
			return fmt.Errorf("provisioning: store organization id: %w", err)
		}
	}
	slog.Info("Provisioning started: in-flight marker written, old credentials wiped")
	return nil
}

// RunAttempt performs the asynchronous remainder of an attempt: rotate the cloud
// key, build and submit the CSR (retrying with capped back-off until the marker's
// window expires), store the issued cert + device_id, call provision/complete, and
// only then clear the in-flight marker.
//
// Precondition (established by BeginProvisioning, or by a marker that survived a
// crash): the in-flight marker is present.
//
// It resumes rather than restarts. If the credentials are already on disk the CSR
// has been issued and only the activation is missing, so the whole key/CSR half is
// skipped and the attempt continues from provision/complete — a restart must not
// spend a second CSR on a certificate it already holds.
//
// Returns nil only once the device is provisioned AND activated. On a window timeout
// it returns an error; on ctx cancellation (shutdown) it returns ctx.Err() and leaves
// everything in place, so the attempt resumes on the next start. What happens to the
// marker and the credentials in each case is decided in completePhase.
func (s *Service) RunAttempt(ctx context.Context) error {
	deadline, ok := s.markerDeadline()
	if !ok {
		return errors.New("provisioning: RunAttempt called with no in-flight marker")
	}

	// Resume: a certificate on disk under a live marker was issued by an earlier run
	// that never got it activated.
	if s.ks.IsProvisioned() {
		deviceID, err := s.ks.DeviceID()
		if err != nil {
			return fmt.Errorf("provisioning: read device_id of the interrupted attempt: %w", err)
		}
		slog.Info("provisioning: certificate already issued, resuming from provision/complete",
			"device_id", deviceID)
		boardToken, err := s.boardToken()
		if err != nil {
			return err
		}
		return s.completePhase(ctx, boardToken, deviceID, deadline)
	}

	if err := s.ks.RotateCloudKey(); err != nil {
		return fmt.Errorf("provisioning: rotate cloud key: %w", err)
	}
	csr, err := s.ks.GenerateCSR(s.id.UHWID())
	if err != nil {
		return fmt.Errorf("provisioning: generate CSR: %w", err)
	}
	boardToken, err := s.boardToken()
	if err != nil {
		return err
	}

	deviceID, certPEM, err := s.submitCSRWithRetry(ctx, boardToken, csr, deadline)
	if err != nil {
		return err
	}

	// Persist credentials. A failure here is a disk problem (full/permissions),
	// not transient: do not retry the network — surface it.
	if err := s.ks.StoreDeviceCert(certPEM); err != nil {
		return fmt.Errorf("provisioning: store cert: %w", err)
	}
	if err := s.ks.StoreDeviceID(deviceID); err != nil {
		return fmt.Errorf("provisioning: store device_id: %w", err)
	}

	// The marker stays: the certificate exists but is not active yet, and until it is
	// this attempt is not finished. A crash here resumes from completePhase.
	return s.completePhase(ctx, boardToken, deviceID, deadline)
}

// completePhase activates the stored certificate and decides what the attempt leaves
// behind. It is the only place that clears the marker on a successful attempt, and the
// only place that discards a certificate it did not issue.
//
// Three outcomes, and the ordering of the switch is the safety property: the branch
// that destroys work is entered only on errRetryWindowExpired — a stated verdict from
// the retry loop — and everything else falls through to the branch that touches
// nothing. An unforeseen error class can therefore cost a retry, never a certificate.
//
//   - Activated: marker cleared, credentials kept → Provisioned, the FSM goes to Run.
//   - Retry window expired: credentials discarded AND marker cleared, the failure
//     remembered in memory → Error, the FSM stays in Provisioning awaiting a fresh
//     /start. Discarding is not a loss: an un-activated certificate cannot connect to
//     the broker, so keeping it would only leave a trap for a later reader of
//     IsProvisioned.
//   - Anything else — a cancelled context (Ctrl-C; note that a systemd SIGTERM kills
//     the process outright and never reaches here), or a failure mode nobody has
//     thought of yet: marker AND credentials left exactly as they are, so the next
//     start resumes from here. Clearing the marker on this path is what would
//     reintroduce the bug this function exists to fix — the board would come back up
//     reporting Provisioned with a certificate the broker rejects.
func (s *Service) completePhase(ctx context.Context, boardToken, deviceID string, deadline time.Time) error {
	err := s.completeWithRetry(ctx, boardToken, deadline)

	switch {
	case err == nil:
		if err := s.clearMarker(); err != nil {
			slog.Warn("provisioning: failed to clear in-flight marker", "error", err)
		}
		slog.Info("Provisioning complete", "device_id", deviceID)
		return nil

	case errors.Is(err, errRetryWindowExpired):
		slog.Error("provisioning: provision/complete never succeeded — the certificate is valid but NOT active, so it is being discarded",
			"device_id", deviceID, "error", err)
		if wipeErr := s.ks.WipeCloudCredentials(); wipeErr != nil {
			// Not fatal, and not allowed to mask the real cause: the remembered failure
			// below already makes State report Error, so a surviving certificate cannot
			// be mistaken for a working one.
			slog.Warn("provisioning: failed to wipe the un-activated credentials", "error", wipeErr)
		}
		if clearErr := s.clearMarker(); clearErr != nil {
			slog.Warn("provisioning: failed to clear in-flight marker", "error", clearErr)
		}
		s.setCompleteFailure(err)
		return err

	default:
		slog.Info("provisioning: provision/complete did not finish, leaving the attempt in place to resume",
			"device_id", deviceID, "error", err)
		return err
	}
}

// boardToken builds the token that authenticates this board to the Provisioning API.
func (s *Service) boardToken() (string, error) {
	privKey, err := s.ks.ProvisioningPrivateKey()
	if err != nil {
		return "", fmt.Errorf("provisioning: load provisioning key: %w", err)
	}
	token, err := s.id.BoardToken(privKey)
	if err != nil {
		return "", fmt.Errorf("provisioning: generate board token: %w", err)
	}
	return token, nil
}

// Start runs the full flow synchronously (prelude + attempt). Convenience for
// tests; the daemon FSM drives BeginProvisioning and RunAttempt separately so it
// can reply to the REST caller right after the prelude.
func (s *Service) Start(ctx context.Context, organizationID string) error {
	if err := s.BeginProvisioning(organizationID); err != nil {
		return err
	}
	return s.RunAttempt(ctx)
}

func (s *Service) submitCSRWithRetry(ctx context.Context, boardToken, csr string, deadline time.Time) (deviceID, certPEM string, err error) {
	backoff := retryInitialBackoff
	for {
		deviceID, certPEM, err = s.client.SubmitCSR(ctx, boardToken, csr)
		if err == nil {
			return deviceID, certPEM, nil
		}
		if ctx.Err() != nil {
			return "", "", ctx.Err()
		}
		if !time.Now().Before(deadline) {
			return "", "", fmt.Errorf("%w during CSR submission: %w", errRetryWindowExpired, err)
		}
		slog.Warn("provisioning: CSR submission failed, will retry", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, retryMaxBackoff)
	}
}

// completeWithRetry calls provision/complete until it succeeds, the shared window
// runs out, or ctx is cancelled. Same back-off shape as submitCSRWithRetry.
//
// Only the window-expiry return wraps errRetryWindowExpired, and only that return
// lets completePhase discard the certificate. A cancelled context returns ctx.Err()
// unwrapped, so it stays on the conservative side of that switch.
//
// Retries are UNCONDITIONAL: no attempt is made to classify a response as permanent
// and give up early. The endpoint's failures observed in practice are transient
// back-end ones, and a wrong "this is permanent" verdict would abandon an attempt
// that was about to succeed — the expensive mistake here, since giving up discards
// the certificate.
//
// One consequence to know about: provision/complete is NOT idempotent, so if a call
// succeeded server-side and its response was lost, the retries below (and any resumed
// attempt) will keep being rejected until the window closes, and the certificate is
// then discarded even though the device was activated. The window between the server
// committing and clearMarker returning is a single round trip, but it is not zero. If
// the API ever grows a distinguishable "already completed" response, treating it as
// success on a resumed attempt closes this hole.
func (s *Service) completeWithRetry(ctx context.Context, boardToken string, deadline time.Time) error {
	backoff := retryInitialBackoff
	for {
		err := s.client.Complete(ctx, boardToken)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if !time.Now().Before(deadline) {
			// Wrapping errRetryWindowExpired is not decoration: it is what authorises
			// completePhase to discard the certificate. See that switch.
			return fmt.Errorf("%w during provision/complete: %w", errRetryWindowExpired, err)
		}
		slog.Warn("provisioning: complete failed, will retry", "error", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, retryMaxBackoff)
	}
}

// ── in-flight marker ────────────────────────────────────────────────────────

func (s *Service) markerPath() string {
	return filepath.Join(s.cfg.DataDir, markerFile)
}

// writeMarker atomically writes the marker carrying ts as an RFC3339Nano
// timestamp (temp file + rename), so a crash never leaves a half-written marker.
func (s *Service) writeMarker(ts time.Time) error {
	data := []byte(ts.UTC().Format(time.RFC3339Nano))
	tmp, err := os.CreateTemp(s.cfg.DataDir, "."+markerFile+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }() // no-op once renamed
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.markerPath())
}

func (s *Service) clearMarker() error {
	if err := os.Remove(s.markerPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// markerTimestamp returns the marker's start timestamp and ok=true if a marker
// is present. A missing marker returns ok=false; a present-but-corrupt marker
// returns the zero time with ok=true, so its window is treated as already
// expired (→ StateError, requiring a fresh start) rather than retried forever.
func (s *Service) markerTimestamp() (time.Time, bool) {
	data, err := os.ReadFile(s.markerPath())
	if err != nil {
		return time.Time{}, false
	}
	ts, perr := time.Parse(time.RFC3339Nano, string(data))
	if perr != nil {
		slog.Warn("provisioning: in-flight marker timestamp unparseable, treating as expired", "content", string(data))
		return time.Time{}, true
	}
	return ts, true
}

func (s *Service) markerDeadline() (time.Time, bool) {
	ts, ok := s.markerTimestamp()
	if !ok {
		return time.Time{}, false
	}
	return ts.Add(provisioningWindow), true
}
