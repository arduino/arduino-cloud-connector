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
// # State is derived, not stored
//
// There is no dedicated provisioning-state file. The state is a pure function
// of two on-disk facts:
//
//   - the credentials (device cert + device_id), via keystore.IsProvisioned, and
//   - an "in-flight" marker file written while a (re)provisioning attempt runs.
//
// The marker takes precedence: while it is present a reprovision is mid-flight,
// so any leftover credentials are the old, now-invalid ones and must be ignored.
// This makes crash recovery deterministic — see BeginProvisioning / RunAttempt.
package provisioning

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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
	StateProvisioned   State = "provisioned"   // device certificate + device_id on disk
	StateError         State = "error"         // an in-flight attempt exceeded its retry window
)

// markerFile records that a (re)provisioning attempt is in flight. Its content
// is the RFC3339 start timestamp; its presence means "credentials, if any, are
// not authoritative". It is written BEFORE the old credentials are wiped (so a
// crash mid-wipe is still recoverable) and removed only once the new
// credentials are safely stored.
const markerFile = "provisioning_inflight"

// provisioningWindow caps the total wall-clock time — measured from the marker
// timestamp, so it survives restarts — spent retrying an attempt before it is
// abandoned (State becomes StateError). A var, not a const, so tests can shorten
// it.
var provisioningWindow = 15 * time.Minute

const (
	retryInitialBackoff = 2 * time.Second
	retryMaxBackoff     = 30 * time.Second
)

// Service manages the provisioning flow and exposes the derived state.
type Service struct {
	cfg    config.Config
	id     *identity.Service
	ks     *keystore.Keystore
	client provisioningapi.Client
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

// State derives the current provisioning state from disk. An in-flight marker
// takes precedence over credentials: within the retry window the state is
// Provisioning, past it Error. With no marker, the state is Provisioned iff the
// credentials are present.
func (s *Service) State() State {
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
// after the marker fails the marker is rolled back, preserving the invariant
// "marker present ⇔ credentials absent".
func (s *Service) BeginProvisioning(organizationID string) error {
	if err := s.writeMarker(time.Now()); err != nil {
		return fmt.Errorf("provisioning: write in-flight marker: %w", err)
	}
	if err := s.ks.WipeCloudCredentials(); err != nil {
		_ = s.clearMarker() // rollback: never leave a marker alongside credentials
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
// key, build and submit the CSR (retrying transient failures with capped
// back-off until the marker's window expires), store the issued cert + device_id,
// clear the in-flight marker, then call provision/complete (best-effort).
//
// Precondition (established by BeginProvisioning, or by a marker that survived a
// crash): the in-flight marker is present and the old credentials are wiped.
//
// Returns nil on success (credentials stored, marker cleared). On a window
// timeout it returns an error and LEAVES the marker, so State reports StateError
// until the next BeginProvisioning. On ctx cancellation (shutdown) it also
// leaves the marker, so the attempt resumes on the next start.
func (s *Service) RunAttempt(ctx context.Context) error {
	deadline, ok := s.markerDeadline()
	if !ok {
		return errors.New("provisioning: RunAttempt called with no in-flight marker")
	}

	if err := s.ks.RotateCloudKey(); err != nil {
		return fmt.Errorf("provisioning: rotate cloud key: %w", err)
	}
	csr, err := s.ks.GenerateCSR(s.id.UHWID())
	if err != nil {
		return fmt.Errorf("provisioning: generate CSR: %w", err)
	}
	privKey, err := s.ks.ProvisioningPrivateKey()
	if err != nil {
		return fmt.Errorf("provisioning: load provisioning key: %w", err)
	}
	boardToken, err := s.id.BoardToken(privKey)
	if err != nil {
		return fmt.Errorf("provisioning: generate board token: %w", err)
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

	// Completion signal: credentials are now safely on disk, so a crash from
	// here on resolves to "provisioned" (marker absent + creds present).
	if err := s.clearMarker(); err != nil {
		slog.Warn("provisioning: failed to clear in-flight marker", "error", err)
	}

	// provision/complete is best-effort — the cert already works for MQTT — but
	// retried within the remaining window.
	if err := s.completeWithRetry(ctx, boardToken, deadline); err != nil {
		slog.Warn("provisioning: provision/complete did not succeed", "error", err)
	}

	slog.Info("Provisioning complete", "device_id", deviceID)
	return nil
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
			return "", "", fmt.Errorf("provisioning: CSR retry window expired: %w", err)
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
			return fmt.Errorf("provisioning: complete retry window expired: %w", err)
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
