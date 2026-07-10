// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioning

import (
	"runtime"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
)

// TestStateDerivation exercises the pure state derivation from the in-flight
// marker and credential presence — including the marker-precedes-credentials
// rule and the retry-window expiry (StateProvisioning → StateError). It is a
// white-box test (constructs a Service directly) so it can drive the marker
// helpers and override provisioningWindow without a network or REST layer.
func TestStateDerivation(t *testing.T) {
	if runtime.GOOS == "windows" {
		// keystore.New enforces POSIX 0600 on private keys, unrepresentable on Windows.
		t.Skip("keystore POSIX-permission model is not representable on Windows")
	}

	cfg := config.Config{DataDir: t.TempDir()}
	ks, err := keystore.New(cfg)
	if err != nil {
		t.Fatalf("keystore.New: %v", err)
	}
	svc := &Service{cfg: cfg, ks: ks}

	// No marker, no credentials → Unprovisioned.
	if got := svc.State(); got != StateUnprovisioned {
		t.Errorf("fresh: got %q want %q", got, StateUnprovisioned)
	}
	if svc.InFlight() {
		t.Error("fresh: InFlight() = true, want false")
	}

	// Marker written now → Provisioning (within window), and in flight.
	if err := svc.writeMarker(time.Now()); err != nil {
		t.Fatalf("writeMarker: %v", err)
	}
	if got := svc.State(); got != StateProvisioning {
		t.Errorf("in-flight: got %q want %q", got, StateProvisioning)
	}
	if !svc.InFlight() {
		t.Error("in-flight: InFlight() = false, want true")
	}

	// Marker older than the window → Error.
	if err := svc.writeMarker(time.Now().Add(-2 * provisioningWindow)); err != nil {
		t.Fatalf("writeMarker (expired): %v", err)
	}
	if got := svc.State(); got != StateError {
		t.Errorf("expired marker: got %q want %q", got, StateError)
	}
	if !svc.InFlight() {
		t.Error("expired marker: InFlight() = false, want true (marker still present)")
	}

	// Marker cleared, still no credentials → back to Unprovisioned.
	if err := svc.clearMarker(); err != nil {
		t.Fatalf("clearMarker: %v", err)
	}
	if got := svc.State(); got != StateUnprovisioned {
		t.Errorf("cleared marker: got %q want %q", got, StateUnprovisioned)
	}

	// Credentials present, no marker → Provisioned (marker takes precedence, but
	// there is none here).
	if err := ks.StoreDeviceCert("dummy-cert"); err != nil {
		t.Fatalf("StoreDeviceCert: %v", err)
	}
	if err := ks.StoreDeviceID("dummy-id"); err != nil {
		t.Fatalf("StoreDeviceID: %v", err)
	}
	if got := svc.State(); got != StateProvisioned {
		t.Errorf("credentials present: got %q want %q", got, StateProvisioned)
	}

	// Marker precedes credentials: with both present, an in-flight marker wins.
	if err := svc.writeMarker(time.Now()); err != nil {
		t.Fatalf("writeMarker (with creds): %v", err)
	}
	if got := svc.State(); got != StateProvisioning {
		t.Errorf("marker + credentials: got %q want %q (marker must take precedence)", got, StateProvisioning)
	}
}
