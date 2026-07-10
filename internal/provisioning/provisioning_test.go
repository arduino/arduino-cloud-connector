// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioning_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"runtime"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning-api/provisioningapitest"
)

// testUHWID is an arbitrary but well-formed 64-hex-char UHWID. The offline
// mock client does not validate it; it only needs to be present.
const testUHWID = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"

// TestProvisioningFlowProducesUsableTLSMaterial drives the full provisioning
// Service flow against the offline provisioning-api client and proves that the
// stored device certificate and cloud private key pair up into a valid mTLS
// client certificate — i.e. the exact material internal/mqtt.buildTLSConfig
// hands to the broker. This is the unit-test analogue of "connect to the
// broker with the provisioned certificate", minus the live broker.
func TestProvisioningFlowProducesUsableTLSMaterial(t *testing.T) {
	if runtime.GOOS == "windows" {
		// The keystore enforces POSIX 0600 on private keys, which Windows
		// cannot represent; the daemon targets Linux. Run this on Linux/macOS.
		t.Skip("keystore POSIX-permission model is not representable on Windows")
	}

	cfg := config.Config{DataDir: t.TempDir()}

	ks, err := keystore.New(cfg)
	if err != nil {
		t.Fatalf("keystore.New: %v", err)
	}

	idSvc := identity.NewWithUHWID(testUHWID, ks)
	svc := provisioning.NewWithClient(cfg, idSvc, ks, provisioningapitest.NewMockClient(cfg))

	if svc.State() != provisioning.StateUnprovisioned {
		t.Fatalf("initial state: got %q want %q", svc.State(), provisioning.StateUnprovisioned)
	}

	if err := svc.Start(context.Background(), ""); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if svc.State() != provisioning.StateProvisioned {
		t.Errorf("post-Start state: got %q want %q", svc.State(), provisioning.StateProvisioned)
	}
	if !svc.IsProvisioned() {
		t.Error("IsProvisioned() = false after successful Start")
	}
	deviceID, err := svc.DeviceID()
	if err != nil {
		t.Fatalf("DeviceID: %v", err)
	}
	if deviceID == "" {
		t.Error("DeviceID is empty after provisioning")
	}

	// Re-assemble the mTLS material exactly as internal/mqtt.buildTLSConfig
	// does, and confirm the certificate and the rotated cloud key form a valid
	// X.509 key pair (this is what the broker handshake relies on).
	certPEM, err := ks.DeviceCertPEM()
	if err != nil {
		t.Fatalf("DeviceCertPEM: %v", err)
	}
	cloudKey, err := ks.CloudPrivateKey()
	if err != nil {
		t.Fatalf("CloudPrivateKey: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(cloudKey)
	if err != nil {
		t.Fatalf("marshal cloud key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})

	if _, err := tls.X509KeyPair([]byte(certPEM), keyPEM); err != nil {
		t.Fatalf("device cert and cloud key do not form a valid TLS key pair: %v", err)
	}
}

// TestProvisioningFailureKeepsInFlightMarker verifies that when a CSR attempt
// is interrupted (here: a cancelled context, the shutdown case), the in-flight
// marker is LEFT in place so the attempt resumes on the next start — and no
// credentials are present. The offline client honours cancellation when a
// latency is simulated.
func TestProvisioningFailureKeepsInFlightMarker(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("keystore POSIX-permission model is not representable on Windows")
	}

	cfg := config.Config{DataDir: t.TempDir()}
	ks, err := keystore.New(cfg)
	if err != nil {
		t.Fatalf("keystore.New: %v", err)
	}
	idSvc := identity.NewWithUHWID(testUHWID, ks)

	// Force the client to honour cancellation by giving it a non-zero latency,
	// then cancel the context up-front so SubmitCSR returns ctx.Err().
	client := provisioningapitest.NewMockClient(cfg, provisioningapitest.WithLatency(time.Minute))
	svc := provisioning.NewWithClient(cfg, idSvc, ks, client)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := svc.Start(ctx, ""); err == nil {
		t.Fatal("Start: expected error on cancelled context, got nil")
	}
	// The prelude wrote the marker and wiped credentials; the cancelled attempt
	// must not have cleared the marker → still in flight, within the window.
	if !svc.InFlight() {
		t.Error("InFlight() = false after an interrupted attempt; marker should persist for resume")
	}
	if svc.State() != provisioning.StateProvisioning {
		t.Errorf("state after interrupted Start: got %q want %q", svc.State(), provisioning.StateProvisioning)
	}
	if svc.IsProvisioned() {
		t.Error("IsProvisioned() = true after failed provisioning")
	}
}
