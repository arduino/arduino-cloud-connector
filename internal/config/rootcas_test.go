// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestCloudRootCAs covers the server-cert trust selection shared by the MQTT
// broker client and the storage client: an empty path means "use system roots"
// (nil pool), a valid PEM is pinned, and bad inputs surface as errors rather than
// silently falling back to the system store — a silent fallback would turn a
// pinning misconfiguration into a weaker trust decision than the operator asked
// for.
//
// Moved here from internal/mqtt when the rule stopped being broker-specific.
func TestCloudRootCAs(t *testing.T) {
	dir := t.TempDir()

	validCA := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(validCA, selfSignedCAPEM(t), 0o600); err != nil {
		t.Fatal(err)
	}

	junk := filepath.Join(dir, "junk.pem")
	if err := os.WriteFile(junk, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("empty path falls back to system roots", func(t *testing.T) {
		pool, err := Config{}.CloudRootCAs()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pool != nil {
			t.Fatalf("expected nil pool (system roots), got %v", pool)
		}
	})

	t.Run("valid CA is pinned", func(t *testing.T) {
		pool, err := Config{MQTTCAFile: validCA}.CloudRootCAs()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pool == nil {
			t.Fatal("expected non-nil pool")
		}
	})

	t.Run("missing file errors", func(t *testing.T) {
		cfg := Config{MQTTCAFile: filepath.Join(dir, "nope.pem")}
		if _, err := cfg.CloudRootCAs(); err == nil {
			t.Fatal("expected error for missing CA file")
		}
	})

	t.Run("non-PEM file errors", func(t *testing.T) {
		if _, err := (Config{MQTTCAFile: junk}).CloudRootCAs(); err == nil {
			t.Fatal("expected error for file with no certificates")
		}
	})
}

// selfSignedCAPEM returns a throwaway self-signed CA certificate in PEM form.
func selfSignedCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}
