// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package mqtt

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

// TestBrokerRootCAs covers the broker server-cert trust selection: an empty
// path means "use system roots" (nil pool), a valid PEM is pinned, and bad
// inputs surface as errors rather than silently falling back.
func TestBrokerRootCAs(t *testing.T) {
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
		pool, err := brokerRootCAs("")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pool != nil {
			t.Fatalf("expected nil pool (system roots), got %v", pool)
		}
	})

	t.Run("valid CA is pinned", func(t *testing.T) {
		pool, err := brokerRootCAs(validCA)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if pool == nil {
			t.Fatal("expected non-nil pool")
		}
	})

	t.Run("missing file errors", func(t *testing.T) {
		if _, err := brokerRootCAs(filepath.Join(dir, "nope.pem")); err == nil {
			t.Fatal("expected error for missing CA file")
		}
	})

	t.Run("non-PEM file errors", func(t *testing.T) {
		if _, err := brokerRootCAs(junk); err == nil {
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
