// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package keystore

import (
	"crypto/ecdsa"
	"encoding/pem"
	"runtime"
	"testing"

	"crypto/x509"
)

// newCloudKeystore builds a Keystore with only the cloud key, bypassing New's
// POSIX-0600 check (not representable on Windows) so GenerateCSR stays
// cross-platform.
func newCloudKeystore(t *testing.T) *Keystore {
	t.Helper()
	ks := &Keystore{dataDir: t.TempDir()}
	if err := ks.ensureKeyPair(fileCloudPrivKey, ""); err != nil {
		t.Fatalf("ensureKeyPair: %v", err)
	}
	return ks
}

func TestGenerateCSRIsCNOnly(t *testing.T) {
	ks := newCloudKeystore(t)
	const uhwid = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"

	csrPEM, err := ks.GenerateCSR(uhwid)
	if err != nil {
		t.Fatalf("GenerateCSR: %v", err)
	}

	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil || block.Type != "CERTIFICATE REQUEST" {
		t.Fatalf("output is not a CERTIFICATE REQUEST PEM block")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificateRequest: %v", err)
	}
	if err := csr.CheckSignature(); err != nil {
		t.Fatalf("CSR self-signature invalid: %v", err)
	}

	// Subject must be CN-only: a stray RDN (the past Country=IT bug) breaks the
	// reconstructed cert's signature against the CA. Expect exactly one RDN.
	if csr.Subject.CommonName != uhwid {
		t.Errorf("CommonName: got %q want %q", csr.Subject.CommonName, uhwid)
	}
	if len(csr.Subject.Names) != 1 {
		t.Errorf("subject must have exactly one RDN (CN-only), got %d: %+v",
			len(csr.Subject.Names), csr.Subject.Names)
	}
	if len(csr.Subject.Country) != 0 || len(csr.Subject.Organization) != 0 ||
		len(csr.Subject.OrganizationalUnit) != 0 || len(csr.Subject.Locality) != 0 {
		t.Errorf("subject must be CN-only, found extra attributes: %+v", csr.Subject)
	}

	// The CSR must carry the cloud key's public key.
	cloudKey, err := ks.CloudPrivateKey()
	if err != nil {
		t.Fatalf("CloudPrivateKey: %v", err)
	}
	csrPub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("CSR public key is not EC: %T", csr.PublicKey)
	}
	if csrPub.X.Cmp(cloudKey.X) != 0 || csrPub.Y.Cmp(cloudKey.Y) != 0 { //nolint:staticcheck

		t.Error("CSR public key does not match the cloud private key")
	}
}

func TestEnsureKeyPairIdempotent(t *testing.T) {
	ks := newCloudKeystore(t)

	first, err := ks.CloudPrivateKey()
	if err != nil {
		t.Fatalf("CloudPrivateKey: %v", err)
	}
	// A second call must not overwrite the existing key.
	if err := ks.ensureKeyPair(fileCloudPrivKey, ""); err != nil {
		t.Fatalf("ensureKeyPair (2nd): %v", err)
	}
	second, err := ks.CloudPrivateKey()
	if err != nil {
		t.Fatalf("CloudPrivateKey: %v", err)
	}
	if first.D.Cmp(second.D) != 0 { //nolint:staticcheck
		t.Error("ensureKeyPair overwrote an existing key (must be idempotent)")
	}
}

func TestRotateCloudKeyChangesMaterial(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("RotateCloudKey enforces POSIX 0600, not representable on Windows")
	}
	ks := newCloudKeystore(t)

	before, err := ks.CloudPrivateKey()
	if err != nil {
		t.Fatalf("CloudPrivateKey (before): %v", err)
	}
	if err := ks.RotateCloudKey(); err != nil {
		t.Fatalf("RotateCloudKey: %v", err)
	}
	after, err := ks.CloudPrivateKey()
	if err != nil {
		t.Fatalf("CloudPrivateKey (after): %v", err)
	}
	if before.D.Cmp(after.D) == 0 { //nolint:staticcheck
		t.Error("RotateCloudKey did not produce fresh key material")
	}
}
