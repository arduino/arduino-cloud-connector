// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioningapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/asn1"
	"encoding/hex"
	"encoding/pem"
	"math/big"
	"strings"
	"testing"
)

// makeCSR builds a real CSR for a fresh EC P-256 key and returns the PEM plus
// the key (so the test can derive the expected public point).
func makeCSR(t *testing.T, uhwid string) (csrPEM string, key *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: uhwid, Country: []string{"IT"}},
	}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})), key
}

// signTBS signs the reconstructed TBSCertificate with the given CA key and
// returns the raw r||s (64 bytes), as the provisioning-api CA would.
func signTBS(t *testing.T, caKey *ecdsa.PrivateKey, p certParams) []byte {
	t.Helper()
	tbs := buildTBS(p)
	digest := sha256.Sum256(tbs)
	r, s, err := ecdsa.Sign(rand.Reader, caKey, digest[:])
	if err != nil {
		t.Fatalf("sign TBS: %v", err)
	}
	out := make([]byte, 64)
	r.FillBytes(out[0:32])
	s.FillBytes(out[32:64])
	return out
}

func TestReconstructDeviceCert(t *testing.T) {
	const deviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
	csrPEM, devKey := makeCSR(t, "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b")

	// Stand-in CA.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}

	akiHex := "1122334455667788990011223344556677889900"
	serialHex := "0102030405060708090a0b0c0d0e0f10"
	notBefore := "2024-01-15T10:30:00Z"

	// Compute the signature over the exact TBS the daemon reconstructs.
	pub, err := publicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("pub from CSR: %v", err)
	}
	aki, _ := hex.DecodeString(akiHex)
	serial, _ := hex.DecodeString(serialHex)
	sig := signTBS(t, caKey, certParams{
		publicKey:   pub,
		deviceID:    deviceID,
		serial:      serial,
		authKeyID:   aki,
		issueYear:   2024,
		issueMonth:  1,
		issueDay:    15,
		issueHour:   10,
		expireYears: certExpireYears,
	})

	sigX := hex.EncodeToString(sig[0:32])
	sigY := hex.EncodeToString(sig[32:64])
	body := strings.Join([]string{deviceID, akiHex, notBefore, serialHex, sigX, sigY}, "|")

	gotID, certPEM, err := reconstructDeviceCert(csrPEM, body)
	if err != nil {
		t.Fatalf("reconstructDeviceCert: %v", err)
	}
	if gotID != deviceID {
		t.Errorf("device id: got %q want %q", gotID, deviceID)
	}

	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatalf("bad PEM output")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificate (DER not well-formed): %v", err)
	}

	// Structural assertions.
	if cert.Subject.CommonName != deviceID {
		t.Errorf("subject CN: got %q want %q", cert.Subject.CommonName, deviceID)
	}
	if cert.Issuer.CommonName != "Arduino" || cert.Issuer.Organization[0] != "Arduino LLC US" {
		t.Errorf("issuer mismatch: %+v", cert.Issuer)
	}
	if cert.SerialNumber.Cmp(new(big.Int).SetBytes(serial)) != 0 {
		t.Errorf("serial: got %x want %x", cert.SerialNumber.Bytes(), serial)
	}
	if cert.NotBefore.Year() != 2024 || cert.NotBefore.Month() != 1 || cert.NotBefore.Day() != 15 {
		t.Errorf("notBefore: %v", cert.NotBefore)
	}
	if cert.NotAfter.Year() != 2024+certExpireYears {
		t.Errorf("notAfter year: got %d want %d", cert.NotAfter.Year(), 2024+certExpireYears)
	}

	// The reconstructed cert public key must match the CSR key.
	certPub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok || certPub.X.Cmp(devKey.X) != 0 || certPub.Y.Cmp(devKey.Y) != 0 { //nolint:staticcheck
		t.Errorf("certificate public key does not match CSR key")
	}

	// The CA signature must verify against the (re)hashed TBS — proves the
	// reconstructed TBS bytes are exactly what was signed.
	if err := cert.CheckSignatureFrom(caCertFor(t, caKey)); err != nil {
		// CheckSignatureFrom needs a parent cert; fall back to manual verify.
		verifySignatureManually(t, cert, caKey)
	}
}

// caCertFor builds a minimal self-signed CA certificate carrying caKey's public
// key, so cert.CheckSignatureFrom can validate the child signature.
func caCertFor(t *testing.T, caKey *ecdsa.PrivateKey) *x509.Certificate {
	t.Helper()
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Arduino"},
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	c, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return c
}

// verifySignatureManually re-hashes the TBS and verifies the ECDSA signature
// directly, independent of issuer-name matching.
func verifySignatureManually(t *testing.T, cert *x509.Certificate, caKey *ecdsa.PrivateKey) {
	t.Helper()
	digest := sha256.Sum256(cert.RawTBSCertificate)
	var sig struct{ R, S *big.Int }
	if _, err := asn1.Unmarshal(cert.Signature, &sig); err != nil {
		t.Fatalf("unmarshal signature: %v", err)
	}
	if !ecdsa.Verify(&caKey.PublicKey, digest[:], sig.R, sig.S) {
		t.Fatalf("CA signature does not verify over reconstructed TBS")
	}
}

func TestReconstructDeviceCertBadFormat(t *testing.T) {
	csrPEM, _ := makeCSR(t, "deadbeef")
	if _, _, err := reconstructDeviceCert(csrPEM, "too|few|fields"); err == nil {
		t.Errorf("expected error for malformed response")
	}
}
