// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Strict-verification tests for the reconstructed device certificate.
//
// compressed_cert_test.go checks that reconstructDeviceCert rebuilds the DER
// the CA signed, by verifying the signature by hand. That proves the bytes are
// right; it does not prove anything about what a real peer does with them.
// These tests close that gap: they take the reconstructed certificate all the
// way through the checks a broker actually performs — a strict X.509 parse,
// chain building for client authentication, and a real mTLS handshake with
// tls.RequireAndVerifyClientCert.
//
// The property is not obvious and is worth pinning, because the certificate is
// deliberately firmware-shaped and looks under-specified by modern standards:
// a fixed 31-year validity (so notAfter is a GeneralizedTime, not a UTCTime),
// a CN-only subject, and NO basicConstraints, keyUsage or extendedKeyUsage —
// only the authority key identifier. A verifier that rejected any of that
// would produce an "unknown certificate authority" at the broker, far from
// this package, which is the failure mode these tests exist to prevent.
//
// # What these tests do NOT cover
//
// The verifier here is external (crypto/x509, and OpenSSL via the manual
// cross-check below), but the ISSUER is this package's own buildTBS. So this
// pins "whatever we reconstruct is acceptable to a strict verifier"; it cannot
// detect a divergence between our reconstruction and what the real Arduino CA
// signs.
//
// One such divergence has already cost real debugging time and is NOT caught
// here: the real cloud reflects the CSR's subject RDNs into the issued
// certificate, so a CSR carrying anything besides the CN (a stray Country=IT
// was the actual bug) yields a real certificate that our CN-only
// reconstruction cannot match, and the broker rejects it. See the warning on
// keystore.GenerateCSR. Catching that class requires a fake CA that reflects
// CSR RDNs the way the cloud does — that belongs to the E2E harness, not here.
//
// # Manual cross-check with OpenSSL
//
// Set CERT_DUMP_DIR to have the test write ca.pem, device.pem, device.key,
// server.pem and server.key, then verify with a completely different X.509
// implementation (the one Mosquitto uses):
//
//	CERT_DUMP_DIR=/tmp/certs go test ./internal/provisioning-api/ -run MTLS
//	openssl verify -x509_strict -purpose sslclient -CAfile ca.pem device.pem
//
// For a full handshake, note two traps: openssl s_server exits immediately if
// stdin is not a terminal, so use -www (it does not read stdin); and a
// readiness probe that opens a TCP connection consumes one accept, so do not
// pair it with -naccept 1.
//
//	openssl s_server -accept 8883 -cert server.pem -key server.key \
//	    -CAfile ca.pem -Verify 1 -www &
//	echo Q | openssl s_client -connect 127.0.0.1:8883 \
//	    -cert device.pem -key device.key -CAfile ca.pem
//
// Last run of that cross-check: OpenSSL 3.2.1 accepted the chain and completed
// the handshake (ECDHE-ECDSA-AES256-GCM-SHA384); a client presenting no
// certificate was refused with alert 40, confirming the server was enforcing.
package provisioningapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// issuerRawDER returns the DER of the fixed Arduino issuer Name exactly as
// buildTBS emits it: SEQUENCE { RDNSequence }.
func issuerRawDER() []byte {
	var out []byte
	out = appendSequenceHeader(out, certIssuer.length())
	out = appendName(out, certIssuer)
	return out
}

// The issuer Name is hand-encoded (appendName: PrintableString, field order
// C/ST/L/O/OU/CN). A chain can only be built if it is byte-identical to the
// CA's subject, so this pins our encoding against the standard one. It also
// means a test or fake CA can be built with a plain pkix.Name and needs no
// RawSubject.
func TestIssuerDNMatchesStandardEncoding(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Country:            []string{"US"},
			Organization:       []string{"Arduino LLC US"},
			OrganizationalUnit: []string{"IT"},
			CommonName:         "Arduino",
		},
		NotBefore: time.Now().Add(-time.Hour),
		NotAfter:  time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	if got, want := parsed.RawSubject, issuerRawDER(); string(got) != string(want) {
		t.Errorf("hand-encoded issuer DN differs from the standard encoding of\n"+
			"C=US, O=Arduino LLC US, OU=IT, CN=Arduino\n  standard:    %x\n  appendName:  %x",
			got, want)
	}
}

// newTestCA builds a CA whose subject DER is byte-identical to the issuer the
// daemon reconstructs, and whose SubjectKeyId is the AKI the device
// certificate will carry (OpenSSL cross-checks AKI against the CA's SKI).
func newTestCA(t *testing.T, aki []byte) (*ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		RawSubject:            issuerRawDER(),
		SubjectKeyId:          aki,
		NotBefore:             time.Now().Add(-24 * time.Hour),
		NotAfter:              time.Now().Add(20 * 365 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	caCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return key, caCert
}

// issueDeviceCert plays the part of the Provisioning API: it signs the TBS the
// daemon is going to reconstruct and returns the pipe-delimited response body
// the real API would send.
func issueDeviceCert(t *testing.T, caKey *ecdsa.PrivateKey, csrPEM, deviceID string, aki, serial []byte, notBefore time.Time) string {
	t.Helper()
	pub, err := publicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("pub from CSR: %v", err)
	}
	digest := sha256.Sum256(buildTBS(certParams{
		publicKey:   pub,
		deviceID:    deviceID,
		serial:      serial,
		authKeyID:   aki,
		issueYear:   notBefore.Year(),
		issueMonth:  int(notBefore.Month()),
		issueDay:    notBefore.Day(),
		issueHour:   notBefore.Hour(),
		expireYears: certExpireYears,
	}))
	r, s, err := ecdsa.Sign(rand.Reader, caKey, digest[:])
	if err != nil {
		t.Fatalf("sign TBS: %v", err)
	}
	sig := make([]byte, 64)
	r.FillBytes(sig[0:32])
	s.FillBytes(sig[32:64])

	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		deviceID,
		hex.EncodeToString(aki),
		notBefore.UTC().Format("2006-01-02T15:04:05Z"),
		hex.EncodeToString(serial),
		hex.EncodeToString(sig[0:32]),
		hex.EncodeToString(sig[32:64]),
	)
}

func TestReconstructedCertPassesStrictMTLSVerification(t *testing.T) {
	const (
		deviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
		uhwid    = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"
	)
	aki, _ := hex.DecodeString("1122334455667788990011223344556677889900")
	serial, _ := hex.DecodeString("0102030405060708090a0b0c0d0e0f10")
	notBefore := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	caKey, caCert := newTestCA(t, aki)

	// The daemon's own path: CSR out, pipe-delimited response in.
	devKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("device key: %v", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{
		Subject: pkix.Name{CommonName: uhwid}, // CN-only, as keystore.GenerateCSR produces
	}, devKey)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	csrPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}))

	body := issueDeviceCert(t, caKey, csrPEM, deviceID, aki, serial, notBefore)

	gotID, certPEM, err := reconstructDeviceCert(csrPEM, body)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if gotID != deviceID {
		t.Fatalf("device id = %q, want %q", gotID, deviceID)
	}

	// A strict parser must accept the DER, including the post-2049 notAfter
	// (GeneralizedTime) the fixed 31-year validity forces.
	block, _ := pem.Decode([]byte(certPEM))
	leaf, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("strict parse: %v", err)
	}
	if leaf.NotAfter.Year() != notBefore.Year()+certExpireYears {
		t.Errorf("notAfter year = %d, want %d", leaf.NotAfter.Year(), notBefore.Year()+certExpireYears)
	}

	// Chain building for client auth. A leaf with no extendedKeyUsage is valid
	// for any purpose, which is what makes the extension-less certificate work.
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       pool,
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		CurrentTime: time.Now(),
	}); err != nil {
		t.Fatalf("chain verify for client auth: %v", err)
	}

	// The real thing: a TLS server demanding and verifying a client certificate.
	srvKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("server key: %v", err)
	}
	srvDER, err := x509.CreateCertificate(rand.Reader, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "broker"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}, caCert, &srvKey.PublicKey, caKey)
	if err != nil {
		t.Fatalf("server cert: %v", err)
	}

	ln, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{srvDER}, PrivateKey: srvKey}},
		ClientCAs:    pool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close() //nolint:errcheck

	srvErr := make(chan error, 1)
	go func() {
		conn, aerr := ln.Accept()
		if aerr != nil {
			srvErr <- aerr
			return
		}
		defer conn.Close() //nolint:errcheck
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		srvErr <- conn.(*tls.Conn).Handshake()
	}()

	cconn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{block.Bytes}, PrivateKey: devKey}},
		RootCAs:      pool,
		ServerName:   "localhost",
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("client handshake: %v (server side: %v)", err, <-srvErr)
	}
	defer cconn.Close() //nolint:errcheck

	if err := <-srvErr; err != nil {
		t.Fatalf("server rejected the reconstructed client certificate: %v", err)
	}

	dumpPEMs(t, caCert.Raw, certPEM, devKey, srvDER, srvKey)
}

// dumpPEMs writes the material to CERT_DUMP_DIR, when set, so the same
// certificate can be checked against OpenSSL by hand (see the package comment).
// A no-op otherwise.
func dumpPEMs(t *testing.T, caDER []byte, certPEM string, devKey *ecdsa.PrivateKey, srvDER []byte, srvKey *ecdsa.PrivateKey) {
	t.Helper()
	dir := os.Getenv("CERT_DUMP_DIR")
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", dir, err)
	}
	write := func(name string, data []byte) {
		if err := os.WriteFile(filepath.Join(dir, name), data, 0o600); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
	keyPEM := func(k *ecdsa.PrivateKey) []byte {
		der, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			t.Fatalf("marshal key: %v", err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	}

	write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}))
	write("device.pem", []byte(certPEM))
	write("device.key", keyPEM(devKey))
	write("server.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srvDER}))
	write("server.key", keyPEM(srvKey))
	t.Logf("wrote ca/device/server PEMs to %s", dir)
}
