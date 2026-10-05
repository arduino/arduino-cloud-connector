// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package pki

import (
	"bytes"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestCA(t *testing.T) *CA {
	t.Helper()
	ca, err := NewCAWithAKI(testAKI)
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	return ca
}

// issueForCleanCSR is the happy path: a CN-only CSR, as the daemon submits.
func issueForCleanCSR(t *testing.T, ca *CA) Issued {
	t.Helper()
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})
	issued, err := ca.IssueDevice(DeviceRequest{CSRPEM: csrPEM, DeviceID: testDeviceID})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	return issued
}

// Two properties without which no chain can ever build, both of them easy to
// get wrong silently.
//
// The subject must be byte-identical to the issuer the device certificate
// names -- it is encoded by crypto/x509 here and by Name.DER() there, on
// purpose, so a wrong DN cannot cancel out. And the SKI must equal the AKI the
// certificates carry, because OpenSSL cross-checks the two when it builds the
// chain: a mismatch verifies under Go and fails under the stack a real broker
// runs.
func TestCASubjectIsTheArduinoIssuerAndSKIIsTheAKI(t *testing.T) {
	ca := newTestCA(t)

	issuerDER, err := ArduinoIssuer.DER()
	if err != nil {
		t.Fatalf("encode issuer: %v", err)
	}
	if !bytes.Equal(ca.Certificate().RawSubject, issuerDER) {
		t.Errorf("CA subject is not the Arduino issuer\n  CA:       %x\n  Name.DER: %x",
			ca.Certificate().RawSubject, issuerDER)
	}
	if !bytes.Equal(ca.Certificate().SubjectKeyId, testAKI) {
		t.Errorf("CA subject key id = %x, want the AKI %x", ca.Certificate().SubjectKeyId, testAKI)
	}
	if !bytes.Equal(ca.AKI(), testAKI) {
		t.Errorf("AKI() = %x, want %x", ca.AKI(), testAKI)
	}
	if !ca.Certificate().IsCA {
		t.Error("the CA certificate is not marked as a CA")
	}
}

// The end-to-end property the harness depends on: what this CA issues must
// pass the checks a broker performs -- a strict parse, then chain building for
// client authentication. It is the same property the production spike pinned
// for the daemon's encoder; here it is pinned for ours.
func TestIssuedDeviceCertVerifiesForClientAuth(t *testing.T) {
	ca := newTestCA(t)
	issued := issueForCleanCSR(t, ca)

	leaf, err := x509.ParseCertificate(issued.CertDER)
	if err != nil {
		t.Fatalf("a strict parser rejected the issued certificate: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       ca.Pool(),
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		CurrentTime: time.Now(),
	}); err != nil {
		t.Fatalf("chain verify for client auth: %v", err)
	}
	if err := leaf.CheckSignatureFrom(ca.Certificate()); err != nil {
		t.Fatalf("the CA signature over the issued certificate does not verify: %v", err)
	}
	if leaf.Subject.CommonName != testDeviceID {
		t.Errorf("subject common name = %q, want the device id", leaf.Subject.CommonName)
	}
}

// The response body is the contract the daemon parses, and it rejects any
// field whose width is wrong. Pinning the shape here means a change to the
// format fails in this package rather than as an opaque provisioning error.
func TestIssuedResponseBodyMatchesTheAPIContract(t *testing.T) {
	ca := newTestCA(t)
	issued := issueForCleanCSR(t, ca)

	fields := strings.Split(issued.ResponseBody, "|")
	if len(fields) != 6 {
		t.Fatalf("response has %d fields, want 6: %q", len(fields), issued.ResponseBody)
	}
	widths := []struct {
		name  string
		field string
		want  int
	}{
		{"device_id", fields[0], 36},
		{"authority_key_identifier", fields[1], 2 * deviceAKILen},
		{"serial", fields[3], 2 * deviceSerialLen},
		{"signature_x", fields[4], 64},
		{"signature_y", fields[5], 64},
	}
	for _, w := range widths {
		if len(w.field) != w.want {
			t.Errorf("%s is %d characters, want %d: %q", w.name, len(w.field), w.want, w.field)
		}
	}
	if fields[0] != testDeviceID {
		t.Errorf("device_id = %q, want %q", fields[0], testDeviceID)
	}
	if got := fields[1]; got != hex.EncodeToString(testAKI) {
		t.Errorf("authority_key_identifier = %q, want the CA AKI %x", got, testAKI)
	}
	// The daemon reads year, month, day and hour off fixed offsets, so the
	// layout matters more than the exact timestamp.
	if _, err := time.Parse("2006-01-02T15:04:05Z", fields[2]); err != nil {
		t.Errorf("not_before %q is not the expected layout: %v", fields[2], err)
	}
	if !strings.HasSuffix(fields[2], "00:00Z") {
		t.Errorf("not_before %q should carry zeroed minutes and seconds", fields[2])
	}
}

// bad_signature is the fault worth having: the response is well-formed, so the
// daemon accepts it and stores the certificate, and the only symptom is the
// broker refusing the connection later.
func TestIssueDeviceBadSignatureProducesACertTheCARejects(t *testing.T) {
	ca := newTestCA(t)
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	issued, err := ca.IssueDeviceBadSignature(DeviceRequest{CSRPEM: csrPEM, DeviceID: testDeviceID})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if len(strings.Split(issued.ResponseBody, "|")) != 6 {
		t.Error("the bad-signature response must still be well-formed, or it tests the wrong thing")
	}
	leaf, err := x509.ParseCertificate(issued.CertDER)
	if err != nil {
		t.Fatalf("the bad-signature certificate must still parse: %v", err)
	}
	if err := leaf.CheckSignatureFrom(ca.Certificate()); err == nil {
		t.Fatal("the rogue-signed certificate verified against the CA")
	}
}

// Issuance reflects the CSR subject, which is what makes a re-polluted CSR
// break the daemon's CN-only reconstruction instead of passing quietly.
func TestIssuedSubjectReflectsAPollutedCSR(t *testing.T) {
	ca := newTestCA(t)
	csrPEM, _ := newTestCSR(t, pkix.Name{Country: []string{"IT"}, CommonName: testUHWID})

	issued, err := ca.IssueDevice(DeviceRequest{CSRPEM: csrPEM, DeviceID: testDeviceID})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if issued.Subject.Country != "IT" {
		t.Errorf("issued subject = %s, want the CSR country reflected", issued.Subject)
	}
	leaf, err := x509.ParseCertificate(issued.CertDER)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(leaf.Subject.Country) != 1 || leaf.Subject.Country[0] != "IT" {
		t.Errorf("certificate subject = %v, want C=IT reflected in", leaf.Subject)
	}
}

func TestIssueDeviceRejectsADeviceIDTheDaemonWouldRefuse(t *testing.T) {
	ca := newTestCA(t)
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	for _, id := range []string{"", "short", testDeviceID + "x"} {
		if _, err := ca.IssueDevice(DeviceRequest{CSRPEM: csrPEM, DeviceID: id}); err == nil {
			t.Errorf("device id %q was accepted; the daemon requires exactly 36 characters", id)
		}
	}
}

// The serial and notBefore default when unset, and a caller-supplied serial is
// honoured -- scenarios that assert on a specific certificate need the second.
func TestIssueDeviceDefaultsSerialAndNotBefore(t *testing.T) {
	ca := newTestCA(t)
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	auto, err := ca.IssueDevice(DeviceRequest{CSRPEM: csrPEM, DeviceID: testDeviceID})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if len(auto.Serial) != deviceSerialLen {
		t.Errorf("default serial is %d bytes, want %d", len(auto.Serial), deviceSerialLen)
	}
	if auto.NotBefore.IsZero() || auto.NotBefore.After(time.Now()) {
		t.Errorf("default notBefore = %s, want a time already in the past", auto.NotBefore)
	}
	if got, want := auto.NotAfter.Year(), auto.NotBefore.Year()+DeviceExpireYears; got != want {
		t.Errorf("notAfter year = %d, want %d", got, want)
	}

	fixed, err := ca.IssueDevice(DeviceRequest{
		CSRPEM: csrPEM, DeviceID: testDeviceID, Serial: testSerial, NotBefore: testNotBefore,
	})
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if !bytes.Equal(fixed.Serial, testSerial) {
		t.Errorf("serial = %x, want the requested %x", fixed.Serial, testSerial)
	}
	if want := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC); !fixed.NotBefore.Equal(want) {
		t.Errorf("notBefore = %s, want %s", fixed.NotBefore, want)
	}
}

// The default notBefore steps off 29 February, because that date plus 31 years
// is not encodable (see DeviceCert.Validity). Without this the suite would
// fail for one day every four years, which is the kind of failure nobody
// believes when it happens.
func TestDefaultNotBeforeStepsOffTheLeapDay(t *testing.T) {
	leap := time.Date(2028, 2, 29, 14, 12, 0, 0, time.UTC)
	got := defaultNotBefore(leap)
	if want := time.Date(2028, 2, 28, 14, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("defaultNotBefore(29 Feb) = %s, want %s", got, want)
	}

	ordinary := time.Date(2026, 9, 14, 14, 12, 33, 0, time.UTC)
	if want := time.Date(2026, 9, 14, 14, 0, 0, 0, time.UTC); !defaultNotBefore(ordinary).Equal(want) {
		t.Errorf("defaultNotBefore trimmed more than the minutes: %s", defaultNotBefore(ordinary))
	}
}

// The broker's server certificate is an ordinary one, and the daemon verifies
// it with its own TLS stack against the CA file the harness writes.
func TestServerCertChainsToTheCAForServerAuth(t *testing.T) {
	ca := newTestCA(t)

	tlsCert, err := ca.ServerCert("localhost", "127.0.0.1")
	if err != nil {
		t.Fatalf("server cert: %v", err)
	}
	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		t.Fatalf("parse server cert: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:       ca.Pool(),
		DNSName:     "localhost",
		KeyUsages:   []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		CurrentTime: time.Now(),
	}); err != nil {
		t.Fatalf("chain verify for server auth: %v", err)
	}
	if len(leaf.IPAddresses) != 1 || leaf.IPAddresses[0].String() != "127.0.0.1" {
		t.Errorf("server cert IP addresses = %v, want 127.0.0.1", leaf.IPAddresses)
	}
}

// The daemon reads the CA from a file given in the environment, so the harness
// has to be able to put it there.
func TestWriteCertPEMProducesALoadableRoot(t *testing.T) {
	ca := newTestCA(t)
	path := filepath.Join(t.TempDir(), "ca.pem")

	if err := ca.WriteCertPEM(path); err != nil {
		t.Fatalf("write: %v", err)
	}
	pemBytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pemBytes) {
		t.Fatal("the written PEM was not accepted as a root certificate")
	}
}

func TestNewCARejectsAWrongSizedAKI(t *testing.T) {
	if _, err := NewCAWithAKI(testAKI[:10]); err == nil {
		t.Fatal("a 10-byte authority key identifier was accepted, want 20")
	}
}
