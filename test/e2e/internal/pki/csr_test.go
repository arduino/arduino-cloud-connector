// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package pki

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"strings"
	"testing"
)

const (
	// testUHWID is what the daemon puts in the CSR common name.
	testUHWID = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"
	// testDeviceID is the identity the API assigns; 36 characters, as the
	// daemon requires.
	testDeviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
)

// newTestCSR builds a PEM CSR with the given subject, the way
// keystore.GenerateCSR does (an EC P-256 key, no extensions).
func newTestCSR(t *testing.T, subject pkix.Name) (string, *ecdsa.PrivateKey) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("device key: %v", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: subject}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})), key
}

// The happy path: a CN-only CSR reflects to exactly CN=<device_id>, which is
// the subject the daemon rebuilds on its own. Any difference here and the
// suite would be red on every run.
func TestReflectCSRSubjectOnACleanCSRIsCommonNameOnly(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	subject, err := ReflectCSRSubject(csrPEM, testDeviceID)
	if err != nil {
		t.Fatalf("reflect: %v", err)
	}
	if want := (Name{CommonName: testDeviceID}); subject != want {
		t.Errorf("subject = %+v, want %+v", subject, want)
	}
	if got, want := subject.String(), "CN="+testDeviceID; got != want {
		t.Errorf("subject string = %q, want %q", got, want)
	}
}

// THE regression guard, and the reason the fake CA reflects at all.
//
// A stray Country=IT in the CSR once made real certificates diverge from the
// daemon's CN-only reconstruction, and the symptom was "unknown certificate
// authority" at the broker -- three components away from the cause. A harness
// CA that signed a CN-only subject would reproduce none of that: the polluted
// attribute would never reach the signed bytes and the scenario would pass.
//
// Reflecting means the polluted attribute lands in the signature, so the
// daemon's rebuild no longer matches and the scenario fails where the bug is.
func TestReflectCSRSubjectCarriesTheStrayCountryThatBrokeProvisioning(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{
		Country:    []string{"IT"},
		CommonName: testUHWID,
	})

	subject, err := ReflectCSRSubject(csrPEM, testDeviceID)
	if err != nil {
		t.Fatalf("reflect: %v", err)
	}
	if subject.Country != "IT" {
		t.Errorf("the CSR country was not reflected: subject = %+v", subject)
	}
	if got, want := subject.String(), "C=IT,CN="+testDeviceID; got != want {
		t.Errorf("subject string = %q, want %q", got, want)
	}
	// And the reflected subject must differ from the CN-only one the daemon
	// rebuilds -- that difference IS the detection.
	clean, err := (Name{CommonName: testDeviceID}).DER()
	if err != nil {
		t.Fatalf("encode clean subject: %v", err)
	}
	polluted, err := subject.DER()
	if err != nil {
		t.Fatalf("encode polluted subject: %v", err)
	}
	if string(clean) == string(polluted) {
		t.Error("the polluted subject encodes identically to the CN-only one, so the bug class stays invisible")
	}
}

// Every attribute type the layout can carry is reflected, not just the country
// that happened to cause the incident.
func TestReflectCSRSubjectCarriesEveryAttribute(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{
		Country:            []string{"IT"},
		Province:           []string{"MI"},
		Locality:           []string{"Monza"},
		Organization:       []string{"Arduino"},
		OrganizationalUnit: []string{"R+D"},
		CommonName:         testUHWID,
	})

	subject, err := ReflectCSRSubject(csrPEM, testDeviceID)
	if err != nil {
		t.Fatalf("reflect: %v", err)
	}
	want := Name{
		Country:            "IT",
		State:              "MI",
		Locality:           "Monza",
		Organization:       "Arduino",
		OrganizationalUnit: "R+D",
		CommonName:         testDeviceID,
	}
	if subject != want {
		t.Errorf("subject = %+v, want %+v", subject, want)
	}
}

// SubjectFromCSR reports the subject AS SUBMITTED -- common name included --
// because that is what goes on the timeline; only the issued certificate
// swaps in the device id.
func TestSubjectFromCSRKeepsTheSubmittedCommonName(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	subject, err := SubjectFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("subject: %v", err)
	}
	if subject.CommonName != testUHWID {
		t.Errorf("common name = %q, want the submitted uhwid %q", subject.CommonName, testUHWID)
	}
}

func TestReflectCSRSubjectRejectsAnEmptyDeviceID(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	if _, err := ReflectCSRSubject(csrPEM, ""); err == nil {
		t.Fatal("reflecting with no device id succeeded, want an error")
	}
}

// The real API will not sign a request it cannot verify, so neither will the
// fake: a daemon that signed the CSR with a stale key after a rotation has to
// fail here, not receive a certificate for a key it no longer holds.
func TestParseCSRRejectsABrokenSignature(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})
	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		t.Fatal("test CSR is not PEM")
	}
	// Flip a bit in the last byte, which is inside the signature.
	tampered := append([]byte(nil), block.Bytes...)
	tampered[len(tampered)-1] ^= 0x01
	tamperedPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: tampered}))

	_, err := ParseCSR(tamperedPEM)
	if err == nil {
		t.Fatal("a CSR with a broken signature was accepted")
	}
	if !strings.Contains(err.Error(), "signature") {
		t.Errorf("error should name the signature, got: %v", err)
	}
}

func TestParseCSRRejectsGarbage(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"not PEM", "hello"},
		{"wrong PEM type", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte{0x30, 0x00}}))},
		{"PEM but not a CSR", string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: []byte{0x30, 0x00}}))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := ParseCSR(tc.in); err == nil {
				t.Fatal("accepted, want an error")
			}
		})
	}
}

// The 64 bytes must be the point the CSR carried, with the uncompressed-point
// marker stripped. They are read out of the raw SubjectPublicKeyInfo, so the
// check compares them against a third route to the same value.
func TestPublicKeyFromCSRReturnsTheUncompressedPoint(t *testing.T) {
	csrPEM, key := newTestCSR(t, pkix.Name{CommonName: testUHWID})

	raw, err := PublicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("public key: %v", err)
	}
	if len(raw) != devicePubKeyLen {
		t.Fatalf("public key is %d bytes, want %d", len(raw), devicePubKeyLen)
	}
	// PublicKey.Bytes is a third route to the same point: 0x04 || X || Y.
	uncompressed, err := key.PublicKey.Bytes()
	if err != nil {
		t.Fatalf("encode the key: %v", err)
	}
	if want := uncompressed[1:]; string(raw) != string(want) {
		t.Errorf("raw point mismatch\n  got:  %x\n  want: %x", raw, want)
	}
}

func TestPublicKeyFromCSRRejectsNonP256Keys(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P384(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: testUHWID}}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	csrPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))

	if _, err := PublicKeyFromCSR(csrPEM); err == nil {
		t.Fatal("a P-384 CSR was accepted, but the certificate layout has room for P-256 only")
	}
}
