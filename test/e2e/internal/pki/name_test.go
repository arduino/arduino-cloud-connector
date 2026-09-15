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
	"encoding/asn1"
	"encoding/hex"
	"math/big"
	"testing"
	"time"
)

// The independent encoder is only worth having if something outside it says
// which encoding is right. crypto/x509 is that witness here: it encodes the
// equivalent pkix.Name and the two DERs must be identical, byte for byte.
//
// This is also the property the whole chain rests on. The device certificate
// names its issuer with Name.DER(), the harness CA names itself through
// crypto/x509, and a verifier compares those two byte strings directly -- so a
// divergence here would surface as an unexplained "unknown certificate
// authority" at the broker, nowhere near this package.
func TestArduinoIssuerMatchesTheStandardEncoding(t *testing.T) {
	got, err := ArduinoIssuer.DER()
	if err != nil {
		t.Fatalf("encode issuer: %v", err)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      ArduinoIssuer.PKIX(),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	if want := parsed.RawSubject; string(got) != string(want) {
		t.Errorf("issuer DN differs from the standard encoding of %s\n  crypto/x509: %x\n  Name.DER:    %x",
			ArduinoIssuer, want, got)
	}
}

// Golden bytes, computed by hand from the DER rules rather than from either
// encoder. They pin two things the standard library cannot: that values are
// PrintableString (tag 0x13, not UTF8String), and the attribute ORDER, which
// is part of the signed bytes and differs from pkix.Name once ST or L appear.
func TestNameDERGolden(t *testing.T) {
	tests := []struct {
		name string
		in   Name
		want string
	}{
		{
			// SEQUENCE { SET { SEQUENCE { OID 2.5.4.3, PrintableString "abc" } } }
			name: "common name only",
			in:   Name{CommonName: "abc"},
			want: "300e310c300a06035504031303616263",
		},
		{
			// Country first, common name last: firmware order.
			name: "country before common name",
			in:   Name{Country: "IT", CommonName: "x"},
			want: "3019310b3009060355040613024954310a30080603550403130178",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			der, err := tc.in.DER()
			if err != nil {
				t.Fatalf("encode %s: %v", tc.in, err)
			}
			if got := hex.EncodeToString(der); got != tc.want {
				t.Errorf("DER mismatch for %s\n  got:  %s\n  want: %s", tc.in, got, tc.want)
			}
		})
	}
}

// The attribute order is C, ST, L, O, OU, CN -- the firmware's, which is NOT
// pkix.Name's once a province or locality is present. Decoding the DER back
// through encoding/asn1 (a third route, neither of the two encoders) is what
// checks the order rather than trusting a hand-written golden.
func TestNameDERAttributeOrderIsFirmwareOrder(t *testing.T) {
	full := Name{
		Country:            "IT",
		State:              "MI",
		Locality:           "Monza",
		Organization:       "Arduino",
		OrganizationalUnit: "IT",
		CommonName:         "x",
	}
	der, err := full.DER()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var seq pkix.RDNSequence
	rest, err := asn1.Unmarshal(der, &seq)
	if err != nil {
		t.Fatalf("the encoded name does not decode as an RDNSequence: %v", err)
	}
	if len(rest) != 0 {
		t.Errorf("%d trailing bytes after the name", len(rest))
	}

	want := []asn1.ObjectIdentifier{oidCountry, oidState, oidLocality, oidOrganization, oidOrganizationalUnit, oidCommonName}
	if len(seq) != len(want) {
		t.Fatalf("got %d attributes, want %d", len(seq), len(want))
	}
	for i, rdn := range seq {
		if len(rdn) != 1 {
			t.Fatalf("attribute %d holds %d values, want exactly 1", i, len(rdn))
		}
		if !rdn[0].Type.Equal(want[i]) {
			t.Errorf("attribute %d is %v, want %v", i, rdn[0].Type, want[i])
		}
	}
}

// An empty attribute is skipped entirely, not emitted as a zero-length
// PrintableString: the firmware omits it, so the signed bytes have no room for
// one.
func TestNameDEROmitsEmptyAttributes(t *testing.T) {
	withEmpties := Name{Country: "", State: "", Organization: "Arduino", CommonName: "x"}
	onlyValues := Name{Organization: "Arduino", CommonName: "x"}

	a, err := withEmpties.DER()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	b, err := onlyValues.DER()
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if string(a) != string(b) {
		t.Errorf("empty attributes changed the encoding\n  with:    %x\n  without: %x", a, b)
	}
}

// A value PrintableString cannot represent is refused rather than encoded.
// Emitting it would produce DER that is invalid but parses in lenient readers,
// so the complaint would arrive from a verifier far away instead of from here.
func TestNameDERRejectsNonPrintableValues(t *testing.T) {
	if _, err := (Name{CommonName: "Città"}).DER(); err == nil {
		t.Fatal("encoding a non-PrintableString value succeeded, want an error")
	}
	// Hyphens and digits must keep working: every device_id is a UUID.
	if _, err := (Name{CommonName: "9f1c2d3e-4567-89ab-cdef-0123456789ab"}).DER(); err != nil {
		t.Fatalf("a UUID common name must encode, got: %v", err)
	}
}

func TestNameStringUsesEncodingOrder(t *testing.T) {
	n := Name{Country: "IT", Organization: "Arduino", CommonName: "x"}
	if got, want := n.String(), "C=IT,O=Arduino,CN=x"; got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
	if !(Name{}).IsEmpty() {
		t.Error("the zero Name must report itself empty")
	}
}
