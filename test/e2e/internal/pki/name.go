// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package pki is the harness's stand-in for the Arduino CA: it issues device
// certificates the way the Provisioning API does, and it encodes their DER
// INDEPENDENTLY of the daemon.
//
// # Why the encoder is reimplemented here
//
// The device certificate is not an internal detail: it is a contract with two
// external parties. The Provisioning API returns only the certificate's
// variable parts (device_id, AKI, notBefore, serial, signature) and the daemon
// rebuilds the rest locally, so the CA must have signed EXACTLY the byte
// layout the daemon reproduces — the layout the Arduino firmware's
// ECP256Certificate defines. If the harness signed the TBS with the daemon's
// own encoder, the suite would only prove that the code agrees with itself: an
// error present on both sides cancels out, the test stays green, and the real
// broker still rejects the certificate with "unknown certificate authority".
//
// So the encoding here is deliberately written with a different technique from
// the daemon's: cryptobyte's nested length-prefixed builders instead of
// hand-computed `append(buf, 0x30, byte(len))`. A copy-paste duplicate would
// catch future drift but not an error already present in both.
//
// # Where the independence is anchored
//
// Two independent encoders that agree prove nothing unless something outside
// both says which one is right. Two anchors do that here:
//
//   - the standard library. ArduinoIssuer.DER() is pinned against the DN that
//     crypto/x509 emits for the equivalent pkix.Name (name_test.go), and every
//     issued certificate is parsed back by crypto/x509 and verified as a chain
//     (ca_test.go).
//   - the daemon itself, but only in crosscheck_test.go: the fake API issues,
//     the REAL provisioning client reconstructs, and the two DERs must be
//     identical. That is the one file allowed to import production code (see
//     .golangci.yml), and it is what reports WHICH side changed instead of a
//     bare "invalid signature".
//
// The CA certificate's own subject is therefore built from a pkix.Name (the
// standard encoder) while the issuer field inside the device certificate comes
// from Name.DER() here. That asymmetry is on purpose: encoding both ends with
// the same code would let a wrong DN cancel out and still build a chain.
package pki

import (
	"crypto/x509/pkix"
	"encoding/asn1"
	"fmt"
	"strings"

	"golang.org/x/crypto/cryptobyte"
	cbasn1 "golang.org/x/crypto/cryptobyte/asn1"
)

// Attribute-type OIDs under the X.500 arc 2.5.4. The values are written as
// arcs and let AddASN1ObjectIdentifier do the base-128 encoding, rather than
// pasting the encoded bytes (0x55, 0x04, …) the firmware and the daemon use.
var (
	oidCountry            = asn1.ObjectIdentifier{2, 5, 4, 6}
	oidState              = asn1.ObjectIdentifier{2, 5, 4, 8}
	oidLocality           = asn1.ObjectIdentifier{2, 5, 4, 7}
	oidOrganization       = asn1.ObjectIdentifier{2, 5, 4, 10}
	oidOrganizationalUnit = asn1.ObjectIdentifier{2, 5, 4, 11}
	oidCommonName         = asn1.ObjectIdentifier{2, 5, 4, 3}
)

// Name is an X.501 distinguished name restricted to the attributes the
// firmware's certificate layout can carry.
//
// It is not pkix.Name: the field ORDER is part of the signed bytes, and the
// firmware emits C, ST, L, O, OU, CN — which is not the order
// pkix.Name.ToRDNSequence() uses once ST or L are present. Modelling the name
// with its own type keeps that order explicit and local.
type Name struct {
	Country            string
	State              string
	Locality           string
	Organization       string
	OrganizationalUnit string
	CommonName         string
}

// ArduinoIssuer is the fixed issuer every Arduino device certificate carries.
// The daemon hardcodes the same value (certIssuer in compressed_cert.go); it
// is restated rather than imported so that a change on either side shows up as
// a failing chain build instead of quietly staying consistent.
var ArduinoIssuer = Name{
	Country:            "US",
	Organization:       "Arduino LLC US",
	OrganizationalUnit: "IT",
	CommonName:         "Arduino",
}

// attribute is one RDN to emit, in firmware order.
type attribute struct {
	oid   asn1.ObjectIdentifier
	value string
}

// attributes returns the non-empty attributes in the order the firmware emits
// them: C, ST, L, O, OU, CN.
func (n Name) attributes() []attribute {
	all := []attribute{
		{oidCountry, n.Country},
		{oidState, n.State},
		{oidLocality, n.Locality},
		{oidOrganization, n.Organization},
		{oidOrganizationalUnit, n.OrganizationalUnit},
		{oidCommonName, n.CommonName},
	}
	out := make([]attribute, 0, len(all))
	for _, a := range all {
		if a.value != "" {
			out = append(out, a)
		}
	}
	return out
}

// IsEmpty reports whether the name carries no attribute at all.
func (n Name) IsEmpty() bool { return len(n.attributes()) == 0 }

// DER encodes the name as the X.509 Name field: SEQUENCE of single-attribute
// SETs, every value a PrintableString.
//
// PrintableString is not a choice, it is the firmware's encoding, and the CA
// signed it. A value outside that character set is rejected rather than
// emitted: DER carrying a PrintableString with illegal characters is invalid,
// and a strict verifier's complaint about it would be reported far from here.
func (n Name) DER() ([]byte, error) {
	for _, a := range n.attributes() {
		if i := strings.IndexFunc(a.value, isNotPrintableStringChar); i >= 0 {
			return nil, fmt.Errorf("pki: %v value %q cannot be a PrintableString (offending rune at %d)", a.oid, a.value, i)
		}
	}

	var b cryptobyte.Builder
	b.AddASN1(cbasn1.SEQUENCE, func(seq *cryptobyte.Builder) {
		for _, a := range n.attributes() {
			seq.AddASN1(cbasn1.SET, func(set *cryptobyte.Builder) {
				set.AddASN1(cbasn1.SEQUENCE, func(pair *cryptobyte.Builder) {
					pair.AddASN1ObjectIdentifier(a.oid)
					pair.AddASN1(cbasn1.PrintableString, func(v *cryptobyte.Builder) {
						v.AddBytes([]byte(a.value))
					})
				})
			})
		}
	})
	der, err := b.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pki: encode name: %w", err)
	}
	return der, nil
}

// String renders the name for timelines and failure reports, in encoding
// order, e.g. `C=IT,CN=9f1c2d3e-…`.
func (n Name) String() string {
	attrs := n.attributes()
	parts := make([]string, 0, len(attrs))
	for _, a := range attrs {
		parts = append(parts, shortRDNName(a.oid)+"="+a.value)
	}
	return strings.Join(parts, ",")
}

func shortRDNName(oid asn1.ObjectIdentifier) string {
	switch {
	case oid.Equal(oidCountry):
		return "C"
	case oid.Equal(oidState):
		return "ST"
	case oid.Equal(oidLocality):
		return "L"
	case oid.Equal(oidOrganization):
		return "O"
	case oid.Equal(oidOrganizationalUnit):
		return "OU"
	case oid.Equal(oidCommonName):
		return "CN"
	default:
		return oid.String()
	}
}

// isNotPrintableStringChar reports whether r is outside the ASN.1
// PrintableString character set (X.680 §41.4).
func isNotPrintableStringChar(r rune) bool {
	switch {
	case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		return false
	case strings.ContainsRune(" '()+,-./:=?", r):
		return false
	default:
		return true
	}
}

// PKIX converts the name for use with crypto/x509.
//
// Only safe for names without a province or locality: pkix.Name emits
// C, O, OU, L, ST, CN while the firmware layout is C, ST, L, O, OU, CN, so for
// a name carrying either the two encodings are NOT interchangeable. It exists
// for the fixed issuer, which carries neither.
func (n Name) PKIX() pkix.Name {
	out := pkix.Name{CommonName: n.CommonName}
	if n.Country != "" {
		out.Country = []string{n.Country}
	}
	if n.State != "" {
		out.Province = []string{n.State}
	}
	if n.Locality != "" {
		out.Locality = []string{n.Locality}
	}
	if n.Organization != "" {
		out.Organization = []string{n.Organization}
	}
	if n.OrganizationalUnit != "" {
		out.OrganizationalUnit = []string{n.OrganizationalUnit}
	}
	return out
}
