// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package pki

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"golang.org/x/crypto/cryptobyte"
	cbasn1 "golang.org/x/crypto/cryptobyte/asn1"
)

// ReflectCSRSubject models what the real Provisioning API does with the CSR's
// subject: every attribute the CSR carries is reflected into the issued
// certificate, with the common name replaced by the assigned device_id.
//
// # This function is the whole reason the fake CA exists
//
// A harness CA that simply signed "CN=<device_id>" would be structurally blind
// to a bug class that has already cost real debugging time. The daemon rebuilds
// the certificate with a CN-only subject, so a CSR polluted with any extra
// attribute makes the REAL certificate diverge from the reconstruction and the
// broker answers "unknown certificate authority" -- far from the cause. A stray
// Country=IT in the CSR was exactly that bug.
//
// Reflecting here is what makes that visible: a regression that re-pollutes the
// CSR subject produces a certificate whose signature covers the polluted
// subject, the daemon's CN-only rebuild no longer matches, and the scenario
// goes red on the spot. internal/keystore/keystore.go carries the comment that
// forbids re-adding subject fields; this is the check that defends it.
//
// # What is inferred and what is observed
//
// That extra attributes are reflected is observed: it is what the Country=IT
// incident proved. That the common name is REPLACED by the device_id rather
// than reflected is inferred from the shape of a working certificate -- the CSR
// carries CN=<uhwid> while the issued certificate carries CN=<device_id>, and
// devices provision successfully against the real API. Both together are what
// makes the happy path (a CN-only CSR) reduce to exactly "CN=<device_id>".
func ReflectCSRSubject(csrPEM, deviceID string) (Name, error) {
	subject, err := SubjectFromCSR(csrPEM)
	if err != nil {
		return Name{}, err
	}
	if deviceID == "" {
		return Name{}, fmt.Errorf("pki: cannot issue a certificate with an empty device id")
	}
	subject.CommonName = deviceID
	return subject, nil
}

// SubjectFromCSR returns the CSR's subject as submitted, which is what the
// fake API records on the timeline: seeing "C=IT,CN=<uhwid>" in the request
// row says immediately what a later handshake failure is about.
func SubjectFromCSR(csrPEM string) (Name, error) {
	csr, err := ParseCSR(csrPEM)
	if err != nil {
		return Name{}, err
	}
	// first is how a multi-valued attribute collapses. The firmware layout has
	// room for one value per type, and a CSR carrying two organizations is
	// already outside the contract; taking the first keeps the divergence
	// visible in the issued subject instead of dropping it.
	first := func(values []string) string {
		if len(values) == 0 {
			return ""
		}
		return values[0]
	}
	return Name{
		Country:            first(csr.Subject.Country),
		State:              first(csr.Subject.Province),
		Locality:           first(csr.Subject.Locality),
		Organization:       first(csr.Subject.Organization),
		OrganizationalUnit: first(csr.Subject.OrganizationalUnit),
		CommonName:         csr.Subject.CommonName,
	}, nil
}

// ParseCSR decodes a PEM CSR and checks its self-signature.
//
// The signature check is not ceremony: the real API rejects a CSR it cannot
// verify, so a daemon that signed the request with the wrong key -- a stale key
// after a rotation, say -- must fail here too rather than be handed a
// certificate for a key it does not hold.
func ParseCSR(csrPEM string) (*x509.CertificateRequest, error) {
	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		return nil, fmt.Errorf("pki: CSR is not valid PEM")
	}
	if block.Type != "CERTIFICATE REQUEST" {
		return nil, fmt.Errorf("pki: CSR PEM block is %q, want CERTIFICATE REQUEST", block.Type)
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("pki: parse CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("pki: CSR signature does not verify: %w", err)
	}
	return csr, nil
}

// PublicKeyFromCSR returns the CSR's public key as the raw uncompressed EC
// point X||Y the certificate embeds.
//
// The bytes are read straight out of the CSR's SubjectPublicKeyInfo rather
// than re-encoded from the parsed key. Two reasons: the certificate has to
// carry exactly the point the CSR carried, and the daemon arrives at the same
// 64 bytes by a different route (an ECDH().Bytes() round-trip), which is the
// independence this package exists for.
func PublicKeyFromCSR(csrPEM string) ([]byte, error) {
	csr, err := ParseCSR(csrPEM)
	if err != nil {
		return nil, err
	}
	pub, ok := csr.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, fmt.Errorf("pki: CSR public key is %T, want an EC key", csr.PublicKey)
	}
	if bits := pub.Curve.Params().BitSize; bits != 256 {
		return nil, fmt.Errorf("pki: CSR public key is a %d-bit curve, want P-256", bits)
	}

	// SubjectPublicKeyInfo ::= SEQUENCE { algorithm AlgorithmIdentifier,
	//                                     subjectPublicKey BIT STRING }
	input := cryptobyte.String(csr.RawSubjectPublicKeyInfo)
	var spki, algorithm cryptobyte.String
	var point []byte
	if !input.ReadASN1(&spki, cbasn1.SEQUENCE) ||
		!spki.ReadASN1(&algorithm, cbasn1.SEQUENCE) ||
		!spki.ReadASN1BitStringAsBytes(&point) {
		return nil, fmt.Errorf("pki: CSR subjectPublicKeyInfo is not a SEQUENCE { algorithm, BIT STRING }")
	}
	if len(point) != devicePubKeyLen+1 {
		return nil, fmt.Errorf("pki: CSR public key is %d bytes, want a %d-byte uncompressed point",
			len(point), devicePubKeyLen+1)
	}
	if point[0] != 0x04 {
		return nil, fmt.Errorf("pki: CSR public key starts with %#x, want the 0x04 uncompressed-point marker", point[0])
	}
	return point[1:], nil
}
