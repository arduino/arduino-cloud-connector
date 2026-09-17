// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package pki

import (
	"encoding/asn1"
	"fmt"
	"math/big"
	"time"

	"golang.org/x/crypto/cryptobyte"
	cbasn1 "golang.org/x/crypto/cryptobyte/asn1"
)

// The device certificate has a fixed shape. Every part of it is a property of
// the firmware layout the CA signs, NOT a choice this package is free to make:
//
//	Certificate      SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
//	TBSCertificate   SEQUENCE {
//	    [0] version 2 (v3)
//	    serialNumber          INTEGER  (16 bytes, positive)
//	    signature             SEQUENCE { ecdsa-with-SHA256 }  -- no parameters
//	    issuer                ArduinoIssuer
//	    validity              SEQUENCE { notBefore, notAfter } -- minute/second zeroed
//	    subject               CN = device_id, plus whatever the CSR reflected in
//	    subjectPublicKeyInfo  id-ecPublicKey / prime256v1, uncompressed point
//	    [3] extensions        SEQUENCE { authorityKeyIdentifier }  -- and NOTHING else
//	}
//
// The certificate carries no basicConstraints, no keyUsage and no
// extendedKeyUsage. That looks under-specified by modern standards, and it is
// the interesting part: the spike in
// internal/provisioning-api/compressed_cert_mtls_test.go proved a strict
// verifier accepts it anyway (crypto/x509, and OpenSSL 3.2.1 under
// -x509_strict -purpose sslclient), because a leaf with no EKU is valid for
// every purpose. Adding those extensions "for correctness" here would make the
// harness issue something the real CA never issues.
const (
	deviceSerialLen = 16 // bytes, as the API returns it
	deviceAKILen    = 20 // bytes, a SHA-1-sized key identifier
	devicePubKeyLen = 64 // raw EC point X||Y
	deviceSigLen    = 64 // raw ECDSA r||s

	// DeviceExpireYears is the fixed validity the firmware applies: notAfter
	// is notBefore's year plus this, with month, day and hour copied verbatim.
	// It is also what pushes notAfter past 2049 and therefore onto the
	// GeneralizedTime encoding.
	DeviceExpireYears = 31
)

var (
	oidECDSAWithSHA256   = asn1.ObjectIdentifier{1, 2, 840, 10045, 4, 3, 2}
	oidECPublicKey       = asn1.ObjectIdentifier{1, 2, 840, 10045, 2, 1}
	oidPrime256v1        = asn1.ObjectIdentifier{1, 2, 840, 10045, 3, 1, 7}
	oidAuthorityKeyIDExt = asn1.ObjectIdentifier{2, 5, 29, 35}
)

// DeviceCert is everything needed to encode one device certificate.
type DeviceCert struct {
	// Subject is the certificate subject. For a well-behaved daemon it is
	// CN=<device_id> and nothing else; see ReflectCSRSubject for why it is a
	// full Name rather than a bare common name.
	Subject Name
	// PublicKey is the uncompressed EC point X||Y taken from the CSR.
	PublicKey []byte
	Serial    []byte
	// AKI identifies the signing CA. All-zero means "no extension at all",
	// which the firmware encodes as an empty extensions container.
	AKI       []byte
	NotBefore time.Time
	// ExpireYears defaults to DeviceExpireYears when zero.
	ExpireYears int
}

// Validity returns the notBefore and notAfter actually encoded: minute and
// second zeroed, notAfter's year advanced by ExpireYears with month, day and
// hour copied verbatim.
//
// The year arithmetic happens on the digits, exactly as the firmware does it,
// so 29 February is refused rather than silently normalised. The firmware
// would emit "0229" even for a notAfter year that has no 29 February; a
// harness that quietly issued 1 March instead would sign bytes the daemon
// cannot reproduce, and the failure would surface as an unexplained bad
// signature hours away from its cause.
func (d DeviceCert) Validity() (notBefore, notAfter time.Time, err error) {
	years := d.ExpireYears
	if years == 0 {
		years = DeviceExpireYears
	}
	t := d.NotBefore.UTC()
	notBefore = time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	notAfter = time.Date(t.Year()+years, t.Month(), t.Day(), t.Hour(), 0, 0, 0, time.UTC)
	if notAfter.Month() != notBefore.Month() || notAfter.Day() != notBefore.Day() {
		return time.Time{}, time.Time{}, fmt.Errorf(
			"pki: notBefore %s plus %d years is not a real date (%s): the firmware copies month and day verbatim, so pick another notBefore",
			notBefore.Format(time.DateOnly), years, notAfter.Format(time.DateOnly))
	}
	return notBefore, notAfter, nil
}

// TBS encodes the TBSCertificate: the bytes the CA signs, and the bytes the
// daemon has to reconstruct from the pipe-delimited response.
func (d DeviceCert) TBS() ([]byte, error) {
	if len(d.PublicKey) != devicePubKeyLen {
		return nil, fmt.Errorf("pki: public key is %d bytes, want %d (raw X||Y)", len(d.PublicKey), devicePubKeyLen)
	}
	if len(d.Serial) != deviceSerialLen {
		return nil, fmt.Errorf("pki: serial is %d bytes, want %d", len(d.Serial), deviceSerialLen)
	}
	if len(d.AKI) != deviceAKILen {
		return nil, fmt.Errorf("pki: authority key identifier is %d bytes, want %d", len(d.AKI), deviceAKILen)
	}
	if d.Subject.IsEmpty() {
		return nil, fmt.Errorf("pki: device certificate subject is empty")
	}

	issuerDER, err := ArduinoIssuer.DER()
	if err != nil {
		return nil, err
	}
	subjectDER, err := d.Subject.DER()
	if err != nil {
		return nil, err
	}
	notBefore, notAfter, err := d.Validity()
	if err != nil {
		return nil, err
	}
	akiExt, err := authorityKeyIDExtension(d.AKI)
	if err != nil {
		return nil, err
	}

	var b cryptobyte.Builder
	b.AddASN1(cbasn1.SEQUENCE, func(tbs *cryptobyte.Builder) {
		// version: [0] EXPLICIT INTEGER 2, i.e. v3
		tbs.AddASN1(contextTag(0).Constructed(), func(v *cryptobyte.Builder) {
			v.AddASN1Int64(2)
		})
		// serialNumber: a positive INTEGER. Going through big.Int is what
		// strips the leading zeros and prepends the 0x00 pad when the top bit
		// is set -- the same two rules the firmware applies by hand.
		tbs.AddASN1BigInt(new(big.Int).SetBytes(d.Serial))
		addSignatureAlgorithm(tbs)
		tbs.AddBytes(issuerDER)
		tbs.AddASN1(cbasn1.SEQUENCE, func(val *cryptobyte.Builder) {
			addCertTime(val, notBefore)
			addCertTime(val, notAfter)
		})
		tbs.AddBytes(subjectDER)
		addSubjectPublicKey(tbs, d.PublicKey)
		// [3] EXPLICIT Extensions
		tbs.AddASN1(contextTag(3).Constructed(), func(exts *cryptobyte.Builder) {
			exts.AddASN1(cbasn1.SEQUENCE, func(list *cryptobyte.Builder) {
				if akiExt != nil {
					list.AddBytes(akiExt)
				}
			})
		})
	})
	der, err := b.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pki: encode TBS: %w", err)
	}
	return der, nil
}

// Certificate wraps an already-signed TBS into the full Certificate DER.
// signature is the raw ECDSA r||s, 64 bytes: the form the Provisioning API
// hands it over in, and therefore the form the daemon rebuilds from.
func (d DeviceCert) Certificate(tbs, signature []byte) ([]byte, error) {
	if len(signature) != deviceSigLen {
		return nil, fmt.Errorf("pki: signature is %d bytes, want %d (raw r||s)", len(signature), deviceSigLen)
	}
	sigValue, err := ecdsaSigValue(signature)
	if err != nil {
		return nil, err
	}

	var b cryptobyte.Builder
	b.AddASN1(cbasn1.SEQUENCE, func(cert *cryptobyte.Builder) {
		cert.AddBytes(tbs)
		addSignatureAlgorithm(cert)
		cert.AddASN1BitString(sigValue)
	})
	der, err := b.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pki: encode certificate: %w", err)
	}
	return der, nil
}

// addSignatureAlgorithm emits the AlgorithmIdentifier for ecdsa-with-SHA256.
// There are deliberately NO parameters: RFC 5758 forbids them for this
// algorithm and the firmware omits them, so the absent NULL is part of the
// signed bytes.
func addSignatureAlgorithm(b *cryptobyte.Builder) {
	b.AddASN1(cbasn1.SEQUENCE, func(alg *cryptobyte.Builder) {
		alg.AddASN1ObjectIdentifier(oidECDSAWithSHA256)
	})
}

// addCertTime emits one Time, choosing the encoding by year the way RFC 5280
// requires: UTCTime through 2049, GeneralizedTime from 2050 on. The 31-year
// validity means every real device certificate exercises BOTH branches, which
// is why the encoding is chosen per field and not once per certificate.
//
// Minute and second are already zero by construction (see Validity). They are
// zeroed there rather than here so the times a caller can inspect and the
// bytes actually encoded cannot disagree.
func addCertTime(b *cryptobyte.Builder, t time.Time) {
	if t.Year() > 2049 {
		b.AddASN1GeneralizedTime(t)
		return
	}
	b.AddASN1UTCTime(t)
}

// addSubjectPublicKey emits the SubjectPublicKeyInfo for an uncompressed P-256
// point. The 0x04 uncompressed-point marker belongs INSIDE the BIT STRING,
// after its unused-bits byte.
func addSubjectPublicKey(b *cryptobyte.Builder, pub []byte) {
	b.AddASN1(cbasn1.SEQUENCE, func(spki *cryptobyte.Builder) {
		spki.AddASN1(cbasn1.SEQUENCE, func(alg *cryptobyte.Builder) {
			alg.AddASN1ObjectIdentifier(oidECPublicKey)
			alg.AddASN1ObjectIdentifier(oidPrime256v1)
		})
		point := make([]byte, 0, 1+len(pub))
		point = append(point, 0x04)
		point = append(point, pub...)
		spki.AddASN1BitString(point)
	})
}

// authorityKeyIDExtension encodes the single extension the certificate
// carries. An all-zero AKI means the firmware emits no extension at all and
// leaves the container empty; returning nil says so.
func authorityKeyIDExtension(aki []byte) ([]byte, error) {
	if !anyNonZero(aki) {
		return nil, nil
	}

	var inner cryptobyte.Builder
	inner.AddASN1(cbasn1.SEQUENCE, func(kid *cryptobyte.Builder) {
		// keyIdentifier is [0] IMPLICIT OCTET STRING, so the tag is primitive:
		// 0x80, not 0xa0. A constructed tag would still be accepted by lenient
		// readers while changing the signed bytes.
		kid.AddASN1(contextTag(0), func(v *cryptobyte.Builder) {
			v.AddBytes(aki)
		})
	})
	innerDER, err := inner.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pki: encode authority key identifier: %w", err)
	}

	var b cryptobyte.Builder
	b.AddASN1(cbasn1.SEQUENCE, func(ext *cryptobyte.Builder) {
		ext.AddASN1ObjectIdentifier(oidAuthorityKeyIDExt)
		// No critical flag: DEFAULT FALSE must be omitted in DER, and the
		// firmware omits it.
		ext.AddASN1OctetString(innerDER)
	})
	der, err := b.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pki: encode authority key identifier extension: %w", err)
	}
	return der, nil
}

// ecdsaSigValue turns the raw r||s into the ECDSA-Sig-Value SEQUENCE that goes
// inside the signature BIT STRING.
func ecdsaSigValue(signature []byte) ([]byte, error) {
	var b cryptobyte.Builder
	b.AddASN1(cbasn1.SEQUENCE, func(sig *cryptobyte.Builder) {
		sig.AddASN1BigInt(new(big.Int).SetBytes(signature[:32]))
		sig.AddASN1BigInt(new(big.Int).SetBytes(signature[32:]))
	})
	der, err := b.Bytes()
	if err != nil {
		return nil, fmt.Errorf("pki: encode signature value: %w", err)
	}
	return der, nil
}

// contextTag builds a context-specific ASN.1 tag. cryptobyte sets the class
// bits itself, which keeps the bare 0xa0/0x80 constants out of this file.
func contextTag(number uint8) cbasn1.Tag {
	return cbasn1.Tag(number).ContextSpecific()
}

func anyNonZero(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return true
		}
	}
	return false
}
