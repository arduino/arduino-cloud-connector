// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioningapi

import (
	"crypto/ecdsa"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Device certificate reconstruction — the Linux-daemon port of the firmware's
// "compressed certificate" rebuild.
//
// The Provisioning API returns only the certificate's variable parts as a
// pipe-delimited string:
//
//	device_id|authority_key_identifier|not_before|serial|signature_asn1_x|signature_asn1_y
//
// The daemon rebuilds the full X.509 cert deterministically (all other fields
// are fixed). The CA signed exactly these bytes, so the DER MUST match the
// firmware's encoding byte-for-byte or the signature won't verify and the
// broker rejects the cert.
//
// Reference firmware:
//   - examples/utility/Provisioning_2.0/CSRHandler.cpp  (response parsing)
//   - Arduino_SecureElement/src/ECP256Certificate.cpp   (DER encoding)
//   - Arduino_SecureElement/src/utility/SElementArduinoCloudCertificate.cpp (issuer)

const (
	certSerialNumberLen   = 16 // bytes
	certAuthorityKeyIDLen = 20 // bytes
	certPublicKeyLen      = 64 // raw EC point X||Y, 32+32
	certSignatureLen      = 64 // raw ECDSA r||s, 32+32
	certExpireYears       = 31 // fixed, matches CSRHandler.cpp
)

// Fixed issuer, identical to SElementArduinoCloudCertificate's SEACC_ISSUER_*.
var certIssuer = certName{
	country:    "US",
	org:        "Arduino LLC US",
	orgUnit:    "IT",
	commonName: "Arduino",
}

// reconstructDeviceCert parses the provision/csr response body and rebuilds the
// device certificate as PEM. csrPEM is the CSR that was submitted; its public
// key is bound into the reconstructed certificate (it is the same key the CA
// certified).
func reconstructDeviceCert(csrPEM, responseBody string) (deviceID, certPEM string, err error) {
	// Response format: device_id|authority_key_identifier|not_before|serial|sig_x|sig_y
	parts := strings.Split(strings.TrimSpace(responseBody), "|")
	if len(parts) != 6 {
		return "", "", fmt.Errorf("provisioning: unexpected CSR response format: got %d fields, want 6", len(parts))
	}
	deviceID = parts[0]
	akiHex, notBefore, serialHex, sigXHex, sigYHex := parts[1], parts[2], parts[3], parts[4], parts[5]

	// Length checks mirror the firmware (CSRHandler.cpp handleParseResponse).
	if len(deviceID) != 36 || len(akiHex) != 2*certAuthorityKeyIDLen ||
		len(serialHex) != 2*certSerialNumberLen ||
		len(sigXHex) != certSignatureLen || len(sigYHex) != certSignatureLen {
		return "", "", errors.New("provisioning: CSR response field length mismatch")
	}

	aki, err := hex.DecodeString(akiHex)
	if err != nil {
		return "", "", fmt.Errorf("provisioning: decode authority_key_identifier: %w", err)
	}
	serial, err := hex.DecodeString(serialHex)
	if err != nil {
		return "", "", fmt.Errorf("provisioning: decode serial: %w", err)
	}
	sigX, err := hex.DecodeString(sigXHex)
	if err != nil {
		return "", "", fmt.Errorf("provisioning: decode signature x: %w", err)
	}
	sigY, err := hex.DecodeString(sigYHex)
	if err != nil {
		return "", "", fmt.Errorf("provisioning: decode signature y: %w", err)
	}
	signature := append(sigX, sigY...) // r||s, 64 bytes

	year, month, day, hour, err := parseNotBefore(notBefore)
	if err != nil {
		return "", "", err
	}

	pubKey, err := publicKeyFromCSR(csrPEM)
	if err != nil {
		return "", "", err
	}

	certDER := buildCert(certParams{
		publicKey:   pubKey,
		deviceID:    deviceID,
		serial:      serial,
		authKeyID:   aki,
		signature:   signature,
		issueYear:   year,
		issueMonth:  month,
		issueDay:    day,
		issueHour:   hour,
		expireYears: certExpireYears,
	})

	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}))
	return deviceID, certPEM, nil
}

// parseNotBefore extracts year/month/day/hour from an ISO-8601-ish timestamp
// (e.g. "2024-01-15T10:30:00Z"), using the same fixed offsets as the firmware
// (SElementArduinoCloudCertificate::rebuild). Minute/second are dropped — the
// certificate validity always zeroes them.
func parseNotBefore(s string) (year, month, day, hour int, err error) {
	if len(s) < 13 {
		return 0, 0, 0, 0, fmt.Errorf("provisioning: not_before too short: %q", s)
	}
	if year, err = strconv.Atoi(s[0:4]); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("provisioning: parse not_before year: %w", err)
	}
	if month, err = strconv.Atoi(s[5:7]); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("provisioning: parse not_before month: %w", err)
	}
	if day, err = strconv.Atoi(s[8:10]); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("provisioning: parse not_before day: %w", err)
	}
	if hour, err = strconv.Atoi(s[11:13]); err != nil {
		return 0, 0, 0, 0, fmt.Errorf("provisioning: parse not_before hour: %w", err)
	}
	return year, month, day, hour, nil
}

// publicKeyFromCSR extracts the raw 64-byte EC point (X||Y) from a PEM CSR.
func publicKeyFromCSR(csrPEM string) ([]byte, error) {
	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		return nil, errors.New("provisioning: invalid CSR PEM")
	}
	req, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("provisioning: parse CSR: %w", err)
	}
	pub, ok := req.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("provisioning: CSR public key is not EC")
	}
	ecdhPub, err := pub.ECDH()
	if err != nil {
		return nil, fmt.Errorf("provisioning: convert EC public key: %w", err)
	}
	// ECDH().Bytes() returns 04 || X || Y (65 bytes for P-256).
	raw := ecdhPub.Bytes()
	buf := make([]byte, certPublicKeyLen)
	copy(buf[0:32], raw[1:33])  // X
	copy(buf[32:64], raw[33:65]) // Y
	return buf, nil
}

// ── X.509 DER encoding (mirrors ECP256Certificate) ─────────────────────────────

type certParams struct {
	publicKey   []byte // 64 bytes, raw X||Y
	deviceID    string // subject common name
	serial      []byte // 16 bytes
	authKeyID   []byte // 20 bytes
	signature   []byte // 64 bytes, raw r||s
	issueYear   int
	issueMonth  int
	issueDay    int
	issueHour   int
	expireYears int
}

// buildCert produces the full DER Certificate: SEQUENCE { tbsCertificate,
// signatureAlgorithm, signatureValue }. Mirrors buildCert + signCert.
func buildCert(p certParams) []byte {
	tbs := buildTBS(p)

	var sig []byte
	sig = appendSignature(sig, p.signature)

	var cert []byte
	cert = appendSequenceHeader(cert, len(tbs)+len(sig))
	cert = append(cert, tbs...)
	cert = append(cert, sig...)
	return cert
}

// buildTBS produces the DER TBSCertificate, mirroring ECP256Certificate::buildCert.
func buildTBS(p certParams) []byte {
	subject := certName{commonName: p.deviceID}

	var inner []byte

	// version: [0] { INTEGER 2 } → v3
	inner = append(inner, 0xa0, 0x03, 0x02, 0x01, 0x02)

	// serial number
	inner = appendSerialNumber(inner, p.serial)

	// signature algorithm (ecdsa-with-SHA256)
	inner = appendEcdsaWithSHA256(inner)

	// issuer
	inner = appendSequenceHeader(inner, certIssuer.length())
	inner = appendName(inner, certIssuer)

	// validity: notBefore + notAfter (notAfter = issueYear + expireYears)
	notAfterYear := p.issueYear + p.expireYears
	valLen := 30
	if p.issueYear > 2049 {
		valLen += 2
	}
	if notAfterYear > 2049 {
		valLen += 2
	}
	inner = append(inner, asn1Sequence, byte(valLen))
	inner = appendDate(inner, p.issueYear, p.issueMonth, p.issueDay, p.issueHour)
	inner = appendDate(inner, notAfterYear, p.issueMonth, p.issueDay, p.issueHour)

	// subject
	inner = appendSequenceHeader(inner, subject.length())
	inner = appendName(inner, subject)

	// subject public key info
	inner = appendPublicKey(inner, p.publicKey)

	// authority key identifier extension (or empty extensions container)
	if authorityKeyIDSet(p.authKeyID) {
		inner = appendAuthorityKeyID(inner, p.authKeyID)
	} else {
		inner = append(inner, 0xa3, 0x02, 0x30, 0x00)
	}

	var tbs []byte
	tbs = appendSequenceHeader(tbs, len(inner))
	tbs = append(tbs, inner...)
	return tbs
}

// ── ASN.1 primitives (1:1 with ECP256Certificate's append* helpers) ────────────

const (
	asn1Integer          = 0x02
	asn1BitString        = 0x03
	asn1ObjectIdentifier = 0x06
	asn1PrintableString  = 0x13
	asn1Sequence         = 0x30
	asn1Set              = 0x31
)

func appendSequenceHeader(buf []byte, length int) []byte {
	buf = append(buf, asn1Sequence)
	switch {
	case length > 255:
		buf = append(buf, 0x82, byte(length>>8), byte(length))
	case length > 127:
		buf = append(buf, 0x81, byte(length))
	default:
		buf = append(buf, byte(length))
	}
	return buf
}

type certName struct {
	country    string
	state      string
	locality   string
	org        string
	orgUnit    string
	commonName string
}

func (n certName) length() int {
	total := 0
	for _, f := range []string{n.country, n.state, n.locality, n.org, n.orgUnit, n.commonName} {
		if len(f) > 0 {
			total += 11 + len(f)
		}
	}
	return total
}

// appendName emits the RDNSequence entries in the firmware's field order
// (C, ST, L, O, OU, CN), each as SET { SEQUENCE { OID 2.5.4.<type>, PrintableString } }.
func appendName(buf []byte, n certName) []byte {
	emit := func(buf []byte, value string, typ byte) []byte {
		if len(value) == 0 {
			return buf
		}
		l := len(value)
		buf = append(buf, asn1Set, byte(l+9))
		buf = append(buf, asn1Sequence, byte(l+7))
		buf = append(buf, asn1ObjectIdentifier, 0x03, 0x55, 0x04, typ)
		buf = append(buf, asn1PrintableString, byte(l))
		buf = append(buf, value...)
		return buf
	}
	buf = emit(buf, n.country, 0x06)
	buf = emit(buf, n.state, 0x08)
	buf = emit(buf, n.locality, 0x07)
	buf = emit(buf, n.org, 0x0a)
	buf = emit(buf, n.orgUnit, 0x0b)
	buf = emit(buf, n.commonName, 0x03)
	return buf
}

// appendSerialNumber emits the serial as a positive DER INTEGER (leading zeros
// stripped, 0x00 prepended when the top bit is set).
func appendSerialNumber(buf, serial []byte) []byte {
	s := serial
	for len(s) > 0 && s[0] == 0x00 {
		s = s[1:]
	}
	if len(s) == 0 {
		return append(buf, asn1Integer, 0x01, 0x00)
	}
	buf = append(buf, asn1Integer)
	if s[0]&0x80 != 0 {
		buf = append(buf, byte(len(s)+1), 0x00)
	} else {
		buf = append(buf, byte(len(s)))
	}
	return append(buf, s...)
}

// appendDate emits a UTCTime (year ≤ 2049) or GeneralizedTime (year > 2049),
// with minute and second forced to zero, matching ECP256Certificate::appendDate.
func appendDate(buf []byte, year, month, day, hour int) []byte {
	if year > 2049 {
		buf = append(buf, 0x18, 0x0f)
		buf = append(buf,
			byte('0'+year/1000),
			byte('0'+(year%1000)/100),
			byte('0'+(year%100)/10),
			byte('0'+year%10),
		)
	} else {
		y := year - 2000
		buf = append(buf, 0x17, 0x0d)
		buf = append(buf, byte('0'+y/10), byte('0'+y%10))
	}
	buf = append(buf, byte('0'+month/10), byte('0'+month%10))
	buf = append(buf, byte('0'+day/10), byte('0'+day%10))
	buf = append(buf, byte('0'+hour/10), byte('0'+hour%10))
	buf = append(buf, '0', '0') // minute
	buf = append(buf, '0', '0') // second
	buf = append(buf, 0x5a)     // 'Z' (UTC)
	return buf
}

// appendPublicKey emits the SubjectPublicKeyInfo for an EC P-256 (prime256v1)
// uncompressed point. Mirrors ECP256Certificate::appendPublicKey.
func appendPublicKey(buf, publicKey []byte) []byte {
	buf = append(buf, asn1Sequence, 2+9+10+4+64) // 0x59
	buf = append(buf, asn1Sequence, 0x13)
	// id-ecPublicKey 1.2.840.10045.2.1
	buf = append(buf, asn1ObjectIdentifier, 0x07, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x02, 0x01)
	// prime256v1 1.2.840.10045.3.1.7
	buf = append(buf, asn1ObjectIdentifier, 0x08, 0x2a, 0x86, 0x48, 0xce, 0x3d, 0x03, 0x01, 0x07)
	buf = append(buf, asn1BitString, 0x42, 0x00, 0x04)
	return append(buf, publicKey...)
}

func appendEcdsaWithSHA256(buf []byte) []byte {
	// SEQUENCE { OID ecdsa-with-SHA256 1.2.840.10045.4.3.2 }
	return append(buf, asn1Sequence, 0x0a, asn1ObjectIdentifier, 0x08,
		0x2a, 0x86, 0x48, 0xce, 0x3d, 0x04, 0x03, 0x02)
}

func appendAuthorityKeyID(buf, authKeyID []byte) []byte {
	buf = append(buf, 0xa3, 0x23) // [3] extensions
	buf = append(buf, asn1Sequence, 0x21)
	buf = append(buf, asn1Sequence, 0x1f)
	// 2.5.29.35 authorityKeyIdentifier
	buf = append(buf, asn1ObjectIdentifier, 0x03, 0x55, 0x1d, 0x23)
	buf = append(buf, 0x04, 0x18) // OCTET STRING
	buf = append(buf, asn1Sequence, 0x16)
	buf = append(buf, 0x80, 0x14) // [0] keyIdentifier
	return append(buf, authKeyID...)
}

func authorityKeyIDSet(authKeyID []byte) bool {
	for _, b := range authKeyID {
		if b != 0 {
			return true
		}
	}
	return false
}

// appendSignature emits the outer signatureAlgorithm and the signatureValue
// BIT STRING wrapping the ECDSA-Sig-Value SEQUENCE { r, s }. Mirrors
// ECP256Certificate::appendSignature.
func appendSignature(buf, signature []byte) []byte {
	// signature algorithm (ecdsa-with-SHA256), repeated in the outer Certificate
	buf = appendEcdsaWithSHA256(buf)

	r := derInteger(signature[0:32])
	s := derInteger(signature[32:64])

	buf = append(buf, asn1BitString, byte(len(r)+len(s)+7), 0x00)
	buf = append(buf, asn1Sequence, byte(len(r)+len(s)+4))
	buf = append(buf, asn1Integer, byte(len(r)))
	buf = append(buf, r...)
	buf = append(buf, asn1Integer, byte(len(s)))
	buf = append(buf, s...)
	return buf
}

// derInteger returns the DER INTEGER content for a fixed-width big-endian value:
// leading zeros stripped, a single 0x00 prepended when the top bit is set.
func derInteger(b []byte) []byte {
	i := 0
	for i < len(b)-1 && b[i] == 0x00 {
		i++
	}
	v := b[i:]
	if v[0]&0x80 != 0 {
		out := make([]byte, 0, len(v)+1)
		out = append(out, 0x00)
		return append(out, v...)
	}
	return v
}
