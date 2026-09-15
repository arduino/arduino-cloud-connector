// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package pki

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"
)

var (
	testAKI, _    = hex.DecodeString("1122334455667788990011223344556677889900")
	testSerial, _ = hex.DecodeString("0102030405060708090a0b0c0d0e0f10")
	testNotBefore = time.Date(2024, 1, 15, 10, 37, 45, 0, time.UTC)
)

// newTestDeviceCert builds a certificate for a fresh key, signed by a fresh
// key, and returns the DER plus the parsed form. Nothing here verifies the
// signature, so the signer does not need to be a CA: this is about the shape
// of the bytes.
func newTestDeviceCert(t *testing.T, d DeviceCert) (der []byte, parsed *x509.Certificate, tbs []byte) {
	t.Helper()
	if d.PublicKey == nil {
		csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})
		pub, err := PublicKeyFromCSR(csrPEM)
		if err != nil {
			t.Fatalf("public key: %v", err)
		}
		d.PublicKey = pub
	}
	tbs, err := d.TBS()
	if err != nil {
		t.Fatalf("encode TBS: %v", err)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("signer key: %v", err)
	}
	digest := sha256.Sum256(tbs)
	r, s, err := ecdsa.Sign(rand.Reader, key, digest[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	sig := make([]byte, deviceSigLen)
	r.FillBytes(sig[:32])
	s.FillBytes(sig[32:])

	der, err = d.Certificate(tbs, sig)
	if err != nil {
		t.Fatalf("encode certificate: %v", err)
	}
	parsed, err = x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("a strict parser rejected the encoded certificate: %v", err)
	}
	return der, parsed, tbs
}

// The firmware-shaped certificate is deliberately sparse, and every one of
// these assertions is a property the real CA signs. In particular the absent
// keyUsage, extendedKeyUsage and basicConstraints are NOT omissions to be
// fixed: adding them would make the harness issue something the real API never
// issues.
func TestDeviceCertHasTheFirmwareShape(t *testing.T) {
	csrPEM, key := newTestCSR(t, pkix.Name{CommonName: testUHWID})
	pub, err := PublicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("public key: %v", err)
	}
	_, cert, _ := newTestDeviceCert(t, DeviceCert{
		Subject:   Name{CommonName: testDeviceID},
		PublicKey: pub,
		Serial:    testSerial,
		AKI:       testAKI,
		NotBefore: testNotBefore,
	})

	issuerDER, err := ArduinoIssuer.DER()
	if err != nil {
		t.Fatalf("encode issuer: %v", err)
	}
	if !bytes.Equal(cert.RawIssuer, issuerDER) {
		t.Errorf("issuer DN is not the Arduino issuer\n  got:  %x\n  want: %x", cert.RawIssuer, issuerDER)
	}
	if cert.Subject.CommonName != testDeviceID {
		t.Errorf("subject common name = %q, want the device id %q", cert.Subject.CommonName, testDeviceID)
	}
	if cert.Version != 3 {
		t.Errorf("version = %d, want 3", cert.Version)
	}
	if want := new(big.Int).SetBytes(testSerial); cert.SerialNumber.Cmp(want) != 0 {
		t.Errorf("serial = %s, want %s", cert.SerialNumber, want)
	}
	if got, want := cert.SignatureAlgorithm, x509.ECDSAWithSHA256; got != want {
		t.Errorf("signature algorithm = %v, want %v", got, want)
	}
	if got, want := cert.PublicKeyAlgorithm, x509.ECDSA; got != want {
		t.Errorf("public key algorithm = %v, want %v", got, want)
	}
	parsedPub, ok := cert.PublicKey.(*ecdsa.PublicKey)
	if !ok {
		t.Fatalf("public key is %T, want an EC key", cert.PublicKey)
	}
	inCert, err := parsedPub.Bytes()
	if err != nil {
		t.Fatalf("encode the certificate key: %v", err)
	}
	fromCSR, err := key.PublicKey.Bytes()
	if err != nil {
		t.Fatalf("encode the CSR key: %v", err)
	}
	if string(inCert) != string(fromCSR) {
		t.Error("the certificate does not carry the CSR public key")
	}
	if !bytes.Equal(cert.AuthorityKeyId, testAKI) {
		t.Errorf("authority key id = %x, want %x", cert.AuthorityKeyId, testAKI)
	}
	if cert.KeyUsage != 0 {
		t.Errorf("keyUsage = %v, want none: the firmware certificate carries no such extension", cert.KeyUsage)
	}
	if len(cert.ExtKeyUsage) != 0 {
		t.Errorf("extKeyUsage = %v, want none", cert.ExtKeyUsage)
	}
	if cert.BasicConstraintsValid {
		t.Error("basicConstraints present, want absent")
	}
	if len(cert.Extensions) != 1 {
		t.Errorf("got %d extensions, want exactly 1 (the authority key identifier)", len(cert.Extensions))
	}
}

// The 31-year validity straddles 2049, so a single certificate exercises both
// time encodings: UTCTime for notBefore, GeneralizedTime for notAfter. And
// minute and second are zeroed, which is why an odd notBefore is used here.
func TestDeviceCertValidityEncodingAndZeroedMinutes(t *testing.T) {
	der, cert, _ := newTestDeviceCert(t, DeviceCert{
		Subject:   Name{CommonName: testDeviceID},
		Serial:    testSerial,
		AKI:       testAKI,
		NotBefore: testNotBefore, // 2024-01-15 10:37:45
	})

	if got, want := cert.NotBefore, time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("notBefore = %s, want %s (minute and second zeroed)", got, want)
	}
	if got, want := cert.NotAfter, time.Date(2055, 1, 15, 10, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Errorf("notAfter = %s, want %s", got, want)
	}
	// The encodings themselves, not just the parsed values: a UTCTime notAfter
	// would parse to 1955 and a GeneralizedTime notBefore would change the
	// signed bytes.
	if !bytes.Contains(der, []byte("240115100000Z")) {
		t.Error("notBefore is not encoded as a UTCTime (240115100000Z)")
	}
	if !bytes.Contains(der, []byte("20550115100000Z")) {
		t.Error("notAfter is not encoded as a GeneralizedTime (20550115100000Z)")
	}
}

// 29 February plus 31 years is not a date. The firmware would copy the digits
// anyway, so the harness refuses rather than silently signing 1 March, which
// the daemon could not reproduce.
func TestDeviceCertRejectsALeapDayNotBefore(t *testing.T) {
	d := DeviceCert{
		Subject:   Name{CommonName: testDeviceID},
		Serial:    testSerial,
		AKI:       testAKI,
		NotBefore: time.Date(2024, 2, 29, 10, 0, 0, 0, time.UTC),
	}
	if _, _, err := d.Validity(); err == nil {
		t.Fatal("a 29 February notBefore was accepted")
	}
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})
	pub, err := PublicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("public key: %v", err)
	}
	d.PublicKey = pub
	if _, err := d.TBS(); err == nil {
		t.Fatal("TBS encoded a certificate whose notAfter does not exist")
	}
}

// An all-zero AKI means the firmware emits no extension at all, and the
// extensions container stays empty. It is the last thing in the TBS, so the
// bytes are checkable directly.
func TestDeviceCertWithZeroAKIEmitsAnEmptyExtensionContainer(t *testing.T) {
	_, cert, tbs := newTestDeviceCert(t, DeviceCert{
		Subject:   Name{CommonName: testDeviceID},
		Serial:    testSerial,
		AKI:       make([]byte, deviceAKILen),
		NotBefore: testNotBefore,
	})

	// a3 02 30 00: [3] { SEQUENCE {} }
	if want := []byte{0xa3, 0x02, 0x30, 0x00}; !bytes.HasSuffix(tbs, want) {
		t.Errorf("TBS does not end with an empty extensions container\n  tail: %x\n  want: %x",
			tbs[max(0, len(tbs)-8):], want)
	}
	if len(cert.Extensions) != 0 {
		t.Errorf("got %d extensions, want none", len(cert.Extensions))
	}
	if len(cert.AuthorityKeyId) != 0 {
		t.Errorf("authority key id = %x, want none", cert.AuthorityKeyId)
	}
}

// The serial goes out as a positive INTEGER: leading zeros stripped, a 0x00
// pad prepended when the top bit is set. Both rules are the firmware's, and
// both are what going through big.Int reproduces.
func TestDeviceCertSerialIsAPositiveInteger(t *testing.T) {
	tests := []struct {
		name   string
		serial string
	}{
		{"leading zeros", "0000030405060708090a0b0c0d0e0f10"},
		{"high bit set", "ff02030405060708090a0b0c0d0e0f10"},
		{"all zeros", "00000000000000000000000000000000"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			serial, err := hex.DecodeString(tc.serial)
			if err != nil {
				t.Fatalf("decode serial: %v", err)
			}
			_, cert, _ := newTestDeviceCert(t, DeviceCert{
				Subject:   Name{CommonName: testDeviceID},
				Serial:    serial,
				AKI:       testAKI,
				NotBefore: testNotBefore,
			})
			if cert.SerialNumber.Sign() < 0 {
				t.Errorf("serial parsed as negative: %s", cert.SerialNumber)
			}
			if want := new(big.Int).SetBytes(serial); cert.SerialNumber.Cmp(want) != 0 {
				t.Errorf("serial = %s, want %s", cert.SerialNumber, want)
			}
		})
	}
}

// Wrong field widths are a harness bug, and they must fail at the point of
// encoding rather than produce a certificate the daemon cannot rebuild.
func TestDeviceCertRejectsWrongFieldWidths(t *testing.T) {
	csrPEM, _ := newTestCSR(t, pkix.Name{CommonName: testUHWID})
	pub, err := PublicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("public key: %v", err)
	}
	valid := DeviceCert{
		Subject:   Name{CommonName: testDeviceID},
		PublicKey: pub,
		Serial:    testSerial,
		AKI:       testAKI,
		NotBefore: testNotBefore,
	}

	tests := []struct {
		name   string
		mutate func(d *DeviceCert)
		want   string
	}{
		{"short public key", func(d *DeviceCert) { d.PublicKey = pub[:32] }, "public key"},
		{"short serial", func(d *DeviceCert) { d.Serial = testSerial[:8] }, "serial"},
		{"short AKI", func(d *DeviceCert) { d.AKI = testAKI[:10] }, "authority key identifier"},
		{"empty subject", func(d *DeviceCert) { d.Subject = Name{} }, "subject"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := valid
			tc.mutate(&d)
			_, err := d.TBS()
			if err == nil {
				t.Fatal("encoded successfully, want an error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q, got: %v", tc.want, err)
			}
		})
	}

	tbs, err := valid.TBS()
	if err != nil {
		t.Fatalf("encode TBS: %v", err)
	}
	if _, err := valid.Certificate(tbs, make([]byte, 32)); err == nil {
		t.Fatal("a 32-byte signature was accepted, want 64 (r||s)")
	}
}
