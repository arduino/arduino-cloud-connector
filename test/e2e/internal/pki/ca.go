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
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"time"
)

// deviceIDLen is the length of the device_id the API assigns: a UUID in
// canonical text form. The daemon rejects any other length outright (it
// mirrors the firmware's parser), so the CA refuses to issue one rather than
// let the harness produce a response the daemon will never accept. A scenario
// that WANTS a rejected response asks the fake API for a malformed one.
const deviceIDLen = 36

// CA is the harness's stand-in for the Arduino CA: it signs the device
// certificates the Provisioning API would issue, and the TLS server
// certificate the harness broker presents.
type CA struct {
	key  *ecdsa.PrivateKey
	cert *x509.Certificate
	aki  []byte

	// rogue signs the bad_signature fault: a response that is well-formed in
	// every respect, so the daemon reconstructs a certificate happily and only
	// the broker refuses it. That is the interesting failure, because it is
	// indistinguishable from a real DER mismatch until you look at the signer.
	rogue *ecdsa.PrivateKey
}

// NewCA generates a CA with a random authority key identifier.
func NewCA() (*CA, error) {
	aki := make([]byte, deviceAKILen)
	if _, err := rand.Read(aki); err != nil {
		return nil, fmt.Errorf("pki: generate authority key identifier: %w", err)
	}
	return NewCAWithAKI(aki)
}

// NewCAWithAKI generates a CA whose subject key identifier is aki, which is
// also the authority key identifier every certificate it issues will carry.
//
// The two MUST be the same value: OpenSSL cross-checks a certificate's AKI
// against the issuer's SKI when it builds the chain, so a CA that ignored the
// AKI it hands out would produce certificates that verify under Go and fail
// under the stack a real broker uses.
func NewCAWithAKI(aki []byte) (*CA, error) {
	if len(aki) != deviceAKILen {
		return nil, fmt.Errorf("pki: authority key identifier is %d bytes, want %d", len(aki), deviceAKILen)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("pki: generate CA key: %w", err)
	}
	rogue, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("pki: generate rogue key: %w", err)
	}

	// The CA subject is encoded by crypto/x509 from a pkix.Name, NOT by
	// Name.DER() in this package, and that asymmetry is deliberate. A chain
	// only builds if the issuer field inside the device certificate is
	// byte-identical to this subject; encoding both ends with the same code
	// would let a wrong DN cancel out and still build. Here the standard
	// library is the independent witness. The equality is asserted below
	// rather than assumed, because pkix.Name orders attributes differently
	// once a province or locality is present.
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               ArduinoIssuer.PKIX(),
		SubjectKeyId:          aki,
		NotBefore:             time.Now().Add(-24 * time.Hour),
		NotAfter:              time.Now().Add(20 * 365 * 24 * time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, fmt.Errorf("pki: create CA certificate: %w", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, fmt.Errorf("pki: parse CA certificate: %w", err)
	}

	issuerDER, err := ArduinoIssuer.DER()
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(cert.RawSubject, issuerDER) {
		return nil, fmt.Errorf(
			"pki: CA subject DN differs from the issuer this package encodes, no chain could ever build\n  x509:     %x\n  Name.DER: %x",
			cert.RawSubject, issuerDER)
	}

	return &CA{key: key, cert: cert, aki: aki, rogue: rogue}, nil
}

// Certificate returns the CA certificate.
func (ca *CA) Certificate() *x509.Certificate { return ca.cert }

// AKI returns the authority key identifier issued certificates carry.
func (ca *CA) AKI() []byte { return bytes.Clone(ca.aki) }

// CertPEM returns the CA certificate as PEM, which is what the daemon wants in
// ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE.
func (ca *CA) CertPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.cert.Raw})
}

// WriteCertPEM writes the CA certificate where the daemon under test can read
// it.
func (ca *CA) WriteCertPEM(path string) error {
	if err := os.WriteFile(path, ca.CertPEM(), 0o644); err != nil {
		return fmt.Errorf("pki: write CA certificate: %w", err)
	}
	return nil
}

// Pool returns a cert pool trusting this CA, for the broker's ClientCAs and
// for verification inside the harness.
func (ca *CA) Pool() *x509.CertPool {
	pool := x509.NewCertPool()
	pool.AddCert(ca.cert)
	return pool
}

// DeviceRequest is one issuance, as the Provisioning API would see it.
type DeviceRequest struct {
	// CSRPEM is the CSR the daemon submitted.
	CSRPEM string
	// DeviceID is the identity being assigned; it becomes the certificate
	// common name.
	DeviceID string
	// Serial is the certificate serial number; a random one is used when nil.
	Serial []byte
	// NotBefore defaults to the current hour when zero.
	NotBefore time.Time
}

// Issued is the result of one issuance: what the API answers, and what the
// harness knows it signed.
type Issued struct {
	DeviceID string
	// Subject is what was actually signed, reflected from the CSR. When the
	// daemon behaves it is exactly CN=<device_id>; anything else is the
	// interesting case (see ReflectCSRSubject).
	Subject   Name
	Serial    []byte
	NotBefore time.Time
	NotAfter  time.Time
	// CertDER is the certificate the CA signed. The daemon never receives it:
	// it reconstructs its own copy from ResponseBody, and crosscheck_test.go
	// is what asserts the two are identical.
	CertDER []byte
	// ResponseBody is the plain-text, pipe-delimited body of
	// POST /v1/onboarding/provision/csr.
	ResponseBody string
}

// IssueDevice signs a device certificate for the submitted CSR and returns the
// response body the real API would send back.
func (ca *CA) IssueDevice(req DeviceRequest) (Issued, error) {
	return ca.issue(req, ca.key)
}

// IssueDeviceBadSignature issues the same well-formed response signed by a key
// that is not the CA.
//
// This is the highest-value fault in the provisioning matrix: every length and
// every field is right, so the daemon parses the response, rebuilds the
// certificate and stores it without complaint. The failure only appears one
// component later, as the broker rejecting the handshake -- which is exactly
// what a real DER divergence looks like from the outside.
func (ca *CA) IssueDeviceBadSignature(req DeviceRequest) (Issued, error) {
	return ca.issue(req, ca.rogue)
}

func (ca *CA) issue(req DeviceRequest, signer *ecdsa.PrivateKey) (Issued, error) {
	if len(req.DeviceID) != deviceIDLen {
		return Issued{}, fmt.Errorf(
			"pki: device id %q is %d characters, want %d: the daemon rejects any other length",
			req.DeviceID, len(req.DeviceID), deviceIDLen)
	}
	subject, err := ReflectCSRSubject(req.CSRPEM, req.DeviceID)
	if err != nil {
		return Issued{}, err
	}
	pub, err := PublicKeyFromCSR(req.CSRPEM)
	if err != nil {
		return Issued{}, err
	}
	serial := req.Serial
	if serial == nil {
		serial = make([]byte, deviceSerialLen)
		if _, err := rand.Read(serial); err != nil {
			return Issued{}, fmt.Errorf("pki: generate serial: %w", err)
		}
	}
	notBefore := req.NotBefore
	if notBefore.IsZero() {
		notBefore = defaultNotBefore(time.Now())
	}

	device := DeviceCert{
		Subject:   subject,
		PublicKey: pub,
		Serial:    serial,
		AKI:       ca.aki,
		NotBefore: notBefore,
	}
	tbs, err := device.TBS()
	if err != nil {
		return Issued{}, err
	}
	digest := sha256.Sum256(tbs)
	r, s, err := ecdsa.Sign(rand.Reader, signer, digest[:])
	if err != nil {
		return Issued{}, fmt.Errorf("pki: sign TBS: %w", err)
	}
	// The API returns r and s as fixed-width 32-byte halves, not as a DER
	// signature, so they are padded rather than trimmed.
	signature := make([]byte, deviceSigLen)
	r.FillBytes(signature[:32])
	s.FillBytes(signature[32:])

	certDER, err := device.Certificate(tbs, signature)
	if err != nil {
		return Issued{}, err
	}
	encodedNotBefore, encodedNotAfter, err := device.Validity()
	if err != nil {
		return Issued{}, err
	}

	return Issued{
		DeviceID:     req.DeviceID,
		Subject:      subject,
		Serial:       serial,
		NotBefore:    encodedNotBefore,
		NotAfter:     encodedNotAfter,
		CertDER:      certDER,
		ResponseBody: responseBody(req.DeviceID, ca.aki, encodedNotBefore, serial, signature),
	}, nil
}

// responseBody renders the pipe-delimited body of provision/csr:
//
//	device_id|authority_key_identifier|not_before|serial|signature_x|signature_y
//
// The field set is the contract, restated here instead of imported: if the API
// ever adds a field, the daemon's parser must be what breaks, not a shared
// constant that keeps both sides agreeing.
func responseBody(deviceID string, aki []byte, notBefore time.Time, serial, signature []byte) string {
	return fmt.Sprintf("%s|%s|%s|%s|%s|%s",
		deviceID,
		hex.EncodeToString(aki),
		notBefore.UTC().Format("2006-01-02T15:04:05Z"),
		hex.EncodeToString(serial),
		hex.EncodeToString(signature[:32]),
		hex.EncodeToString(signature[32:]),
	)
}

// defaultNotBefore is the current hour, stepped back a day on 29 February.
//
// The step-back is not superstition: notAfter copies month and day and adds 31
// years, and 29 February 2028 plus 31 years is not a date (see
// DeviceCert.Validity). Rather than have the suite fail on one specific day
// every four years, the default picks the day before; a caller that passes 29
// February explicitly still gets the error, because then it is asking for
// something the firmware cannot encode.
func defaultNotBefore(now time.Time) time.Time {
	t := now.UTC().Truncate(time.Hour)
	if t.Month() == time.February && t.Day() == 29 {
		t = t.AddDate(0, 0, -1)
	}
	return t
}

// ServerCert issues the TLS server certificate the harness broker presents.
//
// This one is encoded by crypto/x509, not by this package: it is an ordinary
// server certificate verified by the daemon's own TLS stack, not a firmware
// contract, so there is nothing for an independent encoder to protect.
func (ca *CA) ServerCert(hosts ...string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("pki: generate server key: %w", err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("pki: generate server serial: %w", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "harness-broker"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tmpl.IPAddresses = append(tmpl.IPAddresses, ip)
			continue
		}
		tmpl.DNSNames = append(tmpl.DNSNames, h)
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("pki: create server certificate: %w", err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
