// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package provisioningapitest provides a synthetic, in-memory implementation of
// provisioningapi.Client for automated tests.
//
// It used to be the `mock` build-tag variant of the production client. Since
// the daemon now always talks to the real Provisioning API (in both the
// default and the `-tags mock` binary), the mock no longer ships in any
// binary: it lives here as a test helper only, importable from any package's
// tests via NewMockClient.
//
// The device certificates it issues are chained to a per-instance in-memory CA
// and will be rejected by the real Arduino IoT Cloud broker — they are only
// structurally valid (a matching public key and a parseable X.509 cert), which
// is all an offline unit test needs.
package provisioningapitest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	provisioningapi "github.com/arduino/arduino-cloud-connector/internal/provisioning-api"
)

// Option configures a mock client built by NewMockClient.
type Option func(*mockClient)

// WithLatency makes each mock call sleep for d (respecting context
// cancellation) before returning, so a test can observe the intermediate
// provisioning state. The default is 0 (calls return immediately).
func WithLatency(d time.Duration) Option {
	return func(m *mockClient) { m.Latency = d }
}

// NewMockClient returns a provisioningapi.Client backed by a per-instance
// in-memory CA. Each call generates a fresh CA; device certificates issued by
// the returned client during its lifetime all chain to that same CA, so
// multiple provisioning runs against one client produce consistent material.
func NewMockClient(cfg config.Config, opts ...Option) provisioningapi.Client {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		// Cannot recover; bail out loud — this is test-only tooling.
		panic(fmt.Errorf("mock provisioning: generate CA key: %w", err))
	}

	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Arduino Mock CA"},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(365 * 24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		panic(fmt.Errorf("mock provisioning: self-sign CA: %w", err))
	}
	caCert, err := x509.ParseCertificate(caDER)
	if err != nil {
		panic(fmt.Errorf("mock provisioning: parse mock CA: %w", err))
	}

	m := &mockClient{
		cfg:    cfg,
		caKey:  caKey,
		caCert: caCert,
	}
	for _, o := range opts {
		o(m)
	}
	return m
}

type mockClient struct {
	cfg    config.Config
	caKey  *ecdsa.PrivateKey
	caCert *x509.Certificate

	// Latency is the simulated per-call delay. Tests can set it to 0 for speed.
	Latency time.Duration
}

func (m *mockClient) SubmitCSR(ctx context.Context, boardToken, csrPEM string) (deviceID, certPEM string, err error) {
	if err := m.simulateLatency(ctx); err != nil {
		return "", "", err
	}

	block, _ := pem.Decode([]byte(csrPEM))
	if block == nil {
		return "", "", fmt.Errorf("mock provisioning: CSR is not valid PEM")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return "", "", fmt.Errorf("mock provisioning: parse CSR: %w", err)
	}
	if err := csr.CheckSignature(); err != nil {
		return "", "", fmt.Errorf("mock provisioning: CSR signature invalid: %w", err)
	}

	deviceID, err = newUUIDv4()
	if err != nil {
		return "", "", fmt.Errorf("mock provisioning: generate device_id: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return "", "", fmt.Errorf("mock provisioning: generate serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   deviceID,
			Organization: []string{"Arduino Mock"},
		},
		NotBefore:   time.Now().Add(-1 * time.Hour),
		NotAfter:    time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, m.caCert, csr.PublicKey, m.caKey)
	if err != nil {
		return "", "", fmt.Errorf("mock provisioning: sign device cert: %w", err)
	}

	certPEM = string(pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: certDER,
	}))

	slog.Info("provisioning (mock): CSR signed",
		"device_id", deviceID,
		"subject_common_name", csr.Subject.CommonName,
		"board_token_present", boardToken != "")
	return deviceID, certPEM, nil
}

func (m *mockClient) Complete(ctx context.Context, boardToken string) error {
	if err := m.simulateLatency(ctx); err != nil {
		return err
	}
	slog.Info("provisioning (mock): complete acknowledged",
		"board_token_present", boardToken != "")
	return nil
}

// simulateLatency sleeps for m.Latency or returns early if the context is
// cancelled — mirrors how a real HTTP call would behave under cancellation
// (ctx.Err() bubbles up).
func (m *mockClient) simulateLatency(ctx context.Context) error {
	if m.Latency <= 0 {
		return ctx.Err()
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(m.Latency):
		return nil
	}
}

// newUUIDv4 produces a RFC 4122 v4 UUID string without pulling in an external
// dependency. 16 random bytes with the version + variant bits patched.
func newUUIDv4() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant RFC 4122
	h := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32]), nil
}
