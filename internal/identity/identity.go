// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package identity computes the board's UHWID and produces the JWT board token
// used during the Provisioning 2.0 claim flow.
package identity

import (
	"context"
	"crypto/ecdsa"
	"fmt"
	"log/slog"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// Service exposes identity information for the running board.
// It holds a reference to the keystore to access provisioning keys.
type Service struct {
	uhwid string
	ks    keystoreIface
}

// keystoreIface is the subset of keystore.Keystore used by identity.Service.
// Defined as an interface to keep the import cycle-free and aid testing.
type keystoreIface interface {
	ProvisioningPrivateKey() (*ecdsa.PrivateKey, error)
	ProvisioningPublicKeyPEM() (string, error)
}

// New computes and validates the UHWID at startup. Returns an error if no
// stable hardware identifier could be found, or one wrapping ctx.Err() if ctx
// is cancelled while waiting for the hardware identifiers to appear.
//
// The computeUHWID implementation is selected at compile time via build tag:
//   - Default build           → uhwid.go        (hashes MAC + CPU serial)
//   - `go build -tags mock`   → uhwid_mock.go   (random + persisted to disk)
func New(ctx context.Context, cfg config.Config, ks keystoreIface) (*Service, error) {
	uhwid, err := computeUHWID(ctx, cfg)
	if err != nil {
		return nil, err
	}
	slog.Info("UHWID computed", "uhwid", uhwid)
	return &Service{uhwid: uhwid, ks: ks}, nil
}

// NewWithUHWID builds a Service from an explicit UHWID, skipping hardware
// fingerprinting. It exists for tests that need a deterministic identity
// without depending on the host's hardware; production code uses New.
func NewWithUHWID(uhwid string, ks keystoreIface) *Service {
	return &Service{uhwid: uhwid, ks: ks}
}

// UHWID returns the board's unique hardware ID (hex-encoded SHA-256).
func (s *Service) UHWID() string { return s.uhwid }

// ProvisioningPrivateKey returns the EC P-256 private key used to sign board tokens.
func (s *Service) ProvisioningPrivateKey() (*ecdsa.PrivateKey, error) {
	return s.ks.ProvisioningPrivateKey()
}

// PublicKeyPEM returns the PEM-encoded provisioning public key.
func (s *Service) PublicKeyPEM() (string, error) {
	return s.ks.ProvisioningPublicKeyPEM()
}

// BoardToken generates a short-lived ES256 JWT authenticating the board to the
// Provisioning API. The server reads `iss` (= UHWID) to find the board, checks
// the ES256 signature, and enforces a ~10-min window on `iat`.
func (s *Service) BoardToken(privKey *ecdsa.PrivateKey) (string, error) {
	now := time.Now().UTC()
	claims := jwt.MapClaims{
		"iss": s.uhwid, // required: used by server to look up the board identity
		"iat": now.Unix(),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
	signed, err := token.SignedString(privKey)
	if err != nil {
		return "", fmt.Errorf("sign board token: %w", err)
	}
	return signed, nil
}
