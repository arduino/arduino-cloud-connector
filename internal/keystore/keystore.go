// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package keystore manages EC P-256 key pairs and the device certificate
// on the local filesystem.
//
// Layout under DataDir:
//
//	private_provisioning_key.pem  — EC P-256 private key for board token (mode 0600)
//	public_provisioning_key.pem   — corresponding public key             (mode 0644)
//	private_cloud_key.pem         — EC P-256 private key for Cloud cert  (mode 0600)
//	device_cert.pem               — signed certificate from Provisioning API
//	device_id                     — UUID assigned by iot-api (plain text)
//	organization_id               — organization the device belongs to (plain
//	                                text; optional, written only when supplied
//	                                to /v1/provisioning/start)
//
// The thing_id is intentionally NOT persisted: the cloud is the source of truth
// and delivers it via the Thing.begin handshake on every connect.
package keystore

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

const (
	fileProvisioningPrivKey = "private_provisioning_key.pem"
	fileProvisioningPubKey  = "public_provisioning_key.pem"
	fileCloudPrivKey        = "private_cloud_key.pem"
	fileDeviceCert          = "device_cert.pem"
	fileDeviceID            = "device_id"
	fileOrganizationID      = "organization_id"
)

// Keystore manages credential files for the daemon.
type Keystore struct {
	dataDir string
}

// New creates a Keystore rooted at cfg.DataDir. On first call it generates
// EC P-256 key pairs if they do not already exist, and enforces correct
// file permissions on private keys.
func New(cfg config.Config) (*Keystore, error) {
	ks := &Keystore{dataDir: cfg.DataDir}

	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("keystore: create data dir: %w", err)
	}

	if err := ks.ensureKeyPair(fileProvisioningPrivKey, fileProvisioningPubKey); err != nil {
		return nil, fmt.Errorf("keystore: provisioning key pair: %w", err)
	}

	if err := ks.ensureKeyPair(fileCloudPrivKey, ""); err != nil {
		return nil, fmt.Errorf("keystore: cloud key pair: %w", err)
	}

	if err := ks.checkPrivKeyPermissions(fileProvisioningPrivKey); err != nil {
		return nil, err
	}
	if err := ks.checkPrivKeyPermissions(fileCloudPrivKey); err != nil {
		return nil, err
	}

	return ks, nil
}

// ProvisioningPrivateKey loads the provisioning private key from disk.
func (ks *Keystore) ProvisioningPrivateKey() (*ecdsa.PrivateKey, error) {
	return ks.loadPrivKey(fileProvisioningPrivKey)
}

// ProvisioningPublicKeyPEM returns the PEM-encoded provisioning public key.
func (ks *Keystore) ProvisioningPublicKeyPEM() (string, error) {
	data, err := os.ReadFile(filepath.Join(ks.dataDir, fileProvisioningPubKey))
	if err != nil {
		return "", fmt.Errorf("keystore: read public key: %w", err)
	}
	return string(data), nil
}

// CloudPrivateKey loads the cloud (mTLS) private key from disk.
func (ks *Keystore) CloudPrivateKey() (*ecdsa.PrivateKey, error) {
	return ks.loadPrivKey(fileCloudPrivKey)
}

// DeviceCertPEM returns the PEM-encoded device certificate, or an error if
// the certificate is not yet present (i.e. board not yet provisioned).
func (ks *Keystore) DeviceCertPEM() (string, error) {
	data, err := os.ReadFile(filepath.Join(ks.dataDir, fileDeviceCert))
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNotProvisioned
	}
	if err != nil {
		return "", fmt.Errorf("keystore: read device cert: %w", err)
	}
	return string(data), nil
}

// StoreDeviceCert writes the PEM-encoded device certificate to disk.
func (ks *Keystore) StoreDeviceCert(certPEM string) error {
	return ks.writeFile(fileDeviceCert, []byte(certPEM), 0o644)
}

// DeviceID returns the device UUID assigned by iot-api, or ErrNotProvisioned.
func (ks *Keystore) DeviceID() (string, error) {
	data, err := os.ReadFile(filepath.Join(ks.dataDir, fileDeviceID))
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNotProvisioned
	}
	if err != nil {
		return "", fmt.Errorf("keystore: read device_id: %w", err)
	}
	return string(data), nil
}

// StoreDeviceID writes the device UUID to disk.
func (ks *Keystore) StoreDeviceID(deviceID string) error {
	return ks.writeFile(fileDeviceID, []byte(deviceID), 0o644)
}

// OrganizationID returns the organization the device was provisioned into, or
// ErrNotProvisioned if none is on disk. The organization id is optional: it is
// stored only when supplied to /v1/provisioning/start, so its absence is a
// normal condition even for a fully provisioned board.
func (ks *Keystore) OrganizationID() (string, error) {
	data, err := os.ReadFile(filepath.Join(ks.dataDir, fileOrganizationID))
	if errors.Is(err, os.ErrNotExist) {
		return "", ErrNotProvisioned
	}
	if err != nil {
		return "", fmt.Errorf("keystore: read organization_id: %w", err)
	}
	return string(data), nil
}

// StoreOrganizationID writes the organization id to disk.
func (ks *Keystore) StoreOrganizationID(organizationID string) error {
	return ks.writeFile(fileOrganizationID, []byte(organizationID), 0o644)
}

// IsProvisioned reports whether a device certificate and device_id are present.
func (ks *Keystore) IsProvisioned() bool {
	_, err1 := os.Stat(filepath.Join(ks.dataDir, fileDeviceCert))
	_, err2 := os.Stat(filepath.Join(ks.dataDir, fileDeviceID))
	return err1 == nil && err2 == nil
}

// WipeCloudCredentials removes the device certificate, device_id and
// organization_id from disk. Called by POST /v1/provisioning/start to allow
// re-provisioning; clearing the organization_id means a stale one never
// survives into a fresh provisioning (the prelude writes the new one, if any,
// right after). Note: this does NOT touch the cloud private key — use
// RotateCloudKey for that.
func (ks *Keystore) WipeCloudCredentials() error {
	for _, f := range []string{fileDeviceCert, fileDeviceID, fileOrganizationID} {
		path := filepath.Join(ks.dataDir, f)
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("keystore: wipe %s: %w", f, err)
		}
	}
	return nil
}

// RotateCloudKey removes the existing cloud private key (if any) and generates
// a fresh EC P-256 key. Called at the start of every new provisioning flow so
// the CSR is always signed with newly minted key material.
func (ks *Keystore) RotateCloudKey() error {
	path := filepath.Join(ks.dataDir, fileCloudPrivKey)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("keystore: remove %s: %w", fileCloudPrivKey, err)
	}
	slog.Info("keystore: rotating cloud private key for new provisioning")
	if err := ks.ensureKeyPair(fileCloudPrivKey, ""); err != nil {
		return err
	}
	return ks.checkPrivKeyPermissions(fileCloudPrivKey)
}

// GenerateCSR produces a PEM PKCS#10 CSR for the cloud key, with uhwid as the
// CommonName.
//
// The subject MUST be CN-only. The API reflects the CSR's subject into the
// signed cert, but the daemon rebuilds it with a CN-only subject (see
// compressed_cert.go); any extra RDN (Country, Organization, …) makes the
// reconstructed cert diverge from what the CA signed, so the broker rejects it
// with "unknown certificate authority". A stray Country=IT was exactly that
// bug — do not re-add subject fields.
func (ks *Keystore) GenerateCSR(uhwid string) (string, error) {
	privKey, err := ks.CloudPrivateKey()
	if err != nil {
		return "", err
	}

	template := &x509.CertificateRequest{
		Subject: pkix.Name{
			CommonName: uhwid,
		},
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, template, privKey)
	if err != nil {
		return "", fmt.Errorf("keystore: generate CSR: %w", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})
	return string(csrPEM), nil
}

// ── Errors ────────────────────────────────────────────────────────────────────

// ErrNotProvisioned is returned when a credential that requires provisioning
// (device cert, device_id) is not yet present on disk.
var ErrNotProvisioned = errors.New("board not provisioned")

// ── internal helpers ──────────────────────────────────────────────────────────

// ensureKeyPair generates privKeyFile (and optionally pubKeyFile) if they
// do not already exist. Key material is never logged — only file names.
func (ks *Keystore) ensureKeyPair(privKeyFile, pubKeyFile string) error {
	privPath := filepath.Join(ks.dataDir, privKeyFile)
	if _, err := os.Stat(privPath); err == nil {
		return nil // already exists
	}

	if pubKeyFile != "" {
		slog.Info("keystore: key pair not found, generating new EC P-256 key pair",
			"dir", ks.dataDir, "private_key", privKeyFile, "public_key", pubKeyFile)
	} else {
		slog.Info("keystore: private key not found, generating new EC P-256 private key",
			"dir", ks.dataDir, "private_key", privKeyFile)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return fmt.Errorf("generate EC key: %w", err)
	}

	privDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return fmt.Errorf("marshal private key: %w", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})
	if err := ks.writeFile(privKeyFile, privPEM, 0o600); err != nil {
		return err
	}
	slog.Info("keystore: wrote private key", "file", privKeyFile, "mode", "0600")

	if pubKeyFile != "" {
		pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
		if err != nil {
			return fmt.Errorf("marshal public key: %w", err)
		}
		pubPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER})
		if err := ks.writeFile(pubKeyFile, pubPEM, 0o644); err != nil {
			return err
		}
		slog.Info("keystore: wrote public key", "file", pubKeyFile, "mode", "0644")
	}

	return nil
}

func (ks *Keystore) loadPrivKey(filename string) (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(filepath.Join(ks.dataDir, filename))
	if err != nil {
		return nil, fmt.Errorf("keystore: read %s: %w", filename, err)
	}
	block, _ := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("keystore: %s: invalid PEM", filename)
	}
	key, err := x509.ParseECPrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("keystore: parse %s: %w", filename, err)
	}
	return key, nil
}

// writeFile writes data to filename atomically: it writes a temporary file in
// the same directory, sets the mode explicitly (CreateTemp ignores umask but
// defaults to 0600), then renames it over the target. The rename is atomic on
// POSIX, so a crash mid-write never leaves a half-written credential — a reader
// sees either the old file or the complete new one.
func (ks *Keystore) writeFile(filename string, data []byte, mode os.FileMode) error {
	path := filepath.Join(ks.dataDir, filename)

	tmp, err := os.CreateTemp(ks.dataDir, "."+filename+".tmp-*")
	if err != nil {
		return fmt.Errorf("keystore: temp file for %s: %w", filename, err)
	}
	tmpName := tmp.Name()
	// Clean up the temp file on any error path; a no-op once the rename succeeds.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("keystore: write %s: %w", filename, err)
	}
	if err := tmp.Chmod(mode); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("keystore: chmod %s: %w", filename, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("keystore: close %s: %w", filename, err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("keystore: rename %s: %w", filename, err)
	}
	return nil
}
