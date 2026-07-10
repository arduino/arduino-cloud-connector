// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioningapi

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// csrResponseBody builds the pipe-delimited provision/csr response
// (device_id|aki|not_before|serial|sig_x|sig_y), signing the TBS the daemon
// reconstructs from the CSR.
func csrResponseBody(t *testing.T, csrPEM, deviceID string) string {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ca key: %v", err)
	}
	const akiHex = "1122334455667788990011223344556677889900"
	const serialHex = "0102030405060708090a0b0c0d0e0f10"
	const notBefore = "2024-01-15T10:30:00Z"

	pub, err := publicKeyFromCSR(csrPEM)
	if err != nil {
		t.Fatalf("pub from CSR: %v", err)
	}
	aki, _ := hex.DecodeString(akiHex)
	serial, _ := hex.DecodeString(serialHex)
	sig := signTBS(t, caKey, certParams{
		publicKey:   pub,
		deviceID:    deviceID,
		serial:      serial,
		authKeyID:   aki,
		issueYear:   2024,
		issueMonth:  1,
		issueDay:    15,
		issueHour:   10,
		expireYears: certExpireYears,
	})
	return strings.Join([]string{
		deviceID, akiHex, notBefore, serialHex,
		hex.EncodeToString(sig[0:32]), hex.EncodeToString(sig[32:64]),
	}, "|")
}

func TestSubmitCSR(t *testing.T) {
	const deviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
	csrPEM, _ := makeCSR(t, "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b")
	body := csrResponseBody(t, csrPEM, deviceID)

	var gotPath, gotAuth, gotCT string
	var gotReq csrRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotCT = r.Header.Get("Content-Type")
		_ = json.NewDecoder(r.Body).Decode(&gotReq)
		_, _ = io.WriteString(w, body)
	}))
	defer srv.Close()

	client := NewClient(config.Config{ProvisioningAPI: srv.URL})
	gotID, certPEM, err := client.SubmitCSR(context.Background(), "tok-123", csrPEM)
	if err != nil {
		t.Fatalf("SubmitCSR: %v", err)
	}

	// Request shape.
	if gotPath != "/v1/onboarding/provision/csr" {
		t.Errorf("path: got %q", gotPath)
	}
	if gotAuth != "Bearer tok-123" {
		t.Errorf("Authorization: got %q want %q", gotAuth, "Bearer tok-123")
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type: got %q", gotCT)
	}
	if gotReq.CSR != csrPEM {
		t.Errorf("request body CSR mismatch")
	}

	// Response: device id returned and a parseable cert reconstructed.
	if gotID != deviceID {
		t.Errorf("device id: got %q want %q", gotID, deviceID)
	}
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatal("returned cert is not a CERTIFICATE PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	if cert.Subject.CommonName != deviceID {
		t.Errorf("cert CN: got %q want %q", cert.Subject.CommonName, deviceID)
	}
}

func TestSubmitCSRNon200(t *testing.T) {
	csrPEM, _ := makeCSR(t, "deadbeef")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "request not authorized", http.StatusUnauthorized)
	}))
	defer srv.Close()

	client := NewClient(config.Config{ProvisioningAPI: srv.URL})
	if _, _, err := client.SubmitCSR(context.Background(), "tok", csrPEM); err == nil {
		t.Fatal("expected error on 401 response")
	} else if !strings.Contains(err.Error(), "401") {
		t.Errorf("error should mention status 401: %v", err)
	}
}

func TestComplete(t *testing.T) {
	var gotPath, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(config.Config{ProvisioningAPI: srv.URL})
	if err := client.Complete(context.Background(), "tok-xyz"); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if gotPath != "/v1/onboarding/provision/complete" {
		t.Errorf("path: got %q", gotPath)
	}
	if gotAuth != "Bearer tok-xyz" {
		t.Errorf("Authorization: got %q", gotAuth)
	}
}

func TestCompleteNon200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	client := NewClient(config.Config{ProvisioningAPI: srv.URL})
	if err := client.Complete(context.Background(), "tok"); err == nil {
		t.Fatal("expected error on 500 response")
	}
}
