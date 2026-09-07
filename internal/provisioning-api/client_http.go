// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provisioningapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// requestTimeout bounds a single call to the Provisioning API.
//
// It matters because the caller's bound is a wall-clock budget shared by the whole
// attempt (provisioning.provisioningWindow), spent across unconditional retries. With
// no per-call limit one connection that hangs — a silent middlebox, a half-open
// socket — consumes the entire budget in a single attempt, and the attempt then fails
// having tried exactly once. This is a stall detector, not a latency target: past this
// point the connection is stuck, and the budget is better spent on a retry.
const requestTimeout = 10 * time.Second

// NewClient returns the production HTTP-backed provisioning client. The
// mock variant of this function is defined in client_mock.go and selected
// when the binary is built with `-tags mock`.
func NewClient(cfg config.Config) Client {
	slog.Info("provisioning: using HTTP client", "endpoint", cfg.ProvisioningAPI)
	return &httpClient{
		cfg:  cfg,
		http: &http.Client{Timeout: requestTimeout},
	}
}

// httpClient is the real provisioning-api client.
type httpClient struct {
	cfg  config.Config
	http *http.Client
}

// csrRequest is the body of POST /v1/onboarding/provision/csr.
type csrRequest struct {
	CSR string `json:"csr"`
}

// SubmitCSR posts the CSR and reconstructs the device certificate from the
// response.
//
// The provisioning-api does NOT return a ready PEM. Exactly as on the
// microcontroller boards (see firmware CSRHandler.cpp), the response body is a
// plain-text, pipe-delimited string carrying only the variable parts of the
// certificate:
//
//	device_id|authority_key_identifier|not_before|serial|signature_asn1_x|signature_asn1_y
//
// The full X.509 certificate is rebuilt locally (reconstructDeviceCert): all
// other fields are fixed, and the CA signed the deterministic byte layout this
// daemon reproduces. See compressed_cert.go for details.
func (c *httpClient) SubmitCSR(ctx context.Context, boardToken, csr string) (deviceID, certPEM string, err error) {
	body, _ := json.Marshal(csrRequest{CSR: csr})
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.cfg.ProvisioningAPI+"/v1/onboarding/provision/csr",
		bytes.NewReader(body),
	)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+boardToken)

	resp, err := c.http.Do(req)
	if err != nil {
		return "", "", fmt.Errorf("POST provision/csr: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", fmt.Errorf("POST provision/csr: read response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("POST provision/csr: status %d: %s", resp.StatusCode, respBody)
	}

	deviceID, certPEM, err = reconstructDeviceCert(csr, string(respBody))
	if err != nil {
		return "", "", fmt.Errorf("POST provision/csr: %w", err)
	}
	return deviceID, certPEM, nil
}

type completeRequest struct {
	WifiFWVersion string `json:"wifi_fw_version"`
}

func (c *httpClient) Complete(ctx context.Context, boardToken string) error {
	body, _ := json.Marshal(completeRequest{})
	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.cfg.ProvisioningAPI+"/v1/onboarding/provision/complete",
		bytes.NewReader(body),
	)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+boardToken)

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("POST provision/complete: %w", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("POST provision/complete: status %d: %s", resp.StatusCode, b)
	}
	return nil
}
