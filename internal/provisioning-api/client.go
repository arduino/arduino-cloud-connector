// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package provisioningapi is the HTTP client for the Arduino Provisioning API,
// split out of internal/provisioning so HTTP concerns stay behind the Client
// interface. NewClient returns the real HTTP client in every build (including
// `-tags mock`, which differs only in UHWID source and keystore checks); an
// offline test double lives in the provisioningapitest subpackage.
//
// Endpoint contracts (from provisioning-api/internal/v1/):
//
//	POST /v1/onboarding/claim
//	  body:  { board_token, device_name, connection_type }
//	  resp:  { id: <onboarding_uuid> }
//
//	POST /v1/onboarding/provision/csr   (board-token auth)
//	  body:  { csr: "<PEM>" }
//	  resp:  plain-text, pipe-delimited (NOT JSON):
//	         device_id|authority_key_identifier|not_before|serial|sig_x|sig_y
//	  NOTE: the API returns only the variable parts of the certificate, exactly
//	        as for microcontroller boards. The daemon reconstructs the full PEM
//	        locally (see compressed_cert.go), reproducing the firmware's
//	        deterministic DER layout so the CA signature verifies.
//
//	POST /v1/onboarding/provision/complete  (board-token auth)
//	  body:  { wifi_fw_version: "" }
//	  resp:  200 OK
package provisioningapi

import "context"

// Client abstracts the network side of the Provisioning 2.0 flow; the
// provisioning package owns the local side (keys, CSR, state) and delegates the
// round-trips here. Test doubles must honour the same response shapes.
type Client interface {
	// SubmitCSR posts the CSR (board-token auth) and returns the assigned
	// device_id and device certificate PEM, reconstructed from the API's
	// pipe-delimited response (see compressed_cert.go).
	SubmitCSR(ctx context.Context, boardToken, csr string) (deviceID, certPEM string, err error)

	// Complete signals provision/complete.
	Complete(ctx context.Context, boardToken string) error
}
