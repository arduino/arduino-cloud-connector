// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package handlers contains the HTTP handler functions for the daemon REST API.
package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"

	"github.com/arduino/arduino-cloud-connector/internal/cloud"
	"github.com/arduino/arduino-cloud-connector/internal/daemon"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// ── System ────────────────────────────────────────────────────────────────────

func HandleVersion(version string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"version": version})
	})
}

func HandleHealth() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

// ── Identity & Provisioning ───────────────────────────────────────────────────

type identityResponse struct {
	// BoardToken is a short-lived ES256 JWT whose `iss` claim carries the UHWID
	// the server uses to look up the board. See identity.Service.BoardToken.
	BoardToken   string `json:"board_token"`
	PublicKeyPEM string `json:"public_key_pem"`
	// UHWID is the board's hardware ID (also the token's `iss`), exposed here
	// for convenience.
	UHWID string `json:"uhwid"`
}

func HandleIdentity(id *identity.Service) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		privKey, err := id.ProvisioningPrivateKey()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "cannot load provisioning key", err)
			return
		}

		token, err := id.BoardToken(privKey)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "cannot generate board token", err)
			return
		}

		pubKeyPEM, err := id.PublicKeyPEM()
		if err != nil {
			writeError(w, http.StatusInternalServerError, "cannot load public key", err)
			return
		}

		writeJSON(w, http.StatusOK, identityResponse{
			BoardToken:   token,
			PublicKeyPEM: pubKeyPEM,
			UHWID:        id.UHWID(),
		})
	})
}

type cloudStatus struct {
	State   cloud.State `json:"state"`
	ThingID string      `json:"thing_id,omitempty"`
}

type statusResponse struct {
	Provisioning provisioning.State `json:"provisioning"`
	Daemon       daemon.State       `json:"daemon"`
	Cloud        *cloudStatus       `json:"cloud,omitempty"`
	// DeviceID and OrganizationID are present only when stored on disk; the
	// organization id is optional even for a provisioned board.
	DeviceID       string `json:"device_id,omitempty"`
	OrganizationID string `json:"organization_id,omitempty"`
}

func HandleStatus(prov *provisioning.Service, d *daemon.Daemon) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		snap := d.Snapshot()
		resp := statusResponse{
			Provisioning: prov.State(),
			Daemon:       snap.State,
		}
		if deviceID, err := prov.DeviceID(); err == nil {
			resp.DeviceID = deviceID
		}
		if orgID, err := prov.OrganizationID(); err == nil {
			resp.OrganizationID = orgID
		}
		if snap.Cloud != nil {
			resp.Cloud = &cloudStatus{
				State:   snap.Cloud.State,
				ThingID: snap.Cloud.ThingID,
			}
		}
		writeJSON(w, http.StatusOK, resp)
	})
}

// startProvisioningRequest is the optional body of POST /v1/provisioning/start.
// organization_id is optional: when absent or empty the board is provisioned
// without an associated organization.
type startProvisioningRequest struct {
	OrganizationID string `json:"organization_id"`
}

// HandleProvisioningStart requests the daemon FSM to (re)provision. It blocks
// until the synchronous prelude — write the in-flight marker, wipe the old
// credentials and persist the optional organization id — has run, so the
// response reflects whether that committed step succeeded; the longer CSR
// attempt then proceeds asynchronously. The request body is optional and may
// carry an "organization_id". Returns 202 on success, 409 if a provisioning is
// already in progress (or the FSM is not ready), and 500 if the prelude itself
// failed (e.g. a disk error writing the marker or wiping credentials), in which
// case the caller can safely retry.
func HandleProvisioningStart(d *daemon.Daemon) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The body is optional; an empty body (no organization id) is valid.
		var req startProvisioningRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			writeError(w, http.StatusBadRequest, "invalid request body", err)
			return
		}
		if err := d.Reprovision(r.Context(), req.OrganizationID); err != nil {
			if errors.Is(err, daemon.ErrBusy) {
				writeError(w, http.StatusConflict, "provisioning already in progress", err)
				return
			}
			writeError(w, http.StatusInternalServerError, "provisioning prelude failed", err)
			return
		}
		writeJSON(w, http.StatusAccepted, map[string]string{
			"status": "accepted",
		})
	})
}

// ── Cloud Variables ───────────────────────────────────────────────────────────

type sendValueRequest struct {
	Value any `json:"value"`
}

// HandleVariableSend queues a variable's value for ordered delivery to the
// cloud (stored in the registry and published in FIFO order by the daemon's
// outbound worker). The variable is created on first use. Returns 204 once
// queued. If no thing is assigned yet (cloud not steady) the value is NOT queued
// and the handler returns 409 with error "thing_unavailable", so the app can log
// it and keep the value locally until the cloud syncs.
func HandleVariableSend(d *daemon.Daemon) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		var req sendValueRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request body", err)
			return
		}
		if err := d.EnqueueVariable(r.Context(), name, req.Value); err != nil {
			if errors.Is(err, daemon.ErrThingUnavailable) {
				writeError(w, http.StatusConflict, "thing_unavailable", err)
				return
			}
			writeError(w, http.StatusServiceUnavailable, "could not queue value", err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

// steadyReporter reports whether the cloud has an assigned thing and has
// completed its initial last-values sync (Steady). Implemented by *daemon.Daemon.
type steadyReporter interface {
	CloudSteady() bool
}

// HandleVariableEvents streams Server-Sent Events for a variable. The first
// frame is always one of three sync events telling the app how to seed its
// local value:
//
//   - "thing_unavailable": the cloud has no thing assigned yet. The app keeps
//     its local value; a resync frame ("lastvalue"/"lastvalue_missing") is sent
//     later when the cloud reaches Steady (see Registry.NotifyResync).
//   - "lastvalue": the thing is assigned and this variable has a cloud last
//     value (replayed with last_value:true) — the app resolves per sync policy.
//   - "lastvalue_missing": the thing is assigned but this variable has no cloud
//     value — the local value wins and is pushed up.
//
// Every subsequent live change is an "update" event. Multiple apps may
// subscribe to the same variable concurrently — each gets its own first frame.
//
// The connection is established immediately even for an unknown/never-set
// variable: the response headers are flushed up front, before the first frame,
// otherwise the client's EventSource would not open until a write occurs.
func HandleVariableEvents(reg *variables.Registry, steady steadyReporter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")

		snapshot, hasValue, sub := reg.Subscribe(name)
		defer reg.Unsubscribe(name, sub)

		flusher, ok := w.(http.Flusher)
		if !ok {
			writeError(w, http.StatusInternalServerError, "streaming unsupported", errors.New("response writer is not a flusher"))
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush() // open the stream now, before the first frame

		// First frame: the sync state the app resolves against.
		var (
			kind variables.EventKind
			err  error
		)
		switch {
		case !steady.CloudSteady():
			kind = variables.EventThingUnavailable
			err = writeSSE(w, kind, map[string]string{"name": name})
		case hasValue:
			kind = variables.EventLastValue
			err = writeSSE(w, kind, snapshot)
		default:
			kind = variables.EventLastValueMissing
			err = writeSSE(w, kind, map[string]string{"name": name})
		}
		if err != nil {
			// Client gone before the seed, or the payload could not be sent.
			slog.Error("sse: failed to write first frame", "name", name, "event", string(kind), "error", err)
			return
		}
		flusher.Flush()

		for {
			select {
			case <-r.Context().Done():
				return
			case evt, open := <-sub.Events():
				if !open {
					return
				}
				if err := writeSSE(w, evt.Kind, evt); err != nil {
					slog.Error("sse: failed to write event", "name", name, "event", string(evt.Kind), "error", err)
					return
				}
				flusher.Flush()
			}
		}
	})
}

// ── helpers ───────────────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// maxSSEPayload bounds the JSON payload of a single SSE frame. Variable values
// are small scalars or compact structured objects, orders of magnitude below
// this, so a larger payload is anomalous and is rejected rather than sent. The
// bound is also what keeps writeSSE's frame-size computation provably within
// int: len(data) comes from an arbitrary (cloud-controlled) value, and an
// unbounded length summed into the make() capacity could overflow and panic on
// a 32-bit build.
const maxSSEPayload = 1 * 1024 * 1024 // 1 MiB

// writeSSE writes a single named Server-Sent Event with a JSON data payload.
//
// It takes a plain io.Writer (not the http.ResponseWriter): the payload is an
// SSE stream carrying JSON, not an HTML page, so html/template escaping does
// not apply. The whole frame is assembled into one byte slice and written in a
// single Write.
//
// It returns an error when the value cannot be marshalled, when the payload
// exceeds maxSSEPayload, or when the write fails; the caller ends the stream on
// error.
func writeSSE(w io.Writer, event variables.EventKind, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("sse: marshal payload: %w", err)
	}
	if len(data) > maxSSEPayload {
		return fmt.Errorf("sse: payload too large: %d bytes (max %d)", len(data), maxSSEPayload)
	}
	// len(data) is now bounded by maxSSEPayload and event is a short internal
	// constant, so this capacity computation cannot overflow int.
	frame := make([]byte, 0, len("event: \ndata: \n\n")+len(event)+len(data))
	frame = append(frame, "event: "...)
	frame = append(frame, event...)
	frame = append(frame, "\ndata: "...)
	frame = append(frame, data...)
	frame = append(frame, "\n\n"...)
	if _, err := w.Write(frame); err != nil {
		return fmt.Errorf("sse: write frame: %w", err)
	}
	return nil
}

func writeError(w http.ResponseWriter, status int, msg string, err error) {
	writeJSON(w, status, map[string]string{
		"error":   msg,
		"details": err.Error(),
	})
}
