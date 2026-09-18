// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package handlers_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/arduino/arduino-cloud-connector/internal/api/handlers"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/daemon"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// fakeKeystore satisfies the keystore interface identity.NewWithUHWID expects
// (the interface is unexported but its methods are, so this is assignable).
type fakeKeystore struct {
	key *ecdsa.PrivateKey
	pub string
}

func (f fakeKeystore) ProvisioningPrivateKey() (*ecdsa.PrivateKey, error) { return f.key, nil }
func (f fakeKeystore) ProvisioningPublicKeyPEM() (string, error)          { return f.pub, nil }

const testUHWID = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"

// fakeSteady stands in for the daemon's CloudSteady reporter in SSE handler tests.
type fakeSteady struct{ steady bool }

func (f fakeSteady) CloudSteady() bool { return f.steady }

func TestHandleIdentity(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pubDER, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatalf("marshal pub: %v", err)
	}
	pubPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}))

	id := identity.NewWithUHWID(testUHWID, fakeKeystore{key: key, pub: pubPEM})

	rec := httptest.NewRecorder()
	handlers.HandleIdentity(id).ServeHTTP(
		rec, httptest.NewRequest(http.MethodGet, "/v1/identity", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d want 200 (body=%s)", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type: got %q", ct)
	}

	var resp struct {
		BoardToken   string `json:"board_token"`
		PublicKeyPEM string `json:"public_key_pem"`
		UHWID        string `json:"uhwid"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	// uhwid is exposed top-level in the response (the branch change).
	if resp.UHWID != testUHWID {
		t.Errorf("uhwid: got %q want %q", resp.UHWID, testUHWID)
	}
	if resp.PublicKeyPEM != pubPEM {
		t.Errorf("public_key_pem mismatch")
	}
	if resp.BoardToken == "" {
		t.Fatal("board_token empty")
	}

	// The board token's iss claim must carry the same uhwid.
	parsed, err := jwt.Parse(resp.BoardToken, func(tok *jwt.Token) (any, error) {
		if _, ok := tok.Method.(*jwt.SigningMethodECDSA); !ok {
			return nil, fmt.Errorf("unexpected signing method %q", tok.Method.Alg())
		}
		return &key.PublicKey, nil
	}, jwt.WithValidMethods([]string{"ES256"}))
	if err != nil {
		t.Fatalf("parse board token: %v", err)
	}
	claims := parsed.Claims.(jwt.MapClaims)
	if iss, _ := claims["iss"].(string); iss != testUHWID {
		t.Errorf("token iss: got %q want %q", iss, testUHWID)
	}
}

// A GET on a variable's events stream must establish the SSE connection
// immediately, even when the variable is unknown / has never been given a
// value. Regression test: previously the response headers stayed buffered
// until the first event, so EventSource did not open until a value arrived.
func TestVariableEventsOpensForUnknownVariable(t *testing.T) {
	reg := variables.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, fakeSteady{false}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/v1/variables/brandnew/events", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}

	// Do returns as soon as the response headers arrive. With the fix they are
	// flushed up front; without it Do would block until a value is published.
	type result struct {
		resp *http.Response
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		resp, err := http.DefaultClient.Do(req) //nolint:bodyclose
		ch <- result{resp, err}
	}()

	select {
	case res := <-ch:
		if res.err != nil {
			t.Fatalf("Do: %v", res.err)
		}
		defer res.resp.Body.Close() //nolint:errcheck
		if res.resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", res.resp.StatusCode)
		}
		if ct := res.resp.Header.Get("Content-Type"); ct != "text/event-stream" {
			t.Fatalf("Content-Type = %q, want text/event-stream", ct)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SSE connection did not establish for an unknown variable")
	}
}

// readFirstSSEEvent opens the events stream for name and returns the SSE event
// name and data payload of the first frame the daemon sends.
func readFirstSSEEvent(t *testing.T, srvURL, name string) (event, data string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srvURL+"/v1/variables/"+name+"/events", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	type line struct {
		s   string
		err error
	}
	lines := make(chan line)
	go func() {
		reader := bufio.NewReader(resp.Body)
		for {
			s, err := reader.ReadString('\n')
			lines <- line{s, err}
			if err != nil {
				return
			}
		}
	}()

	for {
		select {
		case ln := <-lines:
			if ln.err != nil {
				t.Fatalf("read SSE: %v", ln.err)
			}
			s := strings.TrimRight(ln.s, "\n")
			switch {
			case s == "": // blank line terminates the first event
				return event, data
			case strings.HasPrefix(s, "event:"):
				event = strings.TrimSpace(strings.TrimPrefix(s, "event:"))
			case strings.HasPrefix(s, "data:"):
				data = strings.TrimSpace(strings.TrimPrefix(s, "data:"))
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for first SSE event")
		}
	}
}

// When the cloud is not steady (no thing assigned), the first SSE frame is
// "thing_unavailable".
func TestVariableEventsFirstFrameThingUnavailable(t *testing.T) {
	reg := variables.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, fakeSteady{false}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	event, _ := readFirstSSEEvent(t, srv.URL, "temp")
	if event != string(variables.EventThingUnavailable) {
		t.Errorf("first event = %q, want %q", event, variables.EventThingUnavailable)
	}
}

// When the cloud is steady but the variable has no stored value, the first SSE
// frame is "lastvalue_missing".
func TestVariableEventsFirstFrameLastValueMissing(t *testing.T) {
	reg := variables.NewRegistry()
	mux := http.NewServeMux()
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, fakeSteady{true}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	event, _ := readFirstSSEEvent(t, srv.URL, "temp")
	if event != string(variables.EventLastValueMissing) {
		t.Errorf("first event = %q, want %q", event, variables.EventLastValueMissing)
	}
}

// When the cloud is steady and the variable has a stored value, the first SSE
// frame is "lastvalue" carrying that value.
func TestVariableEventsFirstFrameLastValue(t *testing.T) {
	reg := variables.NewRegistry()
	reg.SetValue("temp", float64(21.5), time.Date(2026, 7, 7, 10, 0, 0, 0, time.UTC))
	mux := http.NewServeMux()
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, fakeSteady{true}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	event, data := readFirstSSEEvent(t, srv.URL, "temp")
	if event != string(variables.EventLastValue) {
		t.Fatalf("first event = %q, want %q", event, variables.EventLastValue)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatalf("unmarshal data %q: %v", data, err)
	}
	if payload["value"] != 21.5 {
		t.Errorf("payload value = %v, want 21.5", payload["value"])
	}
	if payload["last_value"] != true {
		t.Errorf("payload last_value = %v, want true", payload["last_value"])
	}
}

// failingResponseWriter is an http.ResponseWriter + http.Flusher whose Write
// always fails, used to drive the SSE handler's write-error path.
type failingResponseWriter struct {
	header   http.Header
	writeErr error
}

func (f *failingResponseWriter) Header() http.Header       { return f.header }
func (f *failingResponseWriter) Write([]byte) (int, error) { return 0, f.writeErr }
func (f *failingResponseWriter) WriteHeader(int)           {}
func (f *failingResponseWriter) Flush()                    {}

// When writing an SSE frame fails (e.g. the client disconnected), the handler
// logs the failure at ERROR level and ends the stream instead of failing
// silently.
func TestVariableEventsLogsWriteError(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelError})))
	defer slog.SetDefault(prev)

	reg := variables.NewRegistry()
	h := handlers.HandleVariableEvents(reg, fakeSteady{false})

	req := httptest.NewRequest(http.MethodGet, "/v1/variables/temp/events", nil)
	req.SetPathValue("name", "temp")
	w := &failingResponseWriter{header: make(http.Header), writeErr: errors.New("broken pipe")}

	done := make(chan struct{})
	go func() {
		h.ServeHTTP(w, req)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after a write error")
	}

	out := logBuf.String()
	if !strings.Contains(out, "level=ERROR") || !strings.Contains(out, "sse:") || !strings.Contains(out, "name=temp") {
		t.Errorf("expected an ERROR log about the sse write failure, got %q", out)
	}
}

// A PUT while no thing is assigned (cloud not steady) is rejected with 409 and
// error "thing_unavailable", and is NOT queued.
func TestVariableSendRejectsWhenThingUnavailable(t *testing.T) {
	reg := variables.NewRegistry()
	// A freshly-created daemon is in CheckInternet (not steady).
	d := daemon.New(config.Config{}, nil, reg, nil, nil)
	mux := http.NewServeMux()
	mux.Handle("PUT /v1/variables/{name}", handlers.HandleVariableSend(d))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPut, srv.URL+"/v1/variables/temp", strings.NewReader(`{"value":42}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	defer resp.Body.Close() //nolint:errcheck

	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409", resp.StatusCode)
	}
	var payload map[string]string
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload["error"] != "thing_unavailable" {
		t.Errorf("error = %q, want thing_unavailable", payload["error"])
	}
}

// The registry holds the BOARD's value, so an app subscribing while the cloud
// is not steady still gets "lastvalue" as long as the board has a value — not
// "thing_unavailable". Two apps sharing a variable must start from the same
// number whatever the cloud is doing; before the fix the not-steady check came
// first and the second app started blind.
func TestVariableEventsFirstFrameLastValueWhileNotSteady(t *testing.T) {
	reg := variables.NewRegistry()
	reg.SetValue("temp", float64(42), time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC))
	mux := http.NewServeMux()
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, fakeSteady{false}))
	srv := httptest.NewServer(mux)
	defer srv.Close()

	event, data := readFirstSSEEvent(t, srv.URL, "temp")
	if event != string(variables.EventLastValue) {
		t.Fatalf("first event = %q, want %q", event, variables.EventLastValue)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		t.Fatalf("unmarshal data %q: %v", data, err)
	}
	if payload["value"] != float64(42) {
		t.Errorf("value = %v, want 42", payload["value"])
	}
	if payload["last_value"] != true {
		t.Errorf("last_value = %v, want true", payload["last_value"])
	}
}
