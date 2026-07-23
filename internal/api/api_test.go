// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

func TestIsAllowedOrigin_Allowed(t *testing.T) {
	allowed := []string{
		"wails://wails",
		"wails://wails.localhost", // :* rule also accepts no port
		"wails://wails.localhost:59321",
		"http://wails.localhost:8080",
		"http://localhost",
		"http://localhost:3000",
		"https://localhost",
		"https://localhost:5000",
	}
	for _, o := range allowed {
		if !isAllowedOrigin(o) {
			t.Errorf("isAllowedOrigin(%q) = false, want true", o)
		}
	}
}

func TestIsAllowedOrigin_Denied(t *testing.T) {
	denied := []string{
		"",
		"http://localhost.attacker.com",      // the reported prefix bypass
		"http://localhost.attacker.com:3000", // …and with a port
		"http://attacker.localhost:9000",     // subdomain of .localhost → not host "localhost"; guards against suffix matching
		"https://localhost.evil.com",
		"http://wails.localhost.evil.com", // suffix past an allowed host
		"wails://wails.attacker.com",
		"wails://wailsX",     // host is a superstring of "wails"
		"wails://wails:1234", // exact wails://wails must carry no port
		"http://notlocalhost",
		"http://localhost@evil.com", // userinfo trick → real host is evil.com
		"http://evil.com",
		"https://evil.com",
		"ftp://localhost",          // scheme not allowed
		"http://localhost/../evil", // path present
		"http://127.0.0.1",         // loopback IP is not on the allow-list
		"http://127.0.0.1:3000",
	}
	for _, o := range denied {
		if isAllowedOrigin(o) {
			t.Errorf("isAllowedOrigin(%q) = true, want false", o)
		}
	}
}

func TestCorsMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	h := corsMiddleware(config.Config{}, next)

	do := func(method, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/v1/status", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// Allowed origin: Access-Control-Allow-Origin echoes it exactly.
	rec := do(http.MethodGet, "http://localhost:3000")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "http://localhost:3000" {
		t.Errorf("ACAO = %q, want the echoed allowed origin", got)
	}

	// Disallowed origin (the bypass): no CORS headers are emitted.
	rec = do(http.MethodGet, "http://localhost.attacker.com")
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("ACAO = %q, want empty for a disallowed origin", got)
	}

	// OPTIONS preflight from an allowed origin short-circuits with 204 + headers.
	rec = do(http.MethodOptions, "https://localhost:5000")
	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want 204", rec.Code)
	}
	if got := rec.Header().Get("Access-Control-Allow-Origin"); got != "https://localhost:5000" {
		t.Errorf("preflight ACAO = %q, want the echoed allowed origin", got)
	}
}
