// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/arduino/arduino-cloud-connector/internal/api/handlers"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/daemon"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// NewRouter builds the HTTP mux for the daemon REST API.
// All endpoints are served on 127.0.0.1 only.
//
// CORS policy allows wails:// and http://localhost:* (same as arduino-app-cli).
func NewRouter(
	cfg config.Config,
	version string,
	id *identity.Service,
	prov *provisioning.Service,
	reg *variables.Registry,
	d *daemon.Daemon,
) http.Handler {
	mux := http.NewServeMux()

	// ── System ────────────────────────────────────────────────────────────────
	mux.Handle("GET /v1/version", handlers.HandleVersion(version))
	mux.Handle("GET /v1/health", handlers.HandleHealth())

	// ── Identity & Provisioning (App Lab) ─────────────────────────────────────
	mux.Handle("GET /v1/identity", handlers.HandleIdentity(id))
	mux.Handle("GET /v1/status", handlers.HandleStatus(prov, d))
	mux.Handle("POST /v1/provisioning/start", handlers.HandleProvisioningStart(d))

	// ── Cloud Variables (Cloud Brick Apps) ────────────────────────────────────
	mux.Handle("PUT /v1/variables/{name}", handlers.HandleVariableSend(d))
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, d))

	return corsMiddleware(cfg, mux)
}

// corsMiddleware adds CORS headers for the exact allow-list in allowedOrigins
// (wails://wails, wails://wails.localhost:*, http://wails.localhost:*,
// http://localhost:* and https://localhost:*).
func corsMiddleware(_ config.Config, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if isAllowedOrigin(origin) {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, PATCH")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization, Content-Type, X-API-Key")
			w.Header().Set("Access-Control-Max-Age", "86400")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allowedOrigin is one CORS origin rule. scheme and host must match the request
// origin exactly — host is compared in full, never as a prefix. (The previous
// prefix check, origin[:16] == "http://localhost", also matched hostile origins
// such as http://localhost.attacker.com.) When anyPort is true the origin may
// carry any port or none; when false it must carry no port.
type allowedOrigin struct {
	scheme  string
	host    string
	anyPort bool
}

// allowedOrigins is the exact CORS allow-list (mirrors arduino-app-cli):
//
//	wails://wails
//	wails://wails.localhost:*
//	http://wails.localhost:*
//	http://localhost:*
//	https://localhost:*
var allowedOrigins = []allowedOrigin{
	{scheme: "wails", host: "wails", anyPort: false},
	{scheme: "wails", host: "wails.localhost", anyPort: true},
	{scheme: "http", host: "wails.localhost", anyPort: true},
	{scheme: "http", host: "localhost", anyPort: true},
	{scheme: "https", host: "localhost", anyPort: true},
}

// isAllowedOrigin reports whether origin is in the CORS allow-list. It parses
// the origin and matches scheme + host (+ port policy) exactly, so a host that
// merely starts with an allowed name (e.g. localhost.attacker.com) is rejected.
func isAllowedOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	// A well-formed Origin is exactly scheme://host[:port]; reject anything
	// carrying a path, query, fragment, userinfo or opaque part.
	if u.Opaque != "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	host, port := u.Hostname(), u.Port()
	for _, a := range allowedOrigins {
		if u.Scheme != a.scheme || !strings.EqualFold(host, a.host) {
			continue
		}
		if a.anyPort || port == "" {
			return true
		}
	}
	return false
}
