// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package api

import (
	"net/http"

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

// corsMiddleware adds CORS headers to allow wails://, http://wails.localhost:*, http://localhost:* and https://localhost:*.
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

func isAllowedOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	// Allow Wails desktop apps and local development servers.
	// Matches wails://wails and wails://wails.localhost:*
	if len(origin) >= 13 && origin[:13] == "wails://wails" {
		return true
	}
	if len(origin) >= 16 && origin[:16] == "http://localhost" {
		return true
	}
	if len(origin) >= 17 && origin[:17] == "https://localhost" {
		return true
	}
	if len(origin) >= 22 && origin[:22] == "http://wails.localhost" {
		return true
	}
	return false
}
