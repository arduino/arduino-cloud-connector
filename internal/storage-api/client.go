// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package storageapi is the HTTP client for the Arduino Storage Service: it knows
// how to ask that service for a byte range of an App Release, and nothing else.
//
//	GET <url>   with  Range: bytes=<start>-<end>
//	            mTLS-authenticated with the device certificate
//	            server verified per config.CloudRootCAs
//
// # What this package deliberately does not know
//
// It holds no files, no digests, no retry budget and no notion of a deploy job. It
// issues one request, normalises what came back, and hands the open body to the
// caller. Everything that makes a multi-gigabyte transfer survivable — the partial
// file, the resumable SHA-256, checkpointing, chunk sizing, the retry budget, the
// free-space pre-flight, the final verification — lives in internal/downloader,
// which drives this client.
//
// Mapping failures onto anything user-visible (for the App-deploy flow: the
// OTAProgressCmd wire codes) is further up still, in internal/ota.
package storageapi

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"io"
	"log/slog"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// Client fetches byte ranges of App Release artefacts from the Arduino Storage
// Service.
type Client interface {
	// FetchAppRelease issues one GET for the byte range [start, end] of the App
	// Release bundle at url. On success the returned Response holds an OPEN body
	// that the caller must close; on error nothing is left open.
	//
	// The URL is validated on every call (https, with a host) rather than trusted
	// from a previous one, so a malformed URL can never reach the network. It is
	// otherwise used verbatim, host included — the cloud owns it.
	//
	// Errors wrap one of the Err* sentinels below and are carried by *Failure, so
	// the caller's retry loop can ask whether the attempt is worth repeating.
	FetchAppRelease(ctx context.Context, url string, start, end int64) (*Response, error)
}

// Response is one ranged GET, normalised.
//
// Its job is to spare the caller the difference between the two shapes storage can
// answer with: a 206 carrying Content-Range, and a 200 carrying the whole object
// because the server ignored Range. Both arrive here as a start offset, a total
// size and a flag — one shape to write a transfer loop against.
type Response struct {
	// Body is the artefact bytes, open. The caller closes it.
	Body io.ReadCloser
	// Start is the first byte the server is actually sending. With Ranged it comes
	// from Content-Range; otherwise it is 0.
	Start int64
	// Total is the full size of the artefact, always known: a response that does
	// not reveal it is rejected as ErrBadHeaders, since neither the free-space
	// check nor the byte accounting can proceed without it.
	Total int64
	// Ranged reports whether the server honoured the Range request. When false it
	// is streaming the whole object from byte 0, so the caller must not splice the
	// body onto a partial file, and must not ask for a further range — that would
	// fetch the whole object again, forever.
	Ranged bool
}

// Failure sentinels. Every error FetchAppRelease returns wraps exactly one of
// these, so callers can classify an outcome with errors.Is without parsing
// messages. Disk- and transfer-level sentinels live in internal/downloader.
var (
	// ErrURLInvalid: the URL is malformed, or not https.
	ErrURLInvalid = errors.New("storageapi: invalid download URL")
	// ErrConnect: storage could not be reached (DNS, TCP, TLS).
	ErrConnect = errors.New("storageapi: cannot reach storage")
	// ErrBadResponse: storage answered with an unexpected HTTP status.
	ErrBadResponse = errors.New("storageapi: unexpected storage response")
	// ErrBadHeaders: the response headers are missing or contradict the request
	// (no Content-Length, an unparsable Content-Range).
	ErrBadHeaders = errors.New("storageapi: missing or contradictory response headers")
)

// CertProvider supplies the device mTLS material. Satisfied by
// *keystore.Keystore; an interface so this package needs no keystore import and
// tests can hand over throw-away material.
type CertProvider interface {
	CloudPrivateKey() (*ecdsa.PrivateKey, error)
	DeviceCertPEM() (string, error)
}

// NewClient returns the real HTTP client, authenticated with the device
// certificate from ks.
func NewClient(cfg config.Config, ks CertProvider) Client {
	slog.Info("storage: using HTTP client", "pinned_ca", cfg.MQTTCAFile != "")
	return newHTTPClient(cfg, ks)
}
