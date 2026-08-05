// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package storageapi

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// Connection budgets. There is deliberately no overall http.Client.Timeout: a
// ranged chunk of a multi-gigabyte artefact legitimately takes minutes, and the
// whole transfer is bounded by the caller's context (internal/downloader applies
// config.DownloadTimeout). These bound only the phases that should never be slow.
const (
	dialTimeout          = 15 * time.Second
	tlsHandshakeTimeout  = 15 * time.Second
	responseHeaderBudget = 60 * time.Second
	idleConnTimeout      = 30 * time.Second
)

// httpClient is the real storage-api client.
type httpClient struct {
	cfg config.Config
	ks  CertProvider

	// newHTTP is the injection seam for tests: building the real client needs
	// device key material and a reachable TLS peer, neither of which a unit test
	// can afford.
	newHTTP func() (*http.Client, error)
}

func newHTTPClient(cfg config.Config, ks CertProvider) *httpClient {
	c := &httpClient{cfg: cfg, ks: ks}
	c.newHTTP = c.deviceHTTPClient
	return c
}

// FetchAppRelease implements Client.
func (c *httpClient) FetchAppRelease(ctx context.Context, rawURL string, start, end int64) (*Response, error) {
	if err := c.validateURL(rawURL); err != nil {
		return nil, err
	}
	if start < 0 || end < start {
		return nil, Terminal(ErrURLInvalid, "nonsensical range %d-%d", start, end)
	}

	hc, err := c.newHTTP()
	if err != nil {
		return nil, Retryable(ErrConnect, "build mTLS http client").Wrap(err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, Terminal(ErrURLInvalid, "build request").Wrap(err)
	}
	wantRange := fmt.Sprintf("bytes=%d-%d", start, end)
	req.Header.Set("Range", wantRange)

	resp, err := hc.Do(req)
	if err != nil {
		return nil, Retryable(ErrConnect, "GET artefact").Wrap(err)
	}

	out, err := c.classify(resp, start, wantRange)
	if err != nil {
		// The caller never received the body, so closing it is this function's job.
		// Drain-free: it may hold tens of MiB nobody is going to read.
		_ = resp.Body.Close()
		return nil, err
	}
	return out, nil
}

// classify turns an HTTP response into a Response, or into the failure that says
// why it cannot be used. It does not read the body.
func (c *httpClient) classify(resp *http.Response, wantStart int64, wantRange string) (*Response, error) {
	switch resp.StatusCode {
	case http.StatusPartialContent:
		start, total, err := parseContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return nil, Retryable(ErrBadHeaders, "bad Content-Range %q for %s",
				resp.Header.Get("Content-Range"), wantRange).Wrap(err)
		}
		// A response that starts somewhere other than where we asked cannot be
		// appended to a partial file — that would leave a hole or a duplicated span.
		// Reported here rather than by the caller so the check cannot be forgotten by
		// a second caller later.
		if start != wantStart {
			return nil, Retryable(ErrBadHeaders, "range starts at %d, asked for %s", start, wantRange)
		}
		return &Response{Body: resp.Body, Start: start, Total: total, Ranged: true}, nil

	case http.StatusOK:
		// The server ignored Range (or does not support it) and is sending the whole
		// object from byte 0.
		if resp.ContentLength < 0 {
			// Without a known length there is nothing to check the free space or the
			// byte count against.
			return nil, Terminal(ErrBadHeaders, "response has no Content-Length")
		}
		if resp.ContentLength == 0 {
			return nil, Terminal(ErrBadHeaders, "storage advertised an empty artefact")
		}
		return &Response{Body: resp.Body, Start: 0, Total: resp.ContentLength, Ranged: false}, nil

	default:
		return nil, c.statusFailure(resp)
	}
}

// statusFailure maps an unexpected HTTP status onto a failure, deciding whether it
// is worth another attempt. Server-side and rate-limit responses are transient;
// anything else in the 4xx range means this download will never succeed (an
// expired or revoked grant, an artefact that no longer exists).
func (c *httpClient) statusFailure(resp *http.Response) *Failure {
	transient := resp.StatusCode >= 500 ||
		resp.StatusCode == http.StatusRequestTimeout ||
		resp.StatusCode == http.StatusTooManyRequests
	if transient {
		return Retryable(ErrBadResponse, "storage returned %s", resp.Status)
	}
	return Terminal(ErrBadResponse, "storage returned %s", resp.Status)
}

// validateURL enforces the only invariant on a download URL: it must parse, and
// it must be https with a host.
//
// There is deliberately NO host allow-list. The URL is chosen entirely by the
// cloud and consumed verbatim, exactly as the C++ ArduinoIoTCloud library does it
// (OTAInterfaceDefault::startOTA checks the schema and hands parsed_url.host() to
// HttpClient — no configured host anywhere in the library, and the trust anchors
// it uses for the download differ per platform, so the fleet never assumes one
// storage host). Pinning a host here would mean a cloud-side move of the storage
// endpoint breaks every installed daemon until an env var is edited board by
// board, while the MCUs keep working.
//
// What actually protects the deploy: https only, the server certificate verified
// against config.CloudRootCAs, and above all the SHA-256 check the downloader runs
// before the bundle is promoted to its destination — so nothing unverified ever
// reaches the installer.
func (c *httpClient) validateURL(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return Terminal(ErrURLInvalid, "unparsable URL").Wrap(err)
	}
	if u.Scheme != "https" {
		return Terminal(ErrURLInvalid, "scheme is %q, only https is accepted", u.Scheme)
	}
	if u.Host == "" {
		return Terminal(ErrURLInvalid, "URL has no host")
	}
	return nil
}

// ── mTLS ─────────────────────────────────────────────────────────────────────

// deviceHTTPClient builds an HTTPS client authenticated with the device
// certificate — the same key pair and certificate used for the MQTT connection,
// which is why an App deploy needs no credential of its own (RFC-14 §5.3).
//
// The material is loaded on every call, not cached: a reprovision rotates the
// cloud key and issues a new certificate, and a long-lived cached client would
// keep presenting the revoked one.
//
// Server verification follows the same rule as the broker connection, via
// config.CloudRootCAs: ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE when set becomes the
// sole trusted root here too, otherwise the system store is used. The rule has to
// be shared because the trust is: storage presents a certificate signed by the
// private Arduino CA, the same one that signed the device certificate being
// presented here, so a pin that covered only the broker would break the download.
// The C++ library reaches the same conclusion on BearSSL boards, where
// TLSClientOta installs the very same ArduinoIoTCloudTrustAnchor set as the MQTT
// client.
func (c *httpClient) deviceHTTPClient() (*http.Client, error) {
	certPEM, err := c.ks.DeviceCertPEM()
	if err != nil {
		return nil, fmt.Errorf("load device cert: %w", err)
	}
	privKey, err := c.ks.CloudPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("load cloud private key: %w", err)
	}
	privDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("marshal cloud private key: %w", err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privDER})

	cert, err := tls.X509KeyPair([]byte(certPEM), privPEM)
	if err != nil {
		return nil, fmt.Errorf("build X.509 key pair: %w", err)
	}

	pool, err := c.cfg.CloudRootCAs()
	if err != nil {
		return nil, err
	}

	return &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      pool, // nil = system roots
				MinVersion:   tls.VersionTLS12,
				// No MaxVersion cap: the TLS-1.2 ceiling on the broker connection is a
				// workaround for a broker bug (see internal/mqtt), not a property of
				// storage, which should be free to negotiate TLS 1.3.
			},
			DialContext:           (&net.Dialer{Timeout: dialTimeout}).DialContext,
			TLSHandshakeTimeout:   tlsHandshakeTimeout,
			ResponseHeaderTimeout: responseHeaderBudget,
			IdleConnTimeout:       idleConnTimeout,
			ForceAttemptHTTP2:     true,
		},
	}, nil
}

// ── helpers ──────────────────────────────────────────────────────────────────

// parseContentRange parses "bytes <start>-<end>/<total>" and returns start and
// total. A "*" total is rejected: the download needs a definite size.
func parseContentRange(v string) (start, total int64, err error) {
	spec, ok := strings.CutPrefix(strings.TrimSpace(v), "bytes ")
	if !ok {
		return 0, 0, fmt.Errorf("missing %q unit prefix", "bytes")
	}
	rangeSpec, totalSpec, ok := strings.Cut(spec, "/")
	if !ok {
		return 0, 0, errors.New("missing /<total>")
	}
	startSpec, _, ok := strings.Cut(rangeSpec, "-")
	if !ok {
		return 0, 0, errors.New("missing <start>-<end>")
	}
	if start, err = strconv.ParseInt(strings.TrimSpace(startSpec), 10, 64); err != nil {
		return 0, 0, fmt.Errorf("start: %w", err)
	}
	if total, err = strconv.ParseInt(strings.TrimSpace(totalSpec), 10, 64); err != nil {
		return 0, 0, fmt.Errorf("total: %w", err)
	}
	if start < 0 || total <= 0 {
		return 0, 0, fmt.Errorf("nonsensical range %d/%d", start, total)
	}
	return start, total, nil
}
