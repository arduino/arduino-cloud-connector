// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package storage is the fake storage service the daemon downloads App bundles
// from (RFC-14 §5.3).
//
// It is an HTTPS server with a certificate signed by the harness CA, because the
// daemon pins storage to the same root as the broker (MQTT_CA_FILE): a fake on
// plain HTTP, or on a certificate from anywhere else, would be refused before a
// single byte moved, and the refusal would be correct. It asks for the device
// certificate and records who presented it, without enforcing it: whether the
// real storage requires mTLS is not something this fake should decide.
//
// # Bundles are bytes, not Apps
//
// The daemon never opens a bundle (RFC-14 §5.9): it downloads it, checks the
// digest and hands the file to arduino-app-cli. So a bundle here is a block of
// deterministic pseudo-random bytes with a name and the App id it stands for —
// the fake arduino-app-cli resolves the digest of what it receives back to that
// id, which is how an install "contains" an App without anybody building an
// archive.
package storage

import (
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
)

// pathPrefix is where bundles are served from.
const pathPrefix = "/bundles/"

// defaultSize is the size of a bundle that does not declare one: small enough to
// arrive in one ranged request and with no intermediate progress report, which
// keeps the OTA.progress sequence of a scenario fixed.
const defaultSize = 4096

// Respond selects what the fake answers.
type Respond string

const (
	// RespondOK serves the requested range. The default.
	RespondOK Respond = "ok"
	// RespondStatus answers with Directive.Status and no body.
	RespondStatus Respond = "status"
	// RespondCorrupt serves the range with the right headers and the wrong
	// bytes, so the transfer succeeds and the digest check is what fails.
	RespondCorrupt Respond = "corrupt"
	// RespondHang accepts the request and never answers.
	RespondHang Respond = "hang"
	// RespondUnknown is recorded for a bundle that was never declared.
	RespondUnknown Respond = "unknown"
)

// Directive is one queued response for a bundle.
type Directive struct {
	Respond Respond       `yaml:"respond"`
	Status  int           `yaml:"status"`
	Delay   time.Duration `yaml:"delay"`
}

// Bundle declares one downloadable bundle.
type Bundle struct {
	// App is the arduino-app-cli id of the App the bundle stands for, which the
	// fake arduino-app-cli adds to its apps when it installs these bytes.
	App string `yaml:"app"`
	// Size is the bundle length in bytes. defaultSize when zero.
	Size int `yaml:"size"`
	// Responses are the queued answers; an exhausted queue serves the bundle.
	Responses []Directive `yaml:"responses"`
}

// Options configures the fake.
type Options struct {
	// Log receives one event per request. Required.
	Log *eventlog.Log
	// CA signs the server certificate and verifies the client's. Required.
	CA *pki.CA
}

// Server is a running fake.
type Server struct {
	log *eventlog.Log
	ca  *pki.CA

	http *http.Server
	base string

	mu      sync.Mutex
	bundles map[string]*bundle
}

type bundle struct {
	app         string
	content     []byte
	sha256      string
	directives  []Directive
	occurrences int
}

// Start binds an HTTPS listener on loopback and serves until Close.
func Start(opts Options) (*Server, error) {
	if opts.Log == nil {
		return nil, fmt.Errorf("storage: no event log given")
	}
	if opts.CA == nil {
		return nil, fmt.Errorf("storage: no CA given")
	}
	cert, err := opts.CA.ServerCert("localhost", "127.0.0.1")
	if err != nil {
		return nil, fmt.Errorf("storage: server certificate: %w", err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("storage: listen: %w", err)
	}
	s := &Server{
		log:     opts.Log,
		ca:      opts.CA,
		base:    "https://" + ln.Addr().String(),
		bundles: map[string]*bundle{},
	}
	s.http = &http.Server{
		Handler:           http.HandlerFunc(s.handle),
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{cert},
			// Requested, never required: see the package comment.
			ClientAuth: tls.RequestClientCert,
			MinVersion: tls.VersionTLS12,
		},
	}
	go func() {
		if err := s.http.ServeTLS(ln, "", ""); err != nil && err != http.ErrServerClosed {
			s.log.Append(eventlog.SourceStorage, eventlog.KindHarnessNote,
				map[string]any{"error": err.Error()}, nil)
		}
	}()
	return s, nil
}

// Add declares a bundle. Declaring the same name twice is a scenario mistake.
func (s *Server) Add(name string, b Bundle) error {
	if name == "" || strings.ContainsAny(name, "/?#") {
		return fmt.Errorf("storage: bundle name %q must be a plain path segment", name)
	}
	size := b.Size
	if size == 0 {
		size = defaultSize
	}
	if size < 0 {
		return fmt.Errorf("storage: bundle %s: size %d", name, size)
	}
	for i, d := range b.Responses {
		if err := validateDirective(d); err != nil {
			return fmt.Errorf("storage: bundle %s directive %d: %w", name, i+1, err)
		}
	}
	content := contentFor(name, size)
	sum := sha256.Sum256(content)

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.bundles[name]; dup {
		return fmt.Errorf("storage: bundle %s declared twice", name)
	}
	s.bundles[name] = &bundle{
		app:        b.App,
		content:    content,
		sha256:     hex.EncodeToString(sum[:]),
		directives: append([]Directive(nil), b.Responses...),
	}
	return nil
}

// Queue appends responses to a declared bundle at runtime.
func (s *Server) Queue(name string, ds ...Directive) error {
	for i, d := range ds {
		if err := validateDirective(d); err != nil {
			return fmt.Errorf("storage: bundle %s directive %d: %w", name, i+1, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bundles[name]
	if !ok {
		return fmt.Errorf("storage: no bundle %s", name)
	}
	b.directives = append(b.directives, ds...)
	return nil
}

// URL is the download URL of a bundle, declared or not: asking for an
// undeclared one is how a scenario gets a 404.
func (s *Server) URL(name string) string { return s.base + pathPrefix + name }

// SHA256 is the hex digest of a declared bundle.
func (s *Server) SHA256(name string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, ok := s.bundles[name]
	if !ok {
		return "", false
	}
	return b.sha256, true
}

// Lookup resolves a digest back to the bundle it belongs to and the App it
// stands for.
func (s *Server) Lookup(sha256Hex string) (name, app string, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for n, b := range s.bundles {
		if b.sha256 == sha256Hex {
			return n, b.app, true
		}
	}
	return "", "", false
}

// Close stops the server, closing rather than draining connections, for the
// same reason as the provisioning fake: a hang would block the teardown.
func (s *Server) Close() error {
	if err := s.http.Close(); err != nil {
		return fmt.Errorf("storage: close: %w", err)
	}
	return nil
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	name, _ := strings.CutPrefix(r.URL.Path, pathPrefix)

	s.mu.Lock()
	b, known := s.bundles[name]
	var (
		d          Directive
		occurrence int
		content    []byte
	)
	if known {
		b.occurrences++
		occurrence = b.occurrences
		if len(b.directives) > 0 {
			d, b.directives = b.directives[0], b.directives[1:]
		} else {
			d = Directive{Respond: RespondOK}
		}
		content = b.content
	}
	s.mu.Unlock()

	attrs := map[string]any{
		"bundle":     name,
		"method":     r.Method,
		"range":      r.Header.Get("Range"),
		"occurrence": occurrence,
	}
	for k, v := range s.clientCert(r) {
		attrs[k] = v
	}

	if !known {
		attrs["respond"], attrs["status"] = string(RespondUnknown), http.StatusNotFound
		s.log.Append(eventlog.SourceStorage, eventlog.KindHTTPRequest, attrs, nil)
		http.Error(w, "storage: no such bundle", http.StatusNotFound)
		return
	}

	attrs["respond"] = string(d.Respond)
	start, end, ranged, rangeErr := parseRange(r.Header.Get("Range"), len(content))
	status := http.StatusOK
	switch {
	case d.Respond == RespondStatus:
		status = d.Status
	case d.Respond == RespondHang:
		status = 0
	case rangeErr != nil:
		status = http.StatusRequestedRangeNotSatisfiable
	case ranged:
		status = http.StatusPartialContent
	}
	attrs["status"] = status
	// Appended on arrival, as the provisioning fake does: the timeline shows the
	// request when it happened even when the answer is delayed or never comes.
	s.log.Append(eventlog.SourceStorage, eventlog.KindHTTPRequest, attrs, nil)

	if d.Delay > 0 {
		select {
		case <-time.After(d.Delay):
		case <-r.Context().Done():
			return
		}
	}
	switch {
	case d.Respond == RespondHang:
		<-r.Context().Done()
		return
	case d.Respond == RespondStatus:
		w.WriteHeader(d.Status)
		return
	case rangeErr != nil:
		http.Error(w, rangeErr.Error(), status)
		return
	}

	body := content[start : end+1]
	if d.Respond == RespondCorrupt {
		body = append([]byte(nil), body...)
		for i := range body {
			body[i] ^= 0xff
		}
	}
	h := w.Header()
	h.Set("Content-Type", "application/octet-stream")
	h.Set("Content-Length", strconv.Itoa(len(body)))
	if ranged {
		h.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(content)))
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// clientCert records which certificate the daemon presented, if any, and
// whether it chains to the harness CA.
func (s *Server) clientCert(r *http.Request) map[string]any {
	if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
		return map[string]any{"client_cert_cn": "", "client_cert_valid": false}
	}
	leaf := r.TLS.PeerCertificates[0]
	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:     s.ca.Pool(),
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	})
	return map[string]any{"client_cert_cn": leaf.Subject.CommonName, "client_cert_valid": err == nil}
}

// parseRange reads "bytes=<start>-<end>", the only form the daemon sends. An
// empty header means the whole object.
func parseRange(v string, size int) (start, end int, ranged bool, err error) {
	if v == "" {
		return 0, size - 1, false, nil
	}
	spec, ok := strings.CutPrefix(v, "bytes=")
	if !ok {
		return 0, 0, false, fmt.Errorf("storage: unsupported range %q", v)
	}
	from, to, ok := strings.Cut(spec, "-")
	if !ok {
		return 0, 0, false, fmt.Errorf("storage: unsupported range %q", v)
	}
	if start, err = strconv.Atoi(from); err != nil {
		return 0, 0, false, fmt.Errorf("storage: range start in %q: %w", v, err)
	}
	if to == "" {
		end = size - 1
	} else if end, err = strconv.Atoi(to); err != nil {
		return 0, 0, false, fmt.Errorf("storage: range end in %q: %w", v, err)
	}
	if end > size-1 {
		end = size - 1
	}
	if start < 0 || start > end {
		return 0, 0, false, fmt.Errorf("storage: range %q outside %d bytes", v, size)
	}
	return start, end, true, nil
}

// contentFor derives a bundle's bytes from its name, so two bundles always
// differ and a run is reproducible.
func contentFor(name string, size int) []byte {
	out := make([]byte, 0, size+sha256.Size)
	var counter [8]byte
	for i := uint64(0); len(out) < size; i++ {
		binary.BigEndian.PutUint64(counter[:], i)
		sum := sha256.Sum256(append([]byte(name), counter[:]...))
		out = append(out, sum[:]...)
	}
	return out[:size]
}

func validateDirective(d Directive) error {
	switch d.Respond {
	case RespondOK, RespondCorrupt, RespondHang:
		return nil
	case RespondStatus:
		if d.Status < 100 || d.Status > 599 {
			return fmt.Errorf("respond: status needs a status code, got %d", d.Status)
		}
		return nil
	case "":
		return fmt.Errorf("respond is empty")
	default:
		return fmt.Errorf("unknown respond %q", d.Respond)
	}
}
