// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package provapi is the fake Arduino Provisioning API the daemon under test
// talks to. It is one of the harness boundaries: the daemon is pointed at it
// with ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API and never knows the
// difference.
//
// It does three things. It answers provision/csr by having the harness CA
// actually sign a certificate, so the daemon exercises its real
// reconstruction path rather than a canned PEM. It records every request in
// the event log, which is what makes retries assertable (each request carries
// its occurrence number) and what puts an unexpected call on the failure
// timeline. And it injects faults from a per-endpoint queue of directives, so
// a scenario can ask for two 503s before the certificate, or a response signed
// by the wrong key, without any conditional logic in the YAML.
//
// # Paths and names are hardcoded on purpose
//
// The endpoint paths below are written out rather than imported from the
// daemon. That is the point of the harness: if someone renames a route, the
// E2E suite must break. Importing the constant would hide exactly the
// regression the suite exists to catch.
package provapi

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
)

// The routes the daemon calls, relative to the configured base URL.
const (
	pathCSR      = "/v1/onboarding/provision/csr"
	pathComplete = "/v1/onboarding/provision/complete"
)

// Endpoint is how an endpoint is named in scenarios and on the timeline.
type Endpoint string

const (
	EndpointCSR      Endpoint = "provision/csr"
	EndpointComplete Endpoint = "provision/complete"
	// EndpointUnknown is recorded for any other path. The fake deliberately
	// does NOT serve the documented-but-unused /v1/onboarding/claim: a 404
	// plus a timeline row saying which path was called is far easier to read
	// than a fake that quietly answers a call nobody expected the daemon to
	// make.
	EndpointUnknown Endpoint = "unknown"
)

// Respond selects what the fake answers.
type Respond string

const (
	// RespondIssueCert signs a real certificate for the submitted CSR. The
	// happy default for provision/csr.
	RespondIssueCert Respond = "issue_cert"
	// RespondOK answers 200 with an empty body. The happy default for
	// provision/complete.
	RespondOK Respond = "ok"
	// RespondStatus answers with Directive.Status, for retry scenarios.
	RespondStatus Respond = "status"
	// RespondMalformed answers 200 with a body the daemon cannot parse, which
	// exercises the path where the transport succeeded and the payload did not.
	RespondMalformed Respond = "malformed"
	// RespondBadSignature answers 200 with a well-formed response signed by a
	// key that is not the CA: the daemon accepts and stores the certificate,
	// and only the broker later refuses it.
	RespondBadSignature Respond = "bad_signature"
	// RespondHang accepts the request and never answers, so the daemon's
	// per-call timeout is what ends it.
	RespondHang Respond = "hang"
)

// Directive is one queued response.
type Directive struct {
	Respond Respond       `yaml:"respond"`
	Status  int           `yaml:"status"`
	Body    string        `yaml:"body"`
	Delay   time.Duration `yaml:"delay"`
}

// Options configures the fake.
type Options struct {
	// Log receives one event per request. Required: an observation that is not
	// in the log cannot be asserted on.
	Log *eventlog.Log
	// CA issues the certificates. Required.
	CA *pki.CA
	// DeviceID is the identity assigned to whatever CSR arrives. A random UUID
	// is generated when empty.
	DeviceID string
	// Directives are the queued responses per endpoint; an exhausted queue
	// falls back to that endpoint's happy default.
	Directives map[Endpoint][]Directive
}

// Server is a running fake.
type Server struct {
	log      *eventlog.Log
	ca       *pki.CA
	deviceID string

	http *http.Server
	url  string

	mu          sync.Mutex
	directives  map[Endpoint][]Directive
	occurrences map[Endpoint]int
	issued      *pki.Issued
}

// Start binds a listener on loopback and serves until Close.
func Start(opts Options) (*Server, error) {
	if opts.Log == nil {
		return nil, fmt.Errorf("provapi: no event log given")
	}
	if opts.CA == nil {
		return nil, fmt.Errorf("provapi: no CA given")
	}
	deviceID := opts.DeviceID
	if deviceID == "" {
		var err error
		if deviceID, err = randomUUID(); err != nil {
			return nil, err
		}
	}
	directives := map[Endpoint][]Directive{}
	for endpoint, queue := range opts.Directives {
		for i, d := range queue {
			if err := validateDirective(d); err != nil {
				return nil, fmt.Errorf("provapi: %s directive %d: %w", endpoint, i+1, err)
			}
		}
		directives[endpoint] = append([]Directive(nil), queue...)
	}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("provapi: listen: %w", err)
	}
	s := &Server{
		log:         opts.Log,
		ca:          opts.CA,
		deviceID:    deviceID,
		url:         "http://" + ln.Addr().String(),
		directives:  directives,
		occurrences: map[Endpoint]int{},
	}
	s.http = &http.Server{
		Handler:           http.HandlerFunc(s.handle),
		ReadHeaderTimeout: 10 * time.Second,
	}
	go func() {
		// ErrServerClosed is the expected end; anything else is a harness bug
		// that must not be swallowed, so it lands on the timeline.
		if err := s.http.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.log.Append(eventlog.SourceProvisioningAPI, eventlog.KindHarnessNote,
				map[string]any{"error": err.Error()}, nil)
		}
	}()
	return s, nil
}

// URL is what ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API must be set to.
func (s *Server) URL() string { return s.url }

// DeviceID is the identity this fake assigns.
func (s *Server) DeviceID() string { return s.deviceID }

// Issued returns the certificate most recently signed, which is what the
// cross-check compares against the daemon's reconstruction. Absent until the
// first successful provision/csr.
func (s *Server) Issued() (pki.Issued, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.issued == nil {
		return pki.Issued{}, false
	}
	return *s.issued, true
}

// Queue appends directives to an endpoint at runtime, for a scenario that
// injects a fault partway through.
func (s *Server) Queue(endpoint Endpoint, ds ...Directive) error {
	for i, d := range ds {
		if err := validateDirective(d); err != nil {
			return fmt.Errorf("provapi: %s directive %d: %w", endpoint, i+1, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.directives[endpoint] = append(s.directives[endpoint], ds...)
	return nil
}

// Close stops the server.
//
// It closes connections rather than draining them: a scenario that used
// RespondHang leaves a handler parked on a request that will never be
// answered, and a graceful drain would block teardown for as long as the
// daemon's own timeout.
func (s *Server) Close() error {
	if err := s.http.Close(); err != nil {
		return fmt.Errorf("provapi: close: %w", err)
	}
	return nil
}

// plan is the decided response, before anything is written.
type plan struct {
	respond Respond
	status  int
	body    string
	delay   time.Duration
	hang    bool
	attrs   map[string]any
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	endpoint := endpointFor(r.URL.Path)
	body, readErr := io.ReadAll(r.Body)

	p := s.planResponse(endpoint, string(body), readErr)

	// The event is appended on ARRIVAL, carrying the response already decided,
	// so the timeline shows the call at the instant it happened even when the
	// answer is delayed or never comes. One event per request, because every
	// provisioning_api event is significant in the strict sweep and a second
	// row would have to be tolerated by every scenario.
	attrs := map[string]any{
		"endpoint":   string(endpoint),
		"method":     r.Method,
		"path":       r.URL.Path,
		"occurrence": s.countRequest(endpoint),
		"respond":    string(p.respond),
		"status":     p.status,
		// Only the presence of the board token is recorded, never its value: a
		// failing run uploads this timeline as a CI artifact.
		"auth_present": r.Header.Get("Authorization") != "",
	}
	for k, v := range p.attrs {
		attrs[k] = v
	}
	s.log.Append(eventlog.SourceProvisioningAPI, eventlog.KindHTTPRequest, attrs, body)

	if p.delay > 0 {
		select {
		case <-time.After(p.delay):
		case <-r.Context().Done():
			return
		}
	}
	if p.hang {
		<-r.Context().Done()
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(p.status)
	_, _ = io.WriteString(w, p.body)
}

// planResponse pops the next directive for the endpoint and turns it into a
// concrete response.
func (s *Server) planResponse(endpoint Endpoint, body string, readErr error) plan {
	if endpoint == EndpointUnknown {
		return plan{respond: RespondStatus, status: http.StatusNotFound,
			body: "provapi: no such endpoint"}
	}
	if readErr != nil {
		return plan{respond: RespondStatus, status: http.StatusBadRequest,
			body: fmt.Sprintf("provapi: read request body: %v", readErr)}
	}

	d := s.nextDirective(endpoint)
	switch d.Respond {
	case RespondStatus:
		return plan{respond: d.Respond, status: d.Status, body: d.Body, delay: d.Delay}
	case RespondHang:
		return plan{respond: d.Respond, hang: true, delay: d.Delay}
	case RespondMalformed:
		malformed := d.Body
		if malformed == "" {
			// Two fields where the daemon wants six: enough to fail its parse,
			// and recognisable in a log line.
			malformed = "provapi|malformed"
		}
		return plan{respond: d.Respond, status: http.StatusOK, body: malformed, delay: d.Delay}
	case RespondIssueCert, RespondBadSignature:
		if endpoint != EndpointCSR {
			// issue_cert only means something on provision/csr. Falling back to
			// a plain 200 keeps a mis-written scenario from looking like a
			// daemon failure.
			return plan{respond: RespondOK, status: http.StatusOK, body: d.Body, delay: d.Delay}
		}
		return s.issueCert(d, body)
	default: // RespondOK
		return plan{respond: RespondOK, status: http.StatusOK, body: d.Body, delay: d.Delay}
	}
}

// issueCert has the CA sign a certificate for the CSR in the request body.
func (s *Server) issueCert(d Directive, body string) plan {
	var req struct {
		CSR string `json:"csr"`
	}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		return plan{respond: d.Respond, status: http.StatusBadRequest,
			body: fmt.Sprintf("provapi: request body is not the documented {csr} JSON: %v", err)}
	}

	subject, err := pki.SubjectFromCSR(req.CSR)
	if err != nil {
		// A CSR the CA cannot parse or whose signature does not verify is a 400
		// here, as it would be at the real API. The subject goes on the
		// timeline either way, because it is the field that matters.
		return plan{respond: d.Respond, status: http.StatusBadRequest,
			body: fmt.Sprintf("provapi: %v", err)}
	}

	// csr_subject is the single most useful attribute the fake records. The
	// daemon must submit a CN-only subject; any extra attribute is reflected
	// into the signed certificate and breaks the daemon's own reconstruction,
	// so seeing "C=IT,CN=..." on this row names the bug outright.
	attrs := map[string]any{"csr_subject": subject.String()}

	issue := s.ca.IssueDevice
	if d.Respond == RespondBadSignature {
		issue = s.ca.IssueDeviceBadSignature
	}
	issued, err := issue(pki.DeviceRequest{CSRPEM: req.CSR, DeviceID: s.deviceID})
	if err != nil {
		return plan{respond: d.Respond, status: http.StatusInternalServerError,
			body: fmt.Sprintf("provapi: issue certificate: %v", err), attrs: attrs}
	}

	// A retry issues a fresh certificate; the last one is the one the daemon
	// keeps, so that is the one the cross-check compares against.
	s.mu.Lock()
	s.issued = &issued
	s.mu.Unlock()

	attrs["device_id"] = issued.DeviceID
	attrs["cert_subject"] = issued.Subject.String()
	return plan{respond: d.Respond, status: http.StatusOK, body: issued.ResponseBody,
		delay: d.Delay, attrs: attrs}
}

// nextDirective pops the endpoint queue, falling back to the happy default.
func (s *Server) nextDirective(endpoint Endpoint) Directive {
	s.mu.Lock()
	defer s.mu.Unlock()
	if queue := s.directives[endpoint]; len(queue) > 0 {
		s.directives[endpoint] = queue[1:]
		return queue[0]
	}
	if endpoint == EndpointCSR {
		return Directive{Respond: RespondIssueCert}
	}
	return Directive{Respond: RespondOK}
}

// countRequest increments and returns the 1-based occurrence number, which is
// what lets a scenario assert on the third attempt specifically.
func (s *Server) countRequest(endpoint Endpoint) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.occurrences[endpoint]++
	return s.occurrences[endpoint]
}

// endpointFor maps a request path to an endpoint name.
//
// The match is on the suffix so that a base URL with a path prefix (the
// production default is https://api2.arduino.cc/provisioning) works unchanged.
func endpointFor(path string) Endpoint {
	switch {
	case strings.HasSuffix(path, pathCSR):
		return EndpointCSR
	case strings.HasSuffix(path, pathComplete):
		return EndpointComplete
	default:
		return EndpointUnknown
	}
}

func validateDirective(d Directive) error {
	switch d.Respond {
	case RespondIssueCert, RespondOK, RespondMalformed, RespondBadSignature, RespondHang:
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

// randomUUID formats a random version-4 UUID. The daemon requires the
// canonical 36-character form and nothing else about it, so there is no reason
// to take a dependency for this.
func randomUUID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("provapi: generate device id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // RFC 4122 variant
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}
