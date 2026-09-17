// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package provapi

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
)

const (
	testUHWID    = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"
	testDeviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
	testToken    = "board-token-not-to-be-logged"
)

type fixture struct {
	log *eventlog.Log
	ca  *pki.CA
	srv *Server
}

func newFixture(t *testing.T, directives map[Endpoint][]Directive) fixture {
	t.Helper()
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	log := eventlog.New()
	srv, err := Start(Options{Log: log, CA: ca, DeviceID: testDeviceID, Directives: directives})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return fixture{log: log, ca: ca, srv: srv}
}

// newCSR builds a CSR the way the daemon does: EC P-256, CN-only subject.
func newCSR(t *testing.T, subject pkix.Name) string {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{Subject: subject}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der}))
}

// post sends a request the way the daemon's client does, including the board
// token, and returns the status and body.
func post(t *testing.T, client *http.Client, url, path, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+testToken)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", path, err)
	}
	defer resp.Body.Close() //nolint:errcheck
	out, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return resp.StatusCode, string(out)
}

func postCSR(t *testing.T, f fixture, csrPEM string) (int, string) {
	t.Helper()
	body, err := json.Marshal(map[string]string{"csr": csrPEM})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return post(t, http.DefaultClient, f.srv.URL(), pathCSR, string(body))
}

// lastEvent returns the most recent event, failing when there is none.
func lastEvent(t *testing.T, log *eventlog.Log) eventlog.Event {
	t.Helper()
	events := log.Events()
	if len(events) == 0 {
		t.Fatal("no events recorded")
	}
	return events[len(events)-1]
}

func attr(t *testing.T, e eventlog.Event, name string) any {
	t.Helper()
	v, ok := e.Attrs[name]
	if !ok {
		t.Fatalf("event has no %q attribute: %v", name, e.Attrs)
	}
	return v
}

// The happy path, plus the attributes a scenario asserts on. Exactly one event
// per request matters: every provisioning_api event is significant in the
// strict sweep, so a second row would have to be tolerated everywhere.
func TestCSRRequestIssuesACertificateAndRecordsOneEvent(t *testing.T) {
	f := newFixture(t, nil)
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	status, body := postCSR(t, f, csrPEM)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", status, body)
	}
	if fields := strings.Split(body, "|"); len(fields) != 6 {
		t.Fatalf("response has %d fields, want 6: %q", len(fields), body)
	}
	if _, ok := f.srv.Issued(); !ok {
		t.Fatal("no certificate recorded as issued")
	}

	events := f.log.Events()
	if len(events) != 1 {
		t.Fatalf("recorded %d events for one request, want exactly 1", len(events))
	}
	e := events[0]
	if e.Source != eventlog.SourceProvisioningAPI || e.Kind != eventlog.KindHTTPRequest {
		t.Errorf("event is %s/%s, want provisioning_api/http_request", e.Source, e.Kind)
	}
	if !e.Significant() {
		t.Error("a provisioning API call must be significant for the strict sweep")
	}
	for name, want := range map[string]any{
		"endpoint":     string(EndpointCSR),
		"method":       http.MethodPost,
		"path":         pathCSR,
		"occurrence":   1,
		"respond":      string(RespondIssueCert),
		"status":       http.StatusOK,
		"auth_present": true,
		"device_id":    testDeviceID,
		"csr_subject":  "CN=" + testUHWID,
	} {
		if got := attr(t, e, name); got != want {
			t.Errorf("attrs[%q] = %v, want %v", name, got, want)
		}
	}
}

// The board token authenticates the device and a failing run uploads the
// timeline as a CI artifact, so the value must never reach the log -- only the
// fact that a header was there.
func TestTheBoardTokenIsNeverRecorded(t *testing.T) {
	f := newFixture(t, nil)
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	postCSR(t, f, csrPEM)

	e := lastEvent(t, f.log)
	if got := fmt.Sprint(e.Attrs); strings.Contains(got, testToken) {
		t.Errorf("the board token leaked into the event attributes: %s", got)
	}
	if strings.Contains(string(e.Raw), testToken) {
		t.Error("the board token leaked into the recorded request body")
	}
}

// A polluted CSR is served, not refused: the divergence must reach the daemon,
// which is what makes the scenario fail where the bug is. The subject on the
// timeline is what names it.
func TestAPollutedCSRSubjectIsServedAndVisibleOnTheTimeline(t *testing.T) {
	f := newFixture(t, nil)
	csrPEM := newCSR(t, pkix.Name{Country: []string{"IT"}, CommonName: testUHWID})

	status, body := postCSR(t, f, csrPEM)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body %q)", status, body)
	}

	e := lastEvent(t, f.log)
	if got, want := attr(t, e, "csr_subject"), "C=IT,CN="+testUHWID; got != want {
		t.Errorf("csr_subject = %v, want %v", got, want)
	}
	if got, want := attr(t, e, "cert_subject"), "C=IT,CN="+testDeviceID; got != want {
		t.Errorf("cert_subject = %v, want %v", got, want)
	}
}

// Retry scenarios depend on both halves of this: the queue is consumed in
// order, and once it runs out the endpoint goes back to its happy default so a
// scenario can drive failure into success.
func TestDirectiveQueueIsConsumedInOrderThenFallsBack(t *testing.T) {
	f := newFixture(t, map[Endpoint][]Directive{
		EndpointCSR: {
			{Respond: RespondStatus, Status: http.StatusServiceUnavailable},
			{Respond: RespondStatus, Status: http.StatusServiceUnavailable},
			{Respond: RespondIssueCert},
		},
	})
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	for i, want := range []int{503, 503, 200, 200} {
		status, body := postCSR(t, f, csrPEM)
		if status != want {
			t.Fatalf("call %d: status = %d, want %d (body %q)", i+1, status, want, body)
		}
	}

	events := f.log.Events()
	if len(events) != 4 {
		t.Fatalf("recorded %d events, want 4", len(events))
	}
	for i, e := range events {
		if got, want := attr(t, e, "occurrence"), i+1; got != want {
			t.Errorf("event %d: occurrence = %v, want %v", i, got, want)
		}
	}
	// occurrence is what a scenario keys on to assert the third attempt
	// specifically, so it must count every call, failures included.
	if got, want := attr(t, events[2], "respond"), string(RespondIssueCert); got != want {
		t.Errorf("third call responded %v, want %v", got, want)
	}
}

// provision/complete has its own queue and its own default; a scenario that
// fails it exercises the path where a certificate is issued but never
// activated, and the broker then refuses an otherwise valid certificate.
func TestCompleteDefaultsToOKAndHonoursItsQueue(t *testing.T) {
	f := newFixture(t, map[Endpoint][]Directive{
		EndpointComplete: {{Respond: RespondStatus, Status: http.StatusInternalServerError}},
	})

	if status, body := post(t, http.DefaultClient, f.srv.URL(), pathComplete, `{"wifi_fw_version":""}`); status != 500 {
		t.Fatalf("first complete: status = %d, want 500 (body %q)", status, body)
	}
	if status, _ := post(t, http.DefaultClient, f.srv.URL(), pathComplete, `{"wifi_fw_version":""}`); status != 200 {
		t.Fatalf("second complete: status = %d, want 200", status)
	}

	events := f.log.Events()
	if len(events) != 2 {
		t.Fatalf("recorded %d events, want 2", len(events))
	}
	if got, want := attr(t, events[0], "endpoint"), string(EndpointComplete); got != want {
		t.Errorf("endpoint = %v, want %v", got, want)
	}
}

// bad_signature must be indistinguishable from a good response at the HTTP
// level: the daemon has to accept it and store the certificate, so that the
// only symptom is the broker refusing the connection later.
func TestBadSignatureIsWellFormedButDoesNotVerify(t *testing.T) {
	f := newFixture(t, map[Endpoint][]Directive{
		EndpointCSR: {{Respond: RespondBadSignature}},
	})
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	status, body := postCSR(t, f, csrPEM)
	if status != http.StatusOK {
		t.Fatalf("status = %d, want 200: a bad signature must not look like an HTTP failure", status)
	}
	if fields := strings.Split(body, "|"); len(fields) != 6 {
		t.Fatalf("response has %d fields, want 6", len(fields))
	}

	issued, ok := f.srv.Issued()
	if !ok {
		t.Fatal("no certificate recorded as issued")
	}
	leaf, err := x509.ParseCertificate(issued.CertDER)
	if err != nil {
		t.Fatalf("the certificate must still parse: %v", err)
	}
	if err := leaf.CheckSignatureFrom(f.ca.Certificate()); err == nil {
		t.Error("the rogue-signed certificate verified against the CA")
	}
}

func TestMalformedResponseIsServedWith200(t *testing.T) {
	f := newFixture(t, map[Endpoint][]Directive{
		EndpointCSR: {{Respond: RespondMalformed}, {Respond: RespondMalformed, Body: "nonsense"}},
	})
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	status, body := postCSR(t, f, csrPEM)
	if status != http.StatusOK {
		t.Errorf("status = %d, want 200: the transport succeeds, the payload does not parse", status)
	}
	if fields := strings.Split(body, "|"); len(fields) == 6 {
		t.Errorf("the malformed body has the right field count, so it is not malformed: %q", body)
	}
	if _, body := postCSR(t, f, csrPEM); body != "nonsense" {
		t.Errorf("body override ignored: got %q", body)
	}
}

// A call to a path the harness does not serve must be loud. The documented but
// unused /v1/onboarding/claim is the case in point: if the daemon ever starts
// calling it, the timeline has to say so instead of a fake quietly answering.
func TestUnknownPathIs404AndRecorded(t *testing.T) {
	f := newFixture(t, nil)

	status, _ := post(t, http.DefaultClient, f.srv.URL(), "/v1/onboarding/claim", `{}`)
	if status != http.StatusNotFound {
		t.Errorf("status = %d, want 404", status)
	}

	e := lastEvent(t, f.log)
	if got, want := attr(t, e, "endpoint"), string(EndpointUnknown); got != want {
		t.Errorf("endpoint = %v, want %v", got, want)
	}
	if got, want := attr(t, e, "path"), "/v1/onboarding/claim"; got != want {
		t.Errorf("path = %v, want %v", got, want)
	}
}

// The base URL may carry a path prefix in production
// (https://api2.arduino.cc/provisioning), so routing matches on the suffix.
func TestEndpointRoutingIgnoresABasePathPrefix(t *testing.T) {
	tests := map[string]Endpoint{
		pathCSR:                        EndpointCSR,
		"/provisioning" + pathCSR:      EndpointCSR,
		pathComplete:                   EndpointComplete,
		"/provisioning" + pathComplete: EndpointComplete,
		"/v1/onboarding/claim":         EndpointUnknown,
		"/":                            EndpointUnknown,
	}
	for path, want := range tests {
		if got := endpointFor(path); got != want {
			t.Errorf("endpointFor(%q) = %q, want %q", path, got, want)
		}
	}
}

// A hung endpoint must leave the daemon's own per-call timeout in charge, and
// the call still has to appear on the timeline -- otherwise a stall looks like
// a request that was never made.
func TestHangNeverAnswersButIsStillRecorded(t *testing.T) {
	f := newFixture(t, map[Endpoint][]Directive{
		EndpointCSR: {{Respond: RespondHang}},
	})
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})
	body, err := json.Marshal(map[string]string{"csr": csrPEM})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	client := &http.Client{Timeout: 200 * time.Millisecond}
	req, err := http.NewRequest(http.MethodPost, f.srv.URL()+pathCSR, strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if resp, err := client.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatal("the hung endpoint answered")
	}

	e := lastEvent(t, f.log)
	if got, want := attr(t, e, "respond"), string(RespondHang); got != want {
		t.Errorf("respond = %v, want %v", got, want)
	}
	if got, want := attr(t, e, "status"), 0; got != want {
		t.Errorf("status = %v, want %v: nothing was answered", got, want)
	}
}

func TestDelayPostponesTheResponse(t *testing.T) {
	const delay = 80 * time.Millisecond
	f := newFixture(t, map[Endpoint][]Directive{
		EndpointComplete: {{Respond: RespondOK, Delay: delay}},
	})

	start := time.Now()
	if status, _ := post(t, http.DefaultClient, f.srv.URL(), pathComplete, `{}`); status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	if elapsed := time.Since(start); elapsed < delay {
		t.Errorf("answered after %s, want at least %s", elapsed, delay)
	}
}

// A CSR the CA cannot verify is a 400, as at the real API: a daemon that
// signed the request with a stale key must not be handed a certificate.
func TestUnverifiableCSRIsRejected(t *testing.T) {
	f := newFixture(t, nil)

	status, _ := postCSR(t, f, "not a CSR at all")
	if status != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", status)
	}
	if status, _ := post(t, http.DefaultClient, f.srv.URL(), pathCSR, "not json"); status != http.StatusBadRequest {
		t.Errorf("a non-JSON body gave status %d, want 400", status)
	}
}

// Faults injected partway through a scenario go on the queue at runtime.
func TestQueueAppendsDirectivesAtRuntime(t *testing.T) {
	f := newFixture(t, nil)
	csrPEM := newCSR(t, pkix.Name{CommonName: testUHWID})

	if status, _ := postCSR(t, f, csrPEM); status != 200 {
		t.Fatalf("first call: status = %d, want 200", status)
	}
	if err := f.srv.Queue(EndpointCSR, Directive{Respond: RespondStatus, Status: 502}); err != nil {
		t.Fatalf("queue: %v", err)
	}
	if status, _ := postCSR(t, f, csrPEM); status != 502 {
		t.Errorf("after queueing a 502: status = %d, want 502", status)
	}
	if err := f.srv.Queue(EndpointCSR, Directive{Respond: "nope"}); err == nil {
		t.Error("an unknown respond was queued, want an error")
	}
}

func TestStartValidatesItsOptions(t *testing.T) {
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	tests := []struct {
		name string
		opts Options
	}{
		{"no log", Options{CA: ca}},
		{"no CA", Options{Log: eventlog.New()}},
		{"unknown respond", Options{Log: eventlog.New(), CA: ca,
			Directives: map[Endpoint][]Directive{EndpointCSR: {{Respond: "teleport"}}}}},
		{"status without a code", Options{Log: eventlog.New(), CA: ca,
			Directives: map[Endpoint][]Directive{EndpointCSR: {{Respond: RespondStatus}}}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv, err := Start(tc.opts)
			if err == nil {
				_ = srv.Close()
				t.Fatal("Start succeeded, want an error")
			}
		})
	}
}

// Without a device id the fake assigns one, and it has to be the 36-character
// form the daemon accepts.
func TestGeneratedDeviceIDIsACanonicalUUID(t *testing.T) {
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	srv, err := Start(Options{Log: eventlog.New(), CA: ca})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer srv.Close() //nolint:errcheck

	id := srv.DeviceID()
	if len(id) != 36 {
		t.Errorf("device id %q is %d characters, want 36", id, len(id))
	}
	if strings.Count(id, "-") != 4 {
		t.Errorf("device id %q is not in canonical UUID form", id)
	}
}
