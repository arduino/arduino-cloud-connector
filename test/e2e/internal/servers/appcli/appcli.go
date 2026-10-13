// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package appcli is the fake arduino-app-cli: the local daemon the connector
// installs, starts, stops and lists Apps through (RFC-14 §5.4, RFC-16 §5).
//
// # Written from arduino-app-cli, not from the connector's client
//
// The wire shapes below are read off arduino-app-cli's own source and nothing
// else: the SSE framing and the closing pair of events from render/sse.go, the
// events and their payloads from internal/api/handlers (app_install.go,
// app_start.go, app_stop.go, app_list.go, app_details.go). Deriving them from
// the connector's internal/app-cli instead would make the fake agree with the
// client by construction, and a misreading of arduino-app-cli would pass every
// scenario. The one thing that is NOT in arduino-app-cli yet is the
// "<Prefix>: <text>" error message format (RFC-14 §6.1, RFC-16 §5), still
// awaiting the app-cli team's acceptance: a scenario writes those messages
// itself, so the assumption lives in the scenario files where it is visible.
//
// # State, then directives
//
// The fake keeps the one piece of state the connector's behaviour depends on:
// which Apps exist, which is running and which is the default. An `ok` answer
// is decided by that state, as arduino-app-cli would decide it — an install of
// an App that exists is refused, a start while another App runs is refused, an
// unknown App is a 500 — so a scenario about "stop the running App, then start
// the new one" exercises the connector against a consistent world instead of a
// script of answers that may not fit together. Every other answer is a fault a
// scenario injects and changes nothing.
package appcli

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

// Endpoint is how an endpoint is named in scenarios and on the timeline.
type Endpoint string

const (
	EndpointInstall Endpoint = "install" // PUT /v1/apps/install
	EndpointStart   Endpoint = "start"   // POST /v1/apps/{id}/start
	EndpointStop    Endpoint = "stop"    // POST /v1/apps/{id}/stop
	EndpointList    Endpoint = "list"    // GET /v1/apps
	EndpointDefault Endpoint = "default" // PATCH /v1/apps/{id}
	EndpointUnknown Endpoint = "unknown"
)

// Endpoints are the endpoints a scenario can queue directives for.
var Endpoints = []Endpoint{EndpointInstall, EndpointStart, EndpointStop, EndpointList, EndpointDefault}

// streams reports whether an endpoint answers with an SSE stream.
func (e Endpoint) streams() bool {
	return e == EndpointInstall || e == EndpointStart || e == EndpointStop
}

// Respond selects what the fake answers.
type Respond string

const (
	// RespondOK answers as arduino-app-cli would, from the fake's state. The
	// default.
	RespondOK Respond = "ok"
	// RespondError streams the progress, then an `error` event with Code and
	// Message, then the closing pair. Stream endpoints only.
	RespondError Respond = "error"
	// RespondStatus refuses the request before any stream with Status and
	// {"details": Details}, as arduino-app-cli does for a missing App or file.
	RespondStatus Respond = "status"
	// RespondDrop sends the progress and then ends the response without the
	// closing pair (a stream) or with no response at all (a plain call): the
	// arduino-app-cli process dying mid-operation.
	RespondDrop Respond = "drop"
	// RespondHang never finishes. With progress, the headers and the progress
	// go out first: a pull that hangs part way. Without, not even the headers
	// do, which is what arduino-app-cli's install looks like while it extracts.
	RespondHang Respond = "hang"
)

// Directive is one queued response.
type Directive struct {
	Respond Respond `yaml:"respond"`
	// Progress are the percentages streamed before the outcome. An `ok` with
	// none streams the endpoint's default (see defaultProgress).
	Progress []float64 `yaml:"progress"`
	// Code and Message are the error event's, for `error`. Code defaults to
	// INTERNAL_SERVER_ERROR, the code arduino-app-cli uses for most failures.
	Code    string `yaml:"code"`
	Message string `yaml:"message"`
	// Status and Details are the refusal's, for `status`.
	Status  int    `yaml:"status"`
	Details string `yaml:"details"`
	// Delay holds the answer back before anything is written.
	Delay time.Duration `yaml:"delay"`
}

// App is one App as the fake knows it.
type App struct {
	ID string `yaml:"id"`
	// Status is one of arduino-app-cli's: uninitialized, starting, running,
	// stopping, stopped, failed. stopped when empty.
	Status  string `yaml:"status"`
	Default bool   `yaml:"default"`
}

var appStatuses = []string{"uninitialized", "starting", "running", "stopping", "stopped", "failed"}

func (a App) active() bool { return a.Status == "running" || a.Status == "starting" }

// ResolveFunc maps the digest of an uploaded bundle to the bundle name and the
// App it stands for. It is how an install learns which App the bytes contain.
type ResolveFunc func(sha256Hex string) (bundle, app string, ok bool)

// Options configures the fake.
type Options struct {
	// Log receives one event per request. Required.
	Log *eventlog.Log
	// Resolve identifies uploaded bundles. Required for an install to succeed.
	Resolve ResolveFunc
}

// Server is a running fake.
type Server struct {
	log     *eventlog.Log
	resolve ResolveFunc

	http *http.Server
	url  string

	mu          sync.Mutex
	apps        []App
	directives  map[Endpoint][]Directive
	occurrences map[Endpoint]int
}

// Start binds a listener on loopback and serves until Close.
func Start(opts Options) (*Server, error) {
	if opts.Log == nil {
		return nil, errors.New("appcli: no event log given")
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("appcli: listen: %w", err)
	}
	s := &Server{
		log:         opts.Log,
		resolve:     opts.Resolve,
		url:         "http://" + ln.Addr().String(),
		directives:  map[Endpoint][]Directive{},
		occurrences: map[Endpoint]int{},
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/apps/install", s.endpoint(EndpointInstall, s.install))
	mux.HandleFunc("POST /v1/apps/{id}/start", s.endpoint(EndpointStart, s.start))
	mux.HandleFunc("POST /v1/apps/{id}/stop", s.endpoint(EndpointStop, s.stop))
	mux.HandleFunc("GET /v1/apps", s.endpoint(EndpointList, s.list))
	mux.HandleFunc("PATCH /v1/apps/{id}", s.endpoint(EndpointDefault, s.setDefault))
	mux.HandleFunc("/", s.unknown)
	s.http = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		if err := s.http.Serve(ln); err != nil && err != http.ErrServerClosed {
			s.log.Append(eventlog.SourceAppCLI, eventlog.KindHarnessNote,
				map[string]any{"error": err.Error()}, nil)
		}
	}()
	return s, nil
}

// URL is what ARDUINO_CLOUD_CONNECTOR__APP_CLI_URL must be set to.
func (s *Server) URL() string { return s.url }

// SetApps replaces the fake's Apps, for a scenario that starts with some
// installed.
func (s *Server) SetApps(apps []App) error {
	out := make([]App, 0, len(apps))
	defaults := 0
	for _, a := range apps {
		if a.ID == "" {
			return errors.New("appcli: an app needs an id")
		}
		if a.Status == "" {
			a.Status = "stopped"
		}
		if !slices.Contains(appStatuses, a.Status) {
			return fmt.Errorf("appcli: app %s: status %q is not one of %v", a.ID, a.Status, appStatuses)
		}
		if slices.ContainsFunc(out, func(o App) bool { return o.ID == a.ID }) {
			return fmt.Errorf("appcli: app %s declared twice", a.ID)
		}
		if a.Default {
			defaults++
		}
		out = append(out, a)
	}
	if defaults > 1 {
		// arduino-app-cli keeps a single default in one file.
		return errors.New("appcli: more than one default app")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.apps = out
	return nil
}

// Apps is a snapshot of the fake's Apps.
func (s *Server) Apps() []App {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.apps)
}

// Queue appends directives to an endpoint.
func (s *Server) Queue(endpoint Endpoint, ds ...Directive) error {
	if !slices.Contains(Endpoints, endpoint) {
		return fmt.Errorf("appcli: unknown endpoint %q (want one of %v)", endpoint, Endpoints)
	}
	for i, d := range ds {
		if err := validateDirective(endpoint, d); err != nil {
			return fmt.Errorf("appcli: %s directive %d: %w", endpoint, i+1, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.directives[endpoint] = append(s.directives[endpoint], ds...)
	return nil
}

// Close stops the server without draining: a hung stream would block teardown.
func (s *Server) Close() error {
	if err := s.http.Close(); err != nil {
		return fmt.Errorf("appcli: close: %w", err)
	}
	return nil
}

// ── request handling ─────────────────────────────────────────────────────────

// call is one request on its way through the fake.
type call struct {
	endpoint Endpoint
	d        Directive
	attrs    map[string]any
}

// handlerFunc decides an `ok` answer from the state. It runs with the lock
// held, so state reads and writes are one atomic step, and returns what to
// write.
type handlerFunc func(r *http.Request, c *call) answer

// answer is what an `ok` produces.
type answer struct {
	// refuse, when non-zero, is a pre-stream refusal with details.
	refuse  int
	details string
	// errCode/errMessage, when errCode is set, end the stream with an error.
	errCode    string
	errMessage string
	// done is the payload of an install's `done` event.
	done any
	// body is a plain call's JSON answer.
	body any
}

// endpoint wraps an endpoint: it pops the directive, lets the endpoint record
// what it saw and decide an `ok`, appends the event and writes the answer.
func (s *Server) endpoint(e Endpoint, h handlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c := &call{endpoint: e, attrs: map[string]any{
			"endpoint": string(e),
			"method":   r.Method,
			"path":     r.URL.Path,
		}}
		if id := r.PathValue("id"); id != "" {
			c.attrs["app"] = id
		}

		// The request is read before anything is decided: an install's verdict
		// depends on the bytes, and arduino-app-cli itself reads the whole
		// upload before it answers.
		pre := s.readRequest(r, c)

		s.mu.Lock()
		s.occurrences[e]++
		c.attrs["occurrence"] = s.occurrences[e]
		c.d = Directive{Respond: RespondOK}
		if q := s.directives[e]; len(q) > 0 {
			c.d, s.directives[e] = q[0], q[1:]
		}
		var a answer
		switch {
		case pre.refuse != 0:
			a = pre
		case c.d.Respond == RespondOK:
			a = h(r, c)
		}
		s.mu.Unlock()

		c.attrs["respond"] = string(c.d.Respond)
		// Appended on arrival, with the answer already decided, like the other
		// fakes: the timeline shows the call when it happened even when the
		// answer is slow or never comes.
		s.log.Append(eventlog.SourceAppCLI, eventlog.KindHTTPRequest, c.attrs, nil)

		if c.d.Delay > 0 {
			select {
			case <-time.After(c.d.Delay):
			case <-r.Context().Done():
				return
			}
		}
		s.write(w, r, c, a)
	}
}

// readRequest consumes the request body and records what matters in it. A
// request arduino-app-cli would refuse before streaming comes back as a refusal.
func (s *Server) readRequest(r *http.Request, c *call) answer {
	switch c.endpoint {
	case EndpointInstall:
		c.attrs["prepare"] = r.URL.Query().Get("prepare")
		sum, size, filename, err := readUpload(r)
		if err != nil {
			c.attrs["upload_error"] = err.Error()
			return answer{refuse: http.StatusBadRequest, details: "missing required file parameter"}
		}
		c.attrs["bundle_sha256"] = sum
		c.attrs["bundle_size"] = size
		c.attrs["filename"] = filename
		bundle := ""
		if s.resolve != nil {
			bundle, _, _ = s.resolve(sum)
		}
		c.attrs["bundle"] = bundle
	case EndpointDefault:
		var body map[string]any
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<16)).Decode(&body); err != nil {
			return answer{refuse: http.StatusBadRequest, details: "invalid request"}
		}
		// Every field sent, sorted: arduino-app-cli refuses anything but
		// `default` on a release, so the field list is itself an assertion.
		fields := make([]string, 0, len(body))
		for k := range body {
			fields = append(fields, k)
		}
		slices.Sort(fields)
		c.attrs["fields"] = strings.Join(fields, ",")
		if def, ok := body["default"].(bool); ok {
			c.attrs["default"] = def
		}
	default:
		_, _ = io.Copy(io.Discard, r.Body)
	}
	return answer{}
}

// readUpload hashes the multipart "file" field, the way arduino-app-cli's
// install handler takes the archive.
func readUpload(r *http.Request) (sum string, size int64, filename string, err error) {
	mr, err := r.MultipartReader()
	if err != nil {
		return "", 0, "", err
	}
	for {
		part, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			return "", 0, "", errors.New("no file field")
		}
		if err != nil {
			return "", 0, "", err
		}
		if part.FormName() != "file" {
			_, _ = io.Copy(io.Discard, part)
			continue
		}
		h := sha256.New()
		n, err := io.Copy(h, part)
		if err != nil {
			return "", 0, "", err
		}
		drainParts(mr)
		return hex.EncodeToString(h.Sum(nil)), n, part.FileName(), nil
	}
}

func drainParts(mr *multipart.Reader) {
	for {
		part, err := mr.NextPart()
		if err != nil {
			return
		}
		_, _ = io.Copy(io.Discard, part)
	}
}

// ── the `ok` answers, decided from the state (called with s.mu held) ─────────

func (s *Server) install(_ *http.Request, c *call) answer {
	sum, _ := c.attrs["bundle_sha256"].(string)
	var app string
	if s.resolve != nil {
		_, app, _ = s.resolve(sum)
	}
	c.attrs["installed_app"] = app
	switch {
	case app == "":
		// arduino-app-cli could not have read an app.yaml out of bytes the
		// harness never declared.
		c.attrs["outcome"] = "archive_unknown"
		return answer{errCode: "bad_request", errMessage: "AppArchiveRejected: the fake app-cli does not know this archive"}
	case s.find(app) >= 0:
		c.attrs["outcome"] = "app_already_exists"
		return answer{errCode: "app_already_exists", errMessage: fmt.Sprintf("app already exists: %s", app)}
	}
	s.apps = append(s.apps, App{ID: app, Status: "stopped"})
	c.attrs["outcome"] = "installed"
	return answer{done: map[string]any{"id": app, "name": app, "release": app, "target": "e2e"}}
}

func (s *Server) start(r *http.Request, c *call) answer {
	id := r.PathValue("id")
	i := s.find(id)
	if i < 0 {
		c.attrs["outcome"] = "unknown_app"
		return answer{refuse: http.StatusInternalServerError, details: "unable to find the app"}
	}
	if j := slices.IndexFunc(s.apps, App.active); j >= 0 {
		c.attrs["outcome"] = "another_app_running"
		return answer{errCode: "INTERNAL_SERVER_ERROR",
			errMessage: fmt.Sprintf("AnotherAppRunning: app %s is %s", s.apps[j].ID, s.apps[j].Status)}
	}
	s.apps[i].Status = "running"
	c.attrs["outcome"] = "started"
	return answer{}
}

func (s *Server) stop(r *http.Request, c *call) answer {
	i := s.find(r.PathValue("id"))
	if i < 0 {
		c.attrs["outcome"] = "unknown_app"
		return answer{refuse: http.StatusInternalServerError, details: "unable to find the app"}
	}
	// Idempotent, as arduino-app-cli's is: stopping a stopped App succeeds.
	s.apps[i].Status = "stopped"
	c.attrs["outcome"] = "stopped"
	return answer{}
}

func (s *Server) list(_ *http.Request, c *call) answer {
	type listed struct {
		ID      string `json:"id"`
		Name    string `json:"name"`
		Status  string `json:"status,omitempty"`
		Default bool   `json:"default"`
		Release bool   `json:"release"`
	}
	out := make([]listed, 0, len(s.apps))
	for _, a := range s.apps {
		out = append(out, listed{ID: a.ID, Name: a.ID, Status: a.Status, Default: a.Default, Release: true})
	}
	c.attrs["apps"] = len(out)
	c.attrs["outcome"] = "listed"
	return answer{body: map[string]any{"apps": out}}
}

func (s *Server) setDefault(r *http.Request, c *call) answer {
	i := s.find(r.PathValue("id"))
	if i < 0 {
		c.attrs["outcome"] = "unknown_app"
		return answer{refuse: http.StatusInternalServerError, details: "unable to find the app"}
	}
	if def, _ := c.attrs["default"].(bool); def {
		// One default, overwritten: the previous one is simply superseded.
		for j := range s.apps {
			s.apps[j].Default = j == i
		}
	} else {
		s.apps[i].Default = false
	}
	c.attrs["outcome"] = "default_set"
	a := s.apps[i]
	return answer{body: map[string]any{"id": a.ID, "name": a.ID, "status": a.Status, "default": a.Default}}
}

func (s *Server) find(id string) int {
	return slices.IndexFunc(s.apps, func(a App) bool { return a.ID == id })
}

func (s *Server) unknown(w http.ResponseWriter, r *http.Request) {
	s.log.Append(eventlog.SourceAppCLI, eventlog.KindHTTPRequest, map[string]any{
		"endpoint": string(EndpointUnknown), "method": r.Method, "path": r.URL.Path,
		"respond": "unknown",
	}, nil)
	http.NotFound(w, r)
}

// ── writing the answer ───────────────────────────────────────────────────────

func (s *Server) write(w http.ResponseWriter, r *http.Request, c *call, a answer) {
	d := c.d
	if a.refuse != 0 {
		writeJSON(w, a.refuse, map[string]string{"details": a.details})
		return
	}
	switch d.Respond {
	case RespondStatus:
		writeJSON(w, d.Status, map[string]string{"details": d.Details})
		return
	case RespondHang:
		if c.endpoint.streams() && len(d.Progress) > 0 {
			sse := openStream(w)
			for _, p := range d.Progress {
				sse.send("progress", progressEvent(p))
			}
		}
		<-r.Context().Done()
		return
	case RespondDrop:
		if !c.endpoint.streams() {
			hangUp(w)
			return
		}
		sse := openStream(w)
		for _, p := range d.Progress {
			sse.send("progress", progressEvent(p))
		}
		// Returning ends the chunked body cleanly, with no closing pair: what
		// the connector reads when arduino-app-cli goes away mid-stream.
		return
	}

	if !c.endpoint.streams() {
		writeJSON(w, http.StatusOK, a.body)
		return
	}

	sse := openStream(w)
	progress := d.Progress
	if d.Respond == RespondOK && len(progress) == 0 {
		progress = defaultProgress[c.endpoint]
	}
	// A stateful refusal (another App running, an App that already exists)
	// comes before any progress, as it does in arduino-app-cli.
	if d.Respond == RespondOK && a.errCode != "" {
		progress = nil
	}
	for _, p := range progress {
		sse.send("progress", progressEvent(p))
	}
	switch {
	case d.Respond == RespondError:
		code := d.Code
		if code == "" {
			code = "INTERNAL_SERVER_ERROR"
		}
		sse.send("error", map[string]string{"code": code, "message": d.Message})
	case a.errCode != "":
		sse.send("error", map[string]string{"code": a.errCode, "message": a.errMessage})
	case a.done != nil:
		sse.send("done", a.done)
	}
	sse.close()
}

// defaultProgress is what an `ok` streams when the directive gives none. Short
// on purpose: every one is an uplink a strict scenario has to account for.
var defaultProgress = map[Endpoint][]float64{
	EndpointInstall: {0, 100},
	EndpointStart:   {0, 100},
	// arduino-app-cli's stop sends a single progress.
	EndpointStop: {100},
}

func progressEvent(p float64) map[string]any {
	return map[string]any{"name": "", "progress": p}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// hangUp closes the connection without an HTTP response.
func hangUp(w http.ResponseWriter) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		panic("appcli: the response writer cannot be hijacked")
	}
	conn, _, err := hj.Hijack()
	if err == nil {
		_ = conn.Close()
	}
}

// stream writes events exactly as arduino-app-cli's render.SSEStream does.
type stream struct {
	w http.ResponseWriter
	f http.Flusher
}

func openStream(w http.ResponseWriter) stream {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	f, _ := w.(http.Flusher)
	s := stream{w: w, f: f}
	s.flush()
	return s
}

// send is render.SSEStream.send: "event: <type>\n", then "data: " and the JSON
// with the encoder's own newline, then the blank line.
func (s stream) send(event string, data any) {
	bw := bufio.NewWriter(s.w)
	_, _ = bw.WriteString("event: " + event + "\n")
	_, _ = bw.WriteString("data: ")
	_ = json.NewEncoder(bw).Encode(data)
	_, _ = bw.WriteString("\n")
	_ = bw.Flush()
	s.flush()
}

// close is the pair render.SSEStream always ends with, success included.
func (s stream) close() {
	s.send("error", map[string]string{"code": "SERVER_CLOSED"})
	s.send("close", "Stream closed by server")
}

func (s stream) flush() {
	if s.f != nil {
		s.f.Flush()
	}
}

func validateDirective(e Endpoint, d Directive) error {
	switch d.Respond {
	case RespondOK, RespondDrop, RespondHang:
	case RespondError:
		if !e.streams() {
			return fmt.Errorf("respond: error is an event on a stream; %s is a plain call, use status", e)
		}
	case RespondStatus:
		if d.Status < 100 || d.Status > 599 {
			return fmt.Errorf("respond: status needs a status code, got %d", d.Status)
		}
	case "":
		return errors.New("respond is empty")
	default:
		return fmt.Errorf("unknown respond %q", d.Respond)
	}
	if len(d.Progress) > 0 && !e.streams() {
		return fmt.Errorf("progress on %s, which does not stream", e)
	}
	return nil
}
