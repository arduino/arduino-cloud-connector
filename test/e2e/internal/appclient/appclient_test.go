// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appclient

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

const awaitBudget = 5 * time.Second

// stubDaemon stands in for the daemon's REST API. It is hand-written rather
// than borrowed from the daemon on purpose: these paths, JSON keys and SSE
// event names are the contract the harness has to keep speaking, and a test
// that imported the real router would agree with itself about all three.
type stubDaemon struct {
	srv *http.Server

	mu       sync.Mutex
	status   Status
	putCode  int
	startErr int
	puts     []string

	frames chan string // pushed onto every open SSE stream
}

func newStubDaemon(t *testing.T) (*stubDaemon, string) {
	t.Helper()
	d := &stubDaemon{
		status:  Status{Provisioning: "unprovisioned", Daemon: "CheckInternet"},
		putCode: http.StatusNoContent,
		frames:  make(chan string, 16),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+PathHealth, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET "+PathStatus, func(w http.ResponseWriter, _ *http.Request) {
		d.mu.Lock()
		status := d.status
		d.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})
	mux.HandleFunc("GET "+PathIdentity, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(Identity{
			UHWID:      "3a7bd3e2360a3d29",
			BoardToken: "secret-board-token",
		})
	})
	mux.HandleFunc("POST "+PathProvisioningStart, func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		d.mu.Lock()
		code := d.startErr
		d.puts = append(d.puts, "start:"+body)
		d.mu.Unlock()
		if code != 0 {
			w.WriteHeader(code)
			return
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "accepted"})
	})
	mux.HandleFunc("PUT /v1/variables/{name}", func(w http.ResponseWriter, r *http.Request) {
		body, _ := readAll(r)
		d.mu.Lock()
		code := d.putCode
		d.puts = append(d.puts, r.PathValue("name")+":"+body)
		d.mu.Unlock()
		w.WriteHeader(code)
	})
	mux.HandleFunc("GET /v1/variables/{name}/events", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Error("the stub needs a flusher")
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		// The daemon's first frame is always one of the sync kinds.
		_, _ = fmt.Fprintf(w, "event: %s\ndata: {\"name\":%q}\n\n", EventThingUnavailable, name)
		flusher.Flush()
		for {
			select {
			case <-r.Context().Done():
				return
			case frame := <-d.frames:
				_, _ = fmt.Fprint(w, frame)
				flusher.Flush()
			}
		}
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	d.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = d.srv.Serve(ln) }()
	t.Cleanup(func() { _ = d.srv.Close() })
	return d, "http://" + ln.Addr().String()
}

func readAll(r *http.Request) (string, error) {
	body, err := io.ReadAll(r.Body)
	return string(body), err
}

func (d *stubDaemon) setStatus(s Status) {
	d.mu.Lock()
	d.status = s
	d.mu.Unlock()
}

func (d *stubDaemon) recorded() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]string(nil), d.puts...)
}

func newClient(t *testing.T) (*Client, *eventlog.Log, *stubDaemon) {
	t.Helper()
	stub, baseURL := newStubDaemon(t)
	log := eventlog.New()
	c, err := New(log, baseURL)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return c, log, stub
}

func await(t *testing.T, log *eventlog.Log, from eventlog.Cursor, p eventlog.Predicate) (eventlog.Event, eventlog.Cursor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), awaitBudget)
	defer cancel()
	ev, cursor, err := log.Await(ctx, from, p, awaitBudget)
	if err != nil {
		t.Fatalf("awaiting %s: %v", p, err)
	}
	return ev, cursor
}

func pred(label string, constraints ...eventlog.Constraint) eventlog.Predicate {
	return eventlog.Predicate{Label: label, Constraints: constraints}
}

func TestWaitReady(t *testing.T) {
	c, log, _ := newClient(t)
	ctx := context.Background()

	if err := c.WaitReady(ctx, awaitBudget); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	await(t, log, 0, pred("ready note", eventlog.Eq("attrs.action", "daemon_ready")))

	// Nothing listening: the wait must give up with a message naming the
	// endpoint, not hang.
	dead, err := New(eventlog.New(), "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := dead.WaitReady(ctx, 300*time.Millisecond); err == nil {
		t.Error("WaitReady succeeded against nothing")
	} else if !strings.Contains(err.Error(), PathHealth) {
		t.Errorf("error should name the health endpoint: %v", err)
	}
}

// The status is what await_daemon_state waits on, and only transitions are
// recorded: the poller runs several times a second and identical rows would
// bury the timeline.
func TestStatusIsRecordedOnlyWhenItChanges(t *testing.T) {
	c, log, stub := newClient(t)
	ctx := context.Background()

	for range 3 {
		if _, err := c.Status(ctx); err != nil {
			t.Fatalf("Status: %v", err)
		}
	}
	if got := len(log.Events()); got != 1 {
		t.Fatalf("three identical polls produced %d events, want 1", got)
	}

	stub.setStatus(Status{
		Provisioning: "provisioned",
		Daemon:       "Run",
		Cloud:        &Cloud{State: "Steady", ThingID: "thing-1"},
		DeviceID:     "device-1",
	})
	status, err := c.Status(ctx)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.Cloud == nil || status.Cloud.State != "Steady" {
		t.Fatalf("status = %+v, want the cloud section decoded", status)
	}

	ev, _ := await(t, log, 1, pred("status change",
		eventlog.Eq("kind", string(eventlog.KindStatusPoll)),
		eventlog.Eq("attrs.daemon", "Run"),
	))
	for name, want := range map[string]any{
		"provisioning": "provisioned",
		"cloud_state":  "Steady",
		"thing_id":     "thing-1",
		"device_id":    "device-1",
	} {
		if got := ev.Attrs[name]; got != want {
			t.Errorf("attrs[%q] = %v, want %v", name, got, want)
		}
	}
	// Status is context, never a claim: a scenario must not have to consume
	// however many times the daemon happened to be polled.
	if ev.Significant() {
		t.Error("a status poll must not be significant")
	}
}

func TestStatusPollerRecordsTransitions(t *testing.T) {
	c, log, stub := newClient(t)

	stop := c.StartStatusPoller(10 * time.Millisecond)
	defer stop()

	await(t, log, 0, pred("initial state", eventlog.Eq("attrs.daemon", "CheckInternet")))
	stub.setStatus(Status{Provisioning: "provisioning", Daemon: "Provisioning"})
	await(t, log, 0, pred("next state", eventlog.Eq("attrs.daemon", "Provisioning")))
}

func TestStartProvisioning(t *testing.T) {
	c, log, stub := newClient(t)
	ctx := context.Background()

	if err := c.StartProvisioning(ctx, "org-1"); err != nil {
		t.Fatalf("StartProvisioning: %v", err)
	}
	if got := stub.recorded(); len(got) != 1 || !strings.Contains(got[0], `"organization_id":"org-1"`) {
		t.Errorf("the daemon received %v, want the organization id", got)
	}
	ev, _ := await(t, log, 0, pred("app post",
		eventlog.Eq("source", string(eventlog.SourceApp)),
		eventlog.Eq("attrs.path", PathProvisioningStart),
	))
	if got := ev.Attrs["status"]; got != http.StatusAccepted {
		t.Errorf("status = %v, want 202", got)
	}
	// App actions are the harness's own doing: on the timeline for causality,
	// never part of the strict sweep.
	if ev.Significant() {
		t.Error("an app action must not be significant")
	}

	// A 409 is the daemon saying a provisioning is already running, and it has
	// to surface as an error naming the status.
	stub.mu.Lock()
	stub.startErr = http.StatusConflict
	stub.mu.Unlock()
	err := c.StartProvisioning(ctx, "")
	if err == nil {
		t.Fatal("a 409 was not reported")
	}
	if !strings.Contains(err.Error(), "409") {
		t.Errorf("error should name the status: %v", err)
	}
}

func TestPutVariable(t *testing.T) {
	c, _, stub := newClient(t)
	ctx := context.Background()

	if err := c.PutVariable(ctx, "temp", 42.0); err != nil {
		t.Fatalf("PutVariable: %v", err)
	}
	if got := stub.recorded(); len(got) != 1 || got[0] != `temp:{"value":42}` {
		t.Errorf("the daemon received %q, want the value wrapped in the documented body", got)
	}

	// The daemon refuses a value while no thing is assigned. That is an
	// answer, not a transport failure, and a scenario has to be able to tell
	// them apart.
	stub.mu.Lock()
	stub.putCode = http.StatusConflict
	stub.mu.Unlock()
	if err := c.PutVariable(ctx, "temp", 1); err == nil {
		t.Error("a 409 was not reported")
	} else if !strings.Contains(err.Error(), "409") {
		t.Errorf("error should name the status: %v", err)
	}
}

// The board token is a credential, and a failing run uploads the timeline as a
// CI artifact.
func TestIdentityDoesNotRecordTheBoardToken(t *testing.T) {
	c, log, _ := newClient(t)

	id, err := c.Identity(context.Background())
	if err != nil {
		t.Fatalf("Identity: %v", err)
	}
	if id.BoardToken == "" {
		t.Fatal("the client dropped the board token")
	}
	for _, ev := range log.Events() {
		if strings.Contains(fmt.Sprint(ev.Attrs), id.BoardToken) {
			t.Errorf("the board token leaked into the log: %v", ev.Attrs)
		}
	}
}

// The SSE stream is the app's read side: frames become events with their
// payload flattened, so a step can constrain the value.
func TestSSEFramesBecomeEvents(t *testing.T) {
	c, log, stub := newClient(t)

	stream, err := c.SubscribeVariable(context.Background(), "temp")
	if err != nil {
		t.Fatalf("SubscribeVariable: %v", err)
	}
	defer stream.Close()

	// The first frame the daemon always sends.
	ev, cursor := await(t, log, 0, pred("first frame",
		eventlog.Eq("kind", string(eventlog.KindSSEFrame)),
		eventlog.Eq("attrs.event", EventThingUnavailable),
	))
	if got := ev.Attrs["variable"]; got != "temp" {
		t.Errorf("variable = %v, want temp", got)
	}
	// Every SSE frame is significant: the app's view of a value is exactly
	// what the scenarios are about.
	if !ev.Significant() {
		t.Error("an SSE frame must be significant")
	}

	stub.frames <- "event: update\ndata: {\"name\":\"temp\",\"value\":30.5,\"timestamp\":\"2026-09-15T10:00:00Z\"}\n\n"
	ev, cursor = await(t, log, cursor, pred("update frame",
		eventlog.Eq("kind", string(eventlog.KindSSEFrame)),
		eventlog.Eq("attrs.event", EventUpdate),
	))
	if got := ev.Attrs["value"]; got != 30.5 {
		t.Errorf("value = %v (%T), want 30.5", got, got)
	}
	if got := ev.Attrs["name"]; got != "temp" {
		t.Errorf("name = %v", got)
	}

	// A replayed last value carries the flag the app resolves against.
	stub.frames <- "event: lastvalue\ndata: {\"name\":\"temp\",\"value\":21.5,\"last_value\":true}\n\n"
	ev, _ = await(t, log, cursor, pred("lastvalue frame",
		eventlog.Eq("attrs.event", EventLastValue),
	))
	if got := ev.Attrs["last_value"]; got != true {
		t.Errorf("last_value = %v, want true", got)
	}
}

// A frame the harness cannot parse must still be visible: dropping it would
// turn a change in the SSE contract into a silent timeout.
func TestAnUnparsableFrameIsStillRecorded(t *testing.T) {
	c, log, stub := newClient(t)

	stream, err := c.SubscribeVariable(context.Background(), "temp")
	if err != nil {
		t.Fatalf("SubscribeVariable: %v", err)
	}
	defer stream.Close()
	_, cursor := await(t, log, 0, pred("first frame", eventlog.Eq("kind", string(eventlog.KindSSEFrame))))

	stub.frames <- "event: update\ndata: not json\n\n"
	ev, cursor := await(t, log, cursor, pred("undecodable frame",
		eventlog.Eq("kind", string(eventlog.KindSSEFrame)),
		eventlog.Eq("attrs.event", EventUpdate),
	))
	if _, ok := ev.Attrs["decode_error"]; !ok {
		t.Errorf("no decode_error recorded: %v", ev.Attrs)
	}

	stub.frames <- "retry: 1000\n\n"
	await(t, log, cursor, pred("unrecognised line",
		eventlog.Eq("attrs.note", "unrecognised SSE line"),
	))
}

func TestSubscribeReportsAMissingEndpoint(t *testing.T) {
	log := eventlog.New()
	c, err := New(log, "http://127.0.0.1:1")
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if stream, err := c.SubscribeVariable(context.Background(), "temp"); err == nil {
		stream.Close()
		t.Error("SubscribeVariable succeeded against nothing")
	}
}

func TestClosingTheStreamRecordsIt(t *testing.T) {
	c, log, _ := newClient(t)

	stream, err := c.SubscribeVariable(context.Background(), "temp")
	if err != nil {
		t.Fatalf("SubscribeVariable: %v", err)
	}
	stream.Close()

	await(t, log, 0, pred("stream closed", eventlog.Eq("attrs.action", "sse_closed")))
}

func TestNewValidatesItsArguments(t *testing.T) {
	if _, err := New(nil, "http://127.0.0.1:1"); err == nil {
		t.Error("New with no log succeeded")
	}
	if _, err := New(eventlog.New(), ""); err == nil {
		t.Error("New with no base URL succeeded")
	}
}

func TestPathHelpers(t *testing.T) {
	if got, want := VariablePath("temp"), "/v1/variables/temp"; got != want {
		t.Errorf("VariablePath = %q, want %q", got, want)
	}
	if got, want := VariableEventsPath("temp"), "/v1/variables/temp/events"; got != want {
		t.Errorf("VariableEventsPath = %q, want %q", got, want)
	}
	// A name with a slash would otherwise change the route it hits.
	if got, want := VariablePath("a/b"), "/v1/variables/a%2Fb"; got != want {
		t.Errorf("VariablePath = %q, want %q", got, want)
	}
}
