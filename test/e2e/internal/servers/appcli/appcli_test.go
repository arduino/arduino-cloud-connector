// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime/multipart"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

var bundleBytes = []byte("the bytes of bundle app1")

type fixture struct {
	log *eventlog.Log
	srv *Server
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	sum := sha256.Sum256(bundleBytes)
	known := hex.EncodeToString(sum[:])
	log := eventlog.New()
	srv, err := Start(Options{Log: log, Resolve: func(s string) (string, string, bool) {
		if s == known {
			return "app1", "app1:0.1.0", true
		}
		return "", "", false
	}})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return fixture{log: log, srv: srv}
}

// event is one SSE event as a client reads it off the wire.
type event struct {
	name string
	data string
}

func (f fixture) do(t *testing.T, method, path string, body io.Reader, contentType string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, method, f.srv.URL()+path, body)
	if err != nil {
		t.Fatal(err)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func (f fixture) install(t *testing.T, content []byte) *http.Response {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, err := mw.CreateFormFile("file", "bundle.zip")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = part.Write(content)
	_ = mw.Close()
	return f.do(t, http.MethodPut, "/v1/apps/install?prepare=true", &buf, mw.FormDataContentType())
}

// readEvents reads a whole stream. The parser is the SSE grammar, written here
// on purpose rather than borrowed from the connector.
func readEvents(t *testing.T, resp *http.Response) []event {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("Content-Type = %q, want text/event-stream", ct)
	}
	var (
		out []event
		cur event
	)
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.name != "" || cur.data != "" {
				out = append(out, cur)
			}
			cur = event{}
		case strings.HasPrefix(line, "event: "):
			cur.name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			cur.data = strings.TrimPrefix(line, "data: ")
		}
	}
	return out
}

func names(evs []event) []string {
	out := make([]string, 0, len(evs))
	for _, e := range evs {
		out = append(out, e.name)
	}
	return out
}

func (f fixture) lastEvent(t *testing.T) eventlog.Event {
	t.Helper()
	evs := f.log.Events()
	if len(evs) == 0 {
		t.Fatal("no event recorded")
	}
	return evs[len(evs)-1]
}

// The success stream arduino-app-cli sends: progress, done, and then the closing
// pair it sends after EVERY stream, success included (render/sse.go).
func TestInstallOfAKnownBundleStreamsDoneThenTheClosingPair(t *testing.T) {
	f := newFixture(t)
	evs := readEvents(t, f.install(t, bundleBytes))

	if got, want := names(evs), []string{"progress", "progress", "done", "error", "close"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("events = %v, want %v", got, want)
	}
	if evs[0].data != `{"name":"","progress":0}` || evs[1].data != `{"name":"","progress":100}` {
		t.Errorf("progress payloads = %s, %s", evs[0].data, evs[1].data)
	}
	var done struct{ ID string }
	if err := json.Unmarshal([]byte(evs[2].data), &done); err != nil || done.ID != "app1:0.1.0" {
		t.Errorf("done = %s (%v)", evs[2].data, err)
	}
	if evs[3].data != `{"code":"SERVER_CLOSED"}` || evs[4].data != `"Stream closed by server"` {
		t.Errorf("closing pair = %s, %s", evs[3].data, evs[4].data)
	}

	ev := f.lastEvent(t)
	sum := sha256.Sum256(bundleBytes)
	want := map[string]any{
		"endpoint": "install", "bundle": "app1", "bundle_sha256": hex.EncodeToString(sum[:]),
		"bundle_size": int64(len(bundleBytes)), "prepare": "true", "outcome": "installed",
		"installed_app": "app1:0.1.0", "respond": "ok", "occurrence": 1,
	}
	for k, v := range want {
		if ev.Attrs[k] != v {
			t.Errorf("attr %s = %#v, want %#v", k, ev.Attrs[k], v)
		}
	}
	if apps := f.srv.Apps(); len(apps) != 1 || apps[0] != (App{ID: "app1:0.1.0", Status: "stopped"}) {
		t.Errorf("apps = %v", apps)
	}
}

func TestInstallIsRefusedFromTheState(t *testing.T) {
	f := newFixture(t)
	readEvents(t, f.install(t, bundleBytes))

	again := readEvents(t, f.install(t, bundleBytes))
	if got := names(again); !reflect.DeepEqual(got, []string{"error", "error", "close"}) {
		t.Fatalf("second install events = %v: a refusal streams no progress", got)
	}
	if !strings.Contains(again[0].data, `"code":"app_already_exists"`) {
		t.Errorf("second install error = %s", again[0].data)
	}

	unknown := readEvents(t, f.install(t, []byte("bytes nobody declared")))
	if !strings.Contains(unknown[0].data, `"code":"bad_request"`) ||
		!strings.Contains(unknown[0].data, "AppArchiveRejected: ") {
		t.Errorf("unknown archive error = %s", unknown[0].data)
	}
	if got := f.lastEvent(t).Attrs["outcome"]; got != "archive_unknown" {
		t.Errorf("outcome = %v", got)
	}
}

func TestInstallWithoutAFileIsRefusedBeforeTheStream(t *testing.T) {
	f := newFixture(t)
	resp := f.do(t, http.MethodPut, "/v1/apps/install", strings.NewReader("x"), "text/plain")
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest || !strings.Contains(string(body), `"details":"missing required file parameter"`) {
		t.Errorf("answer = %d %s", resp.StatusCode, body)
	}
}

func TestStartIsDecidedByWhatIsRunning(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.SetApps([]App{{ID: "old:1", Status: "running"}, {ID: "new:1"}}); err != nil {
		t.Fatal(err)
	}

	refused := readEvents(t, f.do(t, http.MethodPost, "/v1/apps/new:1/start", nil, ""))
	if got := names(refused); !reflect.DeepEqual(got, []string{"error", "error", "close"}) {
		t.Fatalf("start while another runs = %v", got)
	}
	if !strings.Contains(refused[0].data, `"message":"AnotherAppRunning: app old:1 is running"`) {
		t.Errorf("refusal = %s", refused[0].data)
	}

	stopped := readEvents(t, f.do(t, http.MethodPost, "/v1/apps/old:1/stop", nil, ""))
	if got := names(stopped); !reflect.DeepEqual(got, []string{"progress", "error", "close"}) {
		t.Errorf("stop = %v", got)
	}
	started := readEvents(t, f.do(t, http.MethodPost, "/v1/apps/new:1/start", nil, ""))
	// Neither start nor stop has a done: success is the closing pair with no
	// error before it.
	if got := names(started); !reflect.DeepEqual(got, []string{"progress", "progress", "error", "close"}) {
		t.Errorf("start = %v", got)
	}
	if got := f.srv.Apps(); got[0].Status != "stopped" || got[1].Status != "running" {
		t.Errorf("apps = %v", got)
	}
}

func TestAnUnknownAppIsA500BeforeTheStream(t *testing.T) {
	f := newFixture(t)
	for _, path := range []string{"/v1/apps/nope/start", "/v1/apps/nope/stop"} {
		resp := f.do(t, http.MethodPost, path, nil, "")
		body, _ := io.ReadAll(resp.Body)
		if resp.StatusCode != http.StatusInternalServerError || !strings.Contains(string(body), "unable to find the app") {
			t.Errorf("%s = %d %s", path, resp.StatusCode, body)
		}
	}
}

func TestListAndSetDefault(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.SetApps([]App{{ID: "a:1", Default: true}, {ID: "b:1", Status: "running"}}); err != nil {
		t.Fatal(err)
	}
	resp := f.do(t, http.MethodPatch, "/v1/apps/b:1", strings.NewReader(`{"default":true}`), "application/json")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("PATCH = %d", resp.StatusCode)
	}
	if ev := f.lastEvent(t); ev.Attrs["default"] != true || ev.Attrs["app"] != "b:1" || ev.Attrs["fields"] != "default" {
		t.Errorf("PATCH event = %v", ev)
	}

	var list struct {
		Apps []struct {
			ID, Status string
			Default    bool
		}
	}
	resp = f.do(t, http.MethodGet, "/v1/apps", nil, "")
	if err := json.NewDecoder(resp.Body).Decode(&list); err != nil {
		t.Fatal(err)
	}
	if len(list.Apps) != 2 || list.Apps[0].Default || !list.Apps[1].Default || list.Apps[1].Status != "running" {
		t.Errorf("list = %+v: the new default must supersede the old one", list.Apps)
	}
}

func TestInjectedFaults(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.SetApps([]App{{ID: "a:1"}}); err != nil {
		t.Fatal(err)
	}
	if err := f.srv.Queue(EndpointStart,
		Directive{Respond: RespondError, Progress: []float64{10}, Code: "not_prepared", Message: "missing image"},
		Directive{Respond: RespondDrop, Progress: []float64{10}},
		Directive{Respond: RespondStatus, Status: http.StatusNotFound, Details: "404 page not found"},
	); err != nil {
		t.Fatal(err)
	}

	errored := readEvents(t, f.do(t, http.MethodPost, "/v1/apps/a:1/start", nil, ""))
	if got := names(errored); !reflect.DeepEqual(got, []string{"progress", "error", "error", "close"}) ||
		errored[1].data != `{"code":"not_prepared","message":"missing image"}` {
		t.Errorf("error directive = %v %v", got, errored)
	}

	dropped := readEvents(t, f.do(t, http.MethodPost, "/v1/apps/a:1/start", nil, ""))
	if got := names(dropped); !reflect.DeepEqual(got, []string{"progress"}) {
		t.Errorf("drop directive = %v: it must end without the closing pair", got)
	}

	resp := f.do(t, http.MethodPost, "/v1/apps/a:1/start", nil, "")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status directive = %d", resp.StatusCode)
	}
	// The faults changed nothing: the App never started.
	if got := f.srv.Apps()[0].Status; got != "stopped" {
		t.Errorf("status = %s", got)
	}
}

func TestDropOnAPlainCallHangsUp(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.Queue(EndpointList, Directive{Respond: RespondDrop}); err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(http.MethodGet, f.srv.URL()+"/v1/apps", nil)
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Errorf("got %d, want a broken connection", resp.StatusCode)
	}
}

// A hang without progress sends nothing at all, headers included: the client
// is left waiting for the response, which is what the connector's watchdog has
// to end.
func TestHangSendsNoHeaders(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.Queue(EndpointInstall, Directive{Respond: RespondHang}); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	part, _ := mw.CreateFormFile("file", "b.zip")
	_, _ = part.Write(bundleBytes)
	_ = mw.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPut, f.srv.URL()+"/v1/apps/install", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	if resp, err := http.DefaultClient.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("got headers (%d) from a hang", resp.StatusCode)
	}
	if got := f.lastEvent(t).Attrs["respond"]; got != "hang" {
		t.Errorf("respond = %v", got)
	}
}

func TestQueueAndSetAppsRefuseMistakes(t *testing.T) {
	f := newFixture(t)
	for name, err := range map[string]error{
		"error on a plain call":    f.srv.Queue(EndpointList, Directive{Respond: RespondError}),
		"progress on a plain call": f.srv.Queue(EndpointDefault, Directive{Respond: RespondOK, Progress: []float64{1}}),
		"status without a code":    f.srv.Queue(EndpointStart, Directive{Respond: RespondStatus}),
		"unknown endpoint":         f.srv.Queue("delete", Directive{Respond: RespondOK}),
		"unknown respond":          f.srv.Queue(EndpointStart, Directive{Respond: "maybe"}),
		"unknown status":           f.srv.SetApps([]App{{ID: "a", Status: "sleeping"}}),
		"two defaults":             f.srv.SetApps([]App{{ID: "a", Default: true}, {ID: "b", Default: true}}),
		"duplicate app":            f.srv.SetApps([]App{{ID: "a"}, {ID: "a"}}),
	} {
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
