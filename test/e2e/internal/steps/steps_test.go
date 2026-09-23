// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package steps

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/appclient"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/harness"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

// The identifiers the stub daemon reports. They differ from what newWorld
// seeds on purpose: that is what makes it visible whether a value in the bag
// came from the daemon or from the harness.
const (
	stubDeviceID = "7c0ffee0-1234-4000-8000-0123456789ab"
	stubThingID  = "5eaf00d0-1234-4000-8000-0123456789ab"
	stubOrgID    = "a11ce000-1234-4000-8000-0123456789ab"
)

// stubDaemon serves the endpoints the action primitives touch. It is
// deliberately tiny: the real daemon is driven by the scenario suite itself,
// and what needs testing here is that a step sends the right thing.
func stubDaemon(t *testing.T) (string, *[]string) {
	t.Helper()
	var calls []string

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/identity", func(w http.ResponseWriter, _ *http.Request) {
		calls = append(calls, "identity")
		_, _ = w.Write([]byte(`{"uhwid":"3a7bd3e2360a3d29","board_token":"secret-board-token"}`))
	})
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		calls = append(calls, "status")
		_, _ = fmt.Fprintf(w, `{"provisioning":"Provisioned","daemon":"Connected",`+
			`"device_id":%q,"organization_id":%q,"cloud":{"state":"Steady","thing_id":%q}}`,
			stubDeviceID, stubOrgID, stubThingID)
	})
	mux.HandleFunc("POST /v1/provisioning/start", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		calls = append(calls, "start:"+string(body))
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("PUT /v1/variables/{name}", func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, "put:"+r.PathValue("name"))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /v1/variables/{name}/events", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		calls = append(calls, "sse:"+name)
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()
		_, _ = fmt.Fprintf(w, "event: %s\ndata: {\"name\":%q}\n\n", appclient.EventThingUnavailable, name)
		flusher.Flush()
		<-r.Context().Done()
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String(), &calls
}

// newWorld brings up the real servers and points the app client at the stub,
// so every primitive runs against the components it will use for real -- only
// the daemon is missing.
func newWorld(t *testing.T) (*harness.World, *[]string) {
	t.Helper()
	w, err := harness.SetupServers(harness.Config{
		DeviceID: "9f1c2d3e-4567-89ab-cdef-0123456789ab",
		ThingID:  "b2c3d4e5-6789-4abc-8def-0123456789ab",
	})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	t.Cleanup(func() { _ = w.Teardown(context.Background()) })

	baseURL, calls := stubDaemon(t)
	if w.App, err = appclient.New(w.Log, baseURL); err != nil {
		t.Fatalf("appclient: %v", err)
	}
	return w, calls
}

// params parses a step's parameters the way the scenario loader hands them over.
func params(t *testing.T, y string) *yaml.Node {
	t.Helper()
	var node yaml.Node
	if err := yaml.Unmarshal([]byte(y), &node); err != nil {
		t.Fatalf("bad test parameters %q: %v", y, err)
	}
	if len(node.Content) == 0 {
		return &yaml.Node{}
	}
	return node.Content[0]
}

func run(t *testing.T, w *harness.World, name string, y string, cursor eventlog.Cursor) (*Context, eventlog.Cursor, error) {
	t.Helper()
	fn, ok := Default()[name]
	if !ok {
		t.Fatalf("no such step %q", name)
	}
	sc := &Context{World: w, Cursor: cursor, Name: name, Index: 1}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	next, err := fn(ctx, sc, params(t, y))
	return sc, next, err
}

// The expectation primitive: every parameter becomes a constraint on the
// attribute of the same name, which is what lets a scenario say
// `{cmd: Device.begin, lib_version: 0.0.0-e2e}` with no code per field.
func TestExpectationMatchesOnAttributes(t *testing.T) {
	w, _ := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{
		"cmd":         wire.CmdDeviceBegin,
		"lib_version": "0.0.0-e2e",
	}, nil)

	sc, cursor, err := run(t, w, "expect_mqtt_publish",
		"{cmd: Device.begin, lib_version: 0.0.0-e2e, timeout: 2s}", 0)
	if err != nil {
		t.Fatalf("expect_mqtt_publish: %v", err)
	}
	if cursor != 1 {
		t.Errorf("cursor = %d, want 1 (just past the matched event)", cursor)
	}
	// The consuming step is recorded, which is what annotates the timeline and
	// what the strict sweep subtracts.
	if step, ok := w.Log.ConsumedBy(1); !ok || step != "expect_mqtt_publish" {
		t.Errorf("event 1 consumed by %q (%t), want expect_mqtt_publish", step, ok)
	}
	if !strings.Contains(sc.Detail, "Device.begin") {
		t.Errorf("detail = %q, want it to name the command", sc.Detail)
	}
	if len(sc.Predicate.Constraints) != 4 {
		t.Errorf("predicate has %d constraints, want source, kind and the two parameters",
			len(sc.Predicate.Constraints))
	}
}

// An expectation that is not met returns a timeout carrying the near misses,
// which is what turns "timeout after 15s" into a field-level diff.
func TestExpectationTimesOutWithNearMisses(t *testing.T) {
	w, _ := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{
		"cmd":         wire.CmdDeviceBegin,
		"lib_version": "0.0.0-dev",
	}, nil)

	_, cursor, err := run(t, w, "expect_mqtt_publish",
		"{cmd: Device.begin, lib_version: 0.0.0-e2e, timeout: 100ms}", 0)
	if err == nil {
		t.Fatal("the expectation passed against a different lib_version")
	}
	var timeout *eventlog.TimeoutError
	if !errors.As(err, &timeout) {
		t.Fatalf("error is %T (%v), want a *TimeoutError", err, err)
	}
	if len(timeout.NearMiss) == 0 {
		t.Error("no near miss recorded, so the report cannot say which field differed")
	}
	if cursor != 0 {
		t.Errorf("cursor = %d, want it left where it was on failure", cursor)
	}
}

// The state waits read best with `state:` in YAML, which is not the attribute
// the poller records. The rename is what keeps both sides natural.
func TestStateWaitsRenameTheParameter(t *testing.T) {
	w, _ := newWorld(t)
	w.Log.Append(eventlog.SourceDaemonStatus, eventlog.KindStatusPoll, map[string]any{
		"daemon":       "Provisioning",
		"provisioning": "unprovisioned",
		"cloud_state":  "Steady",
	}, nil)

	for _, tc := range []struct{ step, y string }{
		{"await_daemon_state", "{state: Provisioning, timeout: 2s}"},
		{"await_daemon_cloud_state", "{state: Steady, timeout: 2s}"},
		{"await_daemon_provisioning_state", "{state: unprovisioned, timeout: 2s}"},
	} {
		if _, _, err := run(t, w, tc.step, tc.y, 0); err != nil {
			t.Errorf("%s: %v", tc.step, err)
		}
	}
}

// The cursor is what gives ordering for free: a second expectation for the
// same thing must match the second event, not re-match the first.
func TestTheCursorPreventsRematching(t *testing.T) {
	w, _ := newWorld(t)
	for i := range 2 {
		w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTSubscribe, map[string]any{
			"topic": fmt.Sprintf("/a/d/x/c/dw-%d", i),
		}, nil)
	}

	_, first, err := run(t, w, "expect_mqtt_subscribe", "{timeout: 2s}", 0)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	_, second, err := run(t, w, "expect_mqtt_subscribe", "{timeout: 2s}", first)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if second != first+1 {
		t.Errorf("second cursor = %d, want %d", second, first+1)
	}
}

// cloud_publish is the cloud half of the handshake, and the injection lands on
// the timeline as a harness note -- never as a device publish, or an
// expectation could match the harness's own downlink.
func TestCloudPublish(t *testing.T) {
	w, _ := newWorld(t)

	tests := []struct {
		step string
		y    string
		cmd  string
	}{
		{"cloud_publish", "{cmd: Thing.update, thing_id: b2c3d4e5-6789-4abc-8def-0123456789ab}", wire.CmdThingUpdate},
		{"cloud_publish", "{cmd: Thing.detach, thing_id: b2c3d4e5-6789-4abc-8def-0123456789ab}", wire.CmdThingDetach},
		{"cloud_publish", "{cmd: LastValues.update, values: [{name: temp, value: 21.5}]}", wire.CmdLastValuesUpdate},
	}
	cursor := eventlog.Cursor(0)
	for _, tc := range tests {
		if _, _, err := run(t, w, tc.step, tc.y, cursor); err != nil {
			t.Fatalf("%s %s: %v", tc.step, tc.cmd, err)
		}
		ev, next := awaitNote(t, w, cursor, tc.cmd)
		if ev.Significant() {
			t.Errorf("%s was recorded as significant", tc.cmd)
		}
		cursor = next
	}

	// A command the cloud does not send must be refused by name, not encoded
	// into something the daemon will silently ignore.
	if _, _, err := run(t, w, "cloud_publish", "{cmd: Device.begin}", 0); err == nil {
		t.Error("cloud_publish accepted an uplink command")
	}
}

func TestCloudPublishVar(t *testing.T) {
	w, _ := newWorld(t)

	if _, _, err := run(t, w, "cloud_publish_var", "{variable: temp, value: 30.0}", 0); err != nil {
		t.Fatalf("cloud_publish_var: %v", err)
	}
	ev, _ := awaitEventForTest(t, w, 0, eventlog.Predicate{
		Label: "property downlink",
		Constraints: []eventlog.Constraint{
			eventlog.Eq("kind", string(eventlog.KindHarnessNote)),
			eventlog.Eq("attrs.value.temp", 30.0),
		},
	})
	if got := ev.Attrs["topic"]; got != "/a/t/b2c3d4e5-6789-4abc-8def-0123456789ab/e/i" {
		t.Errorf("topic = %v, want the thing inbound topic", got)
	}

	if _, _, err := run(t, w, "cloud_publish_var", "{}", 0); err == nil {
		t.Error("cloud_publish_var with no values succeeded")
	}
}

// The app-role actions: what they send is what the scenario said.
func TestAppActions(t *testing.T) {
	w, calls := newWorld(t)

	if _, _, err := run(t, w, "app_post", "{path: /v1/provisioning/start}", 0); err != nil {
		t.Fatalf("app_post: %v", err)
	}
	if _, _, err := run(t, w, "app_var_write", "{variable: temp, value: 42.0}", 0); err != nil {
		t.Fatalf("app_var_write: %v", err)
	}
	if _, _, err := run(t, w, "app_var_subscribe", "{variable: temp}", 0); err != nil {
		t.Fatalf("app_var_subscribe: %v", err)
	}
	// The subscription is open before the step returns, so the frame the daemon
	// sends immediately is already on the timeline.
	awaitEventForTest(t, w, 0, eventlog.Predicate{
		Label: "first SSE frame",
		Constraints: []eventlog.Constraint{
			eventlog.Eq("kind", string(eventlog.KindSSEFrame)),
			eventlog.Eq("attrs.variable", "temp"),
		},
	})

	want := []string{"start:", "put:temp", "sse:temp"}
	got := *calls
	if len(got) != len(want) {
		t.Fatalf("the daemon saw %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("call %d = %q, want %q", i, got[i], want[i])
		}
	}
}

// The uhwid is the one value only the daemon knows, and a scenario needs it to
// assert on the CSR subject. Learning it into the bag is the whole point of
// the step.
func TestGetDeviceIdentityLearnsTheUHWID(t *testing.T) {
	w, calls := newWorld(t)

	if _, ok := w.Vars.Get("uhwid"); ok {
		t.Fatal("the bag already holds a uhwid, so this test would prove nothing")
	}
	sc, _, err := run(t, w, "get_device_identity", "{}", 0)
	if err != nil {
		t.Fatalf("get_device_identity: %v", err)
	}
	if got := w.Vars.MustGet("uhwid"); got != "3a7bd3e2360a3d29" {
		t.Errorf("bag[uhwid] = %q, want the daemon value", got)
	}
	if !strings.Contains(sc.Detail, "3a7bd3e2360a3d29") {
		t.Errorf("detail = %q, want it to name the uhwid", sc.Detail)
	}
	if got := *calls; len(got) != 1 || got[0] != "identity" {
		t.Errorf("the daemon saw %v, want one identity call", got)
	}

	// And the value is then usable where it matters: the CSR subject the fake
	// API records.
	resolved, err := w.Vars.Interpolate("CN={uhwid}")
	if err != nil {
		t.Fatalf("interpolate: %v", err)
	}
	if resolved != "CN=3a7bd3e2360a3d29" {
		t.Errorf("interpolated %q", resolved)
	}

	// The board token must not follow it into the bag: the bag is dumped into
	// the artifact.
	for _, key := range w.Vars.Keys() {
		value := w.Vars.MustGet(key)
		if strings.Contains(value, "secret-board-token") {
			t.Errorf("the board token reached the bag as %q", key)
		}
	}
}

func TestStartProvisioning(t *testing.T) {
	w, calls := newWorld(t)

	if _, _, err := run(t, w, "start_provisioning", "{}", 0); err != nil {
		t.Fatalf("start_provisioning: %v", err)
	}
	// No organization: the body is omitted entirely, as the API allows.
	if got := *calls; len(got) != 1 || got[0] != "start:" {
		t.Errorf("the daemon saw %v, want a start with no body", got)
	}

	sc, _, err := run(t, w, "start_provisioning", "{organization_id: org-1}", 0)
	if err != nil {
		t.Fatalf("start_provisioning: %v", err)
	}
	if got := (*calls)[1]; !strings.Contains(got, `"organization_id":"org-1"`) {
		t.Errorf("the daemon saw %q, want the organization id", got)
	}
	if !strings.Contains(sc.Detail, "org-1") {
		t.Errorf("detail = %q", sc.Detail)
	}
	// The organization is worth keeping: a later step asserts the daemon
	// stored it, and the status reports it back.
	if got, _ := w.Vars.Get("organization_id"); got != "org-1" {
		t.Errorf("bag[organization_id] = %q", got)
	}
}

// get_daemon_status exists for the bag, not for the timeline: the identifiers it
// learns must come from the daemon and must beat what Setup seeded from the
// fake Provisioning API. The stub therefore answers with ids that differ from
// the seeds, which is the only way to tell the two apart.
func TestGetDaemonStatusLearnsTheIdentifiersFromTheDaemon(t *testing.T) {
	w, calls := newWorld(t)

	seededDevice := w.Vars.MustGet("device_id")
	seededThing := w.Vars.MustGet("thing_id")
	if seededDevice == stubDeviceID || seededThing == stubThingID {
		t.Fatal("a seed already equals the stub's answer, so this test would prove nothing")
	}
	if _, ok := w.Vars.Get("organization_id"); ok {
		t.Fatal("the bag already holds an organization_id")
	}

	sc, next, err := run(t, w, "get_daemon_status", "{require: [device_id, thing_id]}", 0)
	if err != nil {
		t.Fatalf("get_daemon_status: %v", err)
	}
	if next != 0 {
		t.Errorf("cursor moved to %d: an action must leave it where it was", next)
	}
	for _, tc := range []struct{ key, want string }{
		{"device_id", stubDeviceID},
		{"thing_id", stubThingID},
		{"organization_id", stubOrgID},
	} {
		if got := w.Vars.MustGet(tc.key); got != tc.want {
			t.Errorf("bag[%s] = %q, want the daemon's %q", tc.key, got, tc.want)
		}
	}
	// The report has to name a changed identifier and say what it replaced --
	// that line is the whole point of the step in a re-provisioning run.
	if !strings.Contains(sc.Detail, stubDeviceID) || !strings.Contains(sc.Detail, "was "+seededDevice) {
		t.Errorf("detail = %q, want the new device id and the seed it replaced", sc.Detail)
	}
	if !strings.Contains(sc.Detail, "daemon=Connected") {
		t.Errorf("detail = %q, want the daemon state", sc.Detail)
	}
	if got := *calls; len(got) != 1 || got[0] != "status" {
		t.Errorf("the daemon saw %v, want one status call", got)
	}

	// And the read lands on the timeline, so a failure report shows the status
	// the step acted on rather than only its own summary.
	awaitEventForTest(t, w, 0, eventlog.Predicate{
		Label: "status poll",
		Constraints: []eventlog.Constraint{
			eventlog.Eq("source", string(eventlog.SourceDaemonStatus)),
			eventlog.Eq("kind", string(eventlog.KindStatusPoll)),
			eventlog.Eq("attrs.device_id", stubDeviceID),
		},
	})
}

// The re-provisioning sequence, which is the case the step was added for: no
// device id yet, then one, then a different one.
func TestGetDaemonStatusRelearnsTheDeviceID(t *testing.T) {
	w, _ := newWorld(t)
	seeded := w.Vars.MustGet("device_id")

	const (
		unprovisioned = `{"provisioning":"NotProvisioned","daemon":"Idle"}`
		firstDevice   = `{"provisioning":"Provisioned","daemon":"Connected","device_id":"1111aaaa-0000-4000-8000-000000000001"}`
		secondDevice  = `{"provisioning":"Provisioned","daemon":"Connected","device_id":"2222bbbb-0000-4000-8000-000000000002"}`
	)
	var err error
	if w.App, err = appclient.New(w.Log, sequencedStatusDaemon(t,
		unprovisioned, unprovisioned, firstDevice, secondDevice)); err != nil {
		t.Fatalf("appclient: %v", err)
	}

	// Before provisioning the status carries no device id, and an empty value
	// must not be learned: writing "" over the seed would make cloud_publish
	// address "/a/d//c/dw" -- a topic that never matches, reported as a failed
	// expectation instead of as the step that blanked the value.
	if _, _, err := run(t, w, "get_daemon_status", "{}", 0); err != nil {
		t.Fatalf("get_daemon_status on an unprovisioned daemon: %v", err)
	}
	if got := w.Vars.MustGet("device_id"); got != seeded {
		t.Fatalf("bag[device_id] = %q after an unprovisioned status, want the seed %q", got, seeded)
	}

	// require is what turns that silence into a failure at the step, for a
	// scenario that has reached the point where the id must exist.
	if _, _, err := run(t, w, "get_daemon_status", "{require: [device_id]}", 0); err == nil {
		t.Error("get_daemon_status passed with no device id, want an error")
	}

	if _, _, err := run(t, w, "get_daemon_status", "{require: [device_id]}", 0); err != nil {
		t.Fatalf("get_daemon_status after provisioning: %v", err)
	}
	if got := w.Vars.MustGet("device_id"); got != "1111aaaa-0000-4000-8000-000000000001" {
		t.Fatalf("bag[device_id] = %q, want the first provisioned id", got)
	}

	sc, _, err := run(t, w, "get_daemon_status", "{}", 0)
	if err != nil {
		t.Fatalf("get_daemon_status after re-provisioning: %v", err)
	}
	if got := w.Vars.MustGet("device_id"); got != "2222bbbb-0000-4000-8000-000000000002" {
		t.Errorf("bag[device_id] = %q, want the re-provisioned id", got)
	}
	if !strings.Contains(sc.Detail, "was 1111aaaa-0000-4000-8000-000000000001") {
		t.Errorf("detail = %q, want it to name the id that was replaced", sc.Detail)
	}
	// A topic built afterwards must follow the daemon, not the harness.
	topic, err := w.Vars.Interpolate("/a/d/{device_id}/c/dw")
	if err != nil {
		t.Fatalf("interpolate: %v", err)
	}
	if topic != "/a/d/2222bbbb-0000-4000-8000-000000000002/c/dw" {
		t.Errorf("interpolated %q", topic)
	}
}

// A require naming a field the status does not carry is the scenario's
// mistake, and the message has to say so: "the daemon reports no uhwid" would
// send a reader looking at the daemon for a typo in the YAML.
func TestGetDaemonStatusRejectsAnUnknownRequireField(t *testing.T) {
	w, calls := newWorld(t)

	_, _, err := run(t, w, "get_daemon_status", "{require: [uhwid]}", 0)
	if err == nil {
		t.Fatal("the step passed, want an error")
	}
	if !strings.Contains(err.Error(), "cannot require") ||
		!strings.Contains(err.Error(), "device_id") {
		t.Errorf("error = %v, want it to name the fields the status carries", err)
	}
	// And it is caught before the call: finding a scenario typo must not
	// depend on a reachable daemon.
	if got := *calls; len(got) != 0 {
		t.Errorf("the daemon saw %v, want no call at all", got)
	}
}

// sequencedStatusDaemon serves one status body per call, repeating the last.
// A scenario's status changes over time and the assertions here are about
// exactly that, so the stub has to be able to change its answer.
func sequencedStatusDaemon(t *testing.T, bodies ...string) string {
	t.Helper()
	var (
		mu   sync.Mutex
		call int
	)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/status", func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		body := bodies[min(call, len(bodies)-1)]
		call++
		mu.Unlock()
		_, _ = w.Write([]byte(body))
	})

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}

func TestActionsValidateTheirParameters(t *testing.T) {
	w, _ := newWorld(t)
	tests := []struct{ step, y string }{
		{"app_post", "{}"},
		{"get_device_identity", "{uhwid: x}"},
		{"get_daemon_status", "{device_id: x}"},
		{"start_provisioning", "{organization: x}"},
		{"app_var_write", "{value: 1}"},
		{"app_var_subscribe", "{}"},
		// A misspelled parameter must fail rather than be ignored: the step
		// would otherwise quietly do something else.
		{"app_var_write", "{variable: temp, valu: 1}"},
		{"cloud_publish", "{cmd: Thing.update, thingid: x}"},
		{"stop_daemon", "{}"}, // no daemon in this World
	}
	for _, tc := range tests {
		t.Run(tc.step+" "+tc.y, func(t *testing.T) {
			if _, _, err := run(t, w, tc.step, tc.y, 0); err == nil {
				t.Error("the step succeeded, want an error")
			}
		})
	}
}

func TestTakeTimeout(t *testing.T) {
	if got, err := takeTimeout(map[string]any{}); err != nil || got != DefaultTimeout {
		t.Errorf("no timeout given: got %v, %v; want the default", got, err)
	}
	params := map[string]any{"timeout": "250ms", "cmd": "x"}
	got, err := takeTimeout(params)
	if err != nil {
		t.Fatalf("takeTimeout: %v", err)
	}
	if got != 250*time.Millisecond {
		t.Errorf("timeout = %v, want 250ms", got)
	}
	// It must be removed, or it would become a constraint on a non-existent
	// attribute and nothing would ever match.
	if _, ok := params["timeout"]; ok {
		t.Error("the timeout stayed in the parameters")
	}
	if _, ok := params["cmd"]; !ok {
		t.Error("takeTimeout removed a real parameter")
	}
	if _, err := takeTimeout(map[string]any{"timeout": "soon"}); err == nil {
		t.Error("an unparsable timeout was accepted")
	}
	if got, err := takeTimeout(map[string]any{"timeout": 3}); err != nil || got != 3*time.Second {
		t.Errorf("a bare number should read as seconds: got %v, %v", got, err)
	}
}

// The predicate convention is the scenario query language, and it is shared
// with the tolerate list, so it is worth pinning on its own.
func TestBuildPredicate(t *testing.T) {
	p := BuildPredicate(
		map[string]any{"source": "mqtt"},
		map[string]any{"cmd": "Thing.begin", "thing_id": ""},
		nil,
	)
	fields := map[string]bool{}
	for _, c := range p.Constraints {
		fields[c.Field] = true
	}
	for _, want := range []string{"source", "attrs.cmd", "attrs.thing_id"} {
		if !fields[want] {
			t.Errorf("no constraint on %q (got %v)", want, fields)
		}
	}
	if p.Label == "" {
		t.Error("the predicate has no label, so a failure cannot name it")
	}

	// seq and kind stay event fields; everything else is an attribute.
	p = BuildPredicate(nil, map[string]any{"kind": "mqtt_publish", "seq": 3, "topic": "x"}, nil)
	fields = map[string]bool{}
	for _, c := range p.Constraints {
		fields[c.Field] = true
	}
	for _, want := range []string{"kind", "seq", "attrs.topic"} {
		if !fields[want] {
			t.Errorf("no constraint on %q (got %v)", want, fields)
		}
	}
}

func TestDefaultRegistryCoversTheScenarioVocabulary(t *testing.T) {
	reg := Default()
	for _, name := range []string{
		"await_daemon_state", "await_daemon_cloud_state", "await_daemon_provisioning_state",
		"expect_api_call", "expect_mqtt_connect", "expect_mqtt_disconnect",
		"expect_mqtt_subscribe", "expect_mqtt_unsubscribe", "expect_mqtt_publish",
		"expect_var_publish", "expect_app_event", "expect_tls_error", "expect_daemon_exit",
		"get_device_identity", "get_daemon_status", "start_provisioning",
		"app_post", "app_var_write", "app_var_subscribe",
		"cloud_publish", "cloud_publish_var", "stop_daemon",
	} {
		if _, ok := reg[name]; !ok {
			t.Errorf("the registry has no %q", name)
		}
	}
}

func awaitNote(t *testing.T, w *harness.World, from eventlog.Cursor, cmd string) (eventlog.Event, eventlog.Cursor) {
	t.Helper()
	return awaitEventForTest(t, w, from, eventlog.Predicate{
		Label: "harness note " + cmd,
		Constraints: []eventlog.Constraint{
			eventlog.Eq("kind", string(eventlog.KindHarnessNote)),
			eventlog.Eq("attrs.cmd", cmd),
		},
	})
}

func awaitEventForTest(t *testing.T, w *harness.World, from eventlog.Cursor, p eventlog.Predicate) (eventlog.Event, eventlog.Cursor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ev, cursor, err := w.Log.Await(ctx, from, p, 5*time.Second)
	if err != nil {
		t.Fatalf("awaiting %s: %v", p, err)
	}
	return ev, cursor
}
