// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"context"
	"strings"
	"testing"
	"time"
)

// simulateHandshakeFailure builds a log that mirrors the real boot handshake
// up to the point where the daemon subscribes to the property topic and then
// never publishes LastValues.begin — the failure the report has to explain.
func simulateHandshakeFailure(t *testing.T) Result {
	t.Helper()
	l := New()

	type row struct {
		src   Source
		kind  Kind
		attrs map[string]any
	}
	rows := []row{
		{SourceDaemonLog, KindDaemonLine, map[string]any{"line": "starting, version=0.0.0-e2e"}},
		{SourceNTP, KindNTPProbe, map[string]any{"bytes": 48}},
		{SourceDaemonStatus, KindStatusPoll, map[string]any{"state": "Provisioning"}},
		{SourceProvisioningAPI, KindHTTPRequest, map[string]any{"endpoint": "provision/csr", "status": 200}},
		{SourceProvisioningAPI, KindHTTPRequest, map[string]any{"endpoint": "provision/complete", "status": 200}},
		{SourceMQTT, KindMQTTConnect, map[string]any{"client_id": "9f1c2d3e", "cert_cn": "9f1c2d3e"}},
		{SourceMQTT, KindMQTTSubscribe, map[string]any{"topic": "/a/d/9f1c2d3e/c/dw"}},
		{SourceMQTT, KindMQTTPublish, map[string]any{"cmd": "Device.begin", "lib_version": "0.0.0-dev"}},
		{SourceMQTT, KindMQTTPublish, map[string]any{"cmd": "Thing.begin", "thing_id": ""}},
		{SourceMQTT, KindMQTTPublish, map[string]any{"cmd": "Thing.update", "thing_id": "abc", "injected": true}},
		{SourceMQTT, KindMQTTSubscribe, map[string]any{"topic": "/a/t/abc/e/i"}},
	}
	var events []Event
	for _, r := range rows {
		events = append(events, l.Append(r.src, r.kind, r.attrs, nil))
	}

	// Deliberately no step claims the Device.begin at #8: the report should
	// flag it under "unexpected events" and mark it with + in the timeline,
	// so this one scenario exercises both the failure diff and the strict
	// sweep.
	steps := []StepResult{
		{Index: 1, Name: "await_daemon_state", Detail: "state=Provisioning", Status: StepPassed, Since: events[2].Since, MatchedSeq: 3},
		{Index: 2, Name: "expect_api_call", Detail: "provision/csr", Status: StepPassed, Since: events[3].Since, MatchedSeq: 4},
		{Index: 3, Name: "expect_api_call", Detail: "provision/complete", Status: StepPassed, Since: events[4].Since, MatchedSeq: 5},
		{Index: 4, Name: "expect_mqtt_connect", Detail: "client_id_matches_cert_cn", Status: StepPassed, Since: events[5].Since, MatchedSeq: 6},
		{Index: 5, Name: "expect_mqtt_subscribe", Detail: "/a/d/{device_id}/c/dw", Status: StepPassed, Since: events[6].Since, MatchedSeq: 7},
		{Index: 6, Name: "expect_mqtt_publish", Detail: "cmd=Thing.begin", Status: StepPassed, Since: events[8].Since, MatchedSeq: 9},
		{Index: 7, Name: "cloud_publish", Detail: "cmd=Thing.update", Status: StepPassed, Since: events[9].Since, MatchedSeq: 10},
		{Index: 8, Name: "expect_mqtt_subscribe", Detail: "/a/t/{thing_id}/e/i", Status: StepPassed, Since: events[10].Since, MatchedSeq: 11},
	}
	for _, s := range steps {
		l.Consume(s.MatchedSeq, "step "+itoa(s.Index)+" ✓")
	}

	// Step 9 waits for LastValues.begin from just past the last consumed
	// event, and times out.
	_, _, err := l.Await(context.Background(), Cursor(11), wantCmd("LastValues.begin"), 30*time.Millisecond)
	if err == nil {
		t.Fatal("expected the simulated step to time out")
	}
	steps = append(steps,
		StepResult{Index: 9, Name: "expect_mqtt_publish", Detail: "cmd=LastValues.begin", Status: StepFailed, Err: err},
		StepResult{Index: 10, Name: "cloud_publish", Detail: "cmd=LastValues.update", Status: StepSkipped},
		StepResult{Index: 11, Name: "await_daemon_cloud_state", Detail: "state=Steady", Status: StepSkipped},
	)

	return l.Result("full-lifecycle", steps, nil)
}

// TestReportOnSimulatedFailure is the readability check for the format: run it
// with -v to see exactly what a developer gets on a red scenario.
func TestReportOnSimulatedFailure(t *testing.T) {
	r := simulateHandshakeFailure(t)
	out := r.Format()
	t.Logf("\n%s", out)

	if !r.Failed() {
		t.Fatal("Failed() = false, want true")
	}

	// The header must name where it broke, not just that it broke.
	if !strings.Contains(out, "FAIL at step 9/11") {
		t.Error("header does not name the failing step")
	}
	// Steps must be summarised with their outcome.
	for _, want := range []string{"1 ✓", "9 ✗", "10 –", "not run"} {
		if !strings.Contains(out, want) {
			t.Errorf("step summary missing %q", want)
		}
	}
	// The expectation must appear at the point in the timeline where it was
	// waiting — that placement is the whole reason for this format.
	if !strings.Contains(out, "waiting from here") {
		t.Error("timeline does not splice in the pending expectation")
	}
	waitIdx := strings.Index(out, "waiting from here")
	subIdx := strings.Index(out, "/a/t/abc/e/i")
	if waitIdx < 0 || subIdx < 0 || subIdx > waitIdx {
		t.Error("the wait marker must come after the last event the scenario consumed")
	}
	// Consumed events must be annotated so the reader can see which step took what.
	if !strings.Contains(out, "← step 8 ✓") {
		t.Error("timeline does not annotate consumed events with their step")
	}
}

// The near-miss diff is the difference between "timeout" and a diagnosis.
func TestReportShowsFieldLevelDiff(t *testing.T) {
	l := New()
	l.Append(SourceMQTT, KindMQTTPublish, map[string]any{
		"cmd": "Device.begin", "lib_version": "0.0.0-dev",
	}, nil)

	want := Predicate{
		Label: "mqtt PUBLISH cmd=Device.begin lib_version=0.0.0-e2e",
		Constraints: []Constraint{
			Eq("source", "mqtt"),
			Eq("kind", "mqtt_publish"),
			Eq("attrs.cmd", "Device.begin"),
			Eq("attrs.lib_version", "0.0.0-e2e"),
		},
	}
	_, _, err := l.Await(context.Background(), 0, want, 20*time.Millisecond)

	r := l.Result("version-mismatch", []StepResult{
		{Index: 1, Name: "expect_mqtt_publish", Detail: "cmd=Device.begin", Status: StepFailed, Err: err},
	}, nil)
	out := r.Format()
	t.Logf("\n%s", out)

	for _, s := range []string{
		"3/4 constraints satisfied",
		"attrs.lib_version",
		`want "0.0.0-e2e"`,
		`got "0.0.0-dev"`,
	} {
		if !strings.Contains(out, s) {
			t.Errorf("report missing %q", s)
		}
	}
}

// An unexpected event must be reported as such AND marked in the timeline.
func TestReportMarksUnexpectedEvents(t *testing.T) {
	l := New()
	ev := publish(l, "Device.begin")
	l.Consume(ev.Seq, "step 1 ✓")
	publish(l, "Device.begin") // a second announce with no reconnect: a real bug

	r := l.Result("strict", []StepResult{
		{Index: 1, Name: "expect_mqtt_publish", Detail: "cmd=Device.begin", Status: StepPassed, MatchedSeq: ev.Seq},
	}, nil)
	out := r.Format()
	t.Logf("\n%s", out)

	if !r.Failed() {
		t.Fatal("an unclaimed significant event must fail the scenario")
	}
	if !strings.Contains(out, "unexpected events") {
		t.Error("report does not call out unexpected events")
	}
	if !strings.Contains(out, "+ #2") {
		t.Error("timeline does not mark the unexpected event with +")
	}
	// The header must agree with the verdict. Every step passed here, so the
	// sweep is the only thing that failed the run -- and the header used to
	// read PASS while Failed() said otherwise, which is what a CI artifact
	// opens with.
	header, _, _ := strings.Cut(out, "\n")
	if strings.Contains(header, "PASS") || !strings.Contains(header, "FAIL") {
		t.Errorf("header = %q, want it to report the failure", header)
	}
	if !strings.Contains(header, "unclaimed") {
		t.Errorf("header = %q, want it to name the reason", header)
	}
}

func TestReportOnPass(t *testing.T) {
	l := New()
	ev := publish(l, "Device.begin")
	l.Consume(ev.Seq, "step 1 ✓")

	r := l.Result("happy", []StepResult{
		{Index: 1, Name: "expect_mqtt_publish", Status: StepPassed, MatchedSeq: ev.Seq},
	}, nil)

	if r.Failed() {
		t.Fatal("Failed() = true, want false")
	}
	if !strings.Contains(r.Format(), "PASS") {
		t.Error("a passing report should say PASS")
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
