// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import "testing"

func TestMatchScoresEveryConstraint(t *testing.T) {
	e := Event{
		Seq: 7, Source: SourceMQTT, Kind: KindMQTTPublish,
		Attrs: map[string]any{"cmd": "Thing.begin", "thing_id": ""},
	}
	p := Predicate{Constraints: []Constraint{
		Eq("source", "mqtt"),       // pass
		Eq("kind", "mqtt_publish"), // pass
		Eq("attrs.cmd", "Thing.update"),
		Eq("attrs.missing", "x"),
	}}

	r := p.Match(e)
	if r.OK {
		t.Fatal("OK = true, want false")
	}
	if r.Passed != 2 || r.Total != 4 {
		t.Errorf("score = %d/%d, want 2/4", r.Passed, r.Total)
	}
	if len(r.Failures) != 2 {
		t.Fatalf("failures = %d, want 2", len(r.Failures))
	}
	// A field that is absent must be distinguishable from one that differs:
	// they are different bugs and read differently in a report.
	byField := map[string]ConstraintFailure{}
	for _, f := range r.Failures {
		byField[f.Constraint.Field] = f
	}
	if !byField["attrs.cmd"].Present {
		t.Error("attrs.cmd should be marked present (it exists, it differs)")
	}
	if byField["attrs.missing"].Present {
		t.Error("attrs.missing should be marked absent")
	}
}

// Scenario values come from YAML (int, float64, string, bool) while Attrs hold
// whatever the decoder produced. Comparing with == would fail scenarios for
// reasons that have nothing to do with the daemon.
func TestLooseEqualCrossesTheYAMLTypeBoundary(t *testing.T) {
	cases := []struct {
		name     string
		got      any
		want     any
		expected bool
	}{
		{"int attr vs float yaml", 21, 21.0, true},
		{"float attr vs int yaml", 21.5, 21.5, true},
		{"float attr vs int yaml, differing", 21.5, 21, false},
		{"uint8 vs int", uint8(1), 1, true},
		{"string vs string", "Steady", "Steady", true},
		{"bool vs bool", true, true, true},
		{"bool attr vs string yaml compares by string form", true, "true", true},
		{"nil vs value", nil, "x", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := looseEqual(c.got, c.want); got != c.expected {
				t.Errorf("looseEqual(%#v, %#v) = %v, want %v", c.got, c.want, got, c.expected)
			}
		})
	}
}

func TestOperators(t *testing.T) {
	e := Event{
		Seq: 5, Source: SourceMQTT, Kind: KindMQTTSubscribe,
		Attrs: map[string]any{"topic": "/a/t/abc-123/e/i"},
	}
	cases := []struct {
		c    Constraint
		want bool
	}{
		{Constraint{"attrs.topic", OpContains, "abc-123"}, true},
		{Constraint{"attrs.topic", OpContains, "zzz"}, false},
		{Constraint{"attrs.topic", OpPrefix, "/a/t/"}, true},
		{Constraint{"attrs.topic", OpPrefix, "/a/d/"}, false},
		{Constraint{"seq", OpGt, 4}, true},
		{Constraint{"seq", OpGt, 5}, false},
		{Constraint{"seq", OpLt, 6}, true},
		{Constraint{"kind", OpNe, "mqtt_publish"}, true},
	}
	for _, tc := range cases {
		p := Predicate{Constraints: []Constraint{tc.c}}
		if got := p.Match(e).OK; got != tc.want {
			t.Errorf("%s %s %v = %v, want %v", tc.c.Field, tc.c.Op, tc.c.Want, got, tc.want)
		}
	}
}

// Near misses are ranked by score so the most plausible candidate is printed
// first, and events that matched nothing are dropped so unrelated traffic does
// not bury the one useful line.
func TestNearMissesRankAndFilter(t *testing.T) {
	p := Predicate{Constraints: []Constraint{
		Eq("source", "mqtt"),
		Eq("kind", "mqtt_publish"),
		Eq("attrs.cmd", "Thing.update"),
	}}
	events := []Event{
		{Seq: 1, Source: SourceDaemonLog, Kind: KindDaemonLine},                                       // 0 matched → dropped
		{Seq: 2, Source: SourceMQTT, Kind: KindMQTTSubscribe},                                         // 1 matched
		{Seq: 3, Source: SourceMQTT, Kind: KindMQTTPublish, Attrs: map[string]any{"cmd": "Thing.be"}}, // 2 matched
	}

	got := nearMisses(events, p, 3)
	if len(got) != 2 {
		t.Fatalf("near misses = %d, want 2 (the log line matched nothing)", len(got))
	}
	if got[0].Event.Seq != 3 {
		t.Errorf("best near miss = #%d, want #3 (highest score first)", got[0].Event.Seq)
	}
}

func TestSignificantClassification(t *testing.T) {
	cases := []struct {
		src  Source
		kind Kind
		want bool
	}{
		{SourceMQTT, KindMQTTPublish, true},
		{SourceMQTT, KindMQTTSubscribe, true},
		{SourceMQTT, KindMQTTConnect, true},
		{SourceMQTT, KindMQTTKeepalive, false}, // would fire every 30s
		{SourceMQTT, KindMQTTAck, false},
		{SourceProvisioningAPI, KindHTTPRequest, true},
		{SourceSSE, KindSSEFrame, true},
		{SourceDaemonLog, KindDaemonLine, false},    // continuous
		{SourceDaemonStatus, KindStatusPoll, false}, // continuous
		{SourceNTP, KindNTPProbe, false},
	}
	for _, c := range cases {
		e := Event{Source: c.src, Kind: c.kind}
		if got := e.Significant(); got != c.want {
			t.Errorf("%s/%s significant = %v, want %v", c.src, c.kind, got, c.want)
		}
	}
}
