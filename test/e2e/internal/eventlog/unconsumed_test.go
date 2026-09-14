// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import "testing"

// The sweep must see only what a scenario is actually making claims about.
// Without the significance filter it would fire on every run, because the
// daemon logs continuously, the status poller ticks and MQTT keepalive runs
// every 30 seconds.
func TestUnconsumedIgnoresInsignificantTraffic(t *testing.T) {
	l := New()
	l.Append(SourceDaemonLog, KindDaemonLine, map[string]any{"line": "starting"}, nil)
	l.Append(SourceDaemonStatus, KindStatusPoll, map[string]any{"state": "Run"}, nil)
	l.Append(SourceNTP, KindNTPProbe, nil, nil)
	l.Append(SourceMQTT, KindMQTTKeepalive, nil, nil)
	l.Append(SourceMQTT, KindMQTTAck, nil, nil)

	if got := l.Unconsumed(nil); len(got) != 0 {
		t.Fatalf("unconsumed = %v, want none", got)
	}
}

func TestUnconsumedReportsUnclaimedSignificantEvents(t *testing.T) {
	l := New()
	claimed := publish(l, "Device.begin")
	publish(l, "Device.begin") // a second one: nobody asked for this
	l.Consume(claimed.Seq, "step 7 expect_publish")

	got := l.Unconsumed(nil)
	if len(got) != 1 {
		t.Fatalf("unconsumed = %d, want 1", len(got))
	}
	if got[0].Seq != 2 {
		t.Errorf("unconsumed Seq = %d, want 2", got[0].Seq)
	}
}

// tolerate is not a loophole, it is required for correctness: the daemon
// retries Thing.begin with back-off while waiting for Thing.update, so a
// scenario that injects the reply after a delay legitimately produces extra
// publishes that no step claims.
func TestTolerateAllowsLegitimateRetries(t *testing.T) {
	l := New()
	first := publish(l, "Thing.begin")
	publish(l, "Thing.begin") // retry while waiting for Thing.update
	publish(l, "Thing.begin") // and another
	rogue := publish(l, "Device.begin")
	l.Consume(first.Seq, "step 8")

	tolerate := []Predicate{wantCmd("Thing.begin")}

	got := l.Unconsumed(tolerate)
	if len(got) != 1 {
		t.Fatalf("unconsumed = %d, want 1 (only the unexpected Device.begin)", len(got))
	}
	if got[0].Seq != rogue.Seq {
		t.Errorf("unconsumed Seq = %d, want %d", got[0].Seq, rogue.Seq)
	}
}

// A scenario is failed by an unexpected event even when every step passed —
// that is the whole point of the strict sweep.
func TestResultFailsOnUnconsumedEvenWithAllStepsPassing(t *testing.T) {
	l := New()
	publish(l, "Device.begin") // nobody claims it

	r := l.Result("probe", []StepResult{
		{Index: 1, Name: "await_daemon_state", Status: StepPassed},
	}, nil)

	if !r.Failed() {
		t.Fatal("Failed() = false; an unclaimed significant event must fail the scenario")
	}
	if len(r.Unconsumed) != 1 {
		t.Fatalf("unconsumed = %d, want 1", len(r.Unconsumed))
	}
}

func TestResultPassesWhenEverythingIsClaimed(t *testing.T) {
	l := New()
	ev := publish(l, "Device.begin")
	l.Consume(ev.Seq, "step 1")

	r := l.Result("probe", []StepResult{
		{Index: 1, Name: "expect_publish", Status: StepPassed, MatchedSeq: ev.Seq},
	}, nil)

	if r.Failed() {
		t.Fatalf("Failed() = true, want false. unconsumed=%v", r.Unconsumed)
	}
}
