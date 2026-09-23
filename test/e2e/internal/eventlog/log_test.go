// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func publish(l *Log, cmd string) Event {
	return l.Append(SourceMQTT, KindMQTTPublish, map[string]any{"cmd": cmd}, nil)
}

func wantCmd(cmd string) Predicate {
	return Predicate{
		Label: "mqtt PUBLISH cmd=" + cmd,
		Constraints: []Constraint{
			Eq("source", string(SourceMQTT)),
			Eq("kind", string(KindMQTTPublish)),
			Eq("attrs.cmd", cmd),
		},
	}
}

// The property the whole strict-sweep design rests on: an event that arrived
// BEFORE the step started waiting must still satisfy it. Producers run on
// their own goroutines, so an event landing microseconds ahead of the step
// that wants it is normal, not a fault — a log that only watched for future
// arrivals would race with itself.
func TestAwaitMatchesEventsAlreadyInTheLog(t *testing.T) {
	l := New()
	publish(l, "Device.begin")
	publish(l, "Thing.begin")

	ev, cur, err := l.Await(context.Background(), 0, wantCmd("Thing.begin"), time.Second)
	if err != nil {
		t.Fatalf("Await: %v", err)
	}
	if ev.Seq != 2 {
		t.Errorf("matched Seq = %d, want 2", ev.Seq)
	}
	// The cursor is the Seq of the last event accounted for, so the next scan
	// starts strictly after it.
	if cur != 2 {
		t.Errorf("cursor = %d, want 2 (the Seq just matched)", cur)
	}
}

// And it must also wake on something that arrives later, without polling.
func TestAwaitWakesOnLaterAppend(t *testing.T) {
	l := New()

	done := make(chan Event, 1)
	go func() {
		ev, _, err := l.Await(context.Background(), 0, wantCmd("LastValues.begin"), 5*time.Second)
		if err != nil {
			t.Errorf("Await: %v", err)
			close(done)
			return
		}
		done <- ev
	}()

	// Give the waiter a chance to block, then produce.
	publish(l, "Device.begin")
	publish(l, "LastValues.begin")

	select {
	case ev, ok := <-done:
		if !ok {
			t.Fatal("Await failed")
		}
		if ev.Attrs["cmd"] != "LastValues.begin" {
			t.Fatalf("matched %v", ev.Attrs)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Await did not wake on append")
	}
}

// The cursor is what keeps two identical expectations from both matching the
// same event: the second must see only what came after the first.
func TestAwaitCursorSkipsAlreadyMatched(t *testing.T) {
	l := New()
	publish(l, "Thing.begin")
	publish(l, "Thing.begin")

	first, cur, err := l.Await(context.Background(), 0, wantCmd("Thing.begin"), time.Second)
	if err != nil {
		t.Fatalf("first Await: %v", err)
	}
	second, _, err := l.Await(context.Background(), cur, wantCmd("Thing.begin"), time.Second)
	if err != nil {
		t.Fatalf("second Await: %v", err)
	}
	if first.Seq == second.Seq {
		t.Fatalf("both matched Seq %d; the cursor did not advance", first.Seq)
	}
}

// A timeout must carry the diagnosis, not just the fact. This is what the
// near-miss diff in the report is built from.
func TestAwaitTimeoutCarriesNearMisses(t *testing.T) {
	l := New()
	l.Append(SourceMQTT, KindMQTTPublish, map[string]any{
		"cmd": "Device.begin", "lib_version": "0.0.0-dev",
	}, nil)
	l.Append(SourceDaemonLog, KindDaemonLine, map[string]any{"line": "unrelated"}, nil)

	want := Predicate{
		Label: "mqtt PUBLISH cmd=Device.begin lib_version=0.0.0-e2e",
		Constraints: []Constraint{
			Eq("source", string(SourceMQTT)),
			Eq("kind", string(KindMQTTPublish)),
			Eq("attrs.cmd", "Device.begin"),
			Eq("attrs.lib_version", "0.0.0-e2e"),
		},
	}

	_, _, err := l.Await(context.Background(), 0, want, 50*time.Millisecond)
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("error = %v, want *TimeoutError", err)
	}
	if len(te.NearMiss) != 1 {
		t.Fatalf("near misses = %d, want 1 (the unrelated log line must not be listed)", len(te.NearMiss))
	}
	nm := te.NearMiss[0]
	if nm.Result.Passed != 3 || nm.Result.Total != 4 {
		t.Errorf("score = %d/%d, want 3/4", nm.Result.Passed, nm.Result.Total)
	}
	if len(nm.Result.Failures) != 1 || nm.Result.Failures[0].Constraint.Field != "attrs.lib_version" {
		t.Errorf("failures = %+v, want the single lib_version mismatch", nm.Result.Failures)
	}
	if nm.Result.Failures[0].Got != "0.0.0-dev" {
		t.Errorf("got = %v, want 0.0.0-dev", nm.Result.Failures[0].Got)
	}
}

// The cursor must be honoured on timeout too: an event before it is not a
// near miss, it is somebody else's match.
func TestAwaitTimeoutIgnoresEventsBeforeTheCursor(t *testing.T) {
	l := New()
	publish(l, "Device.begin")

	_, _, err := l.Await(context.Background(), 1, wantCmd("Device.begin"), 50*time.Millisecond)
	var te *TimeoutError
	if !errors.As(err, &te) {
		t.Fatalf("error = %v, want *TimeoutError", err)
	}
	if te.Scanned != 0 {
		t.Errorf("scanned = %d, want 0 (everything was before the cursor)", te.Scanned)
	}
}

func TestAwaitHonoursContextCancellation(t *testing.T) {
	l := New()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := l.Await(ctx, 0, wantCmd("never"), 10*time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
}

// Every source appends from its own goroutine, so Seq must stay unique and
// dense under concurrency. Run with -race.
func TestConcurrentAppendsProduceDenseUniqueSeq(t *testing.T) {
	l := New()
	const writers, each = 8, 50

	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < each; i++ {
				publish(l, "x")
			}
		}()
	}
	wg.Wait()

	events := l.Events()
	if len(events) != writers*each {
		t.Fatalf("events = %d, want %d", len(events), writers*each)
	}
	for i, ev := range events {
		if ev.Seq != i+1 {
			t.Fatalf("event %d has Seq %d; sequence is not dense", i, ev.Seq)
		}
	}
}
