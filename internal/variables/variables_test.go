// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package variables

import (
	"reflect"
	"sync"
	"testing"
	"time"
)

// next reads one event from a subscription, failing on timeout.
func next(t *testing.T, sub *Subscription) UpdateEvent {
	t.Helper()
	select {
	case evt, ok := <-sub.Events():
		if !ok {
			t.Fatal("subscription channel closed unexpectedly")
		}
		return evt
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
		return UpdateEvent{}
	}
}

// A subscriber on a variable that has never been set gets no last-value replay
// (the variable is created on demand by the subscribe itself).
func TestSubscribeNoValueYet(t *testing.T) {
	r := NewRegistry()

	first, sub := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub)

	if first.Kind != EventLastValueMissing {
		t.Fatalf("first.Kind = %q for a never-set variable with a thing assigned, want %q",
			first.Kind, EventLastValueMissing)
	}
}

// SetValue stores the value+timestamp and a later subscriber receives them as
// the first ("last value") snapshot.
func TestSubscribeReplaysLastValue(t *testing.T) {
	r := NewRegistry()

	ts := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	r.SetValue("x", int64(42), ts)

	snapshot, sub := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub)

	if snapshot.Kind != EventLastValue {
		t.Fatalf("snapshot.Kind = %q after SetValue, want %q", snapshot.Kind, EventLastValue)
	}
	if snapshot.Value != int64(42) {
		t.Errorf("snapshot.Value = %v, want 42", snapshot.Value)
	}
	if !snapshot.Timestamp.Equal(ts) {
		t.Errorf("snapshot.Timestamp = %v, want %v", snapshot.Timestamp, ts)
	}
	if !snapshot.LastValue {
		t.Error("snapshot.LastValue = false, want true")
	}
}

// A zero timestamp defaults to now (a local PUT does not carry a cloud time).
func TestSetValueZeroTimestampDefaultsToNow(t *testing.T) {
	r := NewRegistry()

	before := time.Now().UTC()
	r.SetValue("x", int64(1), time.Time{})
	v, err := r.Get("x")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v.Timestamp.Before(before) {
		t.Errorf("Timestamp %v is before the call started %v", v.Timestamp, before)
	}
}

// Live updates after subscribe arrive on the channel (not flagged LastValue).
func TestSubscribeReceivesLiveUpdates(t *testing.T) {
	r := NewRegistry()

	_, sub := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub)

	ts := time.Date(2026, 6, 22, 11, 0, 0, 0, time.UTC)
	r.SetValue("x", int64(7), ts)

	evt := next(t, sub)
	if evt.Value != int64(7) {
		t.Errorf("evt.Value = %v, want 7", evt.Value)
	}
	if !evt.Timestamp.Equal(ts) {
		t.Errorf("evt.Timestamp = %v, want %v", evt.Timestamp, ts)
	}
	if evt.LastValue {
		t.Error("live update flagged LastValue, want false")
	}
}

// The per-subscriber FIFO queue preserves order under a burst of rapid writes,
// dropping nothing — the core ordering guarantee.
func TestFIFOOrderingUnderBurst(t *testing.T) {
	r := NewRegistry()
	_, sub := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub)

	const n = 1000
	go func() {
		for i := 0; i < n; i++ {
			r.SetValue("x", int64(i), time.Now().UTC())
		}
	}()

	for i := 0; i < n; i++ {
		evt := next(t, sub)
		if evt.Value != int64(i) {
			t.Fatalf("event %d out of order: got %v, want %d", i, evt.Value, i)
		}
	}
}

// Every new value becomes the latest last value: a subscriber joining after a
// cloud update sees the newest value, not an earlier one.
func TestLastValueIsAlwaysLatest(t *testing.T) {
	r := NewRegistry()

	r.SetValue("x", int64(1), time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC))
	r.SetValue("x", int64(2), time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC))

	snapshot, sub := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub)

	if snapshot.Kind != EventLastValue || snapshot.Value != int64(2) {
		t.Errorf("snapshot.Value = %v, want 2 (latest)", snapshot.Value)
	}
}

// Multiple concurrent subscribers each get their own last-value replay and all
// receive subsequent live updates in order.
func TestMultipleSubscribers(t *testing.T) {
	r := NewRegistry()
	r.SetValue("x", int64(5), time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC))

	s1snap, sub1 := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub1)
	s2snap, sub2 := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub2)

	if s1snap.Kind != EventLastValue || s2snap.Kind != EventLastValue ||
		s1snap.Value != int64(5) || s2snap.Value != int64(5) {
		t.Fatalf("both subscribers should replay last value 5, got %v / %v", s1snap.Value, s2snap.Value)
	}

	r.SetValue("x", int64(9), time.Date(2026, 6, 22, 13, 0, 0, 0, time.UTC))

	for i, sub := range []*Subscription{sub1, sub2} {
		if evt := next(t, sub); evt.Value != int64(9) {
			t.Errorf("subscriber %d: evt.Value = %v, want 9", i+1, evt.Value)
		}
	}
}

// A cloud value that arrives before any app subscribes is stored (the variable
// is auto-created) and replayed as the last value on the eventual subscribe.
func TestCloudValueBeforeSubscribeIsReplayed(t *testing.T) {
	r := NewRegistry()

	ts := time.Date(2026, 6, 22, 9, 0, 0, 0, time.UTC)
	r.SetValue("cloudvar", "hello", ts) // no prior registration/subscription

	snapshot, sub := r.Subscribe("cloudvar", true)
	defer r.Unsubscribe("cloudvar", sub)

	if snapshot.Kind != EventLastValue || snapshot.Value != "hello" || !snapshot.Timestamp.Equal(ts) {
		t.Errorf("got kind=%q value=%v ts=%v, want %q/hello/%v",
			snapshot.Kind, snapshot.Value, snapshot.Timestamp, EventLastValue, ts)
	}
}

// Unsubscribe closes the subscription's channel and stops its pump.
func TestUnsubscribeClosesChannel(t *testing.T) {
	r := NewRegistry()
	_, sub := r.Subscribe("x", true)
	r.Unsubscribe("x", sub)

	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Fatal("expected closed channel after Unsubscribe")
		}
	case <-time.After(time.Second):
		t.Fatal("channel not closed after Unsubscribe")
	}
}

// Concurrent writers and a subscriber must not race (run under -race).
func TestConcurrentWritersNoRace(t *testing.T) {
	r := NewRegistry()
	_, sub := r.Subscribe("x", true)
	defer r.Unsubscribe("x", sub)

	// Drain in the background.
	done := make(chan struct{})
	go func() {
		for range sub.Events() {
		}
		close(done)
	}()

	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				r.SetValue("x", int64(i), time.Now().UTC())
			}
		}()
	}
	wg.Wait()
	r.Unsubscribe("x", sub)
	<-done
}

func TestGetUnknownVariable(t *testing.T) {
	r := NewRegistry()
	if _, err := r.Get("missing"); err == nil {
		t.Fatal("Get on unknown variable: want error, got nil")
	}
}

func TestWithPrefix(t *testing.T) {
	r := NewRegistry()
	ts := time.Now().UTC()
	r.SetValue("clight:swi", true, ts)
	r.SetValue("clight:hue", 30.0, ts)
	r.SetValue("clight:bri", 70.0, ts)
	r.SetValue("led", false, ts) // unrelated scalar, must not match

	// A subscribed-but-never-set placeholder (nil value, zero ts) must be
	// excluded — it has no value to send in a property packet.
	_, sub := r.Subscribe("clight:sat", true)
	defer r.Unsubscribe("clight:sat", sub)

	got := r.WithPrefix("clight:")
	names := make([]string, len(got))
	for i, v := range got {
		names[i] = v.Name
	}
	want := []string{"clight:bri", "clight:hue", "clight:swi"} // sorted; sat excluded
	if !reflect.DeepEqual(names, want) {
		t.Fatalf("WithPrefix(\"clight:\") names = %v, want %v", names, want)
	}
}
