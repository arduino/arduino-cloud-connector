// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package variables

import (
	"testing"
	"time"
)

// The bug this file guards: runOutbound stores a locally-PUT value before
// publishing it, and the store used to fan the "update" out to every
// subscriber — including the app that had just written it. A read-modify-write
// app (counter = counter + 1) then adopted the stale echo of its own PUT and
// silently lost an increment. The origin id passed to SetValue is what stops
// the frame at the writer's own subscriptions and nowhere else.

// echoTS is a fixed timestamp: none of these tests depend on its value, only
// on which subscribers the frame reaches.
func echoTS() time.Time { return time.Date(2026, 9, 21, 10, 0, 0, 0, time.UTC) }

// An app does not receive the value it wrote itself.
func TestSetValueSuppressesEchoToTheWriter(t *testing.T) {
	r := NewRegistry()

	_, sub := r.Subscribe("counter", "app-1", true)
	defer r.Unsubscribe("counter", sub)

	r.SetValue("counter", int64(0), echoTS(), "app-1")

	noEvent(t, sub, 200*time.Millisecond)
}

// The writer is excluded, everyone else is not: a second app sharing the
// variable is how it learns the first app changed it (the cloud never echoes a
// device's own write back), and a client that sent no id keeps the old
// behaviour.
func TestSetValueStillReachesTheOtherApps(t *testing.T) {
	r := NewRegistry()

	_, writer := r.Subscribe("counter", "app-1", true)
	defer r.Unsubscribe("counter", writer)
	_, other := r.Subscribe("counter", "app-2", true)
	defer r.Unsubscribe("counter", other)
	_, anonymous := r.Subscribe("counter", "", true)
	defer r.Unsubscribe("counter", anonymous)

	r.SetValue("counter", int64(7), echoTS(), "app-1")

	if got := next(t, other); got.Value != int64(7) || got.Kind != EventUpdate {
		t.Errorf("second app got %v (%s), want 7 (%s)", got.Value, got.Kind, EventUpdate)
	}
	if got := next(t, anonymous); got.Value != int64(7) {
		t.Errorf("id-less subscriber got %v, want 7", got.Value)
	}
	noEvent(t, writer, 200*time.Millisecond)
}

// A value with no origin id came from the cloud, not from an app. It must reach
// every subscriber, the one that wrote the variable earlier included: the cloud
// is not an app, and an app that wrote a value must still learn the cloud
// changed it. This is the case the exclusion must NOT swallow.
func TestSetValueFromTheCloudReachesEveryone(t *testing.T) {
	r := NewRegistry()

	_, app := r.Subscribe("counter", "app-1", true)
	defer r.Unsubscribe("counter", app)
	_, anonymous := r.Subscribe("counter", "", true)
	defer r.Unsubscribe("counter", anonymous)

	r.SetValue("counter", int64(99), echoTS(), "")

	if got := next(t, app); got.Value != int64(99) {
		t.Errorf("identified app got %v on a cloud-originated update, want 99", got.Value)
	}
	if got := next(t, anonymous); got.Value != int64(99) {
		t.Errorf("id-less subscriber got %v on a cloud-originated update, want 99", got.Value)
	}
}

// Suppressing the echo must not suppress the STORE: the registry holds the
// board's value, so the write is still what a later subscriber syncs against.
// Getting this wrong would trade a lost increment for a variable that never
// converges.
func TestSetValueStoresTheValueEvenWhenTheWriterIsExcluded(t *testing.T) {
	r := NewRegistry()

	_, writer := r.Subscribe("counter", "app-1", true)
	defer r.Unsubscribe("counter", writer)

	r.SetValue("counter", int64(5), echoTS(), "app-1")

	v, err := r.Get("counter")
	if err != nil {
		t.Fatalf("Get after an excluded write: %v", err)
	}
	if v.Value != int64(5) {
		t.Errorf("stored value = %v, want 5", v.Value)
	}

	first, late := r.Subscribe("counter", "app-2", true)
	defer r.Unsubscribe("counter", late)
	if first.Kind != EventLastValue || first.Value != int64(5) {
		t.Errorf("late subscriber's first frame = %v (%s), want 5 (%s)", first.Value, first.Kind, EventLastValue)
	}
}

// The exclusion is scoped to the outbound path. A last-values frame is the
// cloud confirming a value, which is genuine information for the app that
// wrote it, so ApplyLastValues never filters by origin.
func TestApplyLastValuesReachesTheWriter(t *testing.T) {
	r := NewRegistry()

	_, sub := r.Subscribe("counter", "app-1", true)
	defer r.Unsubscribe("counter", sub)

	r.ApplyLastValues([]Variable{{Name: "counter", Value: int64(3), Timestamp: echoTS()}})

	got := next(t, sub)
	if got.Kind != EventLastValue || got.Value != int64(3) {
		t.Errorf("writer got %v (%s), want 3 (%s)", got.Value, got.Kind, EventLastValue)
	}
}
