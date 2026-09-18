// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package variables

import (
	"testing"
	"time"
)

// noEvent asserts that nothing arrives on the subscription within d. "No frame
// at all" is a deliberate outcome of several rules below, so it needs to be
// asserted as strictly as the frames themselves.
func noEvent(t *testing.T, sub *Subscription, d time.Duration) {
	t.Helper()
	select {
	case evt, ok := <-sub.Events():
		if !ok {
			t.Fatal("subscription channel closed unexpectedly")
		}
		t.Fatalf("received %s (name=%s value=%v ts=%v last_value=%v), want no frame",
			evt.Kind, evt.Name, evt.Value, evt.Timestamp, evt.LastValue)
	case <-time.After(d):
	}
}

// Regression for the bug found by test/e2e/scenarios/thing-change.yaml: after a
// detach and a re-attach to a DIFFERENT thing, a variable belonging to the old
// thing was re-announced as the new thing's cloud last value, with
// last_value=true, carrying the app's own write.
//
// The app writes temp=42 while attached to thing A; thing B's LastValues carry
// only "calc". A subscriber that already knows temp must receive NOTHING: the
// board value stands and the cloud has not contradicted it.
func TestThingChangeDoesNotReannounceForeignValue(t *testing.T) {
	r := NewRegistry()

	// runOutbound calls SetValue before publishing, so the app's PUT lands here.
	appWrite := time.Date(2026, 9, 17, 9, 26, 40, 991363985, time.UTC)
	r.SetValue("temp", 42.0, appWrite)

	first, sub := r.Subscribe("temp", true)
	defer r.Unsubscribe("temp", sub)
	if first.Kind != EventLastValue || first.Value != 42.0 {
		t.Fatalf("first frame = %q/%v, want %q/42", first.Kind, first.Value, EventLastValue)
	}

	// Detach from A, re-attach to B, whose LastValues.update carries only calc.
	r.ApplyLastValues([]Variable{
		{Name: "calc", Value: 21.5, Timestamp: time.Date(2026, 9, 17, 9, 26, 41, 0, time.UTC)},
	})

	noEvent(t, sub, 100*time.Millisecond)
}

// The cache is the BOARD's value, so a subscriber gets it even while the daemon
// is not synced with the cloud. This is what keeps two apps sharing a variable
// on the same number ("temp on the board is 42", not one value per app); before
// the fix this returned thing_unavailable and the second app started blind.
func TestSubscribeWhileNotSteadyReplaysBoardValue(t *testing.T) {
	r := NewRegistry()
	ts := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	r.SetValue("temp", 42.0, ts)

	first, sub := r.Subscribe("temp", false) // cloud NOT steady
	defer r.Unsubscribe("temp", sub)

	if first.Kind != EventLastValue {
		t.Fatalf("first.Kind = %q, want %q", first.Kind, EventLastValue)
	}
	if first.Value != 42.0 || !first.Timestamp.Equal(ts) || !first.LastValue {
		t.Errorf("got value=%v ts=%v last_value=%v, want 42/%v/true",
			first.Value, first.Timestamp, first.LastValue, ts)
	}
}

// No board value and no thing assigned: the subscriber is told
// thing_unavailable, and is owed a verdict once the last values are applied.
func TestSubscribeWhileNotSteadyWithEmptyCacheIsPending(t *testing.T) {
	r := NewRegistry()

	first, sub := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", sub)

	if first.Kind != EventThingUnavailable {
		t.Fatalf("first.Kind = %q, want %q", first.Kind, EventThingUnavailable)
	}
}

// A variable the cloud announced produces EXACTLY ONE frame, of kind lastvalue.
// Before the fix the sync wrote through SetValue (emitting "update", which by
// contract means a live change AFTER the sync) and a second sweep then emitted
// "lastvalue" for the same value: two frames, the first one mislabelled.
func TestApplyLastValuesEmitsExactlyOneLastValueFrame(t *testing.T) {
	r := NewRegistry()

	_, sub := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", sub)

	ts := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)
	r.ApplyLastValues([]Variable{{Name: "temp", Value: 21.5, Timestamp: ts}})

	evt := next(t, sub)
	if evt.Kind != EventLastValue {
		t.Errorf("evt.Kind = %q, want %q", evt.Kind, EventLastValue)
	}
	if evt.Value != 21.5 || !evt.Timestamp.Equal(ts) || !evt.LastValue {
		t.Errorf("got value=%v ts=%v last_value=%v, want 21.5/%v/true",
			evt.Value, evt.Timestamp, evt.LastValue, ts)
	}
	noEvent(t, sub, 100*time.Millisecond)
}

// The announced value is stored, so a later subscriber replays it.
func TestApplyLastValuesStoresTheAnnouncedValue(t *testing.T) {
	r := NewRegistry()
	ts := time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC)

	r.ApplyLastValues([]Variable{{Name: "temp", Value: 21.5, Timestamp: ts}})

	first, sub := r.Subscribe("temp", true)
	defer r.Unsubscribe("temp", sub)
	if first.Kind != EventLastValue || first.Value != 21.5 || !first.Timestamp.Equal(ts) {
		t.Errorf("got kind=%q value=%v ts=%v, want %q/21.5/%v",
			first.Kind, first.Value, first.Timestamp, EventLastValue, ts)
	}
}

// A pending subscriber of a variable the cloud did NOT announce gets its
// verdict: lastvalue_missing, so the brick's leaf leaves the pending state and
// can publish again.
func TestApplyLastValuesResolvesPendingWithMissing(t *testing.T) {
	r := NewRegistry()

	first, sub := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", sub)
	if first.Kind != EventThingUnavailable {
		t.Fatalf("first.Kind = %q, want %q", first.Kind, EventThingUnavailable)
	}

	r.ApplyLastValues([]Variable{{Name: "other", Value: 1.0, Timestamp: time.Now().UTC()}})

	evt := next(t, sub)
	if evt.Kind != EventLastValueMissing {
		t.Errorf("evt.Kind = %q, want %q", evt.Kind, EventLastValueMissing)
	}
	if evt.Value != nil {
		t.Errorf("evt.Value = %v, want nil on a missing last value", evt.Value)
	}
}

// The multi-app case, and the reason the pending flag is per-subscription and
// not per-variable. Two apps subscribe to the same variable at different
// moments: the one still waiting for a verdict gets lastvalue_missing, the one
// that already knows the board value gets nothing.
func TestApplyLastValuesTreatsPendingAndKnowingSubscribersDifferently(t *testing.T) {
	r := NewRegistry()

	// app1 arrives while no thing is assigned and the board has no value yet.
	waiting, subWaiting := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", subWaiting)
	if waiting.Kind != EventThingUnavailable {
		t.Fatalf("app1 first.Kind = %q, want %q", waiting.Kind, EventThingUnavailable)
	}

	// An app writes the variable, so the board now has a value: app1 sees it as
	// a live update, and app2 gets it as its first frame.
	r.SetValue("temp", 42.0, time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC))
	if evt := next(t, subWaiting); evt.Kind != EventUpdate {
		t.Fatalf("app1 saw %q for the local write, want %q", evt.Kind, EventUpdate)
	}
	knowing, subKnowing := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", subKnowing)
	if knowing.Kind != EventLastValue || knowing.Value != 42.0 {
		t.Fatalf("app2 first.Kind = %q value=%v, want %q/42", knowing.Kind, knowing.Value, EventLastValue)
	}

	// The cloud's last values do not mention temp.
	r.ApplyLastValues([]Variable{{Name: "calc", Value: 1.0, Timestamp: time.Now().UTC()}})

	if evt := next(t, subWaiting); evt.Kind != EventLastValueMissing {
		t.Errorf("waiting subscriber got %q, want %q", evt.Kind, EventLastValueMissing)
	}
	noEvent(t, subKnowing, 100*time.Millisecond)
}

// No values at all — an empty or undecodable payload, or an exhausted
// LastValues retry — still resolves every pending subscriber, so an app is
// never left frozen waiting for a frame that will not come. A subscriber that
// already knows the board value is still sent nothing.
func TestApplyLastValuesWithNoValuesResolvesPendingOnly(t *testing.T) {
	r := NewRegistry()

	_, subWaiting := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", subWaiting)

	r.SetValue("calc", 7.0, time.Date(2026, 9, 17, 10, 0, 0, 0, time.UTC))
	_, subKnowing := r.Subscribe("calc", false)
	defer r.Unsubscribe("calc", subKnowing)

	r.ApplyLastValues(nil)

	if evt := next(t, subWaiting); evt.Kind != EventLastValueMissing {
		t.Errorf("pending subscriber got %q, want %q", evt.Kind, EventLastValueMissing)
	}
	noEvent(t, subKnowing, 100*time.Millisecond)
}

// The verdict is delivered once: afterwards the subscriber is no longer
// pending, so a later sync that again does not mention the variable stays
// silent. Without this, every reconnect would re-assert absence and (through
// the brick's apply_missing) force an unthrottled re-publish of the local value.
func TestApplyLastValuesDoesNotRepeatTheVerdict(t *testing.T) {
	r := NewRegistry()

	_, sub := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", sub)

	r.ApplyLastValues(nil)
	if evt := next(t, sub); evt.Kind != EventLastValueMissing {
		t.Fatalf("first verdict = %q, want %q", evt.Kind, EventLastValueMissing)
	}

	r.ApplyLastValues(nil) // e.g. the next broker reconnect
	noEvent(t, sub, 100*time.Millisecond)
}

// An announced variable also clears the pending flag, so the next sync is
// silent for a subscriber that has been told the cloud's value.
func TestApplyLastValuesClearsPendingOnAnnouncedVariable(t *testing.T) {
	r := NewRegistry()

	_, sub := r.Subscribe("temp", false)
	defer r.Unsubscribe("temp", sub)

	r.ApplyLastValues([]Variable{{Name: "temp", Value: 21.5, Timestamp: time.Now().UTC()}})
	if evt := next(t, sub); evt.Kind != EventLastValue {
		t.Fatalf("got %q, want %q", evt.Kind, EventLastValue)
	}

	r.ApplyLastValues(nil) // the cloud no longer has a value for temp
	noEvent(t, sub, 100*time.Millisecond)
}

// A zero timestamp in the payload defaults to now, so the variable still counts
// as having a board value (Subscribe and WithPrefix both key on a non-zero ts).
func TestApplyLastValuesZeroTimestampDefaultsToNow(t *testing.T) {
	r := NewRegistry()

	before := time.Now().UTC()
	r.ApplyLastValues([]Variable{{Name: "temp", Value: 1.0}})

	v, err := r.Get("temp")
	if err != nil {
		t.Fatalf("Get after ApplyLastValues: %v", err)
	}
	if v.Timestamp.Before(before) {
		t.Errorf("Timestamp = %v, want >= %v", v.Timestamp, before)
	}
}
