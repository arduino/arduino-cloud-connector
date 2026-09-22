// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package daemon

import (
	"container/heap"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/daemon/cloud"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// The outbound heap must always surface the earliest-timestamped value first,
// regardless of the order values were pushed — this is what reorders two
// concurrent submissions that reached the worker out of order.
func TestOutboundHeapOrdersByTimestamp(t *testing.T) {
	base := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	h := &outboundHeap{}
	heap.Init(h)

	// Push out of timestamp order (simulating the later submission winning the
	// race into the queue).
	heap.Push(h, outboundValue{name: "c", ts: base.Add(30 * time.Millisecond), seq: 3})
	heap.Push(h, outboundValue{name: "a", ts: base.Add(10 * time.Millisecond), seq: 1})
	heap.Push(h, outboundValue{name: "b", ts: base.Add(20 * time.Millisecond), seq: 2})

	want := []string{"a", "b", "c"}
	for i, name := range want {
		got := heap.Pop(h).(outboundValue)
		if got.name != name {
			t.Fatalf("pop %d: got %q, want %q", i, got.name, name)
		}
	}
}

// Equal timestamps fall back to submission sequence, preserving stable order.
func TestOutboundHeapTiebreakBySeq(t *testing.T) {
	ts := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC)
	h := &outboundHeap{}
	heap.Init(h)

	heap.Push(h, outboundValue{name: "second", ts: ts, seq: 2})
	heap.Push(h, outboundValue{name: "first", ts: ts, seq: 1})

	if got := heap.Pop(h).(outboundValue); got.name != "first" {
		t.Fatalf("got %q, want first (lower seq)", got.name)
	}
	if got := heap.Pop(h).(outboundValue); got.name != "second" {
		t.Fatalf("got %q, want second", got.name)
	}
}

// A multi-value property attribute must be published as the WHOLE property
// (every sibling attribute at its current registry value, with the just-set
// value substituted), so Arduino Cloud does not reset the unmodified attributes
// to their defaults — e.g. a ColoredLight's swi must not flip to false when only
// the colour changes. A plain scalar publishes just itself.
func TestPropertyPacket(t *testing.T) {
	d := &Daemon{reg: variables.NewRegistry()}
	ts := time.Now().UTC()
	d.reg.SetValue("clight:swi", true, ts, "")
	d.reg.SetValue("clight:hue", 30.0, ts, "")
	d.reg.SetValue("clight:sat", 50.0, ts, "")
	d.reg.SetValue("clight:bri", 70.0, ts, "")

	// Scalar: only itself.
	scalar := d.propertyPacket("led", false)
	if len(scalar) != 1 || scalar[0].Name != "led" || scalar[0].Value != false {
		t.Fatalf("scalar packet = %+v, want single {led,false}", scalar)
	}

	// Attribute: the full property, with the just-set hue (123.0) overriding the
	// stale registry value (30.0), and swi preserved at its last value (true).
	got := map[string]any{}
	for _, v := range d.propertyPacket("clight:hue", 123.0) {
		got[v.Name] = v.Value
	}
	want := map[string]any{
		"clight:swi": true,
		"clight:hue": 123.0,
		"clight:sat": 50.0,
		"clight:bri": 70.0,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("property packet = %v, want %v", got, want)
	}
}

// The submitting app's id must survive the whole outbound path — EnqueueVariable
// queues it, runOutbound hands it to the registry — because that is what the
// origin exclusion matches on. Dropping it anywhere in between silently
// restores the bug: the app gets the echo of its own PUT, and a
// read-modify-write app loses the increment it had already computed.
//
// The test drives the real path rather than asserting on the queued struct,
// since the plumbing between the two ends is exactly what used to be missing.
func TestOutboundCarriesClientIDToTheRegistry(t *testing.T) {
	reg := variables.NewRegistry()
	d := &Daemon{
		reg:      reg,
		outbound: make(chan outboundValue, outboundQueueSize),
		state:    StateRun,
		// Steady but with no thing_id: CloudSteady() accepts the value, and
		// publishVariable then bails out with ErrNotSteady before touching the
		// (absent) MQTT client — so the outbound worker runs for real with no
		// broker. runOutbound logs the failed publish and keeps the value
		// locally, which is the behaviour under test here.
		snapshot: Snapshot{State: StateRun, Cloud: &cloud.Snapshot{State: cloud.StateSteady}},
	}

	_, writer := reg.Subscribe("counter", "app-1", true)
	defer reg.Unsubscribe("counter", writer)
	_, other := reg.Subscribe("counter", "app-2", true)
	defer reg.Unsubscribe("counter", other)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.runOutbound(ctx)

	if err := d.EnqueueVariable(ctx, "counter", int64(1), "app-1"); err != nil {
		t.Fatalf("EnqueueVariable: %v", err)
	}

	// The second app is served first; both enqueues happen under one registry
	// lock, so once this arrives the writer's frame would already be queued if
	// the exclusion had failed.
	select {
	case evt := <-other.Events():
		if evt.Value != int64(1) {
			t.Errorf("second app got %v, want 1", evt.Value)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the second app's update")
	}
	select {
	case evt := <-writer.Events():
		t.Errorf("writer received the echo of its own PUT: value=%v", evt.Value)
	case <-time.After(200 * time.Millisecond):
	}
}
