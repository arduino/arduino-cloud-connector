// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package daemon

import (
	"container/heap"
	"testing"
	"time"
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
