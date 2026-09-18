// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package variables implements the Cloud Variable registry.
//
// Per RFC-13 there is no explicit registration step: a variable comes into
// existence the first time it is written (an app PUT or an inbound cloud
// property) or subscribed to. Entries are therefore created on demand by
// SetValue and Subscribe, keyed solely by name.
//
// Ordering: each subscriber receives updates through its own FIFO queue. The
// queue is filled while the registry lock is held, so the delivery order to
// every subscriber matches the order in which values were stored — even when
// several producers (multiple apps, or the cloud) write the same variable
// concurrently or at high rate. Delivery is decoupled by a per-subscriber pump
// goroutine, so a slow consumer never blocks the producer or other subscribers
// and no event is dropped.
package variables

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Variable is a single named cloud variable. Timestamp is the moment the
// current Value was last set — either locally (an app PUT) or by the cloud
// (an inbound property update); it is the "last value" timestamp used by
// clients to resolve device/cloud conflicts (MOST_RECENT_WINS).
type Variable struct {
	Name      string    `json:"name"`
	Value     any       `json:"value,omitempty"`
	Timestamp time.Time `json:"timestamp,omitempty"`
}

// SSE event kinds exchanged with cloud brick apps. The first frame a subscriber
// receives is always one of the three "sync" kinds, telling it how to resolve
// its local value; every subsequent live change is EventUpdate.
//
// The value a sync frame carries is THE BOARD'S value for that variable, not
// specifically the cloud's: the registry is the one place where "temp on this
// board is 10" lives, fed both by inbound cloud traffic and by app writes, so
// that two apps sharing a variable always see the same number. The sync
// policies (DEVICE_WINS / CLOUD_WINS / MOST_RECENT_WINS) are how an app
// arbitrates its own local value against it — between apps just as much as
// between device and cloud. Do not "correct" a sync frame into carrying only
// cloud-sourced values: that would give each app its own truth.
//
//   - EventThingUnavailable: no thing is assigned yet AND the board has no
//     value for this variable. The app keeps its local value and waits; a sync
//     frame follows once the cloud's last values are applied.
//   - EventLastValue: the board has a value for this variable — the app
//     resolves its local value against it per its sync policy.
//   - EventLastValueMissing: the board has no value and the cloud announced
//     none either, so the local value wins and is pushed up.
//   - EventUpdate: a live change after the initial sync — inbound cloud traffic
//     or another app's write.

// EventKind is the SSE event name emitted for a variable event (one of the
// Event* constants below).
type EventKind string

const (
	EventUpdate           EventKind = "update"
	EventLastValue        EventKind = "lastvalue"
	EventLastValueMissing EventKind = "lastvalue_missing"
	EventThingUnavailable EventKind = "thing_unavailable"
)

// UpdateEvent is delivered to SSE subscribers when a variable value changes.
// LastValue marks the first event a subscriber receives — the current stored
// value replayed on subscribe — so clients can recognise it and seed their
// local state (it is omitted, i.e. false, on live updates).
type UpdateEvent struct {
	Name      string    `json:"name"`
	Value     any       `json:"value"`
	Timestamp time.Time `json:"timestamp"`
	LastValue bool      `json:"last_value,omitempty"`
	// Kind is the SSE event name to emit for this event (one of the Event*
	// constants). It is transport metadata, not part of the JSON data payload.
	// Producers always set it: SetValue tags live changes as EventUpdate,
	// ApplyLastValues tags sync frames accordingly.
	Kind EventKind `json:"-"`
}

// Subscription is an ordered (FIFO) stream of updates for one variable. Events
// are buffered in an unbounded internal queue and drained, in order, by a pump
// goroutine onto the channel returned by Events. The caller reads from Events
// until it is closed (by Registry.Unsubscribe).
type Subscription struct {
	events  chan UpdateEvent
	wake    chan struct{} // coalesced "queue non-empty" signal (buffered 1)
	closeCh chan struct{}

	mu     sync.Mutex
	queue  []UpdateEvent
	closed bool

	// pending is true while this subscriber has been told EventThingUnavailable
	// and still owes a sync verdict. It is guarded by the owning Registry's mu
	// (NOT by s.mu above): it is only ever set by Subscribe and cleared by
	// ApplyLastValues, both of which hold the registry lock, so the decision
	// "does this subscriber need a frame" cannot race with the frame itself.
	pending bool
}

func newSubscription() *Subscription {
	s := &Subscription{
		events:  make(chan UpdateEvent),
		wake:    make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
	go s.pump()
	return s
}

// Events returns the FIFO channel of updates. It is closed when the
// subscription is torn down via Registry.Unsubscribe.
func (s *Subscription) Events() <-chan UpdateEvent { return s.events }

// enqueue appends an event to the FIFO queue. It never blocks the caller, so it
// is safe to call while holding the registry lock (which is what guarantees the
// queue order matches the value-store order). Must not be called after stop.
func (s *Subscription) enqueue(evt UpdateEvent) {
	s.mu.Lock()
	s.queue = append(s.queue, evt)
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// pump drains the FIFO queue onto the events channel in order, blocking on a
// slow reader without affecting producers or other subscribers.
func (s *Subscription) pump() {
	defer close(s.events)
	for {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			select {
			case <-s.wake:
				continue
			case <-s.closeCh:
				return
			}
		}
		evt := s.queue[0]
		if len(s.queue) == 1 {
			s.queue = nil // release the backing array when drained
		} else {
			s.queue = s.queue[1:]
		}
		s.mu.Unlock()

		select {
		case s.events <- evt:
		case <-s.closeCh:
			return
		}
	}
}

// stop terminates the pump goroutine (idempotent).
func (s *Subscription) stop() {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		close(s.closeCh)
	}
	s.mu.Unlock()
}

// Registry is a thread-safe store of named cloud variables. Apps write values
// and subscribe via the REST API; the MQTT layer updates values when messages
// arrive from the cloud. Variables are created on demand.
type Registry struct {
	mu        sync.RWMutex
	vars      map[string]*Variable
	listeners map[string][]*Subscription
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry {
	return &Registry{
		vars:      make(map[string]*Variable),
		listeners: make(map[string][]*Subscription),
	}
}

// getOrCreate returns the named variable, creating an empty one if absent.
// The caller must hold r.mu.
func (r *Registry) getOrCreate(name string) *Variable {
	v, ok := r.vars[name]
	if !ok {
		v = &Variable{Name: name}
		r.vars[name] = v
	}
	return v
}

// Get returns a copy of the named variable.
func (r *Registry) Get(name string) (Variable, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	v, ok := r.vars[name]
	if !ok {
		return Variable{}, fmt.Errorf("variable %q not found", name)
	}
	return *v, nil
}

// WithPrefix returns a copy of every variable whose name starts with prefix and
// that has been set at least once (non-zero timestamp), sorted by name. It is
// used to assemble the full attribute set of a multi-value cloud property
// (e.g. prefix "clight:" → clight:swi, clight:hue, clight:sat, clight:bri) so
// that a partial device update can be published as the complete property.
// Never-set placeholder entries created by Subscribe are excluded (they have no
// value to send).
func (r *Registry) WithPrefix(prefix string) []Variable {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Variable
	for name, v := range r.vars {
		if strings.HasPrefix(name, prefix) && !v.Timestamp.IsZero() {
			out = append(out, *v)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SetValue updates the value (and last-value timestamp) of a variable and
// enqueues the update to every subscriber, creating the variable if it does
// not exist yet. It is called both by the MQTT layer when an inbound property
// update arrives (ts = the cloud change time) and by the daemon's outbound
// worker on a local PUT (ts = the API receipt time). A zero ts defaults to
// time.Now(). The new value becomes the variable's "last value", replayed to
// any later subscriber.
//
// The value store and the per-subscriber enqueue happen together under the
// registry lock, so concurrent SetValue calls for the same variable deliver to
// all subscribers in one consistent order (the enqueue itself never blocks).
func (r *Registry) SetValue(name string, value any, ts time.Time) {
	if ts.IsZero() {
		ts = time.Now().UTC()
	}
	evt := UpdateEvent{Name: name, Value: value, Timestamp: ts, Kind: EventUpdate}

	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.getOrCreate(name)
	v.Value = value
	v.Timestamp = ts
	for _, sub := range r.listeners[name] {
		sub.enqueue(evt)
	}
}

// ApplyLastValues stores the last values the cloud announced for the currently
// assigned thing and delivers exactly one sync frame per subscribed variable
// that needs one. It is the single point where a last-values payload becomes
// visible to apps, and it replaces the old "sweep every subscriber and decide
// from the stored timestamp" resync: the payload itself says which variables
// the cloud spoke about, so nothing has to be inferred.
//
//   - a variable IN the payload is stored and announced to every subscriber as
//     EventLastValue (LastValue: true) — the cloud confirmed this value, which
//     is genuine information even for a subscriber that already had it;
//   - a subscribed variable NOT in the payload is left untouched, and only
//     PENDING subscribers are told EventLastValueMissing. A subscriber that
//     already knows the board value is deliberately sent nothing: the cloud has
//     not contradicted it, so there is nothing to say. Re-announcing the cached
//     value as a cloud last value is the defect this replaces — it crossed a
//     thing boundary and passed an app's own write back as cloud truth.
//
// Both passes run under one lock, so a subscriber cannot receive both a
// snapshot from Subscribe and a frame from here for the same sync.
//
// Called with no values when the cloud announced nothing — an empty or
// unreadable payload, or an exhausted LastValues retry — which resolves every
// pending subscriber with EventLastValueMissing so apps stop waiting and can
// publish. Treating a timeout as an absence is deliberate: the request pipeline
// makes a late answer unlikely, and a subscriber left pending is frozen in both
// directions (the brick neither publishes nor applies live updates while it
// waits for a sync frame).
func (r *Registry) ApplyLastValues(values []Variable) {
	now := time.Now().UTC()

	r.mu.Lock()
	defer r.mu.Unlock()

	announced := make(map[string]bool, len(values))
	for _, in := range values {
		ts := in.Timestamp
		if ts.IsZero() {
			ts = now
		}
		announced[in.Name] = true

		v := r.getOrCreate(in.Name)
		v.Value = in.Value
		v.Timestamp = ts

		evt := UpdateEvent{Name: in.Name, Value: in.Value, Timestamp: ts, LastValue: true, Kind: EventLastValue}
		for _, sub := range r.listeners[in.Name] {
			sub.pending = false
			sub.enqueue(evt)
		}
	}

	for name, subs := range r.listeners {
		if announced[name] {
			continue
		}
		evt := UpdateEvent{Name: name, Kind: EventLastValueMissing}
		for _, sub := range subs {
			if !sub.pending {
				continue
			}
			sub.pending = false
			sub.enqueue(evt)
		}
	}
}

// Subscribe registers a FIFO Subscription for the named variable (creating it
// if absent) and returns the sync frame to deliver as the subscriber's first
// event. Deciding the frame here — rather than in the caller — keeps a single
// place that answers "what does this subscriber need to know", and setting the
// pending flag under the same lock means a concurrent ApplyLastValues cannot
// slip between the subscribe and the flag. The caller must call Unsubscribe
// when done.
//
// The returned Kind is one of:
//
//   - EventLastValue: the board has a value for this variable. Returned whether
//     or not the daemon is currently synced with the cloud: the cache IS the
//     board's value, and every app must see the same one.
//   - EventThingUnavailable: no value yet and no thing assigned. The
//     subscription is marked pending — ApplyLastValues owes it a verdict.
//   - EventLastValueMissing: no value, but a thing is assigned and its last
//     values have already been applied, so the cloud has nothing for it.
func (r *Registry) Subscribe(name string, cloudSteady bool) (first UpdateEvent, sub *Subscription) {
	r.mu.Lock()
	defer r.mu.Unlock()
	v := r.getOrCreate(name)
	sub = newSubscription()
	r.listeners[name] = append(r.listeners[name], sub)

	switch {
	case !v.Timestamp.IsZero():
		return UpdateEvent{Name: name, Value: v.Value, Timestamp: v.Timestamp, LastValue: true, Kind: EventLastValue}, sub
	case !cloudSteady:
		sub.pending = true
		return UpdateEvent{Name: name, Kind: EventThingUnavailable}, sub
	default:
		return UpdateEvent{Name: name, Kind: EventLastValueMissing}, sub
	}
}

// Unsubscribe removes the subscription and stops its pump goroutine.
func (r *Registry) Unsubscribe(name string, sub *Subscription) {
	r.mu.Lock()
	list := r.listeners[name]
	for i, s := range list {
		if s == sub {
			r.listeners[name] = append(list[:i], list[i+1:]...)
			break
		}
	}
	r.mu.Unlock()
	sub.stop()
}
