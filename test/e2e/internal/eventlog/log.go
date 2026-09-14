// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// Cursor marks how far a step has already looked: the Seq of the last event
// accounted for, so scanning resumes strictly AFTER it. The zero value means
// "from the beginning of the log".
//
// It is a Seq, not a slice index. The two differ by one, and confusing them is
// the kind of off-by-one that makes a step silently re-match the event its
// predecessor already claimed — so the distinction is stated here rather than
// left to be inferred from the loop.
type Cursor int

// Log is the ordered event log. Safe for concurrent use: every source (the
// broker's hooks, the HTTP fakes, the SSE reader, the daemon's stdout pump)
// appends from its own goroutine while the scenario runner reads.
type Log struct {
	mu     sync.Mutex
	t0     time.Time
	events []Event

	// consumed maps Seq to the step that matched it. It is what the final
	// sweep subtracts from the significant events, and what lets the timeline
	// annotate each row with the step that claimed it.
	consumed map[int]string

	// changed is closed and replaced on every append. Waiters select on it,
	// which is what makes Await a condition wait rather than a poll — closing
	// a channel wakes every waiter at once, and a fresh one is installed for
	// the next round.
	changed chan struct{}

	// aborted latches a terminal condition: the system under test is gone, so
	// no expectation can ever be satisfied again. See Abort.
	aborted *AbortedError
}

// New returns an empty log whose timeline starts now.
func New() *Log {
	return &Log{
		t0:       time.Now(),
		consumed: map[int]string{},
		changed:  make(chan struct{}),
	}
}

// Append records an observation and returns it with its Seq assigned.
func (l *Log) Append(src Source, kind Kind, attrs map[string]any, raw []byte) Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	ev := Event{
		Seq:    len(l.events) + 1,
		At:     now,
		Since:  now.Sub(l.t0),
		Source: src,
		Kind:   kind,
		Attrs:  attrs,
		Raw:    raw,
	}
	l.events = append(l.events, ev)

	close(l.changed)
	l.changed = make(chan struct{})

	return ev
}

// Events returns a copy of everything recorded so far.
func (l *Log) Events() []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]Event(nil), l.events...)
}

// Await blocks until an event at or after from satisfies p, or the timeout
// expires.
//
// It is NOT destructive and it does NOT skip history: events already in the
// log from the cursor onward are matched first. That property is what makes
// the strict sweep safe — an event may legitimately land microseconds before
// the step that consumes it, and a design that only watched for future
// arrivals would race with its own producers.
//
// The returned Cursor is positioned just after the matched event.
func (l *Log) Await(ctx context.Context, from Cursor, p Predicate, timeout time.Duration) (Event, Cursor, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	scanFrom := from
	for {
		l.mu.Lock()
		for i := int(scanFrom); i < len(l.events); i++ {
			if p.Match(l.events[i]).OK {
				ev := l.events[i]
				l.mu.Unlock()
				return ev, Cursor(i + 1), nil
			}
		}
		// The abort latch is checked AFTER scanning, so an event that arrived
		// in the same breath as the crash — the last publish before a panic,
		// say — still satisfies a step that wanted it.
		if l.aborted != nil {
			err := l.aborted.withPending(p)
			l.mu.Unlock()
			return Event{}, from, err
		}
		scanFrom = Cursor(len(l.events))
		changed := l.changed
		l.mu.Unlock()

		select {
		case <-changed:
			// an append, or an abort; loop and re-examine both
		case <-deadline.C:
			return Event{}, from, l.timeoutError(p, from, timeout)
		case <-ctx.Done():
			return Event{}, from, ctx.Err()
		}
	}
}

// Abort latches a terminal condition and wakes every waiter.
//
// It exists because a dead daemon must not be reported as twenty consecutive
// timeouts. Without it, a crash at step 3 of 20 makes each remaining step wait
// out its full budget — minutes of pointless waiting — and the report blames
// the first missing message instead of naming the crash. With it, the pending
// Await returns at once and every later one fails immediately, so the scenario
// stops within milliseconds of the process dying and says why.
//
// The caller should Append a describing event first (exit code, signal,
// stderr tail) so the timeline carries the detail; Abort only handles control
// flow. It is idempotent: a crash often produces several signals and the first
// reason is the true one.
func (l *Log) Abort(reason string, cause error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.aborted != nil {
		return
	}
	l.aborted = &AbortedError{Reason: reason, Cause: cause, AtSeq: len(l.events)}

	close(l.changed)
	l.changed = make(chan struct{})
}

// Aborted returns the latched terminal condition, or nil.
func (l *Log) Aborted() *AbortedError {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.aborted
}

// AbortedError is returned by Await once the system under test is gone. It is
// deliberately distinct from TimeoutError: "nothing happened in time" and "the
// thing we were testing died" call for different reactions from the reader,
// and collapsing them together is what makes a crash look like a slow test.
type AbortedError struct {
	Reason  string
	Cause   error
	AtSeq   int       // events recorded before the abort, for the timeline
	Pending Predicate // what was being awaited when it hit, when there was one
}

func (e *AbortedError) Error() string {
	s := "aborted: " + e.Reason
	if e.Cause != nil {
		s += " (" + e.Cause.Error() + ")"
	}
	return s
}

func (e *AbortedError) Unwrap() error { return e.Cause }

// withPending copies the latch, tagging it with the expectation that was in
// flight. The latch itself stays shared and un-mutated, so concurrent waiters
// do not overwrite each other's context.
func (e *AbortedError) withPending(p Predicate) *AbortedError {
	c := *e
	c.Pending = p
	return &c
}

// timeoutError builds the rich failure, scanning from the ORIGINAL cursor (not
// the advanced one) so the near-miss search sees every candidate the step
// could have accepted.
func (l *Log) timeoutError(p Predicate, from Cursor, timeout time.Duration) *TimeoutError {
	l.mu.Lock()
	defer l.mu.Unlock()

	var candidates []Event
	if int(from) < len(l.events) {
		candidates = l.events[from:]
	}
	return &TimeoutError{
		Predicate: p,
		Timeout:   timeout,
		From:      from,
		NearMiss:  nearMisses(candidates, p, 3),
		Scanned:   len(candidates),
	}
}

// TimeoutError is returned by Await when nothing matched in time. It carries
// the near misses so the report can show a field-level diff instead of the
// word "timeout".
type TimeoutError struct {
	Predicate Predicate
	Timeout   time.Duration
	From      Cursor
	NearMiss  []NearMiss
	Scanned   int
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timeout after %s waiting for %s (%d events examined, %d near misses)",
		e.Timeout, e.Predicate, e.Scanned, len(e.NearMiss))
}

// Consume records that step matched the event with this Seq. The scenario
// runner calls it for every successful Await; the final sweep and the timeline
// annotations both read it.
func (l *Log) Consume(seq int, step string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.consumed[seq] = step
}

// ConsumedBy returns the step that matched this event, if any.
func (l *Log) ConsumedBy(seq int) (string, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.consumed[seq]
	return s, ok
}

// Unconsumed returns every significant event that no step matched and that no
// tolerate rule allows: the scenario is failed on a non-empty result.
//
// This runs ONCE, at the end of a scenario — never as events arrive. Tripping
// on arrival would race with the step about to consume them, since Await
// matches history as well as new arrivals.
//
// tolerate is a list of predicates describing traffic that is correct but that
// no step claims. It is needed, not a loophole: the daemon retries Thing.begin
// with back-off while it waits for Thing.update, so a scenario that injects
// the reply after a delay legitimately sees extra publishes.
func (l *Log) Unconsumed(tolerate []Predicate) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []Event
	for _, ev := range l.events {
		if !ev.Significant() {
			continue
		}
		if _, ok := l.consumed[ev.Seq]; ok {
			continue
		}
		if matchesAny(ev, tolerate) {
			continue
		}
		out = append(out, ev)
	}
	return out
}

func matchesAny(e Event, preds []Predicate) bool {
	for _, p := range preds {
		if p.Match(e).OK {
			return true
		}
	}
	return false
}
