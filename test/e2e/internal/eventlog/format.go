// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// StepStatus is how one scenario step ended.
type StepStatus string

const (
	StepPassed  StepStatus = "passed"
	StepFailed  StepStatus = "failed"
	StepSkipped StepStatus = "skipped" // never ran, because an earlier step failed
)

// StepResult is what the scenario runner records for each step. It is
// deliberately generic — name, a short parameter summary, an outcome — so the
// event log knows nothing about the step vocabulary and the two can evolve
// apart.
type StepResult struct {
	Index      int           `json:"index"` // 1-based, as printed
	Name       string        `json:"name"`  // e.g. "expect_publish"
	Detail     string        `json:"detail,omitempty"`
	Status     StepStatus    `json:"status"`
	MatchedSeq int           `json:"matched_seq,omitempty"`
	Since      time.Duration `json:"since,omitempty"`
	Err        error         `json:"-"`
	ErrText    string        `json:"error,omitempty"`
}

// Result is the outcome of one scenario run: everything needed to write both
// the human report and the JSON artifact.
type Result struct {
	Scenario   string         `json:"scenario"`
	Steps      []StepResult   `json:"steps"`
	Unconsumed []Event        `json:"unconsumed,omitempty"`
	Events     []Event        `json:"events"`
	ConsumedBy map[int]string `json:"consumed_by,omitempty"`

	// Aborted is set when the system under test died mid-scenario. It outranks
	// everything else in the report: the step that happened to be waiting when
	// the daemon crashed is not the interesting fact.
	Aborted   *AbortedError `json:"-"`
	AbortText string        `json:"aborted,omitempty"`
}

// Result assembles the run outcome, including the final unconsumed-event
// sweep. Call it once, when the scenario has finished.
func (l *Log) Result(scenario string, steps []StepResult, tolerate []Predicate) Result {
	unconsumed := l.Unconsumed(tolerate)

	l.mu.Lock()
	consumed := make(map[int]string, len(l.consumed))
	for k, v := range l.consumed {
		consumed[k] = v
	}
	l.mu.Unlock()

	for i := range steps {
		if steps[i].Err != nil && steps[i].ErrText == "" {
			steps[i].ErrText = steps[i].Err.Error()
		}
	}

	r := Result{
		Scenario:   scenario,
		Steps:      steps,
		Unconsumed: unconsumed,
		Events:     l.Events(),
		ConsumedBy: consumed,
		Aborted:    l.Aborted(),
	}
	if r.Aborted != nil {
		r.AbortText = r.Aborted.Error()
	}
	return r
}

// Failed reports whether the scenario should be considered failed: any step
// that did not pass, or any significant event nobody claimed.
func (r Result) Failed() bool {
	if r.Aborted != nil {
		return true
	}
	if len(r.Unconsumed) > 0 {
		return true
	}
	for _, s := range r.Steps {
		if s.Status != StepPassed {
			return true
		}
	}
	return false
}

// Format renders the human-readable report: a step summary, the field-level
// diff for the failure, and the full timeline with the failed expectation
// placed at the instant it was waiting.
//
// The placement is the point. A separate "expected / actual" block forces the
// reader to go hunting in the timeline for the moment things stopped; putting
// the expectation inline shows the gap together with whatever happened next —
// which, more often than not, is the explanation.
func (r Result) Format() string {
	var b strings.Builder

	failed := r.firstFailed()
	switch {
	case r.Aborted != nil:
		fmt.Fprintf(&b, "scenario %s — ABORTED: %s\n", r.Scenario, r.Aborted.Reason)
	// Every step passed and the run still failed: the final sweep found
	// protocol traffic nobody claimed. The header has to say so, because this
	// is the one failure with no failed step to point at, and a report that
	// opens with PASS while the exit code says otherwise is worse than no
	// report at all.
	case failed == nil && len(r.Unconsumed) > 0:
		fmt.Fprintf(&b, "scenario %s — FAIL: all %d steps passed but %d significant event(s) went unclaimed (%d events)\n",
			r.Scenario, len(r.Steps), len(r.Unconsumed), len(r.Events))
	case failed == nil:
		fmt.Fprintf(&b, "scenario %s — PASS (%d steps, %d events)\n",
			r.Scenario, len(r.Steps), len(r.Events))
	default:
		fmt.Fprintf(&b, "scenario %s — FAIL at step %d/%d (%s)\n",
			r.Scenario, failed.Index, len(r.Steps), failed.Name)
	}

	b.WriteString("\n")
	r.writeSteps(&b)

	switch {
	case r.Aborted != nil:
		r.writeAbort(&b, failed)
	case failed != nil:
		r.writeExpectation(&b, *failed)
	}
	if len(r.Unconsumed) > 0 {
		r.writeUnconsumed(&b)
	}
	r.writeTimeline(&b, failed)

	return b.String()
}

func (r Result) firstFailed() *StepResult {
	for i := range r.Steps {
		if r.Steps[i].Status == StepFailed {
			return &r.Steps[i]
		}
	}
	return nil
}

func (r Result) writeSteps(b *strings.Builder) {
	width := 0
	for _, s := range r.Steps {
		if len(s.Name) > width {
			width = len(s.Name)
		}
	}
	for _, s := range r.Steps {
		mark := map[StepStatus]string{
			StepPassed: "✓", StepFailed: "✗", StepSkipped: "–",
		}[s.Status]

		var trailer string
		switch s.Status {
		case StepPassed:
			trailer = formatSince(s.Since)
		case StepFailed:
			trailer = shortErr(s.Err)
		case StepSkipped:
			trailer = "not run"
		}
		fmt.Fprintf(b, "  %2d %s  %-*s  %-28s %s\n", s.Index, mark, width, s.Name, s.Detail, trailer)
	}
	b.WriteString("\n")
}

// writeExpectation prints what the failed step wanted and, when something came
// close, which field differed.
func (r Result) writeExpectation(b *strings.Builder, s StepResult) {
	var te *TimeoutError
	if !errors.As(s.Err, &te) {
		fmt.Fprintf(b, "step %d failed: %v\n\n", s.Index, s.Err)
		return
	}

	fmt.Fprintf(b, "expected, never seen (timeout %s, %d events examined)\n", te.Timeout, te.Scanned)
	fmt.Fprintf(b, "  - %s\n", te.Predicate)

	if len(te.NearMiss) == 0 {
		b.WriteString("  no event came close\n\n")
		return
	}
	for _, nm := range te.NearMiss {
		fmt.Fprintf(b, "  + %s   (#%d, %s)\n",
			nm.Event.attrsOrKind(), nm.Event.Seq, formatSince(nm.Event.Since))
		fmt.Fprintf(b, "    %d/%d constraints satisfied — differs:\n",
			nm.Result.Passed, nm.Result.Total)
		for _, f := range nm.Result.Failures {
			if !f.Present {
				fmt.Fprintf(b, "        %-22s want %-16v  (field absent)\n",
					f.Constraint.Field, quote(f.Constraint.Want))
				continue
			}
			fmt.Fprintf(b, "        %-22s want %-16v  got %v\n",
				f.Constraint.Field, quote(f.Constraint.Want), quote(f.Got))
		}
	}
	b.WriteString("\n")
}

// writeAbort explains a death, and is careful about one thing: the step that
// happened to be waiting when the process died did not cause it. Saying so
// explicitly stops the reader from going to investigate the wrong message.
func (r Result) writeAbort(b *strings.Builder, failed *StepResult) {
	fmt.Fprintf(b, "the system under test died — %s\n", r.Aborted.Reason)
	if r.Aborted.Cause != nil {
		fmt.Fprintf(b, "  cause: %v\n", r.Aborted.Cause)
	}
	fmt.Fprintf(b, "  after %d recorded events\n", r.Aborted.AtSeq)

	// The in-flight expectation is on the error the STEP received, not on the
	// shared latch: Await tags a copy so concurrent waiters do not overwrite
	// each other's context. Read it from there.
	if failed != nil {
		var ae *AbortedError
		if errors.As(failed.Err, &ae) && len(ae.Pending.Constraints) > 0 {
			fmt.Fprintf(b, "  step %d (%s) was waiting for: %s\n",
				failed.Index, failed.Name, ae.Pending)
			b.WriteString("  that expectation did not cause the exit — it is just where the scenario stopped\n")
		}
	}
	b.WriteString("\n")
}

func (r Result) writeUnconsumed(b *strings.Builder) {
	fmt.Fprintf(b, "unexpected events — significant, claimed by no step, not in tolerate (%d)\n",
		len(r.Unconsumed))
	for _, ev := range r.Unconsumed {
		fmt.Fprintf(b, "  + %s\n", ev)
	}
	b.WriteString("\n")
}

// writeTimeline prints every event, annotated with the step that consumed it,
// and splices the failed expectation in at the cursor position where that step
// began waiting.
func (r Result) writeTimeline(b *strings.Builder, failed *StepResult) {
	b.WriteString("timeline\n")

	waitAfter := -1
	var te *TimeoutError
	if failed != nil && errors.As(failed.Err, &te) {
		waitAfter = int(te.From) // the step scanned from here on
	}

	unexpected := map[int]bool{}
	for _, ev := range r.Unconsumed {
		unexpected[ev.Seq] = true
	}

	if waitAfter == 0 {
		r.writeWaitMarker(b, failed, te)
	}
	for _, ev := range r.Events {
		prefix := " "
		if unexpected[ev.Seq] {
			prefix = "+"
		}
		line := fmt.Sprintf("  %s %s", prefix, ev)
		if step, ok := r.ConsumedBy[ev.Seq]; ok {
			line += "   ← " + step
		}
		b.WriteString(line + "\n")

		if ev.Seq == waitAfter {
			r.writeWaitMarker(b, failed, te)
		}
		if r.Aborted != nil && ev.Seq == r.Aborted.AtSeq {
			fmt.Fprintf(b, "      ┄┄ ABORTED here: %s ┄┄\n", r.Aborted.Reason)
		}
	}
}

func (r Result) writeWaitMarker(b *strings.Builder, failed *StepResult, te *TimeoutError) {
	fmt.Fprintf(b, "      ┄┄ step %d (%s) waiting from here ┄┄\n", failed.Index, failed.Name)
	fmt.Fprintf(b, "      - %s   ✗ timeout %s\n", te.Predicate, te.Timeout)
}

// attrsOrKind renders an event compactly for the near-miss diff.
func (e Event) attrsOrKind() string {
	if s := e.attrsString(); s != "" {
		return fmt.Sprintf("%s %s  %s", e.Source, e.Kind, s)
	}
	return fmt.Sprintf("%s %s", e.Source, e.Kind)
}

func shortErr(err error) string {
	if err == nil {
		return ""
	}
	var te *TimeoutError
	if errors.As(err, &te) {
		return fmt.Sprintf("timeout %s", te.Timeout)
	}
	return err.Error()
}

func quote(v any) string {
	if s, ok := v.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprintf("%v", v)
}
