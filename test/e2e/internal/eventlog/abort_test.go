// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// crash records what the daemon supervisor will record: a describing event for
// the timeline, then the latch for control flow.
func crash(l *Log, exitCode int) {
	l.Append(SourceDaemonProcess, KindProcessExit, map[string]any{
		"exit_code":   exitCode,
		"stderr_tail": "panic: runtime error: invalid memory address",
	}, nil)
	l.Abort("daemon exited unexpectedly", errors.New("exit status 2"))
}

// The whole point: a pending expectation must give up the instant the process
// dies, not wait out its budget. Without this a crash at step 3 of 20 costs
// minutes and gets reported as a missing message.
func TestAbortInterruptsAPendingAwaitImmediately(t *testing.T) {
	l := New()

	type outcome struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan outcome, 1)
	go func() {
		start := time.Now()
		_, _, err := l.Await(context.Background(), 0, wantCmd("LastValues.begin"), 30*time.Second)
		done <- outcome{err: err, elapsed: time.Since(start)}
	}()

	crash(l, 2)

	select {
	case got := <-done:
		var ae *AbortedError
		if !errors.As(got.err, &ae) {
			t.Fatalf("error = %v, want *AbortedError", got.err)
		}
		if got.elapsed > 5*time.Second {
			t.Errorf("Await took %v; it should return as soon as the log is aborted", got.elapsed)
		}
		// The in-flight expectation is attached, so the report can say where
		// the scenario stopped without implying it caused the exit.
		if !strings.Contains(ae.Pending.String(), "LastValues.begin") {
			t.Errorf("Pending = %q, want the in-flight predicate", ae.Pending)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Await did not return after the log was aborted")
	}
}

// Every step after the crash must fail instantly too, otherwise the scenario
// still burns one timeout per remaining step.
func TestAwaitFailsImmediatelyOnceAborted(t *testing.T) {
	l := New()
	crash(l, 2)

	start := time.Now()
	_, _, err := l.Await(context.Background(), 0, wantCmd("anything"), 30*time.Second)
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Await took %v; an aborted log must fail without waiting", elapsed)
	}
	var ae *AbortedError
	if !errors.As(err, &ae) {
		t.Fatalf("error = %v, want *AbortedError", err)
	}
}

// An event that landed in the same breath as the crash — the last publish
// before a panic — must still satisfy a step that wanted it. Reporting a
// crash is not a licence to lose data that did arrive.
func TestAbortDoesNotDiscardEventsAlreadyRecorded(t *testing.T) {
	l := New()
	publish(l, "LastValues.begin")
	crash(l, 2)

	ev, _, err := l.Await(context.Background(), 0, wantCmd("LastValues.begin"), time.Second)
	if err != nil {
		t.Fatalf("Await: %v — an event recorded before the abort must still match", err)
	}
	if ev.Attrs["cmd"] != "LastValues.begin" {
		t.Fatalf("matched %v", ev.Attrs)
	}
}

// A crash produces several signals in practice (Wait returns, stderr closes);
// the first reason is the true one.
func TestAbortIsIdempotent(t *testing.T) {
	l := New()
	l.Abort("daemon exited unexpectedly", errors.New("exit status 2"))
	l.Abort("stderr closed", errors.New("EOF"))

	got := l.Aborted()
	if got == nil {
		t.Fatal("Aborted() = nil")
	}
	if got.Reason != "daemon exited unexpectedly" {
		t.Errorf("Reason = %q, want the first one", got.Reason)
	}
}

// An abort fails the scenario on its own, even if every step that ran passed.
func TestResultFailsWhenAborted(t *testing.T) {
	l := New()
	ev := publish(l, "Device.begin")
	l.Consume(ev.Seq, "step 1 ✓")
	crash(l, 2)

	r := l.Result("crash", []StepResult{
		{Index: 1, Name: "expect_publish", Detail: "cmd=Device.begin", Status: StepPassed, MatchedSeq: ev.Seq},
	}, nil)

	if !r.Failed() {
		t.Fatal("Failed() = false; a dead daemon must fail the scenario")
	}
	if r.Aborted == nil {
		t.Fatal("Result.Aborted is nil")
	}
}

// The report must lead with the death, and must say that the waiting step did
// not cause it — otherwise the reader goes and investigates the wrong message.
func TestReportLeadsWithTheCrash(t *testing.T) {
	l := New()
	ev := publish(l, "Device.begin")
	l.Consume(ev.Seq, "step 1 ✓")

	pending := make(chan error, 1)
	go func() {
		_, _, err := l.Await(context.Background(), Cursor(ev.Seq), wantCmd("Thing.begin"), 30*time.Second)
		pending <- err
	}()
	crash(l, 2)
	err := <-pending

	r := l.Result("crash", []StepResult{
		{Index: 1, Name: "expect_publish", Detail: "cmd=Device.begin", Status: StepPassed, MatchedSeq: ev.Seq},
		{Index: 2, Name: "expect_publish", Detail: "cmd=Thing.begin", Status: StepFailed, Err: err},
		{Index: 3, Name: "await_cloud_state", Detail: "state=Steady", Status: StepSkipped},
	}, nil)

	out := r.Format()
	t.Logf("\n%s", out)

	for _, want := range []string{
		"— ABORTED:",
		"the system under test died",
		"exit status 2",
		"did not cause the exit",
		"ABORTED here",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report missing %q", want)
		}
	}
	// A crash must NOT be dressed up as a timeout.
	if strings.Contains(out, "expected, never seen") {
		t.Error("report frames a crash as a missing expectation")
	}
}
