// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package daemonproc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

// helperEnv selects the behaviour of the stand-in daemon. The test binary
// re-executes itself as the child, which is how the supervisor can be tested
// against a real process -- exit codes, streams, signals -- without needing a
// built daemon.
const helperEnv = "E2E_DAEMONPROC_HELPER"

func TestMain(m *testing.M) {
	if behaviour := os.Getenv(helperEnv); behaviour != "" {
		helperMain(behaviour)
		return
	}
	os.Exit(m.Run())
}

// helperMain is the child process. It never returns.
func helperMain(behaviour string) {
	switch behaviour {
	case "quiet":
		fmt.Println("stand-in daemon: started")
		// Wait to be stopped. Long enough that a test never outlasts it.
		time.Sleep(5 * time.Minute)
		os.Exit(0)
	case "exit0":
		fmt.Println("stand-in daemon: done")
		os.Exit(0)
	case "exit3":
		fmt.Println("stand-in daemon: starting")
		fmt.Fprintln(os.Stderr, "stand-in daemon: giving up")
		os.Exit(3)
	case "panic":
		fmt.Println("stand-in daemon: starting")
		fmt.Fprintln(os.Stderr, "panic: interface conversion: nil is not a thing")
		fmt.Fprintln(os.Stderr, "goroutine 1 [running]:")
		fmt.Fprintln(os.Stderr, "main.main()")
		os.Exit(2)
	case "dump-env":
		for _, kv := range os.Environ() {
			if strings.HasPrefix(kv, "ARDUINO_CLOUD_CONNECTOR__") {
				fmt.Println("env " + kv)
			}
		}
		fmt.Println("stand-in daemon: env dumped")
		os.Exit(0)
	case "args":
		fmt.Println("args " + strings.Join(os.Args[1:], " "))
		os.Exit(0)
	default:
		fmt.Fprintf(os.Stderr, "unknown helper behaviour %q\n", behaviour)
		os.Exit(64)
	}
}

// start spawns the test binary as a stand-in daemon.
func start(t *testing.T, behaviour string, env map[string]string) (*eventlog.Log, *Process) {
	t.Helper()
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary: %v", err)
	}
	childEnv := map[string]string{helperEnv: behaviour}
	for k, v := range env {
		childEnv[k] = v
	}

	log := eventlog.New()
	p, err := Start(Options{Log: log, BinaryPath: self, Env: childEnv})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() { _ = p.Kill() })
	return log, p
}

func await(t *testing.T, log *eventlog.Log, p eventlog.Predicate) eventlog.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ev, _, err := log.Await(ctx, 0, p, 10*time.Second)
	if err != nil {
		t.Fatalf("awaiting %s: %v", p, err)
	}
	return ev
}

func pred(label string, constraints ...eventlog.Constraint) eventlog.Predicate {
	return eventlog.Predicate{Label: label, Constraints: constraints}
}

// Both streams become events, which is what lets a failure report interleave
// the daemon's own view with the protocol traffic.
func TestOutputBecomesEvents(t *testing.T) {
	log, p := start(t, "exit3", nil)
	if _, err := p.WaitFor(10 * time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}

	stdout := await(t, log, pred("daemon stdout",
		eventlog.Eq("kind", string(eventlog.KindDaemonLine)),
		eventlog.Eq("attrs.stream", "stdout"),
	))
	if got := stdout.Attrs["line"]; got != "stand-in daemon: starting" {
		t.Errorf("stdout line = %v", got)
	}
	if stdout.Significant() {
		t.Error("a daemon log line must not be significant: the daemon talks continuously")
	}
	await(t, log, pred("daemon stderr",
		eventlog.Eq("kind", string(eventlog.KindDaemonLine)),
		eventlog.Eq("attrs.stream", "stderr"),
		eventlog.Eq("attrs.line", "stand-in daemon: giving up"),
	))
}

// An exit nobody asked for latches the log, so the twenty remaining steps fail
// at once instead of each waiting out its own budget.
func TestAnUnexpectedExitAbortsTheLog(t *testing.T) {
	log, p := start(t, "exit3", nil)

	info, err := p.WaitFor(10 * time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if info.Code != 3 {
		t.Errorf("exit code = %d, want 3", info.Code)
	}

	ev := await(t, log, pred("process exit",
		eventlog.Eq("kind", string(eventlog.KindProcessExit)),
	))
	if got := ev.Attrs["exit_code"]; got != 3 {
		t.Errorf("exit_code = %v, want 3", got)
	}
	if got := ev.Attrs["expected"]; got != false {
		t.Errorf("expected = %v, want false", got)
	}
	if !ev.Significant() {
		t.Error("a process exit must be significant: an unasked-for one is a crash")
	}

	// The latch is what a waiting step now gets: an AbortedError, deliberately
	// NOT a timeout, because "the thing under test died" and "nothing happened
	// in time" call for different reactions.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, _, err = log.Await(ctx, 0, pred("something that will never arrive",
		eventlog.Eq("kind", "mqtt_connect"),
	), 5*time.Second)

	var aborted *eventlog.AbortedError
	if !errors.As(err, &aborted) {
		t.Fatalf("Await returned %T (%v), want an *AbortedError", err, err)
	}
	if !strings.Contains(aborted.Error(), "exited with code 3") {
		t.Errorf("the abort does not say how it died: %v", aborted)
	}
	if !strings.Contains(aborted.Error(), "giving up") {
		t.Errorf("the abort does not carry the stderr tail: %v", aborted)
	}
}

// A panic must be recognised and quoted. "exit status 2" says nothing; the
// panic line says where to look.
func TestAPanicIsRecognisedFromStderr(t *testing.T) {
	log, p := start(t, "panic", nil)
	info, err := p.WaitFor(10 * time.Second)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}

	const want = "panic: interface conversion: nil is not a thing"
	if info.PanicLine != want {
		t.Errorf("PanicLine = %q, want %q", info.PanicLine, want)
	}
	ev := await(t, log, pred("process exit", eventlog.Eq("kind", string(eventlog.KindProcessExit))))
	if got := ev.Attrs["panic"]; got != want {
		t.Errorf("panic attribute = %v, want %q", got, want)
	}
	if aborted := log.Aborted(); aborted == nil {
		t.Fatal("a panic did not abort the log")
	} else if !strings.Contains(aborted.Error(), want) {
		t.Errorf("the abort does not quote the panic: %v", aborted)
	}
	// The stack tail travels with it, which is the other half of a useful
	// report.
	if len(info.StderrTail) < 3 {
		t.Errorf("stderr tail = %v, want the panic and its first frames", info.StderrTail)
	}
}

func TestPanicLineRecognition(t *testing.T) {
	for _, line := range []string{
		"panic: boom",
		"  panic: boom",
		"fatal error: concurrent map writes",
		"panic [recovered]: boom",
	} {
		if !isPanicLine(line) {
			t.Errorf("%q should be recognised as a death", line)
		}
	}
	for _, line := range []string{
		"time=2026-09-15 level=INFO msg=\"panic recovered\"",
		"nothing to see",
		"",
	} {
		if isPanicLine(line) {
			t.Errorf("%q should not be recognised as a death", line)
		}
	}
}

// An exit the scenario asked for is recorded and does NOT abort: otherwise the
// one scenario that tests graceful shutdown could never pass.
func TestAnExpectedExitDoesNotAbort(t *testing.T) {
	log, p := start(t, "exit0", nil)
	p.ExpectExit()

	if _, err := p.WaitFor(10 * time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}
	ev := await(t, log, pred("process exit", eventlog.Eq("kind", string(eventlog.KindProcessExit))))
	if got := ev.Attrs["expected"]; got != true {
		t.Errorf("expected = %v, want true", got)
	}
	if aborted := log.Aborted(); aborted != nil {
		t.Fatalf("an announced exit aborted the log: %v", aborted)
	}
}

// Stop ends the process and says whether the graceful path was actually taken.
// It is not on Windows, which has no SIGTERM -- and a shutdown scenario has to
// know that rather than pass on a kill.
func TestStopEndsTheProcessAndReportsWhetherItWasGraceful(t *testing.T) {
	log, p := start(t, "quiet", nil)
	await(t, log, pred("daemon started",
		eventlog.Eq("attrs.line", "stand-in daemon: started"),
	))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := p.Stop(ctx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !p.Exited() {
		t.Error("the process is still running after Stop")
	}

	wantGraceful := runtime.GOOS != "windows"
	if got := p.GracefulStopExercised(); got != wantGraceful {
		t.Errorf("GracefulStopExercised() = %t, want %t on %s", got, wantGraceful, runtime.GOOS)
	}
	if !wantGraceful {
		// The note is what keeps the coverage gap visible instead of silent.
		await(t, log, pred("sigterm unsupported note",
			eventlog.Eq("attrs.action", "sigterm_unsupported"),
		))
	}
	if aborted := log.Aborted(); aborted != nil {
		t.Errorf("Stop aborted the log: %v", aborted)
	}
}

// The child gets an environment the harness built, not the developer's.
//
// This is what makes a run reproducible: the daemon reads its whole
// configuration from the environment, so an ARDUINO_CLOUD_CONNECTOR__* left
// over in a shell would silently change what the suite tests.
func TestTheChildEnvironmentIsBuiltNotInherited(t *testing.T) {
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__PORT", "9999")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL", "error")

	log, p := start(t, "dump-env", map[string]string{
		"ARDUINO_CLOUD_CONNECTOR__PORT": "1234",
	})
	if _, err := p.WaitFor(10 * time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}

	var seen []string
	for _, ev := range log.Events() {
		line, _ := ev.Attrs["line"].(string)
		if strings.HasPrefix(line, "env ") {
			seen = append(seen, strings.TrimPrefix(line, "env "))
		}
	}
	want := []string{"ARDUINO_CLOUD_CONNECTOR__PORT=1234"}
	if len(seen) != len(want) || seen[0] != want[0] {
		t.Errorf("the child saw %v, want exactly %v: the parent's own variables must not leak", seen, want)
	}
}

// The binary is started as `<binary> daemon`, because that is where the daemon
// lives: at the top level it only prints help and exits.
func TestTheDaemonSubcommandIsPassed(t *testing.T) {
	log, p := start(t, "args", nil)
	if _, err := p.WaitFor(10 * time.Second); err != nil {
		t.Fatalf("wait: %v", err)
	}
	await(t, log, pred("args line", eventlog.Eq("attrs.line", "args daemon")))
}

func TestStartValidatesItsOptions(t *testing.T) {
	self, err := os.Executable()
	if err != nil {
		t.Fatalf("locate the test binary: %v", err)
	}
	tests := []struct {
		name string
		opts Options
	}{
		{"no log", Options{BinaryPath: self}},
		{"no binary", Options{Log: eventlog.New()}},
		{"missing binary", Options{Log: eventlog.New(), BinaryPath: "./does-not-exist"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, err := Start(tc.opts)
			if err == nil {
				_ = p.Kill()
				t.Fatal("Start succeeded, want an error")
			}
		})
	}
}

// Waiting twice must be safe: the runner may wait, and teardown waits again.
func TestWaitIsIdempotent(t *testing.T) {
	_, p := start(t, "exit0", nil)
	p.ExpectExit()

	first, err := p.WaitFor(10 * time.Second)
	if err != nil {
		t.Fatalf("first wait: %v", err)
	}
	second, err := p.WaitFor(10 * time.Second)
	if err != nil {
		t.Fatalf("second wait: %v", err)
	}
	if first.Code != second.Code {
		t.Errorf("two waits disagree: %d and %d", first.Code, second.Code)
	}
}
