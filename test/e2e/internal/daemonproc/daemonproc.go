// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package daemonproc runs the daemon under test as a subprocess.
//
// This is what makes the suite black-box: the harness spawns the REAL built
// binary and touches it only from outside. It therefore also covers what an
// in-process harness structurally cannot -- the signal handling and graceful
// shutdown (a real past bug), the listener setup, and later the .deb packaging
// -- and it tests the artifact that ships rather than a refactored copy of it.
//
// # A dead daemon must fail the scenario at once
//
// Without that, a crash at step 3 of 20 costs minutes: every remaining Await
// waits out its own budget and the report blames the first missing message
// instead of naming the death. So the supervisor appends a describing event and
// then latches the log (eventlog.Abort), which wakes every pending waiter with
// an AbortedError -- a different type from a timeout, because "the thing under
// test died" and "nothing happened in time" call for different reactions.
//
// Only an UNEXPECTED exit aborts. A graceful-shutdown scenario asks for a
// SIGTERM and expects a clean exit, so the supervisor has to be told an exit is
// coming; see ExpectExit.
package daemonproc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

// subcommand is how the binary is started: the daemon lives behind a cobra
// subcommand, not at the top level.
const subcommand = "daemon"

// stderrTailLines is how much of stderr the crash report carries. Enough for a
// panic header and the first frames of its stack, which is what says where it
// died.
const stderrTailLines = 40

// Options configures the process.
type Options struct {
	// Log receives the daemon's output, its exit and the abort. Required.
	Log *eventlog.Log
	// BinaryPath is the daemon binary to run. Required.
	BinaryPath string
	// Env is the environment to pass, on top of the small pass-through set (see
	// buildEnv). Keys are the daemon's own ARDUINO_CLOUD_CONNECTOR__* names.
	Env map[string]string
	// Args are extra arguments after the daemon subcommand.
	Args []string
}

// Process is a running daemon.
type Process struct {
	log *eventlog.Log
	cmd *exec.Cmd

	mu           sync.Mutex
	stderrTail   []string
	panicLine    string
	expectExit   bool
	exited       bool
	exitInfo     ExitInfo
	pumpsDone    sync.WaitGroup
	waitOnce     sync.Once
	waitDone     chan struct{}
	waitErr      error
	gracefulStop bool
}

// ExitInfo is how the daemon ended.
type ExitInfo struct {
	Code int
	// Signal is set when the process was terminated by one, which is how a
	// SIGTERM-driven shutdown is told apart from a self-chosen exit.
	Signal string
	// PanicLine is the first "panic:" or "fatal error:" line seen on stderr.
	// It is what turns "exit status 2" into an explanation.
	PanicLine  string
	StderrTail []string
}

// Start spawns the daemon and begins pumping its output into the log.
func Start(opts Options) (*Process, error) {
	if opts.Log == nil {
		return nil, fmt.Errorf("daemonproc: no event log given")
	}
	if opts.BinaryPath == "" {
		return nil, fmt.Errorf("daemonproc: no binary given")
	}
	abs, err := filepath.Abs(opts.BinaryPath)
	if err != nil {
		return nil, fmt.Errorf("daemonproc: resolve %s: %w", opts.BinaryPath, err)
	}
	if _, err := os.Stat(abs); err != nil {
		return nil, fmt.Errorf("daemonproc: binary %s: %w", abs, err)
	}

	args := append([]string{subcommand}, opts.Args...)
	cmd := exec.Command(abs, args...)
	cmd.Env = buildEnv(opts.Env)

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, fmt.Errorf("daemonproc: stdout pipe: %w", err)
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("daemonproc: stderr pipe: %w", err)
	}

	p := &Process{log: opts.Log, cmd: cmd, waitDone: make(chan struct{})}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("daemonproc: start %s: %w", abs, err)
	}
	p.log.Append(eventlog.SourceDaemonProcess, eventlog.KindHarnessNote, map[string]any{
		"action": "started",
		"binary": abs,
		"pid":    cmd.Process.Pid,
	}, nil)

	p.pumpsDone.Add(2)
	go p.pump("stdout", stdout)
	go p.pump("stderr", stderr)
	return p, nil
}

// PID is the process id, for a report that has to name it.
func (p *Process) PID() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

// ExpectExit tells the supervisor that the next exit is intended, so it is
// recorded but does NOT abort the log.
//
// It exists because the abort is a safety net, not a verdict: a
// graceful-shutdown scenario deliberately ends the process, and treating that
// as a crash would make the one scenario that tests shutdown unable to pass.
func (p *Process) ExpectExit() {
	p.mu.Lock()
	p.expectExit = true
	p.mu.Unlock()
}

// Stop asks the daemon to shut down gracefully and waits for it to go.
//
// SIGTERM is the signal the daemon handles; the graceful paths (the cloud FSM
// draining, the HTTP server shutting down) only run for it. Windows has no
// SIGTERM, so there the process is killed instead and a note says so: the
// graceful path is NOT exercised on that host, and a scenario that claims to
// test shutdown must not quietly pass because of it.
func (p *Process) Stop(ctx context.Context) (ExitInfo, error) {
	p.ExpectExit()
	if p.cmd.Process == nil {
		return ExitInfo{}, fmt.Errorf("daemonproc: process was never started")
	}

	graceful := true
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		graceful = false
		p.log.Append(eventlog.SourceDaemonProcess, eventlog.KindHarnessNote, map[string]any{
			"action": "sigterm_unsupported",
			"os":     runtime.GOOS,
			"error":  err.Error(),
			"note":   "killed instead; the graceful shutdown path was NOT exercised",
		}, nil)
		if kerr := p.cmd.Process.Kill(); kerr != nil {
			return ExitInfo{}, fmt.Errorf("daemonproc: kill after failed SIGTERM: %w", kerr)
		}
	} else {
		p.log.Append(eventlog.SourceDaemonProcess, eventlog.KindHarnessNote, map[string]any{
			"action": "sigterm",
			"pid":    p.PID(),
		}, nil)
	}
	p.mu.Lock()
	p.gracefulStop = graceful
	p.mu.Unlock()

	return p.WaitContext(ctx)
}

// Kill terminates the daemon without asking.
func (p *Process) Kill() error {
	p.ExpectExit()
	if p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return fmt.Errorf("daemonproc: kill: %w", err)
	}
	return nil
}

// GracefulStopExercised reports whether the last Stop actually delivered
// SIGTERM. False on a host without it, which is what a shutdown scenario has
// to check before claiming the graceful path works.
func (p *Process) GracefulStopExercised() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.gracefulStop
}

// WaitContext waits for the daemon to exit, or for ctx to end.
func (p *Process) WaitContext(ctx context.Context) (ExitInfo, error) {
	go p.wait()
	select {
	case <-p.waitDone:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.exitInfo, p.waitErr
	case <-ctx.Done():
		return ExitInfo{}, ctx.Err()
	}
}

// wait reaps the process exactly once, records the exit and latches the log
// when nobody asked for it.
func (p *Process) wait() {
	p.waitOnce.Do(func() {
		defer close(p.waitDone)

		// The pumps must finish first: the panic line and the stderr tail are
		// the most useful part of a crash report, and they are still in flight
		// when the process dies.
		p.pumpsDone.Wait()
		err := p.cmd.Wait()

		info := ExitInfo{Code: p.cmd.ProcessState.ExitCode()}
		if status, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			info.Signal = status.Signal().String()
		}

		p.mu.Lock()
		info.PanicLine = p.panicLine
		info.StderrTail = append([]string(nil), p.stderrTail...)
		expected := p.expectExit
		p.exited = true
		p.exitInfo = info
		// A non-zero exit is data, not an error: the caller reads it off
		// ExitInfo. Only a failure to reap the process at all is an error.
		var exitErr *exec.ExitError
		if err != nil && !errors.As(err, &exitErr) {
			p.waitErr = err
		}
		p.mu.Unlock()

		attrs := map[string]any{
			"exit_code": info.Code,
			"expected":  expected,
		}
		if info.Signal != "" {
			attrs["signal"] = info.Signal
		}
		if info.PanicLine != "" {
			attrs["panic"] = info.PanicLine
		}
		p.log.Append(eventlog.SourceDaemonProcess, eventlog.KindProcessExit, attrs, nil)

		if expected {
			return
		}
		// Nobody asked for this. Latch the log so every pending and future
		// Await fails at once with the death as the reason, instead of each
		// one waiting out its own budget.
		reason := fmt.Sprintf("the daemon exited with code %d", info.Code)
		if info.Signal != "" {
			reason = fmt.Sprintf("the daemon was terminated by %s", info.Signal)
		}
		if info.PanicLine != "" {
			reason = fmt.Sprintf("the daemon panicked: %s", info.PanicLine)
		}
		if len(info.StderrTail) > 0 {
			reason += "\nlast stderr:\n  " + strings.Join(info.StderrTail, "\n  ")
		}
		p.log.Abort(reason, err)
	})
}

// pump copies one output stream into the log, line by line.
//
// Every line becomes an event, which is what lets a failure report interleave
// the daemon's own view with the protocol traffic. Log lines are never
// significant: the daemon talks continuously and a scenario cannot be expected
// to account for what it says.
func (p *Process) pump(stream string, r io.Reader) {
	defer p.pumpsDone.Done()

	scanner := bufio.NewScanner(r)
	// A panic dumps long goroutine lines; the default 64 KiB token limit is
	// enough, but a single oversized line must not silently truncate the rest
	// of the stream, so the buffer is raised rather than left at the default.
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		p.log.Append(eventlog.SourceDaemonLog, eventlog.KindDaemonLine, map[string]any{
			"stream": stream,
			"line":   line,
		}, nil)
		if stream == "stderr" {
			p.recordStderr(line)
		}
	}
}

// recordStderr keeps the tail and the first panic header.
func (p *Process) recordStderr(line string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stderrTail = append(p.stderrTail, line)
	if len(p.stderrTail) > stderrTailLines {
		p.stderrTail = p.stderrTail[len(p.stderrTail)-stderrTailLines:]
	}
	if p.panicLine == "" && isPanicLine(line) {
		p.panicLine = strings.TrimSpace(line)
	}
}

// isPanicLine recognises the two ways the Go runtime announces a death. The
// message matters more than the exit status: "exit status 2" says nothing,
// "panic: nil map write" says where to look.
func isPanicLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	return strings.HasPrefix(trimmed, "panic:") ||
		strings.HasPrefix(trimmed, "fatal error:") ||
		strings.HasPrefix(trimmed, "panic [recovered]:")
}

// Exited reports whether the process has already been reaped.
func (p *Process) Exited() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.exited
}

// StderrTail is the last lines of stderr, for a caller assembling its own
// report.
func (p *Process) StderrTail() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.stderrTail...)
}

// buildEnv assembles the child environment.
//
// It is built from an explicit pass-through list rather than from os.Environ,
// because the daemon reads its whole configuration from the environment: an
// ARDUINO_CLOUD_CONNECTOR__* variable that happens to be set on a developer's
// machine would silently change what the suite tests, and the failure would be
// impossible to reproduce anywhere else.
func buildEnv(overrides map[string]string) []string {
	// PATH because the daemon may shell out; the Windows entries because the
	// Go runtime and crypto/rand need them there.
	passThrough := []string{"PATH", "HOME", "TMPDIR", "SYSTEMROOT", "TEMP", "TMP", "USERPROFILE", "LOCALAPPDATA"}

	env := make([]string, 0, len(passThrough)+len(overrides))
	for _, key := range passThrough {
		if v, ok := os.LookupEnv(key); ok {
			env = append(env, key+"="+v)
		}
	}
	for k, v := range overrides {
		env = append(env, k+"="+v)
	}
	return env
}

// WaitFor is a convenience for a caller that wants a bounded wait without
// building a context.
func (p *Process) WaitFor(timeout time.Duration) (ExitInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return p.WaitContext(ctx)
}
