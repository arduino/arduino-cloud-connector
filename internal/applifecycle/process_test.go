// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package applifecycle

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	appcli "github.com/arduino/arduino-cloud-connector/internal/app-cli"
	"github.com/arduino/arduino-cloud-connector/internal/cmdslot"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// ── test doubles ─────────────────────────────────────────────────────────────

type fakePublisher struct {
	mu   sync.Mutex
	cmds []command.Cmd
}

func (f *fakePublisher) PublishCommand(_ string, cmd command.Cmd) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cmds = append(f.cmds, cmd)
	return nil
}

// response is one AppCommandResponseCmd, flattened for comparison; details is -999
// when absent.
type response struct {
	op      string
	status  command.AppCommandStatus
	details int32
}

const noDetails = -999

func (f *fakePublisher) responses() []response {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []response
	for _, c := range f.cmds {
		r, ok := c.Inner().(command.AppCommandResponseCmd)
		if !ok {
			continue
		}
		d := int32(noDetails)
		if r.Details != nil {
			d = *r.Details
		}
		out = append(out, response{r.OperationID, r.Status, d})
	}
	return out
}

func (f *fakePublisher) last() (response, bool) {
	rs := f.responses()
	if len(rs) == 0 {
		return response{}, false
	}
	return rs[len(rs)-1], true
}

// fakeCLI records the calls made to arduino-app-cli, in order.
type fakeCLI struct {
	mu    sync.Mutex
	calls []string

	apps          []appcli.App
	appsErr       error
	stopErr       map[string]error
	startProgress []int32
	startErr      error
	defaultErr    error
	// startBlock, when non-nil, holds Start until it is closed or ctx is done.
	startBlock chan struct{}
	started    chan struct{}
	// done, when non-nil, is closed when Start returns.
	done chan struct{}
}

func (f *fakeCLI) record(call string) {
	f.mu.Lock()
	f.calls = append(f.calls, call)
	f.mu.Unlock()
}

func (f *fakeCLI) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

func (f *fakeCLI) Apps(context.Context) ([]appcli.App, error) {
	f.record("apps")
	return f.apps, f.appsErr
}

func (f *fakeCLI) Stop(_ context.Context, appID string, onProgress appcli.ProgressFunc) error {
	f.record("stop " + appID)
	if err := f.stopErr[appID]; err != nil {
		return err
	}
	onProgress(100)
	return nil
}

func (f *fakeCLI) SetDefault(_ context.Context, appID string) error {
	f.record("default " + appID)
	return f.defaultErr
}

func (f *fakeCLI) Start(ctx context.Context, appID string, onProgress appcli.ProgressFunc) error {
	f.record("start " + appID)
	if f.done != nil {
		defer close(f.done)
	}
	if f.started != nil {
		close(f.started)
	}
	for _, p := range f.startProgress {
		onProgress(p)
	}
	if f.startBlock != nil {
		select {
		case <-f.startBlock:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.startErr
}

func newProcess(t *testing.T, cli *fakeCLI) (*Process, *fakePublisher, *cmdslot.Slot) {
	t.Helper()
	pub := &fakePublisher{}
	slot := &cmdslot.Slot{}
	p := New(config.Config{InstallTimeout: 5 * time.Second}, "device-1", pub, cli, slot)
	p.SetConnected(true)
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	t.Cleanup(func() {
		cancel()
		<-p.Done()
	})
	return p, pub, slot
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// waitForOutcome waits for the command's success or failure response.
func waitForOutcome(t *testing.T, pub *fakePublisher, op string) response {
	t.Helper()
	var got response
	waitFor(t, "the outcome of "+op, func() bool {
		r, ok := pub.last()
		if ok && r.op == op && r.status != command.AppCommandInProgress {
			got = r
			return true
		}
		return false
	})
	return got
}

func startCmd(op string, parameter *int64) command.AppCommandCmd {
	return command.AppCommandCmd{AppName: "target", Command: command.AppStart, OperationID: op, Parameter: parameter}
}

func param(v int64) *int64 { return &v }

var someApps = []appcli.App{
	{ID: "a", Status: appcli.StatusRunning},
	{ID: "b", Status: appcli.StatusStopped},
	{ID: "c", Status: appcli.StatusStarting},
	{ID: "target", Status: appcli.StatusFailed},
}

// ── start ────────────────────────────────────────────────────────────────────

// Parameter 1, or none at all: every App that is running or starting is stopped, and
// only then is the App started. Its progress reaches the Cloud as it comes, and the
// success carries no details.
func TestStartStopsTheActiveAppsFirst(t *testing.T) {
	for name, parameter := range map[string]*int64{"no parameter": nil, "parameter 1": param(1)} {
		t.Run(name, func(t *testing.T) {
			cli := &fakeCLI{apps: someApps, startProgress: []int32{0, 10, 10, 100}}
			p, pub, _ := newProcess(t, cli)

			p.Deliver(startCmd("op-1", parameter))
			got := waitForOutcome(t, pub, "op-1")

			if want := []string{"apps", "stop a", "stop c", "start target"}; !slices.Equal(cli.Calls(), want) {
				t.Errorf("calls: got %v want %v", cli.Calls(), want)
			}
			want := []response{
				{"op-1", command.AppCommandInProgress, 0},
				{"op-1", command.AppCommandInProgress, 10},
				{"op-1", command.AppCommandInProgress, 10},
				{"op-1", command.AppCommandInProgress, 100},
				{"op-1", command.AppCommandSucceeded, noDetails},
			}
			if !slices.Equal(pub.responses(), want) {
				t.Errorf("responses:\n got %v\nwant %v", pub.responses(), want)
			}
			if got.status != command.AppCommandSucceeded {
				t.Errorf("outcome: %+v", got)
			}
		})
	}
}

// The App itself counts: arduino-app-cli refuses to start an App that is already up,
// so a running target is stopped and started again.
func TestStartRestartsTheAppIfItIsRunning(t *testing.T) {
	cli := &fakeCLI{apps: []appcli.App{{ID: "target", Status: appcli.StatusRunning}}}
	p, pub, _ := newProcess(t, cli)
	p.Deliver(startCmd("op-1", nil))
	waitForOutcome(t, pub, "op-1")
	if want := []string{"apps", "stop target", "start target"}; !slices.Equal(cli.Calls(), want) {
		t.Errorf("calls: got %v want %v", cli.Calls(), want)
	}
}

// Parameter 0: nothing is stopped, the start is attempted as is, and arduino-app-cli's
// refusal for another App running reaches the Cloud as -2.
func TestStartWithParameterZeroKeepsTheOthersAndPropagatesTheRefusal(t *testing.T) {
	cli := &fakeCLI{apps: someApps, startErr: fmt.Errorf("%w: app \"a\" is running", appcli.ErrAnotherAppRunning)}
	p, pub, _ := newProcess(t, cli)

	p.Deliver(startCmd("op-1", param(0)))
	got := waitForOutcome(t, pub, "op-1")

	if want := []string{"start target"}; !slices.Equal(cli.Calls(), want) {
		t.Errorf("calls: got %v want %v", cli.Calls(), want)
	}
	if want := (response{"op-1", command.AppCommandFailed, int32(ErrAnotherAppRunning)}); got != want {
		t.Errorf("outcome: got %+v want %+v", got, want)
	}
}

func TestStartWithParameterZeroSucceedsWhenNothingElseRuns(t *testing.T) {
	cli := &fakeCLI{}
	p, pub, _ := newProcess(t, cli)
	p.Deliver(startCmd("op-1", param(0)))
	if got := waitForOutcome(t, pub, "op-1"); got.status != command.AppCommandSucceeded {
		t.Errorf("outcome: %+v", got)
	}
}

// A parameter that is neither 0 nor 1 does not guess: nothing is touched.
func TestStartRefusesAnUnknownParameter(t *testing.T) {
	cli := &fakeCLI{apps: someApps}
	p, pub, _ := newProcess(t, cli)
	p.Deliver(startCmd("op-1", param(7)))
	got := waitForOutcome(t, pub, "op-1")
	if got != (response{"op-1", command.AppCommandFailed, int32(ErrGenericFailure)}) {
		t.Errorf("outcome: %+v", got)
	}
	if len(cli.Calls()) != 0 {
		t.Errorf("an invalid command reached arduino-app-cli: %v", cli.Calls())
	}
}

// A stop that fails ends the command: starting anyway would only be refused for the
// App still running.
func TestStartFailsWhenAStopFails(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want Error
	}{
		"stop failed":         {fmt.Errorf("%w: compose stop failed", appcli.ErrFailed), ErrGenericFailure},
		"app-cli unreachable": {fmt.Errorf("%w: connection refused", appcli.ErrUnavailable), ErrAppCLIUnreachable},
	} {
		t.Run(name, func(t *testing.T) {
			cli := &fakeCLI{apps: someApps, stopErr: map[string]error{"a": tc.err}}
			p, pub, _ := newProcess(t, cli)
			p.Deliver(startCmd("op-1", nil))
			got := waitForOutcome(t, pub, "op-1")
			if got != (response{"op-1", command.AppCommandFailed, int32(tc.want)}) {
				t.Errorf("outcome: %+v", got)
			}
			if want := []string{"apps", "stop a"}; !slices.Equal(cli.Calls(), want) {
				t.Errorf("calls: got %v want %v", cli.Calls(), want)
			}
		})
	}
}

func TestStartFailsWhenTheAppListCannotBeRead(t *testing.T) {
	cli := &fakeCLI{appsErr: fmt.Errorf("%w: connection refused", appcli.ErrUnavailable)}
	p, pub, _ := newProcess(t, cli)
	p.Deliver(startCmd("op-1", nil))
	if got := waitForOutcome(t, pub, "op-1"); got.details != int32(ErrAppCLIUnreachable) {
		t.Errorf("outcome: %+v", got)
	}
	if want := []string{"apps"}; !slices.Equal(cli.Calls(), want) {
		t.Errorf("calls: got %v want %v", cli.Calls(), want)
	}
}

// Every arduino-app-cli verdict a start can end with, mapped onto RFC-16 §3.
func TestStartMapsFailuresToRFC16Codes(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want Error
	}{
		"not prepared":   {appcli.ErrNotPrepared, ErrAppNotPrepared},
		"another app":    {appcli.ErrAnotherAppRunning, ErrAnotherAppRunning},
		"no disk":        {appcli.ErrNoSpace, ErrNoDiskSpace},
		"not compatible": {appcli.ErrNotCompatible, ErrAppNotCompatible},
		"unreachable":    {appcli.ErrUnavailable, ErrAppCLIUnreachable},
		"generic":        {appcli.ErrFailed, ErrGenericFailure},
		"unclassified":   {errors.New("something else"), ErrGenericFailure},
	} {
		t.Run(name, func(t *testing.T) {
			p, pub, _ := newProcess(t, &fakeCLI{startErr: fmt.Errorf("%w: detail", tc.err)})
			p.Deliver(startCmd("op-1", param(0)))
			if got := waitForOutcome(t, pub, "op-1"); got.details != int32(tc.want) {
				t.Errorf("code: got %d want %d (%s)", got.details, int32(tc.want), tc.want)
			}
		})
	}
}

// ── stop ─────────────────────────────────────────────────────────────────────

// A stop's progress is the command's, so unlike the stops a start makes it is
// reported; the parameter, which RFC-16 defines for start only, changes nothing.
func TestStopStopsTheAppAndReportsItsProgress(t *testing.T) {
	for name, parameter := range map[string]*int64{"no parameter": nil, "parameter 0": param(0), "parameter 1": param(1)} {
		t.Run(name, func(t *testing.T) {
			cli := &fakeCLI{apps: someApps}
			p, pub, _ := newProcess(t, cli)
			p.Deliver(command.AppCommandCmd{AppName: "target", Command: command.AppStop, OperationID: "op-1", Parameter: parameter})
			waitForOutcome(t, pub, "op-1")

			if want := []string{"stop target"}; !slices.Equal(cli.Calls(), want) {
				t.Errorf("calls: got %v want %v (the other apps are none of a stop's business)", cli.Calls(), want)
			}
			want := []response{
				{"op-1", command.AppCommandInProgress, 100},
				{"op-1", command.AppCommandSucceeded, noDetails},
			}
			if !slices.Equal(pub.responses(), want) {
				t.Errorf("responses: got %v want %v", pub.responses(), want)
			}
		})
	}
}

func TestStopFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want Error
	}{
		"compose stop failed": {fmt.Errorf("%w: compose stop failed", appcli.ErrFailed), ErrGenericFailure},
		"unreachable":         {fmt.Errorf("%w: connection refused", appcli.ErrUnavailable), ErrAppCLIUnreachable},
	} {
		t.Run(name, func(t *testing.T) {
			p, pub, _ := newProcess(t, &fakeCLI{stopErr: map[string]error{"target": tc.err}})
			p.Deliver(command.AppCommandCmd{AppName: "target", Command: command.AppStop, OperationID: "op-1"})
			if got := waitForOutcome(t, pub, "op-1"); got != (response{"op-1", command.AppCommandFailed, int32(tc.want)}) {
				t.Errorf("outcome: %+v", got)
			}
		})
	}
}

// ── set default ──────────────────────────────────────────────────────────────

// arduino-app-cli replaces the default atomically, so setting one touches nothing
// else: no list, no stop, whatever the parameter.
func TestSetDefaultTouchesOnlyTheNamedApp(t *testing.T) {
	for name, parameter := range map[string]*int64{"no parameter": nil, "parameter 0": param(0), "parameter 1": param(1)} {
		t.Run(name, func(t *testing.T) {
			cli := &fakeCLI{apps: []appcli.App{{ID: "old", Default: true, Status: appcli.StatusRunning}}}
			p, pub, _ := newProcess(t, cli)
			p.Deliver(command.AppCommandCmd{AppName: "target", Command: command.AppSetDefault, OperationID: "op-1", Parameter: parameter})
			waitForOutcome(t, pub, "op-1")

			if want := []string{"default target"}; !slices.Equal(cli.Calls(), want) {
				t.Errorf("calls: got %v want %v", cli.Calls(), want)
			}
			if want := []response{{"op-1", command.AppCommandSucceeded, noDetails}}; !slices.Equal(pub.responses(), want) {
				t.Errorf("responses: got %v want %v (no progress, no details)", pub.responses(), want)
			}
		})
	}
}

// RFC-16 §5: any error of the call is -3; only no answer at all is -41.
func TestSetDefaultFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want Error
	}{
		"refused":     {fmt.Errorf("%w: unable to edit the app", appcli.ErrFailed), ErrGenericFailure},
		"unreachable": {fmt.Errorf("%w: connection refused", appcli.ErrUnavailable), ErrAppCLIUnreachable},
	} {
		t.Run(name, func(t *testing.T) {
			p, pub, _ := newProcess(t, &fakeCLI{defaultErr: tc.err})
			p.Deliver(command.AppCommandCmd{AppName: "target", Command: command.AppSetDefault, OperationID: "op-1"})
			if got := waitForOutcome(t, pub, "op-1"); got != (response{"op-1", command.AppCommandFailed, int32(tc.want)}) {
				t.Errorf("outcome: %+v", got)
			}
		})
	}
}

// ── the slot and the watchdog ────────────────────────────────────────────────

// One command at a time, whatever its kind: with an App deploy holding the slot the
// start is refused on the spot with -49, and arduino-app-cli is never called.
func TestACommandIsRefusedWhileADeployHoldsTheSlot(t *testing.T) {
	cli := &fakeCLI{}
	p, pub, slot := newProcess(t, cli)
	slot.TryAcquire("ota")

	p.Deliver(startCmd("op-1", nil))
	got, ok := pub.last()
	if !ok || got != (response{"op-1", command.AppCommandFailed, int32(ErrAnotherCommandInProgress)}) {
		t.Fatalf("refusal: got %+v (%v)", got, ok)
	}
	time.Sleep(20 * time.Millisecond)
	if len(cli.Calls()) != 0 {
		t.Errorf("a refused command reached arduino-app-cli: %v", cli.Calls())
	}
}

// A running start holds the slot against everyone else — a second command and a
// deploy alike — and gives it back when it ends.
func TestARunningCommandHoldsTheSlotUntilItEnds(t *testing.T) {
	cli := &fakeCLI{startBlock: make(chan struct{}), started: make(chan struct{})}
	p, pub, slot := newProcess(t, cli)

	p.Deliver(startCmd("op-1", param(0)))
	<-cli.started

	p.Deliver(startCmd("op-2", param(0)))
	if got, _ := pub.last(); got != (response{"op-2", command.AppCommandFailed, int32(ErrAnotherCommandInProgress)}) {
		t.Errorf("second command: got %+v", got)
	}
	if slot.TryAcquire("ota") {
		t.Error("a deploy took the slot from a running command")
	}

	close(cli.startBlock)
	waitForOutcome(t, pub, "op-1")
	waitFor(t, "the slot to be released", func() bool { return slot.Holder() == "" })
}

// One command at a time holds across the kinds too: a stop, delete or set-default
// arriving while a start runs is refused with -49, like a second start.
func TestEveryCommandKindIsRefusedWhileOneRuns(t *testing.T) {
	cli := &fakeCLI{startBlock: make(chan struct{}), started: make(chan struct{})}
	p, pub, _ := newProcess(t, cli)
	p.Deliver(startCmd("op-1", param(0)))
	<-cli.started

	for i, c := range []command.AppCommand{command.AppStart, command.AppStop, command.AppDelete, command.AppSetDefault} {
		op := fmt.Sprintf("op-%d", i+2)
		p.Deliver(command.AppCommandCmd{AppName: "other", Command: c, OperationID: op})
		if got, _ := pub.last(); got != (response{op, command.AppCommandFailed, int32(ErrAnotherCommandInProgress)}) {
			t.Errorf("%s while a start runs: got %+v", c, got)
		}
	}
	close(cli.startBlock)
	if got := waitForOutcome(t, pub, "op-1"); got.status != command.AppCommandSucceeded {
		t.Errorf("the running start was affected: %+v", got)
	}
}

// The same command delivered again while it runs is not a second command: refusing
// it would fail the command that is running.
func TestARedeliveryOfTheRunningCommandIsIgnored(t *testing.T) {
	cli := &fakeCLI{startBlock: make(chan struct{}), started: make(chan struct{})}
	p, pub, _ := newProcess(t, cli)

	p.Deliver(startCmd("op-1", param(0)))
	<-cli.started
	p.Deliver(startCmd("op-1", param(0)))
	close(cli.startBlock)

	got := waitForOutcome(t, pub, "op-1")
	if got.status != command.AppCommandSucceeded {
		t.Errorf("outcome: %+v", got)
	}
	for _, r := range pub.responses() {
		if r.status == command.AppCommandFailed {
			t.Errorf("the redelivery was refused: %v", pub.responses())
		}
	}
	if n := slices.Index(cli.Calls(), "start target"); n < 0 || len(cli.Calls()) != 1 {
		t.Errorf("calls: %v, want one start", cli.Calls())
	}
}

// A start that goes silent is cancelled by the watchdog and reported, rather than
// holding the slot — and the board — forever.
func TestASilentStartIsCancelledByTheWatchdog(t *testing.T) {
	cli := &fakeCLI{startBlock: make(chan struct{})}
	pub := &fakePublisher{}
	p := New(config.Config{InstallTimeout: 30 * time.Millisecond}, "device-1", pub, cli, nil)
	p.SetConnected(true)
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	defer func() { cancel(); <-p.Done() }()

	p.Deliver(startCmd("op-1", param(0)))
	if got := waitForOutcome(t, pub, "op-1"); got != (response{"op-1", command.AppCommandFailed, int32(ErrGenericFailure)}) {
		t.Errorf("outcome: %+v", got)
	}
}

// Progress keeps a command alive: a start that keeps reporting may outlast the
// timeout many times over.
func TestProgressResetsTheWatchdog(t *testing.T) {
	cli := &steadyCLI{every: 10 * time.Millisecond, beats: 10}
	pub := &fakePublisher{}
	p := New(config.Config{InstallTimeout: 40 * time.Millisecond}, "device-1", pub, cli, nil)
	p.SetConnected(true)
	ctx, cancel := context.WithCancel(context.Background())
	go p.Run(ctx)
	defer func() { cancel(); <-p.Done() }()

	p.Deliver(startCmd("op-1", param(0)))
	if got := waitForOutcome(t, pub, "op-1"); got.status != command.AppCommandSucceeded {
		t.Errorf("a steadily reporting start failed: %+v", got)
	}
}

// steadyCLI starts by reporting progress every so often, for longer than the
// watchdog's timeout in total.
type steadyCLI struct {
	every time.Duration
	beats int
}

func (s *steadyCLI) Apps(context.Context) ([]appcli.App, error)              { return nil, nil }
func (s *steadyCLI) Stop(context.Context, string, appcli.ProgressFunc) error { return nil }
func (s *steadyCLI) SetDefault(context.Context, string) error                { return nil }
func (s *steadyCLI) Start(ctx context.Context, _ string, onProgress appcli.ProgressFunc) error {
	for i := range s.beats {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(s.every):
		}
		onProgress(int32(i * 10))
	}
	return nil
}

// ── the rest ─────────────────────────────────────────────────────────────────

// Delete is not implemented yet, and an unknown command never will be: the Cloud is
// told so rather than left waiting.
func TestCommandsNotSupportedYetAreRefused(t *testing.T) {
	for _, c := range []command.AppCommand{command.AppDelete, 9} {
		t.Run(c.String(), func(t *testing.T) {
			cli := &fakeCLI{}
			p, pub, _ := newProcess(t, cli)
			p.Deliver(command.AppCommandCmd{AppName: "target", Command: c, OperationID: "op-1"})
			if got := waitForOutcome(t, pub, "op-1"); got != (response{"op-1", command.AppCommandFailed, int32(ErrGenericFailure)}) {
				t.Errorf("outcome: %+v", got)
			}
			if len(cli.Calls()) != 0 {
				t.Errorf("calls: %v", cli.Calls())
			}
		})
	}
}

// With the broker down the command still runs; its progress is dropped and its
// outcome held, and the outcome goes out — once — when the broker returns.
func TestTheOutcomeIsHeldWhileDisconnected(t *testing.T) {
	cli := &fakeCLI{startProgress: []int32{10, 50}, done: make(chan struct{})}
	p, pub, slot := newProcess(t, cli)
	p.SetConnected(false)

	p.Deliver(startCmd("op-1", param(0)))
	<-cli.done
	waitFor(t, "the command to end", func() bool { return slot.Holder() == "" })
	if rs := pub.responses(); len(rs) != 0 {
		t.Fatalf("published while disconnected: %v", rs)
	}

	p.SetConnected(true) // Run is idle: the reconnect alone must flush
	waitFor(t, "the held outcome", func() bool { return len(pub.responses()) > 0 })
	time.Sleep(20 * time.Millisecond)
	if want := []response{{"op-1", command.AppCommandSucceeded, noDetails}}; !slices.Equal(pub.responses(), want) {
		t.Errorf("after the reconnect: got %v want %v (progress dropped, outcome once)", pub.responses(), want)
	}
}

// A refusal is an outcome too, and a held outcome leaves before anything composed
// after it.
func TestHeldOutcomesGoOutFirstAndInOrder(t *testing.T) {
	cli := &fakeCLI{startProgress: []int32{30}}
	p, pub, slot := newProcess(t, cli)
	p.SetConnected(false)

	slot.TryAcquire("ota")
	p.Deliver(startCmd("op-1", nil)) // refused: a deploy holds the slot
	slot.Release("ota")

	p.connected.Store(true) // up, without the reconnect signal: the next publish must flush
	p.Deliver(startCmd("op-2", param(0)))
	waitForOutcome(t, pub, "op-2")

	want := []response{
		{"op-1", command.AppCommandFailed, int32(ErrAnotherCommandInProgress)},
		{"op-2", command.AppCommandInProgress, 30},
		{"op-2", command.AppCommandSucceeded, noDetails},
	}
	if !slices.Equal(pub.responses(), want) {
		t.Errorf("order:\n got %v\nwant %v", pub.responses(), want)
	}
}

// The outbox is bounded and drops the oldest.
func TestTheOutboxDropsTheOldestWhenFull(t *testing.T) {
	p := New(config.Config{}, "device-1", &fakePublisher{}, &fakeCLI{}, nil)
	for i := range outboxLimit + 2 {
		p.fail(fmt.Sprintf("op-%d", i), ErrAnotherCommandInProgress)
	}
	if len(p.outbox) != outboxLimit {
		t.Fatalf("outbox holds %d, want %d", len(p.outbox), outboxLimit)
	}
	if got := p.outbox[0].OperationID; got != "op-2" {
		t.Errorf("oldest survivor: got %s want op-2", got)
	}
}

func TestErrorCodesMatchRFC16(t *testing.T) {
	for code, want := range map[Error]struct {
		value int32
		name  string
	}{
		ErrAppNotPrepared:           {-1, "AppNotPrepared"},
		ErrAnotherAppRunning:        {-2, "AnotherAppRunning"},
		ErrGenericFailure:           {-3, "GenericFailure"},
		ErrNoDiskSpace:              {-40, "NoDiskSpace"},
		ErrAppCLIUnreachable:        {-41, "AppCLINotReachable"},
		ErrAppNotCompatible:         {-44, "AppNotCompatible"},
		ErrAnotherCommandInProgress: {-49, "AnotherCommandInProgress"},
	} {
		if int32(code) != want.value || code.String() != want.name {
			t.Errorf("%s: got %d %q want %d %q", want.name, int32(code), code.String(), want.value, want.name)
		}
	}
}
