// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package applifecycle carries out the App lifecycle commands the Cloud sends
// (RFC-16): start, stop, delete and set-as-default an App already installed on the
// board. Start, stop and set-default are implemented; delete is refused with
// ErrGenericFailure for now.
//
// # Protocol
//
//   - The Cloud sends AppCommandCmd (0x013001) with an operation id.
//   - The board answers with AppCommandResponseCmd (0x013002) under the same id:
//     in progress with a percentage, as often as arduino-app-cli reports one; then
//     exactly one success (no details) or failure (a negative code, see errors.go).
//
// # One command at a time
//
// A lifecycle command takes the board's single command slot (internal/cmdslot), the
// same one an App deploy takes, so it neither runs alongside a deploy nor alongside
// another lifecycle command. The first to arrive wins; a command that finds the slot
// taken is answered on the spot with ErrAnotherCommandInProgress (-49). The same
// command delivered again while it runs is not a second command and is ignored.
//
// # Broker outages
//
// A response that cannot be published is handled like the deploy's terminal
// reports. An outcome — success, failure, a -49 refusal — is sent once and is what
// closes the operation on the Cloud, so it is held in an outbox and sent, oldest
// first and ahead of anything newer, when the broker returns. A progress report is
// dropped: the next one, or the outcome, says more than a stale percentage would.
// The outbox is memory, so what it holds is lost if the daemon stops first.
//
// # Silence
//
// arduino-app-cli's streams can go quiet for long stretches, so a command is bounded
// by a stall watchdog rather than a deadline: InstallTimeout without a progress
// event cancels it. Every progress event resets the watchdog, a stop's included.
package applifecycle

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	appcli "github.com/arduino/arduino-cloud-connector/internal/app-cli"
	"github.com/arduino/arduino-cloud-connector/internal/cmdslot"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// Publisher publishes uplink commands. Satisfied by mqtt.Client.
type Publisher interface {
	PublishCommand(deviceID string, cmd command.Cmd) error
}

// AppCLI is the part of arduino-app-cli the commands use. Satisfied by
// *appcli.Client, which follows every stream on the caller's goroutine — the process
// relies on that to publish progress from its own goroutine only.
type AppCLI interface {
	Apps(ctx context.Context) ([]appcli.App, error)
	Start(ctx context.Context, appID string, onProgress appcli.ProgressFunc) error
	Stop(ctx context.Context, appID string, onProgress appcli.ProgressFunc) error
	SetDefault(ctx context.Context, appID string) error
}

// Process carries out lifecycle commands, one at a time. Construct with New, drive
// with Run, feed with Deliver and SetConnected.
type Process struct {
	deviceID string
	pub      Publisher
	cli      AppCLI
	slot     *cmdslot.Slot
	timeout  time.Duration

	// queue holds the one command admitted through the slot until Run takes it.
	queue chan command.AppCommandCmd
	done  chan struct{}

	// connected gates publishing: false means nothing published now would reach
	// the Cloud.
	connected atomic.Bool
	// connUp carries a signal each time the broker comes up, so an idle Run can
	// flush the outbox without waiting for the next command. Buffered, so
	// SetConnected never blocks.
	connUp chan struct{}

	// outbox holds the outcomes that could not be published, under outMu. outMu is
	// also held across every publish, so a held outcome always leaves before anything
	// composed after it, whichever goroutine publishes it.
	outMu  sync.Mutex
	outbox []command.AppCommandResponseCmd

	// running is the operation id of the command holding the slot, "" when none.
	mu      sync.Mutex
	running string
}

// New builds the process. slot is the command slot shared with the App deploy.
func New(cfg config.Config, deviceID string, pub Publisher, cli AppCLI, slot *cmdslot.Slot) *Process {
	if slot == nil {
		slot = &cmdslot.Slot{}
	}
	return &Process{
		deviceID: deviceID,
		pub:      pub,
		cli:      cli,
		slot:     slot,
		timeout:  cfg.InstallTimeout,
		// One is enough: the slot admits one command at a time.
		queue:  make(chan command.AppCommandCmd, 1),
		done:   make(chan struct{}),
		connUp: make(chan struct{}, 1),
	}
}

// Done returns a channel closed when Run has returned.
func (p *Process) Done() <-chan struct{} { return p.done }

// SetConnected tells the process whether a message published now would reach the
// Cloud. A command in progress keeps running while disconnected; its progress is
// lost, its outcome held until the broker returns.
func (p *Process) SetConnected(v bool) {
	if p.connected.Swap(v) == v || !v {
		return
	}
	select {
	case p.connUp <- struct{}{}:
	default: // a signal is already pending; one is enough
	}
}

// Deliver hands a command to the process. Non-blocking: called from the Cloud FSM's
// event loop, which must never stall behind an App start.
func (p *Process) Deliver(cmd command.AppCommandCmd) {
	p.mu.Lock()
	running := p.running
	p.mu.Unlock()
	if cmd.OperationID != "" && cmd.OperationID == running {
		// The same command again — an MQTT redelivery, or the Cloud repeating itself.
		// Refusing it would fail the very command that is running.
		slog.Info("applifecycle: command repeats the one in progress, ignoring",
			"operation_id", cmd.OperationID)
		return
	}

	holder := slotHolder(cmd.OperationID)
	if !p.slot.TryAcquire(holder) {
		slog.Warn("applifecycle: another command is in progress, refusing",
			"operation_id", cmd.OperationID, "command", cmd.Command, "in_progress", p.slot.Holder())
		p.fail(cmd.OperationID, ErrAnotherCommandInProgress)
		return
	}
	p.mu.Lock()
	p.running = cmd.OperationID
	p.mu.Unlock()

	select {
	case p.queue <- cmd:
	default:
		// Unreachable while the slot admits one command at a time. Releasing matters
		// anyway: a slot held for a command that was never queued would refuse every
		// command after it.
		p.release(cmd.OperationID)
		slog.Warn("applifecycle: queue full, refusing", "operation_id", cmd.OperationID)
		p.fail(cmd.OperationID, ErrAnotherCommandInProgress)
	}
}

// Run carries out the commands Deliver admits until ctx is cancelled.
func (p *Process) Run(ctx context.Context) {
	defer close(p.done)
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.connUp:
			p.flush()
		case cmd := <-p.queue:
			p.execute(ctx, cmd)
			p.release(cmd.OperationID)
		}
	}
}

func (p *Process) execute(ctx context.Context, cmd command.AppCommandCmd) {
	slog.Info("applifecycle: command received",
		"operation_id", cmd.OperationID, "command", cmd.Command, "app", cmd.AppName)
	switch cmd.Command {
	case command.AppStart:
		p.finish(ctx, cmd, p.start(ctx, cmd))
	case command.AppStop:
		p.finish(ctx, cmd, p.stop(ctx, cmd))
	case command.AppSetDefault:
		p.finish(ctx, cmd, p.setDefault(ctx, cmd))
	default:
		slog.Warn("applifecycle: command not supported yet",
			"operation_id", cmd.OperationID, "command", cmd.Command)
		p.fail(cmd.OperationID, ErrGenericFailure)
	}
}

// finish reports a command's outcome.
func (p *Process) finish(ctx context.Context, cmd command.AppCommandCmd, err error) {
	if err == nil {
		slog.Info("applifecycle: command succeeded", "operation_id", cmd.OperationID, "command", cmd.Command)
		p.respond(command.AppCommandResponseCmd{OperationID: cmd.OperationID, Status: command.AppCommandSucceeded})
		return
	}
	if ctx.Err() != nil {
		// Shutdown, not a failure of the command: nothing can be said about it.
		slog.Info("applifecycle: command interrupted by shutdown", "operation_id", cmd.OperationID)
		return
	}
	code := decodeError(err)
	slog.Error("applifecycle: command failed",
		"operation_id", cmd.OperationID, "command", cmd.Command, "app", cmd.AppName,
		"code", code, "code_value", int32(code), "error", err)
	p.fail(cmd.OperationID, code)
}

func (p *Process) release(operationID string) {
	p.mu.Lock()
	if p.running == operationID {
		p.running = ""
	}
	p.mu.Unlock()
	p.slot.Release(slotHolder(operationID))
}

// slotHolder names a lifecycle command in the shared command slot.
func slotHolder(operationID string) string { return "app-command:" + operationID }

// progress reports a command in progress.
func (p *Process) progress(operationID string, percent int32) {
	p.respond(command.AppCommandResponseCmd{
		OperationID: operationID, Status: command.AppCommandInProgress, Details: &percent,
	})
}

// fail reports a failed command.
func (p *Process) fail(operationID string, code Error) {
	c := int32(code)
	p.respond(command.AppCommandResponseCmd{
		OperationID: operationID, Status: command.AppCommandFailed, Details: &c,
	})
}

// respond publishes one response, or holds it if it is an outcome and the broker is
// down. Called from Run's goroutine, and from Deliver's for a refusal.
func (p *Process) respond(r command.AppCommandResponseCmd) {
	p.outMu.Lock()
	defer p.outMu.Unlock()

	if !p.connected.Load() {
		if r.Status == command.AppCommandInProgress {
			slog.Debug("applifecycle: broker not connected, progress dropped",
				"operation_id", r.OperationID)
			return
		}
		p.hold(r)
		return
	}
	p.flushLocked()
	p.publish(r)
}

// outboxLimit caps the held outcomes. One command runs at a time, but every command
// refused during an outage adds a refusal, so it is larger than the deploy's; the
// oldest goes first.
const outboxLimit = 16

// hold keeps an outcome for the next connection. outMu must be held.
func (p *Process) hold(r command.AppCommandResponseCmd) {
	if len(p.outbox) >= outboxLimit {
		slog.Warn("applifecycle: outbox full, dropping the oldest held outcome",
			"operation_id", p.outbox[0].OperationID, "limit", outboxLimit)
		p.outbox = p.outbox[1:]
	}
	p.outbox = append(p.outbox, r)
	slog.Info("applifecycle: broker down, holding the outcome until it reconnects",
		"operation_id", r.OperationID, "status", r.Status, "held", len(p.outbox))
}

// flush sends everything held, if the broker is up.
func (p *Process) flush() {
	p.outMu.Lock()
	defer p.outMu.Unlock()
	p.flushLocked()
}

// flushLocked sends everything held, oldest first. outMu must be held.
func (p *Process) flushLocked() {
	if len(p.outbox) == 0 || !p.connected.Load() {
		return
	}
	held := p.outbox
	p.outbox = nil
	for _, r := range held {
		// Not re-held on error: failing to publish while connected is not solved by
		// trying again later.
		p.publish(r)
	}
}

func (p *Process) publish(r command.AppCommandResponseCmd) {
	if err := p.pub.PublishCommand(p.deviceID, command.From(r)); err != nil {
		slog.Warn("applifecycle: publishing the response failed",
			"operation_id", r.OperationID, "status", r.Status, "error", err)
	}
}
