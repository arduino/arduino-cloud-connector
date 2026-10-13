// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package applifecycle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// Start's parameter (RFC-16 §3).
const (
	// keepOthers: start the App only if nothing else is running; arduino-app-cli's
	// refusal reaches the Cloud as ErrAnotherAppRunning.
	keepOthers int64 = 0
	// stopOthers: stop every running App first. Also what a start with no parameter
	// does.
	stopOthers int64 = 1
)

// errStalled is the watchdog firing: no progress for the process's timeout.
var errStalled = errors.New("applifecycle: arduino-app-cli reported no progress")

// start starts the App the command names.
//
// With stopOthers (or no parameter) every App that is running or starting is stopped
// first — the named App included, since arduino-app-cli refuses to start an App that
// is already up — so the start cannot be refused for another App running. With
// keepOthers it is attempted as is.
func (p *Process) start(ctx context.Context, cmd command.AppCommandCmd) error {
	mode := stopOthers
	if cmd.Parameter != nil {
		mode = *cmd.Parameter
	}
	if mode != stopOthers && mode != keepOthers {
		return fmt.Errorf("applifecycle: start parameter %d is neither %d nor %d", mode, keepOthers, stopOthers)
	}

	return p.watched(ctx, func(ctx context.Context, beat func()) error {
		if mode == stopOthers {
			if err := p.stopActive(ctx, beat); err != nil {
				return err
			}
		}
		return p.cli.Start(ctx, cmd.AppName, func(percent int32) {
			beat()
			p.progress(cmd.OperationID, percent)
		})
	})
}

// stopActive stops every App that is running or starting. A stop's progress keeps
// the command alive but is not reported: the Cloud asked for a start, and its
// progress bar is the start's.
func (p *Process) stopActive(ctx context.Context, beat func()) error {
	apps, err := p.cli.Apps(ctx)
	if err != nil {
		return fmt.Errorf("list the apps: %w", err)
	}
	for _, app := range apps {
		if !app.Active() {
			continue
		}
		slog.Info("applifecycle: stopping a running app before the start",
			"app", app.ID, "status", app.Status)
		if err := p.cli.Stop(ctx, app.ID, func(int32) { beat() }); err != nil {
			return fmt.Errorf("stop %s: %w", app.ID, err)
		}
	}
	return nil
}

// watched runs fn under the stall watchdog: p.timeout without a beat cancels fn's
// context, and the command fails with errStalled. fn calls beat on every progress
// event, on the goroutine that runs fn.
func (p *Process) watched(ctx context.Context, fn func(ctx context.Context, beat func()) error) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()

	beats := make(chan struct{}, 1)
	stalled := make(chan struct{})
	go func() {
		timer := time.NewTimer(p.timeout)
		defer timer.Stop()
		for {
			select {
			case <-wctx.Done():
				return
			case <-beats:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(p.timeout)
			case <-timer.C:
				slog.Error("applifecycle: no progress within the stall timeout, cancelling",
					"timeout", p.timeout)
				close(stalled)
				cancel()
				return
			}
		}
	}()

	err := fn(wctx, func() {
		select {
		case beats <- struct{}{}:
		default: // a beat is already pending; one is enough to reset the timer
		}
	})
	if err != nil {
		// Only a stall if the watchdog actually fired: a real error must not be
		// misreported as a timeout.
		select {
		case <-stalled:
			return fmt.Errorf("%w for %s", errStalled, p.timeout)
		default:
		}
	}
	return err
}
