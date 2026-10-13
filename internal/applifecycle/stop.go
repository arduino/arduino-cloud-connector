// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package applifecycle

import (
	"context"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// stop stops the App the command names. Its progress is the command's, so it is
// reported. Stopping an App that is not running succeeds: arduino-app-cli's stop is
// idempotent. The command's parameter is ignored — RFC-16 defines one for start only.
func (p *Process) stop(ctx context.Context, cmd command.AppCommandCmd) error {
	return p.watched(ctx, func(ctx context.Context, beat func()) error {
		return p.cli.Stop(ctx, cmd.AppName, func(percent int32) {
			beat()
			p.progress(cmd.OperationID, percent)
		})
	})
}
