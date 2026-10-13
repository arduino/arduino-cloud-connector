// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package applifecycle

import (
	"context"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// setDefault makes the App the command names the one the board starts at boot.
//
// Unlike a start it needs nothing done first: arduino-app-cli keeps a single default
// and replaces it atomically, so a previous default is simply superseded. It is a
// plain request with no progress, bounded by the client's own timeout rather than by
// the watchdog. The command's parameter is ignored.
func (p *Process) setDefault(ctx context.Context, cmd command.AppCommandCmd) error {
	return p.cli.SetDefault(ctx, cmd.AppName)
}
