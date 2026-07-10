// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !mock

package system

import (
	"context"
	"log/slog"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// netConfigDetectTimeout caps the network detection (shell lookups in
// GetCurrentNetwork) so a slow/hung interface query never stalls the handshake.
const netConfigDetectTimeout = 20 * time.Second

// NetConfig renders the active network connection as a command.DeviceNetConfigCmd
// for the cloud announcement. ok is false when the connection can't be
// determined, so the caller skips publishing (UI-only metadata).
func NetConfig() (command.DeviceNetConfigCmd, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), netConfigDetectTimeout)
	defer cancel()

	net, err := GetCurrentNetwork(ctx)
	if err != nil {
		slog.Debug("system: network detection failed", "error", err)
		return command.DeviceNetConfigCmd{}, false
	}

	switch net.Type {
	case NetworkTypeWiFi:
		return command.DeviceNetConfigCmd{
			Type: command.NetworkTypeWiFi,
			SSID: net.SSID,
		}, true
	case NetworkTypeEthernet:
		// The cloud only needs the adapter type; addresses left unset (DHCP).
		return command.DeviceNetConfigCmd{Type: command.NetworkTypeEthernet}, true
	default:
		return command.DeviceNetConfigCmd{}, false
	}
}
