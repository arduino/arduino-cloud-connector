// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build mock

package system

import "github.com/arduino/arduino-cloud-connector/internal/mqtt/command"

// NetConfig returns a fixed Wi-Fi config in mock builds so the App Lab FE sees a
// stable network announcement regardless of the dev host's connectivity.
func NetConfig() (command.DeviceNetConfigCmd, bool) {
	return command.DeviceNetConfigCmd{
		Type: command.NetworkTypeWiFi,
		SSID: "SSIDTEST1",
	}, true
}
