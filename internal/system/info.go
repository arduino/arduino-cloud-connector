// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package system

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"

	paths "github.com/arduino/go-paths-helper"
)

type NetworkType string

const (
	NetworkTypeEthernet NetworkType = "eth"
	NetworkTypeWiFi     NetworkType = "wifi"
)

func (nt NetworkType) String() string {
	return string(nt)
}

type Network struct {
	Type NetworkType
	ID   string
	SSID string
}

func GetCurrentNetwork(ctx context.Context) (Network, error) {
	iface, err := getDefaultRouteInterface(ctx)
	if err != nil {
		return Network{}, fmt.Errorf("failed to get active interface: %w", err)
	}

	if iface == "" {
		return Network{}, fmt.Errorf("no active network interface found")
	}

	net := Network{
		ID: iface,
	}

	// Decide the medium from sysfs (name-agnostic), not from the SSID lookup: only
	// query nmcli when the interface is actually wireless, so a wired interface
	// never triggers the (slow) Wi-Fi scan nor a spurious "not a Wi-Fi device" error.
	if isWireless(iface) {
		net.Type = NetworkTypeWiFi
		ssid, err := getWiFiSSID(ctx, iface)
		if err == nil {
			net.SSID = ssid
		}
	} else {
		net.Type = NetworkTypeEthernet
	}

	return net, nil
}

// isWireless reports whether iface is a Wi-Fi interface, via the "wireless" dir
// (legacy) or the "phy80211" link (cfg80211/mac80211) in its sysfs node. This is
// name-independent: it keys off the interface name without interpreting it.
func isWireless(iface string) bool {
	for _, sub := range []string{"wireless", "phy80211"} {
		if paths.New("/sys/class/net", iface, sub).Exist() {
			return true
		}
	}
	return false
}

// TODO Test on IPv6 only system
func getDefaultRouteInterface(ctx context.Context) (string, error) {
	ipv4Cmd := "ip route get $(host -t A time.arduino.cc | awk '/has address/ {print $NF; exit}') | awk '{print $5}'"
	ipv6Cmd := "ip -6 route get $(host -t AAAA time.arduino.cc | awk '/has IPv6 address/ {print $NF; exit}') | awk '{print $3}'"

	// Try IPv4 first
	cmd := exec.CommandContext(ctx, "sh", "-c", ipv4Cmd)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err == nil {
		result := strings.TrimSpace(out.String())
		if result != "" {
			return result, nil
		}
	}

	// Fall back to IPv6
	cmd = exec.CommandContext(ctx, "sh", "-c", ipv6Cmd)
	out.Reset()
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return "", err
	}
	return strings.TrimSpace(out.String()), nil
}

func getWiFiSSID(ctx context.Context, iface string) (string, error) {
	// --rescan no reuses the cached scan results: a fresh Wi-Fi scan can block for
	// many seconds and was overrunning the detection timeout (returning empty →
	// the interface being misclassified as Ethernet).
	cmd := exec.CommandContext(ctx, "nmcli", "-t", "-f", "active,ssid", "dev", "wifi", "list", "ifname", iface, "--rescan", "no")
	var out, stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		slog.Warn("system: failed to read Wi-Fi SSID",
			"iface", iface, "error", err, "stderr", strings.TrimSpace(stderr.String()))
		return "", err
	}
	// Output may contain multiple lines with format: yes:<SSID> or no:<SSID>.
	output := strings.TrimSpace(out.String())
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "yes:") {
			continue
		}

		ssid := strings.TrimSpace(strings.TrimPrefix(line, "yes:"))
		return ssid, nil
	}

	return "", nil
}

func GetWifiPermanentAddress(ctx context.Context, iface string) (string, error) {
	macAddressPaths := []string{
		"/sys/devices/platform/soc@0/1c00000.pci/pci0000:00/0000:00:00.0/0000:01:00.0/0000:02:01.0/0000:03:00.0/ieee80211/phy0/macaddress",
		"/sys/devices/platform/soc@0/c800000.wifi/ieee80211/phy0/macaddress",
	}

	for _, path := range macAddressPaths {
		data, err := paths.New(path).ReadFile()
		if err == nil {
			macAddress := strings.TrimSpace(string(data))
			// Remove colons and convert to uppercase
			macAddress = strings.ReplaceAll(macAddress, ":", "")
			macAddress = strings.ToUpper(macAddress)
			return macAddress, nil
		}
	}

	return "", fmt.Errorf("failed to read WiFi MAC address from any known path")
}

func GetSocSerial() (string, error) {
	data, err := paths.New("/sys/devices/soc0/serial_number").ReadFile()
	if err != nil {
		return "", fmt.Errorf("failed to read serial number: %w", err)
	}
	return strings.TrimSpace(string(data)), nil
}
