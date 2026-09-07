// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !mock

package identity

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/system"
)

// UHWID composition is fixed: it is the SHA-256 of "<WiFi permanent MAC>:<SoC
// serial>". BOTH identifiers are mandatory — a partial UHWID (e.g. serial only)
// hashes to a different, wrong value, which would change the board's cloud
// identity. The UHWID is never persisted, so it is recomputed on every start.
//
// The WiFi PHY's macaddress sysfs node only appears once the WiFi driver and
// firmware have finished probing, which on these SoCs can happen seconds after
// the daemon starts at boot. To avoid computing a wrong (serial-only) UHWID
// during that window, computeUHWID retries a few times with a fixed back-off
// and fails hard if a required identifier is still missing after the last
// attempt — never returning a degraded UHWID.
//
// uhwidMaxAttempts / uhwidRetryInterval are package vars so tests can shorten
// the wait.
var (
	uhwidMaxAttempts   = 4
	uhwidRetryInterval = 10 * time.Second

	// Seams for the two hardware reads, overridable in tests (the real readers
	// hit sysfs and cannot run on a dev host / Windows).
	readWifiMAC = func(ctx context.Context) (string, error) {
		// GetWifiPermanentAddress reads the WiFi PHY's permanent MAC straight
		// from sysfs and ignores the interface argument, so "" is fine.
		return system.GetWifiPermanentAddress(ctx, "")
	}
	readSocSerial = func() (string, error) { return system.GetSocSerial() }
)

// computeUHWID assembles the hash input from the two mandatory hardware
// identifiers (permanent WiFi MAC + SoC serial) and returns the hex-encoded
// SHA-256 digest. It retries with a fixed back-off while an identifier is not
// yet readable (see the package doc above) and returns an error if the set is
// still incomplete after the last attempt.
//
// Cancelling ctx aborts the wait between attempts and returns an error wrapping
// ctx.Err(), so a stop signal is honoured during the boot-time probe window.
//
// The mock build (uhwid_mock.go) replaces this with a random + persisted
// implementation.
func computeUHWID(ctx context.Context, _ config.Config) (string, error) {
	var lastErr error
	for attempt := 1; attempt <= uhwidMaxAttempts; attempt++ {
		uhwid, err := buildUHWID(ctx)
		if err == nil {
			return uhwid, nil
		}
		lastErr = err

		slog.Warn("UHWID: hardware identifiers not ready yet",
			"attempt", attempt, "max_attempts", uhwidMaxAttempts,
			"retry_in", uhwidRetryInterval, "error", err)
		if attempt < uhwidMaxAttempts {
			// The back-off is the entire cost of this loop — an attempt is two
			// sysfs reads — so it is the only thing worth interrupting. Left as
			// a bare sleep, a stop signal arriving in this window is absorbed
			// for up to (uhwidMaxAttempts-1) * uhwidRetryInterval before the
			// daemon can react to it.
			select {
			case <-time.After(uhwidRetryInterval):
			case <-ctx.Done():
				return "", fmt.Errorf("UHWID: interrupted while waiting for hardware identifiers: %w", ctx.Err())
			}
		}
	}

	return "", fmt.Errorf("UHWID: hardware identifiers unavailable after %d attempts: %w",
		uhwidMaxAttempts, lastErr)
}

// buildUHWID performs a single attempt: both the WiFi MAC and the SoC serial
// must be present, or it returns an error (no partial UHWID).
func buildUHWID(ctx context.Context) (string, error) {
	mac, err := readWifiMAC(ctx)
	if err != nil {
		return "", fmt.Errorf("WiFi permanent MAC address unavailable: %w", err)
	}
	if mac == "" {
		return "", fmt.Errorf("WiFi permanent MAC address unavailable: empty value")
	}

	serial, err := readSocSerial()
	if err != nil {
		return "", fmt.Errorf("SoC serial unavailable: %w", err)
	}
	if serial == "" {
		return "", fmt.Errorf("SoC serial unavailable: empty value")
	}

	slog.Info("UHWID: hardware identifiers available", "mac", mac, "serial", serial)

	// Composition is fixed as "<MAC>:<serial>" — must not change, the cloud
	// identity depends on it.
	input := mac + ":" + serial
	sum := sha256.Sum256([]byte(input))
	return hex.EncodeToString(sum[:]), nil
}
