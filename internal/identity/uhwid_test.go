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
	"errors"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// withStubReaders swaps the hardware-read seams (and the retry timing) for the
// duration of a test, restoring the originals afterwards.
func withStubReaders(t *testing.T, mac func(context.Context) (string, error), serial func() (string, error)) {
	t.Helper()
	origMAC, origSerial := readWifiMAC, readSocSerial
	origAttempts, origInterval := uhwidMaxAttempts, uhwidRetryInterval
	readWifiMAC, readSocSerial = mac, serial
	uhwidRetryInterval = time.Millisecond // keep tests fast
	t.Cleanup(func() {
		readWifiMAC, readSocSerial = origMAC, origSerial
		uhwidMaxAttempts, uhwidRetryInterval = origAttempts, origInterval
	})
}

func expectedUHWID(mac, serial string) string {
	sum := sha256.Sum256([]byte(mac + ":" + serial))
	return hex.EncodeToString(sum[:])
}

func TestComputeUHWIDBothPresent(t *testing.T) {
	const mac, serial = "AABBCCDDEEFF", "0123456789abcdef"
	withStubReaders(t,
		func(context.Context) (string, error) { return mac, nil },
		func() (string, error) { return serial, nil },
	)

	got, err := computeUHWID(config.Config{})
	if err != nil {
		t.Fatalf("computeUHWID: %v", err)
	}
	if want := expectedUHWID(mac, serial); got != want {
		t.Errorf("UHWID: got %q want %q", got, want)
	}
}

// The MAC is unavailable for the first two attempts (the boot-time WiFi probe
// race) then becomes readable; computeUHWID must retry and succeed.
func TestComputeUHWIDRetriesUntilMACReady(t *testing.T) {
	const mac, serial = "AABBCCDDEEFF", "0123456789abcdef"
	calls := 0
	withStubReaders(t,
		func(context.Context) (string, error) {
			calls++
			if calls < 3 {
				return "", errors.New("failed to read WiFi MAC address from any known path")
			}
			return mac, nil
		},
		func() (string, error) { return serial, nil },
	)

	got, err := computeUHWID(config.Config{})
	if err != nil {
		t.Fatalf("computeUHWID: %v", err)
	}
	if want := expectedUHWID(mac, serial); got != want {
		t.Errorf("UHWID: got %q want %q", got, want)
	}
	if calls != 3 {
		t.Errorf("WiFi MAC read attempts: got %d want 3", calls)
	}
}

// If the MAC never becomes available, computeUHWID must fail after exactly
// uhwidMaxAttempts — never returning a partial (serial-only) UHWID.
func TestComputeUHWIDFailsWhenMACNeverReady(t *testing.T) {
	calls := 0
	withStubReaders(t,
		func(context.Context) (string, error) {
			calls++
			return "", errors.New("failed to read WiFi MAC address from any known path")
		},
		func() (string, error) { return "0123456789abcdef", nil },
	)

	if _, err := computeUHWID(config.Config{}); err == nil {
		t.Fatal("expected an error when the WiFi MAC is never available")
	}
	if calls != uhwidMaxAttempts {
		t.Errorf("WiFi MAC read attempts: got %d want %d", calls, uhwidMaxAttempts)
	}
}

// A missing SoC serial is equally fatal — both identifiers are mandatory.
func TestComputeUHWIDFailsWhenSerialMissing(t *testing.T) {
	withStubReaders(t,
		func(context.Context) (string, error) { return "AABBCCDDEEFF", nil },
		func() (string, error) { return "", nil },
	)

	if _, err := computeUHWID(config.Config{}); err == nil {
		t.Fatal("expected an error when the SoC serial is empty")
	}
}
