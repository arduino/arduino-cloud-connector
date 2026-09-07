// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build mock

package identity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// mockUHWIDFile is the persistence location for the synthetic UHWID. Lives
// under cfg.DataDir alongside the keystore so a `task clean` (or a docker
// volume reset) wipes it together with the rest of the dev state.
const mockUHWIDFile = "mock_uhwid"

// computeUHWID returns a deterministic-per-installation, random-on-first-run
// UHWID. The value is generated on first invocation and persisted to
// <DataDir>/mock_uhwid; subsequent runs read it back so that the App Lab
// front-end sees a stable board identity across daemon restarts.
//
// To rotate the identity (simulate a different board): delete the file.
//
// ctx is unused here — there is no hardware to wait for, so nothing to
// interrupt; it is present to match the default build's signature.
func computeUHWID(_ context.Context, cfg config.Config) (string, error) {
	if cfg.DataDir == "" {
		return "", errors.New("mock UHWID: DataDir is empty; cannot persist mock identity")
	}

	path := filepath.Join(cfg.DataDir, mockUHWIDFile)

	if data, err := os.ReadFile(path); err == nil {
		uhwid := strings.TrimSpace(string(data))
		if isValidUHWID(uhwid) {
			slog.Info("UHWID (mock): loaded from disk", "path", path)
			return uhwid, nil
		}
		slog.Warn("UHWID (mock): file present but malformed, regenerating",
			"path", path, "len", len(uhwid))
	} else if !os.IsNotExist(err) {
		return "", fmt.Errorf("mock UHWID: read %s: %w", path, err)
	}

	// First run (or file removed): generate a fresh value. We hash 32 random
	// bytes through SHA-256 so the on-wire shape (64-hex-char string) is
	// indistinguishable from a real hardware-derived UHWID — the front-end
	// can use the same validation regexes against both flavours.
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return "", fmt.Errorf("mock UHWID: read random seed: %w", err)
	}
	sum := sha256.Sum256(seed[:])
	uhwid := hex.EncodeToString(sum[:])

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return "", fmt.Errorf("mock UHWID: create data dir: %w", err)
	}
	if err := os.WriteFile(path, []byte(uhwid+"\n"), 0o644); err != nil {
		return "", fmt.Errorf("mock UHWID: write %s: %w", path, err)
	}
	slog.Warn("UHWID (mock): generated new synthetic identity; persisted to disk",
		"path", path,
		"hint", "delete the file to simulate a different board on next start")
	return uhwid, nil
}

// isValidUHWID checks that a string looks like a 64-char lowercase hex digest.
// We require the strict shape so a hand-edited file is rejected and we
// regenerate cleanly rather than propagating malformed data downstream.
func isValidUHWID(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}
