// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build mock

package keystore

import "log/slog"

// checkPrivKeyPermissions is a no-op in mock builds.
//
// The mock binary is dev-only and gets exercised on three host shapes:
// WSL (where data dirs under /mnt/c report a synthesized 0o777 because the
// 9p/drvfs mount does not propagate POSIX mode bits), macOS (POSIX, modes
// honoured), and native Linux (POSIX, modes honoured). The first case
// makes a numeric 0600 check non-deterministic across the team, and the
// keys generated here are synthetic anyway — the real Arduino IoT Cloud
// broker would reject the corresponding self-signed device cert regardless
// of how the local file system protects it.
//
// The production check (perm.go, built when the `mock` tag is absent) keeps
// the strict 0600 enforcement that protects the Debian/Ubuntu target.
func (ks *Keystore) checkPrivKeyPermissions(filename string) error {
	slog.Debug("keystore (mock): skipping POSIX-mode permission check",
		"file", filename,
		"reason", "mock build runs on heterogeneous dev hosts; keys here are synthetic")
	return nil
}
