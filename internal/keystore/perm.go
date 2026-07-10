// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !mock

package keystore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// checkPrivKeyPermissions returns an error if the private key file is readable
// by anyone other than the owner.
//
// Production target (Debian/Ubuntu/Yocto): a leaked private key would let
// any local process impersonate the board to the Arduino IoT Cloud, so the
// daemon refuses to start with anything other than POSIX 0600.
//
// The mock build (perm_mock.go) replaces this with a no-op: developer
// machines run a mix of WSL (where the project may sit on /mnt/c and thus
// not honour POSIX modes at all), native Linux, and macOS, and the keys
// produced under -tags mock are throw-away synthetic material that the
// real broker would reject anyway.
func (ks *Keystore) checkPrivKeyPermissions(filename string) error {
	path := filepath.Join(ks.dataDir, filename)
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil // will be created by ensureKeyPair
	}
	if err != nil {
		return fmt.Errorf("keystore: stat %s: %w", filename, err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf(
			"keystore: %s has unsafe permissions %o — must be 0600 (owner-only)",
			filename, info.Mode().Perm(),
		)
	}
	return nil
}
