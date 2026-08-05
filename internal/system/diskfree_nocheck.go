// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build mock || (!linux && !darwin)

package system

import (
	"log/slog"
	"math"
)

// DiskFreeBytes measures nothing in the two cases that share this file:
//
//   - a mock build, which runs on a demo host whose disk says nothing about the
//     target board;
//   - a platform without the statfs the real implementation needs — Windows, in
//     practice, since Linux is the production target and darwin covers a
//     developer's Mac. Covering each remaining OS would mean per-OS syscall code
//     that nothing in CI would ever exercise.
//
// The two conditions are unrelated but the right behaviour is the same, which is
// why one implementation serves both: report unlimited space so any capacity check
// passes, and warn so the absence of a check is on the record rather than silent.
// Refusing a download because the check is unavailable would be worse than letting
// the write fail with ENOSPC if the space really is missing.
func DiskFreeBytes(dir string) (int64, error) {
	slog.Warn("system: free-space check not performed, assuming there is enough", "dir", dir)
	return math.MaxInt64, nil
}
