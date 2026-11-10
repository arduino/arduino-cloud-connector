// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

//go:build !mock && (linux || darwin)

package system

import (
	"fmt"
	"syscall"
)

// DiskFreeBytes reports the space available to an unprivileged writer in the
// filesystem holding dir. Bavail (not Bfree) is the right field: it excludes the
// reserved blocks only root may consume, and the daemon runs as the `arduino`
// user.
//
// darwin shares this implementation only so the check works on a developer's Mac:
// its statfs exposes the same Bavail/Bsize pair with the same meaning as the
// Linux target's.
func DiskFreeBytes(dir string) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Statfs(dir, &st); err != nil {
		return 0, fmt.Errorf("system: statfs %s: %w", dir, err)
	}
	return int64(st.Bavail) * int64(st.Bsize), nil
}
