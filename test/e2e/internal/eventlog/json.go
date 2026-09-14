// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// WriteJSON dumps the full result next to the human report.
//
// It exists for the failure you cannot reproduce locally: CI uploads the
// artifact, and the run can then be picked apart with jq instead of read as
// three hundred lines of text. Raw payloads are included, so a wrong byte is
// recoverable after the fact rather than requiring the run to happen again.
func (r Result) WriteJSON(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("eventlog: create artifact dir: %w", err)
	}
	data, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return fmt.Errorf("eventlog: marshal result: %w", err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0o644); err != nil {
		return fmt.Errorf("eventlog: write %s: %w", path, err)
	}
	return nil
}
