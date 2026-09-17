// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/scenario"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/steps"
)

// defaultDaemonBin is where `task build:mock` leaves the binary, relative to
// this module. Having a default is what makes `task test:e2e` work right after
// `task build:mock` without anyone exporting anything.
const defaultDaemonBin = "../../build/arduino-cloud-connector-mock"

// artifactsDir matches the -artifacts default of the binary and the path the
// CI workflow uploads.
const artifactsDir = "_artifacts"

// scenarioTimeout bounds one scenario. Every step already has its own timeout,
// so this only catches a step that is somehow not bounded at all -- it must be
// generous enough never to be the thing that fails a slow CI runner.
const scenarioTimeout = 5 * time.Minute

// TestScenarios runs every file under scenarios/ as its own subtest.
//
// This is the driver CI uses, and it is the same code path as the binary: both
// call runScenario, so the two cannot drift into asserting different things.
//
// It is gated on -short rather than on a build tag. The harness's own unit
// tests and this driver live in one module and CI runs `go test -short` on
// every PR to keep that fast; the scenarios then run in their own job without
// the flag. A build tag would have meant remembering to pass it in three
// places for the suite to run at all -- and a suite that silently does not run
// is worse than one that is plainly absent.
func TestScenarios(t *testing.T) {
	if testing.Short() {
		t.Skip("scenarios spawn the daemon binary; -short is for the harness's own unit tests")
	}

	bin := daemonBinary(t)
	scenarios, err := scenario.LoadDir("scenarios")
	if err != nil {
		t.Fatalf("loading scenarios: %v", err)
	}
	if len(scenarios) == 0 {
		t.Fatal("no scenarios found in scenarios/")
	}

	reg := steps.Default()
	for _, sc := range scenarios {
		// Validated up front, outside the subtest: an unknown step name is a
		// mistake in the file, not a failure of the run, and the message names
		// the whole vocabulary.
		if err := sc.Validate(reg); err != nil {
			t.Fatalf("%v", err)
		}
	}

	for _, sc := range scenarios {
		// Not parallel, on purpose: each scenario spawns a daemon and binds
		// four listeners, and interleaving two of those makes both timelines
		// unreadable for no gain while there are this few of them.
		t.Run(sc.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), scenarioTimeout)
			defer cancel()

			result := runScenario(ctx, bin, sc, reg)

			textPath, jsonPath, err := scenario.WriteArtifacts(artifactsDir, result)
			if err != nil {
				t.Errorf("writing artifacts: %v", err)
			}
			if !result.Failed() {
				// The step table even on a pass, for the same reason the
				// binary prints it: a count says nothing about what was
				// actually asserted. Visible under `go test -v`.
				t.Logf("\n%s\nreport %s", result.Summary(), textPath)
				return
			}
			// The whole report, not a summary. The step table, the field-level
			// diff and the timeline around the failure are the point of the
			// formatter, and `go test -v` output is where a developer looks
			// first.
			t.Errorf("\n%s\nreport %s\nevents %s", result.Format(), textPath, jsonPath)
		})
	}
}

// daemonBinary resolves the binary under test, and FAILS rather than skips
// when there is none.
//
// Skipping would be the friendlier choice locally and the wrong one in CI: a
// job whose env var was renamed would go green having tested nothing, which is
// the exact failure this suite exists to prevent. -short is the sanctioned way
// to not run the scenarios.
func daemonBinary(t *testing.T) string {
	t.Helper()
	bin := os.Getenv("E2E_DAEMON_BIN")
	if bin == "" {
		bin = defaultDaemonBin
	}
	if _, err := os.Stat(bin); err != nil {
		abs, _ := filepath.Abs(bin)
		t.Fatalf("daemon binary %s not found (%v).\n"+
			"Build it with `task build:mock` or point E2E_DAEMON_BIN at one.\n"+
			"The mock build tag is required: it is what makes the UHWID readable "+
			"off a dev machine and DeviceNetConfig deterministic.", abs, err)
	}
	return bin
}
