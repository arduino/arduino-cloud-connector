// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Command e2e drives the arduino-cloud-connector daemon through its external
// boundaries and checks the scenarios under scenarios/.
//
// It stands up a fake Provisioning API (HTTP), a connectivity probe responder
// (UDP/NTP), and an MQTT broker with mutual TLS; then it spawns the real
// daemon binary pointed at all three and asserts on what crosses those
// boundaries. The daemon is never linked in — see .golangci.yml for why that
// is enforced rather than merely intended.
//
// The same scenarios also run under `go test` (see e2e_test.go), which is what
// CI uses: that gives one subtest per scenario, -run filtering and JUnit
// output for free. This binary exists for the cases `go test` serves badly —
// running the harness against a daemon on a real board or a staging host,
// where the operator wants a plain command and an exit code.
//
// Usage:
//
//	e2e -daemon ../../build/arduino-cloud-connector-mock
//	e2e -daemon <path> -run full-lifecycle -artifacts /tmp/e2e
//
// Please, read the README.md file for further information.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/harness"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/scenario"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/steps"
)

// Version is overridden at build time with -ldflags.
var Version = "0.0.0-dev"

type options struct {
	daemonBin string
	scenarios string
	artifacts string
	run       string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	opts, showVersion, err := parseFlags()
	if err != nil {
		return err
	}
	if showVersion {
		fmt.Printf("e2e %s\n", Version)
		return nil
	}

	all, err := scenario.LoadDir(opts.scenarios)
	if err != nil {
		return err
	}
	selected := filter(all, opts.run)
	if len(selected) == 0 {
		return fmt.Errorf("no scenarios found in %s (filter %q)", opts.scenarios, opts.run)
	}

	// Every selected scenario is validated before the first one runs. A typo in
	// the second file must not be discovered after the first has spawned a
	// daemon and spent thirty seconds of somebody's time.
	reg := steps.Default()
	for _, sc := range selected {
		if err := sc.Validate(reg); err != nil {
			return err
		}
	}

	// Ctrl-C cancels the run rather than killing it: the deferred teardown is
	// what stops the daemon and removes its data directory, and skipping it
	// leaves a process holding that directory behind.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var failed []string
	for _, sc := range selected {
		result := runScenario(ctx, opts.daemonBin, sc, reg)

		textPath, jsonPath, werr := scenario.WriteArtifacts(opts.artifacts, result)
		if werr != nil {
			// Worth reporting but not worth losing the result over: the report
			// is on stdout either way.
			fmt.Fprintf(os.Stderr, "e2e: %v\n", werr)
		}

		if result.Failed() {
			failed = append(failed, sc.Name)
			// The whole report, on a failure: the operator running this binary
			// by hand has no CI artifact browser to open.
			fmt.Print(result.Format())
		} else {
			fmt.Printf("scenario %s — PASS (%d steps, %d events)\n",
				result.Scenario, len(result.Steps), len(result.Events))
		}
		if textPath != "" {
			fmt.Printf("  report %s\n  events %s\n", textPath, jsonPath)
		}

		if ctx.Err() != nil {
			return fmt.Errorf("interrupted after %s", sc.Name)
		}
	}

	if len(failed) > 0 {
		return fmt.Errorf("%d of %d scenario(s) failed: %s",
			len(failed), len(selected), strings.Join(failed, ", "))
	}
	fmt.Printf("all %d scenario(s) passed\n", len(selected))
	return nil
}

// runScenario brings up a World of its own for one scenario and runs it.
//
// A fresh World per scenario, not one shared: each gets its own data directory,
// its own device identity and its own event log, so a scenario cannot pass
// because of state an earlier one left behind.
//
// The servers and the daemon are started in two steps rather than through
// harness.Setup, because Setup tears the World down on a failed start and the
// event log goes with it. That log is the only thing that explains a daemon
// which would not come up: its own stderr lines and its exit event are in
// there. Keeping the World alive turns "start_daemon: timeout" into a report
// with the daemon's last words in it.
func runScenario(ctx context.Context, daemonBin string, sc scenario.Scenario, reg steps.Registry) eventlog.Result {
	w, err := harness.SetupServers(harness.Config{DaemonBinary: daemonBin})
	if err != nil {
		// No World means no log and no artifact; this is the one failure that
		// can only be an error string.
		return eventlog.Result{
			Scenario: sc.Name,
			Steps: []eventlog.StepResult{{
				Index: 1, Name: "setup_servers", Status: eventlog.StepFailed,
				Err: err, ErrText: err.Error(),
			}},
		}
	}
	defer func() {
		if terr := w.Teardown(context.WithoutCancel(ctx)); terr != nil {
			fmt.Fprintf(os.Stderr, "e2e: teardown %s: %v\n", sc.Name, terr)
		}
	}()

	if err := w.StartDaemon(ctx); err != nil {
		return w.Log.Result(sc.Name, []eventlog.StepResult{{
			Index: 1, Name: "start_daemon", Status: eventlog.StepFailed, Err: err,
		}}, nil)
	}
	return scenario.Run(ctx, w, sc, reg)
}

func parseFlags() (options, bool, error) {
	var (
		opts        options
		showVersion bool
	)
	// E2E_DAEMON_BIN is the default so the CI job and the Taskfile can set it
	// once in the environment instead of threading it through every command.
	flag.StringVar(&opts.daemonBin, "daemon", os.Getenv("E2E_DAEMON_BIN"),
		"path to the arduino-cloud-connector binary under test (env: E2E_DAEMON_BIN)")
	flag.StringVar(&opts.scenarios, "scenarios", "scenarios", "directory holding scenario YAML files")
	flag.StringVar(&opts.artifacts, "artifacts", "_artifacts", "directory for timelines and JSON dumps")
	flag.StringVar(&opts.run, "run", "", "only run scenarios whose name contains this substring")
	flag.BoolVar(&showVersion, "version", false, "print the harness version and exit")
	flag.Parse()

	if showVersion {
		return opts, true, nil
	}
	if opts.daemonBin == "" {
		return opts, false, fmt.Errorf("no daemon binary given: pass -daemon or set E2E_DAEMON_BIN")
	}
	if _, err := os.Stat(opts.daemonBin); err != nil {
		return opts, false, fmt.Errorf("daemon binary %s: %w", opts.daemonBin, err)
	}
	return opts, false, nil
}

// filter keeps the scenarios whose name contains the substring, which is the
// same shape as `go test -run` without promising regexp semantics it does not
// have.
func filter(scenarios []scenario.Scenario, substring string) []scenario.Scenario {
	if substring == "" {
		return scenarios
	}
	out := make([]scenario.Scenario, 0, len(scenarios))
	for _, sc := range scenarios {
		if strings.Contains(sc.Name, substring) {
			out = append(out, sc)
		}
	}
	return out
}
