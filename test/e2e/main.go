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
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

	names, err := discoverScenarios(opts.scenarios, opts.run)
	if err != nil {
		return err
	}
	if len(names) == 0 {
		return fmt.Errorf("no scenarios found in %s (filter %q)", opts.scenarios, opts.run)
	}

	// The scenario runner lands with internal/scenario and internal/steps.
	// Until then this reports honestly rather than exiting 0 on work it did
	// not do — an E2E tool that silently passes is worse than one that is
	// plainly unfinished.
	return fmt.Errorf("scenario runner not wired yet: %d scenario(s) discovered (%v), daemon=%s",
		len(names), names, opts.daemonBin)
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

// discoverScenarios lists the scenario files, optionally filtered by a
// substring of the file name.
func discoverScenarios(dir, filter string) ([]string, error) {
	entries, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, fmt.Errorf("scan %s: %w", dir, err)
	}
	var names []string
	for _, path := range entries {
		name := filepath.Base(path)
		name = name[:len(name)-len(filepath.Ext(name))]
		if filter != "" && !strings.Contains(name, filter) {
			continue
		}
		names = append(names, name)
	}
	return names, nil
}
