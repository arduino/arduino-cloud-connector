// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"go.bug.st/cleanup"

	"github.com/arduino/arduino-cloud-connector/cmd/arduino-cloud-connector/daemon"
	"github.com/arduino/arduino-cloud-connector/cmd/arduino-cloud-connector/version"
	"github.com/arduino/arduino-cloud-connector/internal/config"
)

// Version is set at build time with -ldflags.
var Version string = "0.0.0-dev"

func main() {
	cfg, err := config.NewFromEnv()
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %s\n", err)
		os.Exit(1)
	}

	rootCmd := &cobra.Command{
		Use:   "arduino-cloud-connector",
		Short: "Arduino Cloud Connector — connects a Linux board to Arduino IoT Cloud",
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			// Log level comes solely from the environment
			// (ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL); unset defaults to info.
			level, err := parseLogLevel(cfg.LogLevel)
			if err != nil {
				return err
			}
			slog.SetLogLoggerLevel(level)
			return nil
		},
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	rootCmd.AddCommand(
		daemon.NewDaemonCmd(cfg, Version),
		version.NewVersionCmd(Version),
	)

	ctx := context.Background()

	// SIGINT (Ctrl-C) — interactive runs.
	ctx, _ = cleanup.InterruptableContext(ctx)

	// SIGTERM — what systemd sends on `systemctl stop`/`restart`, and the only
	// way the daemon is ever stopped on a board. The helper above registers
	// os.Interrupt only, so without this the root context is never cancelled:
	// the process is killed outright and every graceful-shutdown path hanging
	// off ctx (daemon FSM handlers, Cloud FSM teardown, srv.Shutdown) is dead
	// code in production. stop() restores the default disposition on return.
	ctx, stop := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer stop()

	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}

func parseLogLevel(level string) (slog.Level, error) {
	var l slog.Level
	if err := l.UnmarshalText([]byte(level)); err != nil {
		return 0, fmt.Errorf("invalid log level %q: %w", level, err)
	}
	return l, nil
}
