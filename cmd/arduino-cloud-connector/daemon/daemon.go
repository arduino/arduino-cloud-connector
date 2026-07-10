// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package daemon

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/arduino/arduino-cloud-connector/internal/api"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/daemon"
	"github.com/arduino/arduino-cloud-connector/internal/identity"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

func NewDaemonCmd(cfg config.Config, version string) *cobra.Command {
	return &cobra.Command{
		Use:   "daemon",
		Short: "Start the Arduino Cloud Connector",
		RunE: func(cmd *cobra.Command, args []string) error {
			return run(cmd, cfg, version)
		},
	}
}

func run(cmd *cobra.Command, cfg config.Config, version string) error {
	ctx := cmd.Context()
	cfg.Version = version
	slog.Info("Starting Arduino Cloud Connector", "version", version)

	// ── Subsystem initialisation ──────────────────────────────────────────
	ks, err := keystore.New(cfg)
	if err != nil {
		return fmt.Errorf("keystore: %w", err)
	}

	idSvc, err := identity.New(cfg, ks)
	if err != nil {
		return fmt.Errorf("identity: %w", err)
	}

	varRegistry := variables.NewRegistry()
	provSvc := provisioning.New(cfg, idSvc, ks)
	mqttClient := mqtt.NewPahoClient(cfg, ks)

	// ── Daemon FSM ────────────────────────────────────────────────────────
	// Owns the Cloud FSM (spawned inside StateRun) and supervises the
	// daemon's overall lifecycle: CheckInternet → Provisioning → Run.
	daemonFSM := daemon.New(cfg, ks, varRegistry, provSvc, mqttClient)

	// ── REST API ──────────────────────────────────────────────────────────
	// The same handler is served on two listeners: a 127.0.0.1 TCP socket (used
	// by App Lab / wails on the host) and an optional UNIX-domain socket (used
	// by app containers, bind-mounted in — keeps the API off every network
	// interface). A single http.Server can serve multiple listeners; Shutdown
	// closes them all.
	handler := api.NewRouter(cfg, version, idSvc, provSvc, varRegistry, daemonFSM)
	addr := fmt.Sprintf("127.0.0.1:%d", cfg.Port)
	srv := &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	// Drive the daemon FSM in a goroutine. Returns on ctx cancellation, after
	// a clean shutdown of the Cloud FSM (if running).
	go daemonFSM.Run(ctx)

	// Shut down HTTP server when context is cancelled; wait for the daemon FSM
	// to finish its graceful shutdown before returning.
	go func() {
		<-ctx.Done()
		_ = srv.Shutdown(ctx) //nolint:contextcheck
	}()

	// Optional UNIX-domain-socket listener, served concurrently. A failure to
	// bind it is logged but non-fatal: the TCP listener (and thus the FE) keeps
	// working, and the cause is visible in the logs.
	if unixListener := newUnixListener(cfg.Socket); unixListener != nil {
		go func() {
			slog.Info("REST API listening (unix socket)", "socket", cfg.Socket)
			if serr := srv.Serve(unixListener); serr != nil && serr != http.ErrServerClosed {
				slog.Error("unix socket server stopped", "error", serr)
			}
		}()
	}

	slog.Info("REST API listening", "addr", addr)
	err = srv.ListenAndServe()
	if err != nil && err != http.ErrServerClosed {
		return fmt.Errorf("http server: %w", err)
	}

	// Wait for the daemon FSM (and its child Cloud FSM) to shut down cleanly.
	<-daemonFSM.Done()
	return nil
}

// newUnixListener creates the UNIX-domain-socket listener for the REST API, or
// returns nil (logging the reason) if disabled or it cannot be created. A stale
// socket file from a previous run is removed first (net.Listen fails if the
// path exists), and the socket is chmod'd 0660 so only the owner/group — the
// daemon user and the bind-mounting app container running as the same user —
// can reach it.
func newUnixListener(socketPath string) net.Listener {
	if socketPath == "" {
		return nil // explicitly disabled
	}
	if err := os.MkdirAll(filepath.Dir(socketPath), 0o755); err != nil {
		slog.Error("unix socket: cannot create parent directory", "socket", socketPath, "error", err)
		return nil
	}
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		slog.Error("unix socket: cannot remove stale socket", "socket", socketPath, "error", err)
		return nil
	}
	l, err := net.Listen("unix", socketPath)
	if err != nil {
		slog.Error("unix socket: cannot listen", "socket", socketPath, "error", err)
		return nil
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		slog.Error("unix socket: cannot set permissions", "socket", socketPath, "error", err)
		_ = l.Close()
		return nil
	}
	return l
}
