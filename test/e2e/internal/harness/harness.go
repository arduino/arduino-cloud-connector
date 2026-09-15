// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package harness wires the pieces together into a World: the CA, the three
// servers the daemon talks to, the app-role client, the daemon process itself
// and the scenario variable bag.
//
// World lives in its own package rather than in scenario, and that is not
// filing: steps need the World, scenario needs the step registry, so putting
// World in scenario would make the two import each other.
//
// # The environment is the whole configuration
//
// The daemon reads everything from ARDUINO_CLOUD_CONNECTOR__* variables, which
// is what makes the black-box approach possible at all: the harness points it
// at a fake provisioning API, a fake connectivity probe and its own broker
// without touching a line of production code. The names are written out below
// because if one is renamed the suite must break -- that is a regression in the
// daemon's own contract with its packaging and its operators.
package harness

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/appclient"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/daemonproc"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/broker"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/ntp"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/provapi"
)

// The daemon's configuration environment.
const (
	EnvPort            = "ARDUINO_CLOUD_CONNECTOR__PORT"
	EnvSocket          = "ARDUINO_CLOUD_CONNECTOR__SOCKET"
	EnvDataDir         = "ARDUINO_CLOUD_CONNECTOR__DATA_DIR"
	EnvMQTTBroker      = "ARDUINO_CLOUD_CONNECTOR__MQTT_BROKER"
	EnvMQTTCAFile      = "ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE"
	EnvProvisioningAPI = "ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API"
	EnvNTPProbeHost    = "ARDUINO_CLOUD_CONNECTOR__NTP_PROBE_HOST"
	EnvLogLevel        = "ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL"
)

// Defaults for a scenario that does not care.
const (
	defaultLogLevel           = "debug"
	defaultStatusPollInterval = 200 * time.Millisecond
	defaultReadyTimeout       = 30 * time.Second
	defaultStopTimeout        = 20 * time.Second
)

// Config is what a scenario run needs to know before anything starts.
type Config struct {
	// DaemonBinary is the built daemon to drive. Required to start the daemon;
	// the servers come up without it.
	DaemonBinary string
	// WorkDir holds the daemon data directory and the CA file. A temporary
	// directory is created when empty, and removed by Teardown.
	WorkDir string
	// DeviceID and ThingID are the identities the cloud assigns. Random when
	// empty. They are in the bag from the start, because a scenario refers to
	// them before the daemon has learned them.
	DeviceID string
	ThingID  string

	LogLevel           string
	StatusPollInterval time.Duration
	ReadyTimeout       time.Duration
}

// World is everything a step can touch.
type World struct {
	Log     *eventlog.Log
	CA      *pki.CA
	ProvAPI *provapi.Server
	NTP     *ntp.Server
	Broker  *broker.Server
	App     *appclient.Client
	Daemon  *daemonproc.Process
	Vars    *Bag

	DataDir   string
	CAFile    string
	DaemonURL string

	cfg        Config
	workDirOwn bool // the harness created it and must remove it
	stopPoller func()

	mu      sync.Mutex
	streams []*appclient.Stream
}

// AddStream hands a subscription to the World, which closes it at teardown.
//
// The stream outlives the step that opened it -- that is the point of
// subscribing -- so something has to own it. A leaked one would keep the daemon
// writing frames into a reader nobody reads.
func (w *World) AddStream(s *appclient.Stream) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.streams = append(w.streams, s)
}

// SetupServers brings up everything except the daemon: the CA, the fake
// provisioning API, the connectivity probe, the broker and the app client.
//
// Split from StartDaemon so the wiring can be exercised without a binary --
// and so a scenario could later point the same harness at a daemon running
// somewhere else.
func SetupServers(cfg Config) (*World, error) {
	cfg = withDefaults(cfg)

	workDir := cfg.WorkDir
	owned := false
	if workDir == "" {
		dir, err := os.MkdirTemp("", "acc-e2e-*")
		if err != nil {
			return nil, fmt.Errorf("harness: temp dir: %w", err)
		}
		workDir, owned = dir, true
	}

	w := &World{
		Log:        eventlog.New(),
		Vars:       NewBag(),
		cfg:        cfg,
		workDirOwn: owned,
		DataDir:    filepath.Join(workDir, "data"),
		CAFile:     filepath.Join(workDir, "ca.pem"),
	}
	if err := os.MkdirAll(w.DataDir, 0o755); err != nil {
		w.cleanupWorkDir(workDir)
		return nil, fmt.Errorf("harness: data dir: %w", err)
	}

	ca, err := pki.NewCA()
	if err != nil {
		w.cleanupWorkDir(workDir)
		return nil, err
	}
	w.CA = ca
	// The daemon pins the broker's root from this file, exactly as a board
	// does, so it has to exist before the daemon starts.
	if err := ca.WriteCertPEM(w.CAFile); err != nil {
		w.cleanupWorkDir(workDir)
		return nil, err
	}

	if w.ProvAPI, err = provapi.Start(provapi.Options{
		Log:      w.Log,
		CA:       ca,
		DeviceID: cfg.DeviceID,
	}); err != nil {
		w.cleanupWorkDir(workDir)
		return nil, err
	}
	if w.NTP, err = ntp.Start(w.Log); err != nil {
		_ = w.ProvAPI.Close()
		w.cleanupWorkDir(workDir)
		return nil, err
	}
	if w.Broker, err = broker.Start(broker.Options{Log: w.Log, CA: ca}); err != nil {
		_ = w.NTP.Close()
		_ = w.ProvAPI.Close()
		w.cleanupWorkDir(workDir)
		return nil, err
	}

	port, err := freePort()
	if err != nil {
		_ = w.closeServers()
		w.cleanupWorkDir(workDir)
		return nil, err
	}
	w.DaemonURL = fmt.Sprintf("http://127.0.0.1:%d", port)
	if w.App, err = appclient.New(w.Log, w.DaemonURL); err != nil {
		_ = w.closeServers()
		w.cleanupWorkDir(workDir)
		return nil, err
	}

	w.Vars.Set("device_id", w.ProvAPI.DeviceID())
	w.Vars.Set("thing_id", cfg.ThingID)
	w.Vars.Set("api_url", w.ProvAPI.URL())
	w.Vars.Set("broker_url", w.Broker.URL())
	w.Vars.Set("ntp_addr", w.NTP.Addr())
	w.Vars.Set("daemon_url", w.DaemonURL)
	w.Vars.Set("data_dir", w.DataDir)
	w.cfg.WorkDir = workDir
	return w, nil
}

// Env is the environment the daemon is started with. Exported because it is
// also what an operator needs to reproduce a run by hand.
func (w *World) Env() map[string]string {
	port := "0"
	if _, p, err := net.SplitHostPort(trimScheme(w.DaemonURL)); err == nil {
		port = p
	}
	return map[string]string{
		EnvPort:            port,
		EnvDataDir:         w.DataDir,
		EnvProvisioningAPI: w.ProvAPI.URL(),
		EnvMQTTBroker:      w.Broker.URL(),
		EnvMQTTCAFile:      w.CAFile,
		EnvNTPProbeHost:    w.NTP.Addr(),
		EnvLogLevel:        w.cfg.LogLevel,
		// The unix socket is pointed at the work directory rather than left at
		// its default: the default lives under /run, which the suite has no
		// business writing to, and binding it is non-fatal anyway.
		EnvSocket: filepath.Join(w.cfg.WorkDir, "daemon.sock"),
	}
}

// StartDaemon spawns the binary and waits for its REST listener.
func (w *World) StartDaemon(ctx context.Context) error {
	if w.cfg.DaemonBinary == "" {
		return errors.New("harness: no daemon binary configured")
	}
	proc, err := daemonproc.Start(daemonproc.Options{
		Log:        w.Log,
		BinaryPath: w.cfg.DaemonBinary,
		Env:        w.Env(),
	})
	if err != nil {
		return err
	}
	w.Daemon = proc

	if err := w.App.WaitReady(ctx, w.cfg.ReadyTimeout); err != nil {
		// The daemon's own log lines and its exit event are already in the log,
		// so the report explains this rather than just reporting a timeout.
		return err
	}
	w.stopPoller = w.App.StartStatusPoller(w.cfg.StatusPollInterval)
	return nil
}

// Setup brings up the servers and the daemon.
func Setup(ctx context.Context, cfg Config) (*World, error) {
	w, err := SetupServers(cfg)
	if err != nil {
		return nil, err
	}
	if err := w.StartDaemon(ctx); err != nil {
		_ = w.Teardown(ctx)
		return nil, err
	}
	return w, nil
}

// Teardown stops everything, in the order that keeps the log readable: the
// daemon first, so its shutdown lines land before the servers disappear from
// under it.
func (w *World) Teardown(ctx context.Context) error {
	var errs []error
	if w.stopPoller != nil {
		w.stopPoller()
		w.stopPoller = nil
	}
	// The SSE streams go before the daemon: closing them from this side is what
	// the app would do, and it keeps the daemon's shutdown lines free of
	// broken-pipe noise.
	w.mu.Lock()
	streams := w.streams
	w.streams = nil
	w.mu.Unlock()
	for _, s := range streams {
		s.Close()
	}
	if w.Daemon != nil && !w.Daemon.Exited() {
		stopCtx, cancel := context.WithTimeout(ctx, defaultStopTimeout)
		if _, err := w.Daemon.Stop(stopCtx); err != nil {
			// A daemon that will not go is killed rather than left behind: a
			// leaked process would hold the data directory and break the next
			// scenario for an unrelated reason.
			errs = append(errs, err)
			if kerr := w.Daemon.Kill(); kerr != nil {
				errs = append(errs, kerr)
			}
		}
		cancel()
	}
	if err := w.closeServers(); err != nil {
		errs = append(errs, err)
	}
	w.cleanupWorkDir(w.cfg.WorkDir)
	return errors.Join(errs...)
}

func (w *World) closeServers() error {
	var errs []error
	if w.Broker != nil {
		errs = append(errs, w.Broker.Close())
		w.Broker = nil
	}
	if w.NTP != nil {
		errs = append(errs, w.NTP.Close())
		w.NTP = nil
	}
	if w.ProvAPI != nil {
		errs = append(errs, w.ProvAPI.Close())
		w.ProvAPI = nil
	}
	return errors.Join(errs...)
}

func (w *World) cleanupWorkDir(dir string) {
	if !w.workDirOwn || dir == "" {
		return
	}
	// Best effort: a leftover temp directory is noise, a failed teardown that
	// masked the real failure is worse.
	_ = os.RemoveAll(dir)
}

func withDefaults(cfg Config) Config {
	if cfg.LogLevel == "" {
		cfg.LogLevel = defaultLogLevel
	}
	if cfg.StatusPollInterval == 0 {
		cfg.StatusPollInterval = defaultStatusPollInterval
	}
	if cfg.ReadyTimeout == 0 {
		cfg.ReadyTimeout = defaultReadyTimeout
	}
	if cfg.DeviceID == "" {
		cfg.DeviceID = RandomUUID()
	}
	if cfg.ThingID == "" {
		cfg.ThingID = RandomUUID()
	}
	return cfg
}

// freePort asks the kernel for an unused port and hands it back.
//
// There is a window between closing this listener and the daemon binding it,
// which is unavoidable without passing a file descriptor to another process.
// The alternative -- the daemon's default port -- is worse: it would make two
// scenarios in the same run collide, and collide flakily.
func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("harness: find a free port: %w", err)
	}
	defer ln.Close() //nolint:errcheck
	_, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		return 0, fmt.Errorf("harness: parse %s: %w", ln.Addr(), err)
	}
	n, err := strconv.Atoi(port)
	if err != nil {
		return 0, fmt.Errorf("harness: port %q: %w", port, err)
	}
	return n, nil
}

func trimScheme(url string) string {
	for _, prefix := range []string{"http://", "https://"} {
		if len(url) > len(prefix) && url[:len(prefix)] == prefix {
			return url[len(prefix):]
		}
	}
	return url
}

// RandomUUID formats a random version-4 UUID, which is the shape of both the
// device id and the thing id.
func RandomUUID() string {
	var b [16]byte
	if _, err := io.ReadFull(rand.Reader, b[:]); err != nil {
		// crypto/rand does not fail in practice, and a harness that cannot
		// generate an identity has nothing useful to do.
		panic(fmt.Sprintf("harness: generate uuid: %v", err))
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
