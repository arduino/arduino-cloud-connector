// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"context"
	"crypto/x509"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// SetupServers is the whole wiring, minus the binary: if any of this is wrong
// the daemon would start against the real cloud, or against nothing.
func TestSetupServersWiresEverything(t *testing.T) {
	w, err := SetupServers(Config{})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	defer func() {
		if err := w.Teardown(context.Background()); err != nil {
			t.Errorf("Teardown: %v", err)
		}
	}()

	if w.Log == nil || w.CA == nil || w.ProvAPI == nil || w.NTP == nil || w.Broker == nil || w.App == nil {
		t.Fatalf("a component is missing: %+v", w)
	}
	if w.Daemon != nil {
		t.Error("SetupServers started a daemon")
	}

	// The CA file is what the daemon pins the broker's root from, exactly as a
	// board does, so it has to be on disk and loadable before anything starts.
	pemBytes, err := os.ReadFile(w.CAFile)
	if err != nil {
		t.Fatalf("read the CA file: %v", err)
	}
	if !x509.NewCertPool().AppendCertsFromPEM(pemBytes) {
		t.Error("the written CA file is not a usable root")
	}
	if _, err := os.Stat(w.DataDir); err != nil {
		t.Errorf("data dir: %v", err)
	}
}

// The environment IS the configuration: every one of these has to be set, or
// the daemon quietly falls back to a production default -- the real
// provisioning API, the real broker, time.arduino.cc.
func TestEnvCoversEveryDaemonSetting(t *testing.T) {
	w, err := SetupServers(Config{LogLevel: "debug"})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	defer w.Teardown(context.Background()) //nolint:errcheck

	env := w.Env()
	for _, key := range []string{
		EnvPort, EnvDataDir, EnvProvisioningAPI, EnvMQTTBroker,
		EnvMQTTCAFile, EnvNTPProbeHost, EnvLogLevel, EnvSocket,
	} {
		if env[key] == "" {
			t.Errorf("%s is not set", key)
		}
	}
	if got := env[EnvProvisioningAPI]; got != w.ProvAPI.URL() {
		t.Errorf("%s = %q, want the fake API", EnvProvisioningAPI, got)
	}
	if got := env[EnvMQTTBroker]; got != w.Broker.URL() || !strings.HasPrefix(got, "mqtts://") {
		t.Errorf("%s = %q, want the harness broker over mqtts", EnvMQTTBroker, got)
	}
	if got := env[EnvNTPProbeHost]; got != w.NTP.Addr() {
		t.Errorf("%s = %q, want the fake probe", EnvNTPProbeHost, got)
	}
	if got := env[EnvMQTTCAFile]; got != w.CAFile {
		t.Errorf("%s = %q, want the written CA", EnvMQTTCAFile, got)
	}
	// The port must be the one the app client is pointed at, or the harness
	// would talk to a daemon that is listening somewhere else.
	if !strings.HasSuffix(w.DaemonURL, ":"+env[EnvPort]) {
		t.Errorf("port %q does not match the daemon URL %q", env[EnvPort], w.DaemonURL)
	}
	// The socket must not be the production default under /run.
	if strings.HasPrefix(env[EnvSocket], "/run") {
		t.Errorf("%s = %q, want a path inside the work directory", EnvSocket, env[EnvSocket])
	}
}

// The bag is seeded before the first step runs, because a scenario refers to
// the device and thing ids long before the daemon has learned them.
func TestSetupSeedsTheBag(t *testing.T) {
	w, err := SetupServers(Config{DeviceID: "9f1c2d3e-4567-89ab-cdef-0123456789ab", ThingID: "thing-1"})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	defer w.Teardown(context.Background()) //nolint:errcheck

	for key, want := range map[string]string{
		"device_id":  "9f1c2d3e-4567-89ab-cdef-0123456789ab",
		"thing_id":   "thing-1",
		"api_url":    w.ProvAPI.URL(),
		"broker_url": w.Broker.URL(),
		"ntp_addr":   w.NTP.Addr(),
		"daemon_url": w.DaemonURL,
		"data_dir":   w.DataDir,
	} {
		got, ok := w.Vars.Get(key)
		if !ok {
			t.Errorf("the bag has no %q (it holds %v)", key, w.Vars.Keys())
			continue
		}
		if got != want {
			t.Errorf("bag[%q] = %q, want %q", key, got, want)
		}
	}
	// The device id the fake will actually assign must be the one in the bag,
	// or an expectation on it can never match.
	if got := w.ProvAPI.DeviceID(); got != w.Vars.MustGet("device_id") {
		t.Errorf("the fake assigns %q while the bag says %q", got, w.Vars.MustGet("device_id"))
	}
}

func TestDefaultIdentitiesAreGenerated(t *testing.T) {
	w, err := SetupServers(Config{})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	defer w.Teardown(context.Background()) //nolint:errcheck

	// The daemon rejects a device id that is not 36 characters, so a generated
	// one has to be a canonical UUID.
	if got := w.Vars.MustGet("device_id"); len(got) != 36 {
		t.Errorf("generated device id %q is %d characters, want 36", got, len(got))
	}
	if got := w.Vars.MustGet("thing_id"); len(got) != 36 {
		t.Errorf("generated thing id %q is %d characters, want 36", got, len(got))
	}
	if w.Vars.MustGet("device_id") == w.Vars.MustGet("thing_id") {
		t.Error("the device and thing ids are the same value")
	}
}

// Teardown has to clean up after itself: a leaked temp directory is noise, but
// a leaked listener would make the next scenario fail for an unrelated reason.
func TestTeardownRemovesWhatItCreated(t *testing.T) {
	w, err := SetupServers(Config{})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	workDir := filepath.Dir(w.DataDir)
	brokerAddr := w.Broker.Addr()

	if err := w.Teardown(context.Background()); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if _, err := os.Stat(workDir); !os.IsNotExist(err) {
		t.Errorf("the work directory survived teardown: %v", err)
	}
	if err := dialClosed(brokerAddr); err != nil {
		t.Errorf("the broker is still listening: %v", err)
	}
	// Twice must be safe: a scenario that failed during setup tears down on
	// the way out, and the deferred teardown runs again.
	if err := w.Teardown(context.Background()); err != nil {
		t.Errorf("second Teardown: %v", err)
	}
}

// A caller-supplied work directory is left alone: it is usually the operator's
// own, and deleting it would be a surprise.
func TestAGivenWorkDirIsNotRemoved(t *testing.T) {
	dir := t.TempDir()
	w, err := SetupServers(Config{WorkDir: dir})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	if err := w.Teardown(context.Background()); err != nil {
		t.Fatalf("Teardown: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Errorf("the given work directory was removed: %v", err)
	}
}

func TestStartDaemonNeedsABinary(t *testing.T) {
	w, err := SetupServers(Config{})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	defer w.Teardown(context.Background()) //nolint:errcheck

	if err := w.StartDaemon(context.Background()); err == nil {
		t.Error("StartDaemon succeeded with no binary configured")
	}
}

func TestRandomUUIDShape(t *testing.T) {
	seen := map[string]bool{}
	for range 100 {
		id := RandomUUID()
		if len(id) != 36 {
			t.Fatalf("%q is %d characters, want 36", id, len(id))
		}
		if strings.Count(id, "-") != 4 {
			t.Fatalf("%q is not canonical", id)
		}
		if seen[id] {
			t.Fatalf("%q was generated twice", id)
		}
		seen[id] = true
	}
}

// dialClosed reports nil when nothing is listening on addr any more.
func dialClosed(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return nil
	}
	_ = conn.Close()
	return fmt.Errorf("%s still accepts connections", addr)
}
