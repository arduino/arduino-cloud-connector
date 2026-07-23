// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Tests live in package cloud (white-box) so that individual test cases can
// install testTimers on the FSM to accelerate back-off delays.

package cloud

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
	"github.com/arduino/arduino-cloud-connector/internal/senml"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// ── fake MQTT client ──────────────────────────────────────────────────────────
//
// fakeClient is a controllable in-process test double for mqtt.Client.
//
// Tests push inbound broker traffic via injectCommand.
// Outbound traffic (commands published by the FSM) is captured in published
// and can be inspected with publishedCmds().

type fakeClient struct {
	mu sync.Mutex

	connected  bool
	connLostFn func(error)
	cmdFn      func(mqtt.CommandMessage)
	propFn     func(mqtt.PropertyMessage)

	published []command.Cmd
}

func (c *fakeClient) OnConnectionLost(fn func(error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connLostFn = fn
}

func (c *fakeClient) Connect(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = true
	return nil
}

func (c *fakeClient) Disconnect() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = false
}

func (c *fakeClient) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

func (c *fakeClient) SubscribeCommandChannel(_ string, fn func(mqtt.CommandMessage)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cmdFn = fn
	return nil
}

func (c *fakeClient) SubscribePropertyTopic(_ string, fn func(mqtt.PropertyMessage)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.propFn = fn
	return nil
}

func (c *fakeClient) UnsubscribePropertyTopic(_ string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.propFn = nil
	return nil
}

func (c *fakeClient) PublishCommand(_ string, cmd command.Cmd) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.published = append(c.published, cmd)
	return nil
}

func (c *fakeClient) PublishProperty(_ string, _ []byte) error {
	return nil
}

// injectCommand delivers a downlink command to the FSM as if the broker sent it.
func (c *fakeClient) injectCommand(cmd command.Cmd) {
	c.mu.Lock()
	fn := c.cmdFn
	c.mu.Unlock()
	if fn != nil {
		fn(mqtt.CommandMessage{Cmd: cmd})
	}
}

// dropConnection simulates an unexpected broker disconnect.
func (c *fakeClient) dropConnection(cause error) {
	c.mu.Lock()
	c.connected = false
	fn := c.connLostFn
	c.mu.Unlock()
	if fn != nil {
		fn(cause)
	}
}

// publishedCmds returns a snapshot of every command the FSM has published so far.
func (c *fakeClient) publishedCmds() []command.Cmd {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]command.Cmd, len(c.published))
	copy(out, c.published)
	return out
}

// ── test setup ────────────────────────────────────────────────────────────────

// newTestFSM constructs a Cloud FSM wired to a fakeClient and a temp keystore
// that already has a device_id. All back-off and timeout durations are
// overridden to millisecond values so tests run in near-zero wall time.
func newTestFSM(t *testing.T) (*fakeClient, *FSM) {
	t.Helper()

	if runtime.GOOS == "windows" {
		// keystore.New enforces POSIX 0600 on private keys, which Windows
		// cannot represent; the daemon targets Linux. Run this on Linux/macOS.
		t.Skip("keystore POSIX-permission model is not representable on Windows")
	}

	dir := t.TempDir()
	cfg := config.Config{DataDir: dir, Version: "test"}

	ks, err := keystore.New(cfg)
	require.NoError(t, err)
	require.NoError(t, ks.StoreDeviceID("test-device-id"))

	client := &fakeClient{}
	reg := variables.NewRegistry()

	fsm, err := New(cfg, ks, reg, client)
	require.NoError(t, err)

	// Accelerate all back-off timers so tests complete quickly.
	// connBackoffBase is set to 50 ms (not 5 ms) so that StateReconnecting
	// is observable via 5 ms polling before the FSM moves on.
	fsm.timers = timers{
		connBackoffBase:   50 * time.Millisecond,
		thingBeginRetry:   5 * time.Millisecond,
		lastValuesTimeout: 50 * time.Millisecond,
	}

	return client, fsm
}

// runFSM starts the FSM in a background goroutine and registers a t.Cleanup
// that cancels the context and waits for a clean exit. The returned cancel
// function can be used in tests that want to trigger shutdown explicitly.
func runFSM(t *testing.T, fsm *FSM) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		select {
		case <-fsm.Done():
		case <-time.After(3 * time.Second):
			t.Error("FSM did not shut down within 3 seconds after context cancellation")
		}
	})
	go fsm.Run(ctx)
	return cancel
}

// waitFor polls fsm.Snapshot().State every 5 ms until it reaches want or a
// 2-second deadline expires. It is the primary synchronisation primitive in
// these tests — the FSM runs in a goroutine and updates state asynchronously.
func waitFor(t *testing.T, fsm *FSM, want State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fsm.Snapshot().State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for state %q — last observed: %q", want, fsm.Snapshot().State)
}

// driveToSteady drives a freshly started FSM all the way to StateSteady by
// injecting the minimal set of cloud messages required by the handshake.
//
// It waits for StateAwaitingThingID (not StateAnnouncingDevice) as the first
// synchronisation point: AnnouncingDevice is transitory — it has no blocking
// select loop and transitions too quickly for polling to catch reliably.
// By the time the FSM enters AwaitingThingID, runAnnouncingDevice has
// fully completed (cmdFn is registered, DeviceBeginCmd has been published).
func driveToSteady(t *testing.T, client *fakeClient, fsm *FSM) {
	t.Helper()
	waitFor(t, fsm, StateAwaitingThingID)
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: testThingID}))
	waitFor(t, fsm, StateSyncingLastValues)
	client.injectCommand(command.NewCmd(command.LastValuesUpdateCmd{}))
	waitFor(t, fsm, StateSteady)
}

const testThingID command.ThingID = "00000000-0000-0000-0000-000000000001"

// ── tests ─────────────────────────────────────────────────────────────────────

// TestFSM_HappyPath_FullHandshake verifies the complete Device.begin →
// Thing.begin → LastValues.begin handshake that runs on every broker connect.
// It checks both state transitions and the commands published at each step.
// Security regression: variable values exchanged with the cloud must never be
// written to the logs (possible user PII). On the last-values sync path
// (logEach=true) applyProperties may log the property NAME, but never its value.
func TestApplyProperties_NeverLogsVariableValue(t *testing.T) {
	var logBuf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	defer slog.SetDefault(prev)

	const sentinel = "PII-SENTINEL-VALUE"
	payload, err := senml.Encode([]senml.Variable{{Name: "temp", Value: sentinel}})
	require.NoError(t, err)

	reg := variables.NewRegistry()
	f := newFSM(config.Config{}, "dev-id", reg, &fakeClient{})

	// logEach=true is the last-values sync path that historically logged v.Value.
	require.NoError(t, f.applyProperties(payload, true))

	out := logBuf.String()
	require.NotContains(t, out, sentinel, "variable value must never appear in the logs")
	require.Contains(t, out, "temp", "the property name may still be logged")

	// The value is still stored in the registry — only the logging changed.
	got, err := reg.Get("temp")
	require.NoError(t, err)
	require.Equal(t, sentinel, got.Value)
}

func TestFSM_HappyPath_FullHandshake(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)

	// ── AnnouncingDevice → AwaitingThingID ──────────────────────────────────
	// AnnouncingDevice is transitory (no select loop) and cannot be reliably
	// observed by polling.  We wait for AwaitingThingID — the first stable
	// state — instead.  By the time the FSM arrives here, runAnnouncingDevice
	// has fully completed: the command channel is subscribed and DeviceBeginCmd
	// has been published.
	waitFor(t, fsm, StateAwaitingThingID)

	// DeviceBeginCmd is always the first command published (in runAnnouncingDevice,
	// before the state transition to AwaitingThingID). By the time waitFor returns
	// the FSM may have already published one or more ThingBeginCmd retries, so we
	// only assert on the first element rather than the total count.
	cmds := client.publishedCmds()
	require.NotEmpty(t, cmds, "FSM must have published at least Device.begin")
	begin, ok := cmds[0].Inner().(command.DeviceBeginCmd)
	require.True(t, ok, "first published command should be DeviceBeginCmd, got %T", cmds[0].Inner())
	require.Equal(t, "test", begin.LibVersion)

	// ThingBeginCmd is published inside runAwaitingThingID, just *after* the
	// state transition, so it may arrive a few µs after waitFor returns.
	// Use require.Eventually so we don't take a fragile early snapshot.
	require.Eventually(t, func() bool {
		for _, cmd := range client.publishedCmds() {
			if tb, ok := cmd.Inner().(command.ThingBeginCmd); ok && tb.ThingID == "" {
				return true
			}
		}
		return false
	}, 2*time.Second, 5*time.Millisecond,
		"FSM must publish ThingBeginCmd{ThingID:\"\"} once in AwaitingThingID")

	// Cloud assigns a thing_id.
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: testThingID}))

	// ── SyncingLastValues ────────────────────────────────────────────────────
	// FSM subscribes to the property topic, sends LastValues.begin, and waits.
	waitFor(t, fsm, StateSyncingLastValues)
	require.Equal(t, string(testThingID), fsm.Snapshot().ThingID)

	// Cloud delivers the last-known property values.
	client.injectCommand(command.NewCmd(command.LastValuesUpdateCmd{}))

	// ── Steady ───────────────────────────────────────────────────────────────
	waitFor(t, fsm, StateSteady)

	snap := fsm.Snapshot()
	require.Equal(t, StateSteady, snap.State)
	require.Equal(t, string(testThingID), snap.ThingID)
}

// TestFSM_Steady_ThingDetach verifies that a ThingDetachCmd received in steady
// state moves the FSM back to AwaitingThingID and clears the thing_id.
func TestFSM_Steady_ThingDetach(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	client.injectCommand(command.NewCmd(command.ThingDetachCmd{ThingID: testThingID}))

	waitFor(t, fsm, StateAwaitingThingID)
	require.Equal(t, "", fsm.Snapshot().ThingID, "thing_id must be cleared after detach")
}

// TestFSM_Steady_ThingReassignment verifies that when the cloud assigns a new
// thing_id in steady state, the FSM stays in Steady (no broker reconnect) but
// updates the Snapshot and resubscribes to the new thing's property topic.
func TestFSM_Steady_ThingReassignment(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	const newThingID command.ThingID = "99999999-9999-9999-9999-999999999999"
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: newThingID}))

	// The FSM must stay in Steady and update the thing_id in place.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		snap := fsm.Snapshot()
		if snap.State == StateSteady && snap.ThingID == string(newThingID) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	snap := fsm.Snapshot()
	t.Fatalf("want Steady + thing_id=%q, got state=%q thing_id=%q", newThingID, snap.State, snap.ThingID)
}

// TestFSM_Steady_ThingUpdateEmpty verifies that a ThingUpdateCmd with an empty
// thing_id in steady state is treated as an implicit detach: the FSM moves to
// AwaitingThingID even without an explicit ThingDetachCmd.
func TestFSM_Steady_ThingUpdateEmpty(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: ""}))

	waitFor(t, fsm, StateAwaitingThingID)
	require.Equal(t, "", fsm.Snapshot().ThingID)
}

// TestFSM_Steady_BrokerDisconnect verifies that an unexpected connection loss
// in steady state triggers autonomous reconnection: the FSM enters Reconnecting
// and then re-runs the full handshake without any external intervention.
func TestFSM_Steady_BrokerDisconnect(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	client.dropConnection(errors.New("network timeout"))

	// FSM should move to Reconnecting immediately.
	waitFor(t, fsm, StateReconnecting)
	// After the (50 ms test) back-off it completes the re-handshake.
	// We wait for AwaitingThingID rather than AnnouncingDevice because
	// AnnouncingDevice is transitory and cannot be reliably polled.
	waitFor(t, fsm, StateAwaitingThingID)
}

// TestFSM_Syncing_LastValuesTimeout verifies that when the cloud does not
// deliver a LastValuesUpdateCmd within the timeout, the FSM proceeds to Steady
// anyway. This prevents a board from getting permanently stuck on first connect.
func TestFSM_Syncing_LastValuesTimeout(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)

	waitFor(t, fsm, StateAwaitingThingID)
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: "thing-timeout"}))
	waitFor(t, fsm, StateSyncingLastValues)

	// Do NOT inject LastValuesUpdateCmd — let the 50 ms test timeout expire.
	waitFor(t, fsm, StateSteady)
	require.Equal(t, "thing-timeout", fsm.Snapshot().ThingID)
}

// TestFSM_Syncing_ThingDetach verifies that a ThingDetachCmd received while
// waiting for last values aborts the sync and returns to AwaitingThingID.
func TestFSM_Syncing_ThingDetach(t *testing.T) {
	client, fsm := newTestFSM(t)
	runFSM(t, fsm)

	waitFor(t, fsm, StateAwaitingThingID)
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: "thing-detach"}))
	waitFor(t, fsm, StateSyncingLastValues)

	client.injectCommand(command.NewCmd(command.ThingDetachCmd{ThingID: "thing-detach"}))

	waitFor(t, fsm, StateAwaitingThingID)
	require.Equal(t, "", fsm.Snapshot().ThingID)
}

// TestFSM_GracefulShutdown verifies that cancelling the context while in
// steady state causes the FSM to transition to Disconnected and close its
// Done channel, allowing the caller to detect clean exit.
func TestFSM_GracefulShutdown(t *testing.T) {
	client, fsm := newTestFSM(t)
	cancel := runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	cancel()

	select {
	case <-fsm.Done():
		// Clean exit — expected.
	case <-time.After(3 * time.Second):
		t.Fatal("FSM did not shut down after context cancellation")
	}

	require.Equal(t, StateDisconnected, fsm.Snapshot().State)
}
