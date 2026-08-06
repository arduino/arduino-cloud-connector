// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Tests for how the Cloud FSM hosts the App-deploy (OTA) process.
//
// The property under test throughout: a deploy job is valid from the moment the
// board reaches the broker, with or without a thing attached. The job and its
// progress travel on the device-keyed command topics, so the thing handshake is
// irrelevant to it — and a board that is never assigned a thing must still be
// deployable.
//
// These tests deliberately build the FSM with newFSM instead of New, so they need
// no keystore and therefore run on every platform (New enforces POSIX 0600 on
// private keys, which Windows cannot represent — see newTestFSM's skip).

package cloud

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/downloader/downloadertest"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
	"github.com/arduino/arduino-cloud-connector/internal/ota"
	"github.com/arduino/arduino-cloud-connector/internal/senml"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// ── harness ──────────────────────────────────────────────────────────────────

// blockingStorage is a storage client whose download hangs until release is
// closed, so a test can assert on behaviour while a transfer is provably in
// flight. No HTTP is involved: how the bytes are actually fetched is
// internal/storage-api's business and is tested there.
func blockingStorage(release <-chan struct{}) *downloadertest.FakeDownloader {
	return &downloadertest.FakeDownloader{Block: release}
}

// newTestFSMWithOTA wires a Cloud FSM to a fakeClient and a real OTA process
// backed by the given storage client.
func newTestFSMWithOTA(t *testing.T, storage *downloadertest.FakeDownloader, inst appinstaller.Installer) (*fakeClient, *FSM, *ota.OTAFSM) {
	t.Helper()

	const deviceID = "test-device-id"
	cfg := config.Config{
		DataDir:         t.TempDir(),
		AppDownloadDir:  t.TempDir(),
		MaxBundleSize:   1 << 30,
		DownloadTimeout: 30 * time.Second,
		InstallTimeout:  2 * time.Second,
		Version:         "test",
	}

	client := &fakeClient{}
	f := newFSM(cfg, deviceID, variables.NewRegistry(), client)
	// Keep the published-command log free of host-dependent noise.
	f.netConfig = nil
	f.timers = timers{
		connBackoffBase: 5 * time.Millisecond,
		// Long enough that Thing.begin retries do not flood the log while a test
		// deliberately sits in AwaitingThingID.
		thingBeginRetry:   2 * time.Second,
		lastValuesTimeout: 2 * time.Second,
	}

	otaFSM := ota.New(cfg, deviceID, client, storage, inst)
	f.ota = otaFSM

	return client, f, otaFSM
}

func waitForOTAState(t *testing.T, p *ota.OTAFSM, want ota.State) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if p.Snapshot().State == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for OTA state %s — last observed: %s", want, p.Snapshot().State)
}

func deployJob(id byte) command.Cmd {
	return command.NewCmd(command.OTAUpdateCmd{
		ID:  [16]byte{id},
		URL: "https://api2.arduino.cc/apps/owner/app-uuid/bundle.zip",
	})
}

func publishedOTABegins(client *fakeClient) int {
	n := 0
	for _, c := range client.publishedCmds() {
		if _, ok := c.Inner().(command.OTABeginCmd); ok {
			n++
		}
	}
	return n
}

// publishedDeviceBegins counts Device.begin announcements, i.e. completed
// connection cycles. A stable signal to wait on after forcing a disconnect —
// unlike StateReconnecting, which the FSM passes through too fast to poll for.
func publishedDeviceBegins(client *fakeClient) int {
	n := 0
	for _, c := range client.publishedCmds() {
		if _, ok := c.Inner().(command.DeviceBeginCmd); ok {
			n++
		}
	}
	return n
}

// ── tests ────────────────────────────────────────────────────────────────────

func TestFSM_AppDeployAcceptedWithoutAThing(t *testing.T) {
	// The core requirement. The FSM parks in AwaitingThingID for as long as no
	// thing is assigned — potentially forever — and a deploy job arriving there
	// must be executed, not dropped.
	release := make(chan struct{})
	defer close(release)
	client, fsm, otaFSM := newTestFSMWithOTA(t, blockingStorage(release), appinstaller.Unavailable())
	runFSM(t, fsm)

	waitFor(t, fsm, StateAwaitingThingID)
	client.injectCommand(deployJob(1))

	// It got all the way to the transfer with no thing in sight.
	waitForOTAState(t, otaFSM, ota.StateFetch)
	require.Equal(t, StateAwaitingThingID, fsm.Snapshot().State,
		"the FSM must stay in AwaitingThingID while the deploy runs")
}

func TestFSM_AppDeployAcceptedWhileSyncingLastValues(t *testing.T) {
	// The remaining post-connection state. A job arriving in the sync window used
	// to be dropped on the floor.
	release := make(chan struct{})
	defer close(release)
	client, fsm, otaFSM := newTestFSMWithOTA(t, blockingStorage(release), appinstaller.Unavailable())
	runFSM(t, fsm)

	waitFor(t, fsm, StateAwaitingThingID)
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: testThingID}))
	waitFor(t, fsm, StateSyncingLastValues)

	client.injectCommand(deployJob(2))
	waitForOTAState(t, otaFSM, ota.StateFetch)
	require.Equal(t, StateSyncingLastValues, fsm.Snapshot().State)
}

func TestFSM_AppDeployAcceptedInSteady(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	client, fsm, otaFSM := newTestFSMWithOTA(t, blockingStorage(release), appinstaller.Unavailable())
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	client.injectCommand(deployJob(3))
	waitForOTAState(t, otaFSM, ota.StateFetch)
	require.Equal(t, StateSteady, fsm.Snapshot().State)
}

// TestFSM_NoAppDigestAnnouncedOnConnection is the FSM-level half of the decision
// that OTABeginCmd is not a connection-time announcement.
//
// A Linux board can hold and run several Apps, some installed by hand from App Lab,
// so there is no single installed digest for the board to announce. Connecting — and
// reconnecting — must therefore publish no OTABegin at all: the only one that ever
// goes out carries a bundle this daemon has just installed.
func TestFSM_NoAppDigestAnnouncedOnConnection(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	client, fsm, _ := newTestFSMWithOTA(t, blockingStorage(release), appinstaller.Unavailable())
	runFSM(t, fsm)

	waitFor(t, fsm, StateAwaitingThingID)
	time.Sleep(100 * time.Millisecond)
	require.Zero(t, publishedOTABegins(client),
		"connecting published an OTABegin, but no App has been installed by this daemon")

	// A full reconnect cycle is not an occasion to announce anything either.
	client.dropConnection(errors.New("broker went away"))
	require.Eventually(t, func() bool { return publishedDeviceBegins(client) >= 2 },
		3*time.Second, 5*time.Millisecond, "the FSM did not reconnect")

	time.Sleep(100 * time.Millisecond)
	require.Zero(t, publishedOTABegins(client), "a reconnect published an OTABegin")
}

// TestFSM_VariablesKeepFlowingDuringAppDownload answers the question directly: a
// deploy in progress must not stall Cloud Variables.
//
// It holds because the OTA process runs on its own goroutine and deliverOTA is a
// non-blocking hand-off, so the FSM's event loop never waits on the download. The
// outbound direction (app PUT → broker) is not exercised here because it does not
// live in this FSM at all: internal/daemon.runOutbound is a separate goroutine
// reading its own queue, structurally untouched by a deploy. The only resource the
// two paths share is the Paho client's mutex, whose publishes are bounded by
// brokerAckTimeout.
func TestFSM_VariablesKeepFlowingDuringAppDownload(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	client, fsm, otaFSM := newTestFSMWithOTA(t, blockingStorage(release), appinstaller.Unavailable())
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	client.injectCommand(deployJob(4))
	waitForOTAState(t, otaFSM, ota.StateFetch) // the transfer is provably in flight

	// 1. An inbound cloud property update is still decoded and stored.
	payload, err := senml.Encode([]senml.Variable{{Name: "led", Value: true}})
	require.NoError(t, err)
	client.injectProperty(payload)

	require.Eventually(t, func() bool {
		v, err := fsm.reg.Get("led")
		return err == nil && v.Value == true
	}, 3*time.Second, 5*time.Millisecond,
		"an inbound variable update was not applied while a bundle was downloading")

	// 2. The command channel is still being serviced: a thing reassignment is
	//    handled in-place, which requires the dispatcher to be running.
	const otherThing command.ThingID = "00000000-0000-0000-0000-000000000002"
	client.injectCommand(command.NewCmd(command.ThingUpdateCmd{ThingID: otherThing}))
	require.Eventually(t, func() bool { return fsm.Snapshot().ThingID == otherThing.String() },
		3*time.Second, 5*time.Millisecond,
		"the FSM stopped servicing commands while a bundle was downloading")

	// 3. A second inbound update after that, to show the loop is still live rather
	//    than having drained one queued event.
	payload2, err := senml.Encode([]senml.Variable{{Name: "level", Value: 42.0}})
	require.NoError(t, err)
	client.injectProperty(payload2)
	require.Eventually(t, func() bool {
		v, err := fsm.reg.Get("level")
		return err == nil && v.Value == 42.0
	}, 3*time.Second, 5*time.Millisecond)

	// 4. And the download was never disturbed by any of it.
	require.Equal(t, ota.StateFetch, otaFSM.Snapshot().State)
	require.Equal(t, StateSteady, fsm.Snapshot().State)
}

func TestFSM_AppDownloadSurvivesBrokerDisconnect(t *testing.T) {
	// Losing MQTT must not cost the transfer: publishing is muted, the download
	// continues. Same as the C++ OTA FSM, which from Idle onwards runs
	// "independently from the mqttClient".
	release := make(chan struct{})
	defer close(release)
	client, fsm, otaFSM := newTestFSMWithOTA(t, blockingStorage(release), appinstaller.Unavailable())
	runFSM(t, fsm)
	driveToSteady(t, client, fsm)

	client.injectCommand(deployJob(5))
	waitForOTAState(t, otaFSM, ota.StateFetch)

	client.dropConnection(errors.New("broker went away"))
	// Wait for a whole reconnection cycle to complete, evidenced by a second
	// Device.begin. StateReconnecting itself is not worth polling for: the FSM
	// passes through it in microseconds under test timers.
	require.Eventually(t, func() bool { return publishedDeviceBegins(client) >= 2 },
		3*time.Second, 5*time.Millisecond, "the FSM did not reconnect")

	// The transfer was never disturbed by the disconnect or the reconnect.
	require.Equal(t, ota.StateFetch, otaFSM.Snapshot().State,
		"the download was abandoned when the broker dropped")
}
