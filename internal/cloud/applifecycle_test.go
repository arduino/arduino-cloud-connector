// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package cloud

import (
	"context"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	appcli "github.com/arduino/arduino-cloud-connector/internal/app-cli"
	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
	"github.com/arduino/arduino-cloud-connector/internal/applifecycle"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/downloader/downloadertest"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// recordingCLI carries out any call at once and remembers each one.
type recordingCLI struct {
	mu      sync.Mutex
	started []string
	calls   []string
}

func (r *recordingCLI) record(call string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
}

func (r *recordingCLI) Apps(context.Context) ([]appcli.App, error) { return nil, nil }
func (r *recordingCLI) Stop(_ context.Context, appID string, _ appcli.ProgressFunc) error {
	r.record("stop " + appID)
	return nil
}
func (r *recordingCLI) SetDefault(_ context.Context, appID string) error {
	r.record("default " + appID)
	return nil
}
func (r *recordingCLI) Start(_ context.Context, appID string, _ appcli.ProgressFunc) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.started = append(r.started, appID)
	return nil
}

func succeeded(client *fakeClient, op string) bool {
	for _, c := range client.publishedCmds() {
		if r, ok := c.Inner().(command.AppCommandResponseCmd); ok &&
			r.OperationID == op && r.Status == command.AppCommandSucceeded {
			return true
		}
	}
	return false
}

// Like a deploy, a lifecycle command needs no thing: one arriving while the FSM waits
// for one is carried out, and the FSM stays where it is.
func TestFSM_AppCommandAcceptedWithoutAThing(t *testing.T) {
	client, fsm, _ := newTestFSMWithOTA(t, &downloadertest.FakeDownloader{}, appinstaller.Unavailable())
	cli := &recordingCLI{}
	fsm.apps = applifecycle.New(config.Config{InstallTimeout: 2 * time.Second}, fsm.deviceID, client, cli, nil)
	runFSM(t, fsm)

	waitFor(t, fsm, StateAwaitingThingID)
	keep := int64(0)
	client.injectCommand(command.NewCmd(command.AppCommandCmd{
		AppName: "blink:1.0.0", Command: command.AppStart, OperationID: "op-1", Parameter: &keep,
	}))

	require.Eventually(t, func() bool { return succeeded(client, "op-1") }, 5*time.Second, 5*time.Millisecond)
	cli.mu.Lock()
	require.Equal(t, []string{"blink:1.0.0"}, cli.started, "the app name is handed over verbatim as the id")
	cli.mu.Unlock()
	require.Equal(t, StateAwaitingThingID, fsm.Snapshot().State)
}

// Stop and set-default travel the same way: the Cloud's bytes, decoded, routed past
// the thing handshake, carried out, and answered under the operation id.
func TestFSM_EveryAppCommandIsRoutedFromTheCloudBytes(t *testing.T) {
	for _, tc := range []struct {
		name string
		hex  string // tag(0x13001) [ "blink:1.0.0", <command>, "op-1" ]
		call string
	}{
		{"stop", "da00013001 83 6b626c696e6b3a312e302e30 00 646f702d31", "stop blink:1.0.0"},
		{"set default", "da00013001 83 6b626c696e6b3a312e302e30 03 646f702d31", "default blink:1.0.0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := hex.DecodeString(strings.ReplaceAll(tc.hex, " ", ""))
			require.NoError(t, err)
			cmd, err := command.Decode(raw)
			require.NoError(t, err)

			client, fsm, _ := newTestFSMWithOTA(t, &downloadertest.FakeDownloader{}, appinstaller.Unavailable())
			cli := &recordingCLI{}
			fsm.apps = applifecycle.New(config.Config{InstallTimeout: 2 * time.Second}, fsm.deviceID, client, cli, nil)
			runFSM(t, fsm)
			waitFor(t, fsm, StateAwaitingThingID)

			client.injectCommand(cmd)
			require.Eventually(t, func() bool { return succeeded(client, "op-1") }, 5*time.Second, 5*time.Millisecond)
			cli.mu.Lock()
			require.Equal(t, []string{tc.call}, cli.calls)
			cli.mu.Unlock()
		})
	}
}
