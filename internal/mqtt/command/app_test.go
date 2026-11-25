// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package command_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// The RFC-16 vectors below are written out by hand from the tables in RFC-16 §3, not
// produced by this package: there is no C++ reference for these messages yet.

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	require.NoError(t, err)
	return b
}

func TestDecode_AppCommandCmd_WithParameter(t *testing.T) {
	// DA 00013001                 # tag(77825)
	//    84                       # array(4)
	//       6A 617070313A302E312E30   # "app1:0.1.0"
	//       01                    # 1 start
	//       64 6F702D31           # "op-1"
	//       00                    # parameter 0
	cmd, err := command.Decode(mustHex(t, "da00013001 84 6a617070313a302e312e30 01 646f702d31 00"))
	require.NoError(t, err)
	got, ok := cmd.Inner().(command.AppCommandCmd)
	require.True(t, ok, "decoded %T", cmd.Inner())
	require.Equal(t, "app1:0.1.0", got.AppName)
	require.Equal(t, command.AppStart, got.Command)
	require.Equal(t, "op-1", got.OperationID)
	require.NotNil(t, got.Parameter)
	require.Equal(t, int64(0), *got.Parameter)
}

// The parameter is optional: a three-element array is a command without one, which
// is not the same as a parameter of 0.
func TestDecode_AppCommandCmd_WithoutParameter(t *testing.T) {
	cmd, err := command.Decode(mustHex(t, "da00013001 83 6a617070313a302e312e30 01 646f702d31"))
	require.NoError(t, err)
	got := cmd.Inner().(command.AppCommandCmd)
	require.Equal(t, command.AppStart, got.Command)
	require.Nil(t, got.Parameter)
}

// One message carries every command: the second element says which.
func TestDecode_AppCommandCmd_EveryCommand(t *testing.T) {
	for code, want := range map[string]command.AppCommand{
		"00": command.AppStop, "01": command.AppStart, "02": command.AppDelete, "03": command.AppSetDefault,
	} {
		t.Run(want.String(), func(t *testing.T) {
			cmd, err := command.Decode(mustHex(t, "da00013001 83 6a617070313a302e312e30 "+code+" 646f702d31"))
			require.NoError(t, err)
			got := cmd.Inner().(command.AppCommandCmd)
			require.Equal(t, want, got.Command)
			require.Equal(t, "app1:0.1.0", got.AppName)
			require.Equal(t, "op-1", got.OperationID)
		})
	}
}

func TestDecode_AppCommandCmd_RejectsMalformed(t *testing.T) {
	for name, payload := range map[string]string{
		"two elements":         "da00013001 82 6a617070313a302e312e30 01",
		"five elements":        "da00013001 85 6a617070313a302e312e30 01 646f702d31 00 00",
		"number for the name":  "da00013001 83 01 01 646f702d31",
		"text for the command": "da00013001 83 6a617070313a302e312e30 6131 646f702d31",
		"not an array":         "da00013001 6131",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := command.Decode(mustHex(t, payload))
			require.Error(t, err)
		})
	}
}

func TestEncode_AppCommandResponseCmd(t *testing.T) {
	percent, code := int32(50), int32(-2)
	for name, tc := range map[string]struct {
		cmd  command.AppCommandResponseCmd
		want string
	}{
		// DA 00013002, array(2) [ "op-1", 0 ] — no details on success.
		"succeeded": {command.AppCommandResponseCmd{OperationID: "op-1", Status: command.AppCommandSucceeded},
			"da00013002 82 646f702d31 00"},
		// array(3) [ "op-1", 2, 50 ]
		"in progress": {command.AppCommandResponseCmd{OperationID: "op-1", Status: command.AppCommandInProgress, Details: &percent},
			"da00013002 83 646f702d31 02 1832"},
		// array(3) [ "op-1", 1, -2 ]
		"failed": {command.AppCommandResponseCmd{OperationID: "op-1", Status: command.AppCommandFailed, Details: &code},
			"da00013002 83 646f702d31 01 21"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := command.Encode(command.From(tc.cmd))
			require.NoError(t, err)
			require.Equal(t, mustHex(t, tc.want), got)
		})
	}
}

// The test helper that injects a Cloud command must produce the Cloud's bytes.
func TestEncode_AppCommandCmd_MatchesTheCloudBytes(t *testing.T) {
	p := int64(1)
	got, err := command.Encode(command.NewCmd(command.AppCommandCmd{
		AppName: "app1:0.1.0", Command: command.AppStart, OperationID: "op-1", Parameter: &p,
	}))
	require.NoError(t, err)
	require.Equal(t, mustHex(t, "da00013001 84 6a617070313a302e312e30 01 646f702d31 01"), got)
}

func TestEncode_AppsListCmd(t *testing.T) {
	for name, tc := range map[string]struct {
		cmd  command.AppsListCmd
		want string
	}{
		// DA 00013000                     # tag(77824)
		//    86                           # array(6): two Apps × 3
		//       6A 617070313A302E312E30   # "app1:0.1.0"
		//       01                        # 1 running
		//       F5                        # true, default
		//       6A 617070323A312E302E30   # "app2:1.0.0"
		//       20                        # -1 failure
		//       F4                        # false
		"two apps": {command.AppsListCmd{Apps: []command.AppInfo{
			{Name: "app1:0.1.0", Status: command.AppRunning, Default: true},
			{Name: "app2:1.0.0", Status: command.AppFailure},
		}}, "da00013000 86 6a617070313a302e312e30 01 f5 6a617070323a312e302e30 20 f4"},
		// array(0): a board with no Apps.
		"no apps": {command.AppsListCmd{}, "da00013000 80"},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := command.Encode(command.From(tc.cmd))
			require.NoError(t, err)
			require.Equal(t, mustHex(t, tc.want), got)
		})
	}
}

func TestDecode_AppsListCmd_RoundTripsAndRejectsAPartialApp(t *testing.T) {
	cmd, err := command.Decode(mustHex(t, "da00013000 83 6a617070313a302e312e30 00 f4"))
	require.NoError(t, err)
	require.Equal(t, command.AppsListCmd{Apps: []command.AppInfo{
		{Name: "app1:0.1.0", Status: command.AppStopped},
	}}, cmd.Inner())

	_, err = command.Decode(mustHex(t, "da00013000 82 6a617070313a302e312e30 00"))
	require.Error(t, err)
}
