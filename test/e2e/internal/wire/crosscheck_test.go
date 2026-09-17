// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// The cross-check between the harness's wire codec and the daemon's.
//
// Together with internal/pki/crosscheck_test.go this is the only place in the
// harness allowed to import production code (see the depguard exemption in
// test/e2e/.golangci.yml): comparing two implementations is something only a
// file that sees both can do.
//
// The two directions are checked separately because that is how the harness
// uses them. Uplink commands are ENCODED by the daemon and decoded here, so
// the daemon's encoder is the input. Downlink commands are encoded here and
// DECODED by the daemon, so the daemon's decoder is the judge. SenML goes both
// ways. Everything else in the harness never touches the daemon's codec, which
// is what keeps a mistake shared by both sides from cancelling out.
package wire_test

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
	"github.com/arduino/arduino-cloud-connector/internal/senml"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

// encodeWithDaemon runs the daemon's own encoder, the way the FSM does when it
// publishes.
func encodeWithDaemon(t *testing.T, cmd command.Cmd) []byte {
	t.Helper()
	payload, err := command.Encode(cmd)
	if err != nil {
		t.Fatalf("the daemon could not encode %v: %v", cmd, err)
	}
	return payload
}

// Uplink: the daemon publishes, the harness reads. A field the harness gets
// wrong here is a scenario that can never match, so every command the daemon
// sends during the handshake is covered.
func TestHarnessDecodesWhatTheDaemonPublishes(t *testing.T) {
	tests := []struct {
		name       string
		cmd        command.Cmd
		wantName   string
		wantTag    uint64
		wantFields map[string]any
	}{
		{
			name:       "Device.begin",
			cmd:        command.From(command.DeviceBeginCmd{LibVersion: "0.0.0-e2e"}),
			wantName:   wire.CmdDeviceBegin,
			wantTag:    wire.TagDeviceBegin,
			wantFields: map[string]any{"lib_version": "0.0.0-e2e"},
		},
		{
			name:       "Thing.begin with no thing yet",
			cmd:        command.From(command.ThingBeginCmd{ThingID: ""}),
			wantName:   wire.CmdThingBegin,
			wantTag:    wire.TagThingBegin,
			wantFields: map[string]any{"thing_id": ""},
		},
		{
			name:       "Thing.begin with a thing",
			cmd:        command.From(command.ThingBeginCmd{ThingID: "thing-1"}),
			wantName:   wire.CmdThingBegin,
			wantTag:    wire.TagThingBegin,
			wantFields: map[string]any{"thing_id": "thing-1"},
		},
		{
			name:       "LastValues.begin",
			cmd:        command.From(command.LastValuesBeginCmd{}),
			wantName:   wire.CmdLastValuesBegin,
			wantTag:    wire.TagLastValuesBegin,
			wantFields: map[string]any{},
		},
		{
			name:       "Timezone.request",
			cmd:        command.From(command.TimezoneRequestCmd{}),
			wantName:   wire.CmdTimezoneRequest,
			wantTag:    wire.TagTimezoneRequest,
			wantFields: map[string]any{},
		},
		{
			// The shape a -tags mock build reports, which is what CI runs.
			name: "DeviceNetConfig over WiFi",
			cmd: command.From(command.DeviceNetConfigCmd{
				Type: command.NetworkTypeWiFi,
				SSID: "SSIDTEST1",
			}),
			wantName:   wire.CmdDeviceNetConfig,
			wantTag:    wire.TagDeviceNetConfig,
			wantFields: map[string]any{"network_type": "wifi", "ssid": "SSIDTEST1"},
		},
		{
			// Unset addresses go out as empty byte strings, not null: that is
			// the firmware's way of saying DHCP, and the daemon has a dedicated
			// encoder mode for it.
			name: "DeviceNetConfig over Ethernet with unset addresses",
			cmd: command.From(command.DeviceNetConfigCmd{
				Type: command.NetworkTypeEthernet,
				IP:   []byte{10, 0, 0, 1},
			}),
			wantName: wire.CmdDeviceNetConfig,
			wantTag:  wire.TagDeviceNetConfig,
			wantFields: map[string]any{
				"network_type": "ethernet",
				"ip":           "0a000001",
				"dns":          "",
				"gateway":      "",
				"netmask":      "",
			},
		},
		{
			name:       "DeviceNetConfig over a type with no extra fields",
			cmd:        command.From(command.DeviceNetConfigCmd{Type: command.NetworkTypeCellular}),
			wantName:   wire.CmdDeviceNetConfig,
			wantTag:    wire.TagDeviceNetConfig,
			wantFields: map[string]any{"network_type": "cellular"},
		},
		{
			name:       "OTA.begin",
			cmd:        command.From(command.OTABeginCmd{SHA256: [32]byte{0xaa, 0xbb}}),
			wantName:   wire.CmdOTABegin,
			wantTag:    wire.TagOTABegin,
			wantFields: map[string]any{"sha256": "aabb" + strings.Repeat("00", 30)},
		},
		{
			name: "OTA.progress",
			cmd: command.From(command.OTAProgressCmd{
				ID: [16]byte{1}, State: 2, StateData: -3, Timestamp: 4,
			}),
			wantName: wire.CmdOTAProgress,
			wantTag:  wire.TagOTAProgress,
			wantFields: map[string]any{
				"id":         "01" + strings.Repeat("00", 15),
				"state":      int64(2),
				"state_data": int64(-3),
				"timestamp":  int64(4),
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := wire.DecodeCommand(encodeWithDaemon(t, tc.cmd))
			if err != nil {
				t.Fatalf("the harness could not decode what the daemon encoded: %v", err)
			}
			if got.Name != tc.wantName {
				t.Errorf("name = %q, want %q", got.Name, tc.wantName)
			}
			if got.Tag != tc.wantTag {
				t.Errorf("tag = %#x, want %#x -- the two sides disagree on the tag number", got.Tag, tc.wantTag)
			}
			if !reflect.DeepEqual(got.Fields, tc.wantFields) {
				t.Errorf("fields =\n  %#v\nwant\n  %#v", got.Fields, tc.wantFields)
			}
		})
	}
}

// Downlink: the harness publishes, the daemon reads. Here the daemon's decoder
// is the judge, so a wrong tag or a wrong field order shows up as a decode
// error or a zero field rather than as a scenario that hangs.
func TestDaemonDecodesWhatTheHarnessPublishes(t *testing.T) {
	values, err := wire.EncodeSenML([]wire.Value{{Name: "temp", Value: 21.5}})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}

	tests := []struct {
		name    string
		payload []byte
		want    any
	}{
		{
			name:    "Thing.update",
			payload: wire.EncodeThingUpdate("thing-1"),
			want:    command.ThingUpdateCmd{ThingID: "thing-1"},
		},
		{
			name:    "Thing.update with no thing attached",
			payload: wire.EncodeThingUpdate(""),
			want:    command.ThingUpdateCmd{ThingID: ""},
		},
		{
			name:    "Thing.detach",
			payload: wire.EncodeThingDetach("thing-1"),
			want:    command.ThingDetachCmd{ThingID: "thing-1"},
		},
		{
			name:    "LastValues.update",
			payload: wire.EncodeLastValuesUpdate(values),
			want:    command.LastValuesUpdateCmd{Values: values},
		},
		{
			name:    "Timezone.update",
			payload: wire.EncodeTimezoneUpdate(-3600, 42),
			want:    command.TimezoneUpdateCmd{Offset: -3600, Until: 42},
		},
		{
			name: "OTA.update",
			payload: wire.EncodeOTAUpdate([]byte{1}, "https://example.invalid/fw.bin",
				make([]byte, 32), make([]byte, 32)),
			want: command.OTAUpdateCmd{
				ID:         [16]byte{1},
				URL:        "https://example.invalid/fw.bin",
				InitialSHA: [32]byte{},
				FinalSHA:   [32]byte{},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := command.Decode(tc.payload)
			if err != nil {
				t.Fatalf("the daemon could not decode what the harness encoded (%x): %v", tc.payload, err)
			}
			if got := cmd.Inner(); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("the daemon read\n  %#v\nwant\n  %#v", got, tc.want)
			}
		})
	}
}

// The tag number is not decoration: an unregistered one must be refused by the
// daemon. Without this, a harness that wrote the wrong tag would look like a
// daemon that ignores commands.
func TestTheDaemonRefusesAnUnknownTag(t *testing.T) {
	// One past Thing.update.
	payload := wire.EncodeThingUpdate("thing-1")
	payload[4]++ // the low byte of the four-byte tag argument

	if cmd, err := command.Decode(payload); err == nil {
		t.Errorf("the daemon accepted tag 0x10401 as %v", cmd)
	}
}

// SenML, daemon to harness: what the daemon publishes on the property topic
// must be readable here, including the base time its encoder factors out.
func TestHarnessDecodesTheDaemonsSenML(t *testing.T) {
	stamp := time.Unix(1726300000, 0).UTC()
	vars := []senml.Variable{
		{Name: "temp", Type: senml.TypeFloat, Value: 21.5, Timestamp: stamp},
		{Name: "count", Type: senml.TypeInt, Value: int64(7), Timestamp: stamp},
		{Name: "label", Type: senml.TypeString, Value: "on", Timestamp: stamp},
		{Name: "switch", Type: senml.TypeBool, Value: true, Timestamp: stamp},
	}

	payload, err := senml.Encode(vars)
	if err != nil {
		t.Fatalf("the daemon could not encode: %v", err)
	}
	got, err := wire.DecodeSenML(payload)
	if err != nil {
		t.Fatalf("the harness could not decode the daemon's SenML: %v", err)
	}

	want := map[string]any{"temp": 21.5, "count": int64(7), "label": "on", "switch": true}
	if len(got) != len(want) {
		t.Fatalf("decoded %d values, want %d: %#v", len(got), len(want), got)
	}
	for _, v := range got {
		if w, ok := want[v.Name]; !ok {
			t.Errorf("unexpected value %q", v.Name)
		} else if !reflect.DeepEqual(v.Value, w) {
			t.Errorf("%q = %#v (%T), want %#v (%T)", v.Name, v.Value, v.Value, w, w)
		}
		// The daemon factors the shared timestamp into a base time on the first
		// record; resolving it must give the original back.
		if v.Time != float64(stamp.Unix()) {
			t.Errorf("%q time = %v, want %v (the base time was not resolved)", v.Name, v.Time, float64(stamp.Unix()))
		}
	}
}

// SenML, harness to daemon: the payload a scenario injects, read by the
// daemon's decoder, with the value types it will apply to its registry.
func TestDaemonDecodesTheHarnessSenML(t *testing.T) {
	payload, err := wire.EncodeSenML([]wire.Value{
		{Name: "temp", Value: 21.5},
		{Name: "count", Value: int64(7)},
		{Name: "label", Value: "on"},
		{Name: "switch", Value: true},
		{Name: "stamped", Value: 1.25, Time: 1726300000},
	})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}

	vars, err := senml.Decode(payload)
	if err != nil {
		t.Fatalf("the daemon could not decode the harness SenML: %v", err)
	}

	type typed struct {
		t senml.ValueType
		v any
	}
	want := map[string]typed{
		"temp":    {senml.TypeFloat, 21.5},
		"count":   {senml.TypeInt, int64(7)},
		"label":   {senml.TypeString, "on"},
		"switch":  {senml.TypeBool, true},
		"stamped": {senml.TypeFloat, 1.25},
	}
	if len(vars) != len(want) {
		t.Fatalf("the daemon decoded %d variables, want %d: %#v", len(vars), len(want), vars)
	}
	for _, v := range vars {
		w, ok := want[v.Name]
		if !ok {
			t.Errorf("unexpected variable %q", v.Name)
			continue
		}
		if v.Type != w.t {
			t.Errorf("%q type = %q, want %q", v.Name, v.Type, w.t)
		}
		if !reflect.DeepEqual(v.Value, w.v) {
			t.Errorf("%q = %#v (%T), want %#v (%T)", v.Name, v.Value, v.Value, w.v, w.v)
		}
	}
	// The one timestamp survives the harness encoder and the daemon's
	// unit-guessing reader.
	for _, v := range vars {
		if v.Name != "stamped" {
			continue
		}
		if got, want := v.Timestamp.Unix(), int64(1726300000); got != want {
			t.Errorf("stamped timestamp = %d, want %d", got, want)
		}
	}
}

// An empty SenML batch is what the cloud sends for a thing with no properties;
// the daemon must read it as "no variables" rather than as an error.
func TestDaemonAcceptsAnEmptyHarnessBatch(t *testing.T) {
	payload, err := wire.EncodeSenML(nil)
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	vars, err := senml.Decode(payload)
	if err != nil {
		t.Fatalf("the daemon rejected an empty batch: %v", err)
	}
	if len(vars) != 0 {
		t.Errorf("got %#v, want no variables", vars)
	}
}
