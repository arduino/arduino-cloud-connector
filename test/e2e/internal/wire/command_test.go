// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package wire

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

// Golden bytes assembled by hand from the tag number and the CBOR rules, so
// the encoders are pinned to something outside both implementations. The tag
// arguments are all above 65535, which is what forces the four-byte form
// (0xda): getting that wrong would change every command on the wire.
func TestCommandGoldenBytes(t *testing.T) {
	tests := []struct {
		name string
		got  []byte
		want string
	}{
		{
			// tag(0x10400) [ "abc" ]
			name: "thing update",
			got:  EncodeThingUpdate("abc"),
			want: "da000104008163616263",
		},
		{
			// tag(0x11000) [ "abc" ]
			name: "thing detach",
			got:  EncodeThingDetach("abc"),
			want: "da000110008163616263",
		},
		{
			// tag(0x10600) [ h'0102' ]
			name: "last values update",
			got:  EncodeLastValuesUpdate([]byte{1, 2}),
			want: "da00010600814201 02",
		},
		{
			// tag(0x10900) [ 3600, 0 ] -- offset shortens to two bytes, until to one
			name: "timezone update",
			got:  EncodeTimezoneUpdate(3600, 0),
			want: "da0001090082190e1000",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := strings.ReplaceAll(tc.want, " ", "")
			if got := hex.EncodeToString(tc.got); got != want {
				t.Errorf("got  %s\nwant %s", got, want)
			}
		})
	}
}

// Decoding the uplink commands the daemon publishes. The fields are what a
// scenario constrains, so their names are part of the harness's interface.
func TestDecodeUplinkCommands(t *testing.T) {
	tests := []struct {
		name       string
		payload    string
		wantName   string
		wantTag    uint64
		wantFields map[string]any
	}{
		{
			// tag(0x10700) [ "1.2.3" ]
			name:       "device begin",
			payload:    "da000107008165312e322e33",
			wantName:   CmdDeviceBegin,
			wantTag:    TagDeviceBegin,
			wantFields: map[string]any{"lib_version": "1.2.3"},
		},
		{
			// tag(0x10300) [ "" ] -- the empty thing id of the first Thing.begin
			name:       "thing begin with no thing",
			payload:    "da000103008160",
			wantName:   CmdThingBegin,
			wantTag:    TagThingBegin,
			wantFields: map[string]any{"thing_id": ""},
		},
		{
			// tag(0x10500) []
			name:       "last values begin",
			payload:    "da0001050080",
			wantName:   CmdLastValuesBegin,
			wantTag:    TagLastValuesBegin,
			wantFields: map[string]any{},
		},
		{
			// tag(0x11100) [ 1, "SSIDTEST1" ] -- what the mock netconfig reports
			name:       "device net config over wifi",
			payload:    "da00011100820169535349445445535431",
			wantName:   CmdDeviceNetConfig,
			wantTag:    TagDeviceNetConfig,
			wantFields: map[string]any{"network_type": "wifi", "ssid": "SSIDTEST1"},
		},
		{
			// tag(0x11100) [ 6, h'0a000001', h'', h'', h'' ]
			name:     "device net config over ethernet with DHCP fields unset",
			payload:  "da00011100850644 0a000001 40 40 40",
			wantName: CmdDeviceNetConfig,
			wantTag:  TagDeviceNetConfig,
			wantFields: map[string]any{
				"network_type": "ethernet",
				"ip":           "0a000001",
				"dns":          "",
				"gateway":      "",
				"netmask":      "",
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := DecodeCommand(mustHex(t, tc.payload))
			if err != nil {
				t.Fatalf("DecodeCommand: %v", err)
			}
			if cmd.Name != tc.wantName {
				t.Errorf("name = %q, want %q", cmd.Name, tc.wantName)
			}
			if cmd.Tag != tc.wantTag {
				t.Errorf("tag = %#x, want %#x", cmd.Tag, tc.wantTag)
			}
			if !reflect.DeepEqual(cmd.Fields, tc.wantFields) {
				t.Errorf("fields =\n  %#v\nwant\n  %#v", cmd.Fields, tc.wantFields)
			}
		})
	}
}

// The nested SenML blob is handed back whole, not decoded eagerly: the caller
// decides whether it needs the values.
func TestDecodeLastValuesUpdateCarriesTheNestedBlob(t *testing.T) {
	values, err := EncodeSenML([]Value{{Name: "temp", Value: 21.5}})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}

	cmd, err := DecodeCommand(EncodeLastValuesUpdate(values))
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if cmd.Name != CmdLastValuesUpdate {
		t.Fatalf("name = %q, want %q", cmd.Name, CmdLastValuesUpdate)
	}
	if got, want := cmd.Fields["values_len"], len(values); got != want {
		t.Errorf("values_len = %v, want %v", got, want)
	}
	decoded, err := DecodeSenML(cmd.Values)
	if err != nil {
		t.Fatalf("DecodeSenML of the nested blob: %v", err)
	}
	if len(decoded) != 1 || decoded[0].Name != "temp" || decoded[0].Value != 21.5 {
		t.Errorf("nested values = %#v, want temp=21.5", decoded)
	}
}

// A tag nobody has taught the harness about must still land on the timeline,
// named by number. Silently dropping it would hide a new command the daemon
// started sending.
func TestDecodeUnknownCommandIsNamedByTag(t *testing.T) {
	// tag(0x11200) [ 1, 2 ]
	cmd, err := DecodeCommand(mustHex(t, "da00011200 82 01 02"))
	if err != nil {
		t.Fatalf("DecodeCommand: %v", err)
	}
	if cmd.Name != "0x11200" {
		t.Errorf("name = %q, want the tag number", cmd.Name)
	}
	if got, want := cmd.Fields["fields"], 2; got != want {
		t.Errorf("fields = %v, want %v", got, want)
	}
}

func TestDecodeCommandRejectsMalformedPayloads(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"not CBOR at all", "ff"},
		{"not tagged", "8165312e322e33"},
		{"tagged but not an array", "da000107006161"},
		{"device begin with no version", "da0001070080"},
		{"device begin with a number where the version goes", "da00010700810 1"},
		{"net config with no type", "da000111008 0"},
		{"wifi net config with no ssid", "da000111008101"},
		{"trailing bytes after the command", "da000107008165312e322e3300"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if cmd, err := DecodeCommand(mustHex(t, tc.payload)); err == nil {
				t.Errorf("DecodeCommand succeeded: %+v", cmd)
			}
		})
	}
}

// Every downlink encoder must survive the harness's own reader. This is the
// weaker of the two checks -- crosscheck_test.go is what proves the DAEMON
// reads them -- but it keeps a typo from reaching that far.
func TestDownlinkEncodersRoundTripThroughDecodeCommand(t *testing.T) {
	tests := []struct {
		name     string
		payload  []byte
		wantName string
		check    func(t *testing.T, cmd Command)
	}{
		{
			name:     "thing update",
			payload:  EncodeThingUpdate("thing-1"),
			wantName: CmdThingUpdate,
			check: func(t *testing.T, cmd Command) {
				if got := cmd.Fields["thing_id"]; got != "thing-1" {
					t.Errorf("thing_id = %v", got)
				}
			},
		},
		{
			name:     "thing detach",
			payload:  EncodeThingDetach("thing-1"),
			wantName: CmdThingDetach,
			check: func(t *testing.T, cmd Command) {
				if got := cmd.Fields["thing_id"]; got != "thing-1" {
					t.Errorf("thing_id = %v", got)
				}
			},
		},
		{
			name:     "timezone update",
			payload:  EncodeTimezoneUpdate(-3600, 42),
			wantName: CmdTimezoneUpdate,
			check: func(t *testing.T, cmd Command) {
				if got := cmd.Fields["offset"]; got != int64(-3600) {
					t.Errorf("offset = %v (%T)", got, got)
				}
				if got := cmd.Fields["until"]; got != int64(42) {
					t.Errorf("until = %v (%T)", got, got)
				}
			},
		},
		{
			name:     "ota update",
			payload:  EncodeOTAUpdate(make([]byte, 16), "https://example.invalid/fw.bin", make([]byte, 32), make([]byte, 32)),
			wantName: CmdOTAUpdate,
			check: func(t *testing.T, cmd Command) {
				if got := cmd.Fields["url"]; got != "https://example.invalid/fw.bin" {
					t.Errorf("url = %v", got)
				}
				if got := cmd.Fields["id"]; got != "00000000000000000000000000000000" {
					t.Errorf("id = %v", got)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := DecodeCommand(tc.payload)
			if err != nil {
				t.Fatalf("DecodeCommand: %v", err)
			}
			if cmd.Name != tc.wantName {
				t.Fatalf("name = %q, want %q", cmd.Name, tc.wantName)
			}
			tc.check(t, cmd)
		})
	}
}

// The tag numbers themselves, restated here so a change to the constants has
// to be made twice and deliberately. They come from CBOR.h in the C++ library.
func TestCommandTagNumbers(t *testing.T) {
	want := map[string]uint64{
		CmdOTABegin:         0x10000,
		CmdOTAUpdate:        0x10100,
		CmdOTAProgress:      0x10200,
		CmdThingBegin:       0x10300,
		CmdThingUpdate:      0x10400,
		CmdLastValuesBegin:  0x10500,
		CmdLastValuesUpdate: 0x10600,
		CmdDeviceBegin:      0x10700,
		CmdTimezoneRequest:  0x10800,
		CmdTimezoneUpdate:   0x10900,
		CmdThingDetach:      0x11000,
		CmdDeviceNetConfig:  0x11100,
	}
	if len(commandNames) != len(want) {
		t.Fatalf("the harness knows %d commands, the table lists %d", len(commandNames), len(want))
	}
	for tag, name := range commandNames {
		if got, ok := want[name]; !ok || got != tag {
			t.Errorf("%s is tag %#x, want %#x", name, tag, got)
		}
	}
}
