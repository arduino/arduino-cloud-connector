// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package command_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// Golden-byte test vectors are derived from the Arduino IoT Cloud C++ library:
//   extras/test/src/test_command_encode.cpp
//   extras/test/src/test_command_decode.cpp
//
// These bytes represent the canonical wire format produced and consumed by the
// cloud backend. Any change here is a protocol-level breaking change.
//
// CBOR structure for all command messages: tag(<n>)([field1, field2, ...])
//   - DA XX XX XX XX  = 4-byte tag
//   - 8N              = array(N)
//   - 80              = array(0)  (empty params)

// ── Encode — device→cloud uplink ─────────────────────────────────────────────

func TestEncode_DeviceBeginCmd(t *testing.T) {
	// DA 00010700         # tag(67328) DeviceBeginCmd
	//    81               # array(1)
	//       65            # text(5)
	//          322E302E30 # "2.0.0"
	want := []byte{
		0xda, 0x00, 0x01, 0x07, 0x00, 0x81, 0x65, 0x32,
		0x2e, 0x30, 0x2e, 0x30,
	}
	got, err := command.Encode(command.From(command.DeviceBeginCmd{LibVersion: "2.0.0"}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_ThingBeginCmd(t *testing.T) {
	// DA 00010300               # tag(66304) ThingBeginCmd
	//    81                     # array(1)
	//       68                  # text(8)
	//          7468696E675F6964 # "thing_id"
	want := []byte{
		0xda, 0x00, 0x01, 0x03, 0x00, 0x81, 0x68,
		0x74, 0x68, 0x69, 0x6e, 0x67, 0x5f, 0x69, 0x64,
	}
	got, err := command.Encode(command.From(command.ThingBeginCmd{ThingID: "thing_id"}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_ThingBeginCmd_EmptyThingID(t *testing.T) {
	// DA 00010300   # tag(66304) ThingBeginCmd
	//    81         # array(1)
	//       60      # text(0) ""
	// Sent by the daemon on the first Thing.begin before any thing_id is known.
	want := []byte{0xda, 0x00, 0x01, 0x03, 0x00, 0x81, 0x60}
	got, err := command.Encode(command.From(command.ThingBeginCmd{ThingID: ""}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_LastValuesBeginCmd(t *testing.T) {
	// DA 00010500   # tag(66816) LastValuesBeginCmd
	//    80         # array(0) — no parameters
	want := []byte{0xda, 0x00, 0x01, 0x05, 0x00, 0x80}
	got, err := command.Encode(command.From(command.LastValuesBeginCmd{}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_DeviceNetConfigCmd_WiFi(t *testing.T) {
	// Golden vector from C++ test_command_encode.cpp ("with WiFi"):
	// DA 00011100      # tag(69888) DeviceNetConfigCmdUp
	//    82            # array(2)
	//       01         # unsigned(1) — typeID WIFI
	//       64         # text(4)
	//          53534944 # "SSID"
	want := []byte{
		0xda, 0x00, 0x01, 0x11, 0x00, 0x82, 0x01,
		0x64, 0x53, 0x53, 0x49, 0x44,
	}
	got, err := command.Encode(command.From(command.DeviceNetConfigCmd{
		Type: command.NetworkTypeWiFi,
		SSID: "SSID",
	}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_DeviceNetConfigCmd_Ethernet(t *testing.T) {
	// Golden vector from C++ test_command_encode.cpp ("with Ethernet", IPv4):
	// DA 00011100   # tag(69888)
	//    85         # array(5)
	//       06      # unsigned(6) — typeID ETHERNET
	//       44 C0A80002   # bytes(4) 192.168.0.2
	//       44 08080808   # bytes(4) 8.8.8.8
	//       44 C0A80101   # bytes(4) 192.168.1.1
	//       44 FFFFFF00   # bytes(4) 255.255.255.0
	want := []byte{
		0xda, 0x00, 0x01, 0x11, 0x00, 0x85, 0x06,
		0x44, 0xc0, 0xa8, 0x00, 0x02,
		0x44, 0x08, 0x08, 0x08, 0x08,
		0x44, 0xc0, 0xa8, 0x01, 0x01,
		0x44, 0xff, 0xff, 0xff, 0x00,
	}
	got, err := command.Encode(command.From(command.DeviceNetConfigCmd{
		Type:    command.NetworkTypeEthernet,
		IP:      []byte{192, 168, 0, 2},
		DNS:     []byte{8, 8, 8, 8},
		Gateway: []byte{192, 168, 1, 1},
		Netmask: []byte{255, 255, 255, 0},
	}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_DeviceNetConfigCmd_EthernetDHCP(t *testing.T) {
	// Ethernet with no addresses: each IP must encode as a zero-length byte
	// string (0x40), not CBOR null — the cloud reads this as DHCP/unset.
	// DA 00011100  85  06  40 40 40 40
	want := []byte{
		0xda, 0x00, 0x01, 0x11, 0x00, 0x85, 0x06,
		0x40, 0x40, 0x40, 0x40,
	}
	got, err := command.Encode(command.From(command.DeviceNetConfigCmd{
		Type: command.NetworkTypeEthernet,
	}))
	require.NoError(t, err)
	require.Equal(t, want, got)
}

// ── Decode — cloud→device downlink ───────────────────────────────────────────

const goldenThingID command.ThingID = "e4494d55-872a-4fd2-9646-92f87949394c"

// thingIDBytes is the CBOR encoding of the 36-char UUID above:
//
//	78 24  = text(36)
//	65 34 34 39 34 64 35 35 2D ...
var thingIDBytes = []byte{
	0x78, 0x24,
	0x65, 0x34, 0x34, 0x39, 0x34, 0x64, 0x35, 0x35,
	0x2D, 0x38, 0x37, 0x32, 0x61, 0x2D, 0x34, 0x66,
	0x64, 0x32, 0x2D, 0x39, 0x36, 0x34, 0x36, 0x2D,
	0x39, 0x32, 0x66, 0x38, 0x37, 0x39, 0x34, 0x39,
	0x33, 0x39, 0x34, 0x63,
}

func TestDecode_ThingUpdateCmd(t *testing.T) {
	// DA 00010400   # tag(66560) ThingUpdateCmd
	//    81         # array(1)
	//       78 24   # text(36)
	//       "e4494d55-872a-4fd2-9646-92f87949394c"
	payload := append([]byte{0xDA, 0x00, 0x01, 0x04, 0x00, 0x81}, thingIDBytes...)

	cmd, err := command.Decode(payload)
	require.NoError(t, err)

	msg, ok := cmd.Inner().(command.ThingUpdateCmd)
	require.True(t, ok, "expected ThingUpdateCmd, got %T", cmd.Inner())
	require.Equal(t, goldenThingID, msg.ThingID)
}

func TestDecode_ThingUpdateCmd_EmptyThingID(t *testing.T) {
	// DA 00010400   # tag(66560) ThingUpdateCmd
	//    81         # array(1)
	//       60      # text(0) "" — cloud says "no thing assigned yet"
	payload := []byte{0xDA, 0x00, 0x01, 0x04, 0x00, 0x81, 0x60}

	cmd, err := command.Decode(payload)
	require.NoError(t, err)

	msg, ok := cmd.Inner().(command.ThingUpdateCmd)
	require.True(t, ok)
	require.Empty(t, msg.ThingID)
}

func TestDecode_ThingDetachCmd(t *testing.T) {
	// DA 00011000   # tag(69632) ThingDetachCmd
	//    81         # array(1)
	//       78 24   # text(36)
	//       "e4494d55-872a-4fd2-9646-92f87949394c"
	payload := append([]byte{0xDA, 0x00, 0x01, 0x10, 0x00, 0x81}, thingIDBytes...)

	cmd, err := command.Decode(payload)
	require.NoError(t, err)

	msg, ok := cmd.Inner().(command.ThingDetachCmd)
	require.True(t, ok, "expected ThingDetachCmd, got %T", cmd.Inner())
	require.Equal(t, goldenThingID, msg.ThingID)
}

func TestDecode_LastValuesUpdateCmd(t *testing.T) {
	// DA 00010600                        # tag(67072) LastValuesUpdateCmd
	//    81                              # array(1)
	//       4D                           # bytes(13)
	//          00 01 02 03 04 05 06 07 08 09 10 11 12
	payload := []byte{
		0xDA, 0x00, 0x01, 0x06, 0x00, 0x81, 0x4D,
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x10, 0x11, 0x12,
	}
	wantValues := []byte{
		0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06,
		0x07, 0x08, 0x09, 0x10, 0x11, 0x12,
	}

	cmd, err := command.Decode(payload)
	require.NoError(t, err)

	msg, ok := cmd.Inner().(command.LastValuesUpdateCmd)
	require.True(t, ok, "expected LastValuesUpdateCmd, got %T", cmd.Inner())
	require.Equal(t, wantValues, msg.Values)
}

func TestDecode_TimezoneUpdateCmd(t *testing.T) {
	// DA 00010900       # tag(67840) TimezoneUpdateCmd
	//    82             # array(2)
	//       1A 65DCB821 # unsigned(1708963873) → Offset
	//       1A 78ACA191 # unsigned(2024579473) → Until
	payload := []byte{
		0xDA, 0x00, 0x01, 0x09, 0x00, 0x82,
		0x1A, 0x65, 0xDC, 0xB8, 0x21,
		0x1A, 0x78, 0xAC, 0xA1, 0x91,
	}

	cmd, err := command.Decode(payload)
	require.NoError(t, err)

	msg, ok := cmd.Inner().(command.TimezoneUpdateCmd)
	require.True(t, ok, "expected TimezoneUpdateCmd, got %T", cmd.Inner())
	require.Equal(t, int32(1708963873), msg.Offset)
	require.Equal(t, uint32(2024579473), msg.Until)
}

// ── Error cases ───────────────────────────────────────────────────────────────

func TestDecode_InvalidCBOR(t *testing.T) {
	_, err := command.Decode([]byte{0xFF, 0xFE})
	require.Error(t, err)
}

func TestDecode_EmptyPayload(t *testing.T) {
	_, err := command.Decode([]byte{})
	require.Error(t, err)
}

func TestDecode_ThingUpdateCmd_NumberInsteadOfString(t *testing.T) {
	// ThingUpdateCmd where the thing_id field is an integer, not a string.
	// The cloud must never send this, but the daemon must not crash on it.
	// DA 00010400   # tag(66560)
	//    81         # array(1)
	//       1A 65DCB821  # unsigned(1708963873) — wrong type
	payload := []byte{0xDA, 0x00, 0x01, 0x04, 0x00, 0x81, 0x1A, 0x65, 0xDC, 0xB8, 0x21}
	_, err := command.Decode(payload)
	require.Error(t, err)
}

func TestDecode_ThingDetachCmd_NumberInsteadOfString(t *testing.T) {
	// Same malformed pattern for ThingDetachCmd.
	payload := []byte{0xDA, 0x00, 0x01, 0x10, 0x00, 0x81, 0x1A, 0x65, 0xDC, 0xB8, 0x21}
	_, err := command.Decode(payload)
	require.Error(t, err)
}

func TestDecode_UnknownTag(t *testing.T) {
	// A tag number not in the registered set — should return an error, not silently
	// produce an untyped value.
	// DA 000FFFFF   # tag(1048575) — not registered
	//    80         # array(0)
	payload := []byte{0xDA, 0x00, 0x0F, 0xFF, 0xFF, 0x80}
	_, err := command.Decode(payload)
	require.Error(t, err)
}

// ── Round-trip ────────────────────────────────────────────────────────────────

func TestRoundTrip_DeviceBeginCmd(t *testing.T) {
	original := command.DeviceBeginCmd{LibVersion: "1.2.3"}
	encoded, err := command.Encode(command.From(original))
	require.NoError(t, err)

	decoded, err := command.Decode(encoded)
	require.NoError(t, err)

	msg, ok := decoded.Inner().(command.DeviceBeginCmd)
	require.True(t, ok)
	require.Equal(t, original.LibVersion, msg.LibVersion)
}

func TestRoundTrip_ThingBeginCmd(t *testing.T) {
	original := command.ThingBeginCmd{ThingID: "f47ac10b-58cc-4372-a567-0e02b2c3d479"}
	encoded, err := command.Encode(command.From(original))
	require.NoError(t, err)

	decoded, err := command.Decode(encoded)
	require.NoError(t, err)

	msg, ok := decoded.Inner().(command.ThingBeginCmd)
	require.True(t, ok)
	require.Equal(t, original.ThingID, msg.ThingID)
}

func TestRoundTrip_LastValuesBeginCmd(t *testing.T) {
	encoded, err := command.Encode(command.From(command.LastValuesBeginCmd{}))
	require.NoError(t, err)

	decoded, err := command.Decode(encoded)
	require.NoError(t, err)

	_, ok := decoded.Inner().(command.LastValuesBeginCmd)
	require.True(t, ok)
}

// ── Cmd.String() ─────────────────────────────────────────────────────────────

func TestCmdString_ShowsTagAndValue(t *testing.T) {
	cmd := command.From(command.DeviceBeginCmd{LibVersion: "0.1.0"})
	s := cmd.String()
	require.Contains(t, s, "10700") // tag number
	require.Contains(t, s, "0.1.0") // field value
}
