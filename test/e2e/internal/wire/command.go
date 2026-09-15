// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package wire

import (
	"encoding/hex"
	"fmt"
)

// Command tag numbers, on the command channel (/a/d/<device_id>/c/up and
// /c/dw). Every command is tag(<n>)([field, field, ...]).
//
// These are written out rather than imported from the daemon, and that is the
// point: the numbers come from the C++ ArduinoIoTCloud library (src/cbor/CBOR.h)
// and are a contract with it. If someone changes a tag on the daemon side, the
// E2E suite must break; importing the constant would keep both sides agreeing
// while the real cloud stopped understanding the device.
const (
	TagOTABegin         uint64 = 0x10000 // up
	TagOTAUpdate        uint64 = 0x10100 // down
	TagOTAProgress      uint64 = 0x10200 // up
	TagThingBegin       uint64 = 0x10300 // up
	TagThingUpdate      uint64 = 0x10400 // down
	TagLastValuesBegin  uint64 = 0x10500 // up
	TagLastValuesUpdate uint64 = 0x10600 // down
	TagDeviceBegin      uint64 = 0x10700 // up
	TagTimezoneRequest  uint64 = 0x10800 // up
	TagTimezoneUpdate   uint64 = 0x10900 // down
	TagThingDetach      uint64 = 0x11000 // down
	TagDeviceNetConfig  uint64 = 0x11100 // up
)

// Command names as scenarios write them and as the timeline prints them.
const (
	CmdOTABegin         = "OTA.begin"
	CmdOTAUpdate        = "OTA.update"
	CmdOTAProgress      = "OTA.progress"
	CmdThingBegin       = "Thing.begin"
	CmdThingUpdate      = "Thing.update"
	CmdLastValuesBegin  = "LastValues.begin"
	CmdLastValuesUpdate = "LastValues.update"
	CmdDeviceBegin      = "Device.begin"
	CmdTimezoneRequest  = "Timezone.request"
	CmdTimezoneUpdate   = "Timezone.update"
	CmdThingDetach      = "Thing.detach"
	CmdDeviceNetConfig  = "DeviceNetConfig"
)

// commandNames is the tag-to-name map. An unknown tag is reported by number so
// a new command shows up as "0x11200" on the timeline instead of vanishing.
var commandNames = map[uint64]string{
	TagOTABegin:         CmdOTABegin,
	TagOTAUpdate:        CmdOTAUpdate,
	TagOTAProgress:      CmdOTAProgress,
	TagThingBegin:       CmdThingBegin,
	TagThingUpdate:      CmdThingUpdate,
	TagLastValuesBegin:  CmdLastValuesBegin,
	TagLastValuesUpdate: CmdLastValuesUpdate,
	TagDeviceBegin:      CmdDeviceBegin,
	TagTimezoneRequest:  CmdTimezoneRequest,
	TagTimezoneUpdate:   CmdTimezoneUpdate,
	TagThingDetach:      CmdThingDetach,
	TagDeviceNetConfig:  CmdDeviceNetConfig,
}

// Network adapter type IDs, the first element of DeviceNetConfig. Restated
// from the C++ getEncodingParams for the same reason as the tags.
var networkTypeNames = map[int64]string{
	0: "unknown",
	1: "wifi",
	2: "lora",
	3: "gsm",
	4: "nb",
	5: "catm1",
	6: "ethernet",
	7: "cellular",
}

// Command is a decoded command message.
type Command struct {
	// Name is the protocol name ("Device.begin"), or "0x<tag>" when the tag is
	// not one this package knows.
	Name string
	Tag  uint64
	// Fields are the decoded array elements, keyed the way scenarios refer to
	// them: thing_id, lib_version, network_type, ssid… They go straight into
	// the event attributes, so a step can constrain any of them.
	Fields map[string]any
	// Values is the nested SenML+CBOR blob of LastValues.update. Decode it
	// with DecodeSenML.
	Values []byte
}

// DecodeCommand parses a command payload.
//
// The array elements are read positionally, by index, because position IS the
// protocol -- the fields have no names on the wire. That is also why a wrong
// field order on the daemon side surfaces here as a type error or a shifted
// value rather than passing silently.
func DecodeCommand(payload []byte) (Command, error) {
	v, err := Decode(payload)
	if err != nil {
		return Command{}, fmt.Errorf("wire: decode command: %w", err)
	}
	tagged, ok := v.(Tag)
	if !ok {
		return Command{}, fmt.Errorf("wire: command payload is %T, want a CBOR tag", v)
	}
	items, ok := tagged.Content.([]any)
	if !ok {
		return Command{}, fmt.Errorf("wire: command 0x%05x content is %T, want an array", tagged.Number, tagged.Content)
	}

	cmd := Command{
		Name:   commandName(tagged.Number),
		Tag:    tagged.Number,
		Fields: map[string]any{},
	}
	switch tagged.Number {
	case TagDeviceBegin:
		s, err := stringAt(items, 0, "lib_version")
		if err != nil {
			return Command{}, err
		}
		cmd.Fields["lib_version"] = s
	case TagThingBegin, TagThingUpdate, TagThingDetach:
		s, err := stringAt(items, 0, "thing_id")
		if err != nil {
			return Command{}, err
		}
		cmd.Fields["thing_id"] = s
	case TagLastValuesBegin, TagTimezoneRequest:
		// No fields; the empty array is the whole message.
	case TagLastValuesUpdate:
		b, err := bytesAt(items, 0, "values")
		if err != nil {
			return Command{}, err
		}
		cmd.Values = b
		cmd.Fields["values_len"] = len(b)
	case TagDeviceNetConfig:
		if err := decodeNetConfig(items, &cmd); err != nil {
			return Command{}, err
		}
	case TagTimezoneUpdate:
		offset, err := intAt(items, 0, "offset")
		if err != nil {
			return Command{}, err
		}
		until, err := intAt(items, 1, "until")
		if err != nil {
			return Command{}, err
		}
		cmd.Fields["offset"] = offset
		cmd.Fields["until"] = until
	case TagOTABegin:
		b, err := bytesAt(items, 0, "sha256")
		if err != nil {
			return Command{}, err
		}
		cmd.Fields["sha256"] = hex.EncodeToString(b)
	case TagOTAUpdate:
		if err := decodeOTAUpdate(items, &cmd); err != nil {
			return Command{}, err
		}
	case TagOTAProgress:
		if err := decodeOTAProgress(items, &cmd); err != nil {
			return Command{}, err
		}
	default:
		// An unknown command is still worth putting on the timeline: the tag
		// and the element count say what arrived.
		cmd.Fields["fields"] = len(items)
	}
	return cmd, nil
}

// decodeNetConfig reads the type-dependent array: [1, ssid] for WiFi,
// [6, ip, dns, gateway, netmask] for Ethernet, [type] otherwise.
func decodeNetConfig(items []any, cmd *Command) error {
	id, err := intAt(items, 0, "network_type")
	if err != nil {
		return err
	}
	name, known := networkTypeNames[id]
	if !known {
		name = fmt.Sprintf("type_%d", id)
	}
	cmd.Fields["network_type"] = name

	switch name {
	case "wifi":
		ssid, err := stringAt(items, 1, "ssid")
		if err != nil {
			return err
		}
		cmd.Fields["ssid"] = ssid
	case "ethernet":
		for i, field := range []string{"ip", "dns", "gateway", "netmask"} {
			b, err := bytesAt(items, i+1, field)
			if err != nil {
				return err
			}
			// An empty byte string means unset, which is how the firmware says
			// "DHCP". Hex keeps it readable on the timeline either way.
			cmd.Fields[field] = hex.EncodeToString(b)
		}
	}
	return nil
}

func decodeOTAUpdate(items []any, cmd *Command) error {
	id, err := bytesAt(items, 0, "id")
	if err != nil {
		return err
	}
	url, err := stringAt(items, 1, "url")
	if err != nil {
		return err
	}
	initial, err := bytesAt(items, 2, "initial_sha")
	if err != nil {
		return err
	}
	final, err := bytesAt(items, 3, "final_sha")
	if err != nil {
		return err
	}
	cmd.Fields["id"] = hex.EncodeToString(id)
	cmd.Fields["url"] = url
	cmd.Fields["initial_sha"] = hex.EncodeToString(initial)
	cmd.Fields["final_sha"] = hex.EncodeToString(final)
	return nil
}

func decodeOTAProgress(items []any, cmd *Command) error {
	id, err := bytesAt(items, 0, "id")
	if err != nil {
		return err
	}
	state, err := intAt(items, 1, "state")
	if err != nil {
		return err
	}
	stateData, err := intAt(items, 2, "state_data")
	if err != nil {
		return err
	}
	timestamp, err := intAt(items, 3, "timestamp")
	if err != nil {
		return err
	}
	cmd.Fields["id"] = hex.EncodeToString(id)
	cmd.Fields["state"] = state
	cmd.Fields["state_data"] = stateData
	cmd.Fields["timestamp"] = timestamp
	return nil
}

func commandName(tag uint64) string {
	if name, ok := commandNames[tag]; ok {
		return name
	}
	return fmt.Sprintf("0x%05x", tag)
}

// ── downlink encoders ────────────────────────────────────────────────────────
//
// Only the commands the harness plays the CLOUD for. The uplink commands are
// never encoded here: the daemon produces those, and the harness's job is to
// read them.

// EncodeThingUpdate is the cloud answering Thing.begin with the assigned thing.
func EncodeThingUpdate(thingID string) []byte {
	return encodeStringCommand(TagThingUpdate, thingID)
}

// EncodeThingDetach is the cloud detaching the thing after an operator action.
func EncodeThingDetach(thingID string) []byte {
	return encodeStringCommand(TagThingDetach, thingID)
}

// EncodeLastValuesUpdate carries the last known property values, themselves a
// SenML+CBOR blob (see EncodeSenML) nested as a byte string.
func EncodeLastValuesUpdate(values []byte) []byte {
	b := appendTag(nil, TagLastValuesUpdate)
	b = appendArrayHeader(b, 1)
	return appendBytes(b, values)
}

// EncodeTimezoneUpdate answers Timezone.request.
func EncodeTimezoneUpdate(offset int32, until uint32) []byte {
	b := appendTag(nil, TagTimezoneUpdate)
	b = appendArrayHeader(b, 2)
	b = appendInt(b, int64(offset))
	return appendInt(b, int64(until))
}

// EncodeOTAUpdate pushes OTA metadata. The daemon does not implement it; it is
// here so a scenario can check that an unimplemented command is ignored
// without wedging the connection.
func EncodeOTAUpdate(id []byte, url string, initialSHA, finalSHA []byte) []byte {
	b := appendTag(nil, TagOTAUpdate)
	b = appendArrayHeader(b, 4)
	b = appendBytes(b, id)
	b = appendText(b, url)
	b = appendBytes(b, initialSHA)
	return appendBytes(b, finalSHA)
}

func encodeStringCommand(tag uint64, value string) []byte {
	b := appendTag(nil, tag)
	b = appendArrayHeader(b, 1)
	return appendText(b, value)
}

// ── positional accessors ─────────────────────────────────────────────────────

func itemAt(items []any, i int, field string) (any, error) {
	if i >= len(items) {
		return nil, fmt.Errorf("wire: %s missing: the array has %d element(s), wanted index %d", field, len(items), i)
	}
	return items[i], nil
}

func stringAt(items []any, i int, field string) (string, error) {
	v, err := itemAt(items, i, field)
	if err != nil {
		return "", err
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("wire: %s is %T, want a text string", field, v)
	}
	return s, nil
}

func bytesAt(items []any, i int, field string) ([]byte, error) {
	v, err := itemAt(items, i, field)
	if err != nil {
		return nil, err
	}
	b, ok := v.([]byte)
	if !ok {
		return nil, fmt.Errorf("wire: %s is %T, want a byte string", field, v)
	}
	return b, nil
}

func intAt(items []any, i int, field string) (int64, error) {
	v, err := itemAt(items, i, field)
	if err != nil {
		return 0, err
	}
	switch n := v.(type) {
	case int64:
		return n, nil
	case uint64:
		return 0, fmt.Errorf("wire: %s is %d, too large for an int64", field, n)
	default:
		return 0, fmt.Errorf("wire: %s is %T, want an integer", field, v)
	}
}
