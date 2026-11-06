// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package wire

import (
	"encoding/hex"
	"fmt"
	"strings"
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

	// The App lifecycle family (RFC-16 §3). Not in CBOR.h: these messages are
	// Linux-only, so the reference is the RFC's own tables, restated here for the
	// same reason as the rest.
	TagAppsList           uint64 = 0x13000 // up
	TagAppCommand         uint64 = 0x13001 // down
	TagAppCommandResponse uint64 = 0x13002 // up
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

	CmdAppsList           = "Apps.list"
	CmdAppCommand         = "App.command"
	CmdAppCommandResponse = "App.commandResponse"
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

	TagAppsList:           CmdAppsList,
	TagAppCommand:         CmdAppCommand,
	TagAppCommandResponse: CmdAppCommandResponse,
}

// App lifecycle commands, the second element of App.command (RFC-16 §3).
var appCommandNames = map[int64]string{
	0: "stop",
	1: "start",
	2: "delete",
	3: "set_default",
}

// AppCommandCode is the wire number of an App.command command name, the inverse
// of appCommandNames.
func AppCommandCode(name string) (int64, bool) {
	for code, n := range appCommandNames {
		if n == name {
			return code, true
		}
	}
	return 0, false
}

// App command response statuses, the second element of App.commandResponse.
var appStatusNames = map[int64]string{
	0: "ok",
	1: "fail",
	2: "progress",
}

// App states in Apps.list.
var appStateNames = map[int64]string{
	1:  "running",
	0:  "stopped",
	-1: "failure",
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
	case TagAppCommand:
		if err := decodeAppCommand(items, &cmd); err != nil {
			return Command{}, err
		}
	case TagAppCommandResponse:
		if err := decodeAppCommandResponse(items, &cmd); err != nil {
			return Command{}, err
		}
	case TagAppsList:
		if err := decodeAppsList(items, &cmd); err != nil {
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
	state, err := simpleAt(items, 1, "state")
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
	cmd.Fields["state"] = int64(state)
	cmd.Fields["state_data"] = stateData
	cmd.Fields["timestamp"] = timestamp
	return nil
}

// decodeAppCommand reads [app, command, op_id(, parameter)]. Only the harness
// sends it; decoding it keeps the timeline readable if one is ever recorded.
func decodeAppCommand(items []any, cmd *Command) error {
	if len(items) != 3 && len(items) != 4 {
		return fmt.Errorf("wire: App.command has %d element(s), want 3 or 4", len(items))
	}
	app, err := stringAt(items, 0, "app")
	if err != nil {
		return err
	}
	code, err := intAt(items, 1, "command")
	if err != nil {
		return err
	}
	opID, err := stringAt(items, 2, "op_id")
	if err != nil {
		return err
	}
	cmd.Fields["app"] = app
	cmd.Fields["command"] = nameOr(appCommandNames, code)
	cmd.Fields["op_id"] = opID
	if len(items) == 4 {
		p, err := intAt(items, 3, "parameter")
		if err != nil {
			return err
		}
		cmd.Fields["parameter"] = p
	}
	return nil
}

// decodeAppCommandResponse reads [op_id, status(, details)].
//
// elements is recorded because it carries a rule of its own: a success has NO
// details (RFC-16 §3), and an attribute that is merely absent cannot be asserted
// on, while `elements: 2` can.
func decodeAppCommandResponse(items []any, cmd *Command) error {
	if len(items) != 2 && len(items) != 3 {
		return fmt.Errorf("wire: App.commandResponse has %d element(s), want 2 or 3", len(items))
	}
	opID, err := stringAt(items, 0, "op_id")
	if err != nil {
		return err
	}
	status, err := intAt(items, 1, "status")
	if err != nil {
		return err
	}
	cmd.Fields["op_id"] = opID
	cmd.Fields["status"] = nameOr(appStatusNames, status)
	cmd.Fields["elements"] = len(items)
	if len(items) == 3 {
		details, err := intAt(items, 2, "details")
		if err != nil {
			return err
		}
		cmd.Fields["details"] = details
	}
	return nil
}

// decodeAppsList reads the flat [name, state, default, ...] array. The apps are
// recorded as one comma-separated "name=state" string, a trailing * marking the
// default, so a whole list is one attribute a scenario can compare.
func decodeAppsList(items []any, cmd *Command) error {
	if len(items)%3 != 0 {
		return fmt.Errorf("wire: Apps.list has %d element(s), want a multiple of 3", len(items))
	}
	apps := make([]string, 0, len(items)/3)
	for i := 0; i < len(items); i += 3 {
		name, err := stringAt(items, i, "app name")
		if err != nil {
			return err
		}
		state, err := intAt(items, i+1, "app state")
		if err != nil {
			return err
		}
		def, ok := items[i+2].(bool)
		if !ok {
			return fmt.Errorf("wire: app default is %T, want a bool", items[i+2])
		}
		entry := name + "=" + nameOr(appStateNames, state)
		if def {
			entry += "*"
		}
		apps = append(apps, entry)
	}
	cmd.Fields["apps"] = strings.Join(apps, ",")
	cmd.Fields["count"] = len(apps)
	return nil
}

// nameOr names a protocol number, or spells it out when this package does not
// know it, so an unexpected value is visible rather than silently mapped.
func nameOr(names map[int64]string, n int64) string {
	if name, ok := names[n]; ok {
		return name
	}
	return fmt.Sprintf("unknown(%d)", n)
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

// EncodeOTAUpdate starts an App deploy (RFC-14): the job id, the bundle URL, the
// digest the cloud believes is installed and the one the download must hash to.
func EncodeOTAUpdate(id []byte, url string, initialSHA, finalSHA []byte) []byte {
	b := appendTag(nil, TagOTAUpdate)
	b = appendArrayHeader(b, 4)
	b = appendBytes(b, id)
	b = appendText(b, url)
	b = appendBytes(b, initialSHA)
	return appendBytes(b, finalSHA)
}

// EncodeAppCommand asks the board to act on one of its Apps (RFC-16 §3):
// [app, command, op_id] or, with a parameter, [app, command, op_id, parameter].
func EncodeAppCommand(app string, command int64, opID string, parameter *int64) []byte {
	n := 3
	if parameter != nil {
		n = 4
	}
	b := appendTag(nil, TagAppCommand)
	b = appendArrayHeader(b, n)
	b = appendText(b, app)
	b = appendInt(b, command)
	b = appendText(b, opID)
	if parameter != nil {
		b = appendInt(b, *parameter)
	}
	return b
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

// simpleAt reads a CBOR simple value. An integer is refused on purpose: the C++
// library sends the OTA state as a simple value, and a daemon that sent an integer
// instead would be speaking a dialect no firmware speaks.
func simpleAt(items []any, i int, field string) (Simple, error) {
	v, err := itemAt(items, i, field)
	if err != nil {
		return 0, err
	}
	s, ok := v.(Simple)
	if !ok {
		return 0, fmt.Errorf("wire: %s is %T, want a CBOR simple value", field, v)
	}
	return s, nil
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
