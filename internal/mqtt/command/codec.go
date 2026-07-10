// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package command

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/fxamacker/cbor/v2"
)

// ── Tag registry ──────────────────────────────────────────────────────────────

type tagEntry struct {
	tag uint64
	ty  reflect.Type
}

// registeredTags maps each CBOR tag number to its concrete Go type. The order
// matches CBOR.h in the C++ ArduinoIoTCloud library.
//
// Tag numbers summary:
//
//	0x10000  OTABeginCmd          Up
//	0x10100  OTAUpdateCmd         Down
//	0x10200  OTAProgressCmd       Up
//	0x10300  ThingBeginCmd        Up
//	0x10400  ThingUpdateCmd       Down
//	0x10500  LastValuesBeginCmd   Up
//	0x10600  LastValuesUpdateCmd  Down
//	0x10700  DeviceBeginCmd       Up
//	0x10800  TimezoneRequestCmd   Up
//	0x10900  TimezoneUpdateCmd    Down
//	0x11000  ThingDetachCmd       Down
//	0x11100  DeviceNetConfigCmd   Up   (tag emitted by its own MarshalCBOR)
var registeredTags = []tagEntry{
	{0x10000, reflect.TypeOf(OTABeginCmd{})},
	{0x10100, reflect.TypeOf(OTAUpdateCmd{})},
	{0x10200, reflect.TypeOf(OTAProgressCmd{})},
	{0x10300, reflect.TypeOf(ThingBeginCmd{})},
	{0x10400, reflect.TypeOf(ThingUpdateCmd{})},
	{0x10500, reflect.TypeOf(LastValuesBeginCmd{})},
	{0x10600, reflect.TypeOf(LastValuesUpdateCmd{})},
	{0x10700, reflect.TypeOf(DeviceBeginCmd{})},
	{0x10800, reflect.TypeOf(TimezoneRequestCmd{})},
	{0x10900, reflect.TypeOf(TimezoneUpdateCmd{})},
	{0x11000, reflect.TypeOf(ThingDetachCmd{})},
	{0x11100, reflect.TypeOf(DeviceNetConfigCmd{})},
}

var _dm cbor.DecMode
var _em cbor.EncMode

func init() {
	ts := cbor.NewTagSet()
	for _, e := range registeredTags {
		if err := ts.Add(
			cbor.TagOptions{EncTag: cbor.EncTagRequired, DecTag: cbor.DecTagRequired},
			e.ty, e.tag,
		); err != nil {
			panic(fmt.Errorf("command: register CBOR tag 0x%x for %v: %w", e.tag, e.ty, err))
		}
	}

	var err error
	_dm, err = cbor.DecOptions{}.DecModeWithTags(ts)
	if err != nil {
		panic(fmt.Errorf("command: build CBOR DecMode: %w", err))
	}
	_em, err = cbor.EncOptions{}.EncModeWithTags(ts)
	if err != nil {
		panic(fmt.Errorf("command: build CBOR EncMode: %w", err))
	}
}

// ── Cmd wrapper ───────────────────────────────────────────────────────────────

// Cmd wraps a typed command value. Callers use Inner() with a type switch to
// handle the concrete type:
//
//	switch msg := cmd.Inner().(type) {
//	case command.ThingUpdateCmd:
//	    id := msg.ThingID
//	case command.ThingDetachCmd:
//	    ...
//	}
type Cmd struct {
	inner interface{}
}

// Inner returns the concrete command value for type switching.
func (c Cmd) Inner() interface{} { return c.inner }

// String returns a human-readable representation for logging.
func (c Cmd) String() string {
	if c.inner == nil {
		return "<nil>"
	}
	ty := reflect.TypeOf(c.inner)
	idx := slices.IndexFunc(registeredTags, func(e tagEntry) bool { return e.ty == ty })
	if idx < 0 {
		return fmt.Sprintf("<%T>", c.inner)
	}
	return fmt.Sprintf("0x%05x=%+v", registeredTags[idx].tag, c.inner)
}

// From wraps a typed uplink command into a Cmd for passing to
// mqtt.Client.PublishCommand. The type constraint prevents wrapping arbitrary
// structs or downlink-only types (ThingUpdateCmd, ThingDetachCmd, etc.).
func From[T DeviceBeginCmd | DeviceNetConfigCmd | ThingBeginCmd | LastValuesBeginCmd |
	OTABeginCmd | OTAProgressCmd | TimezoneRequestCmd](v T) Cmd {
	return Cmd{inner: v}
}

// NewCmd wraps any registered command value into a Cmd without a type
// constraint. Intended for constructing downlink messages (ThingUpdateCmd,
// ThingDetachCmd, LastValuesUpdateCmd, …) in tests that need to inject them
// into a fake MQTT client. Production code should use From (uplink) or
// Decode (received payloads).
func NewCmd(inner any) Cmd {
	return Cmd{inner: inner}
}

// ── Encode / Decode ───────────────────────────────────────────────────────────

// Decode parses a raw CBOR-tagged payload from the command downlink topic
// (/a/d/<device_id>/c/dw) and returns a typed Cmd. Returns an error if the
// payload is malformed or carries an unrecognised tag.
func Decode(data []byte) (Cmd, error) {
	var inner interface{}
	if err := _dm.Unmarshal(data, &inner); err != nil {
		return Cmd{}, fmt.Errorf("command decode: %w", err)
	}
	ty := reflect.TypeOf(inner)
	if !slices.ContainsFunc(registeredTags, func(e tagEntry) bool { return e.ty == ty }) {
		return Cmd{}, fmt.Errorf("command decode: unrecognised decoded type %T", inner)
	}
	return Cmd{inner: inner}, nil
}

// Encode serialises a typed uplink command to its CBOR-tagged wire format for
// publishing on /a/d/<device_id>/c/up.
func Encode(c Cmd) ([]byte, error) {
	if c.inner == nil {
		return nil, fmt.Errorf("command encode: nil Cmd")
	}
	b, err := _em.Marshal(c.inner)
	if err != nil {
		return nil, fmt.Errorf("command encode %T: %w", c.inner, err)
	}
	return b, nil
}
