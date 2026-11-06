// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package command defines the typed command structs exchanged on the Arduino
// IoT Cloud MQTT command channel (/a/d/<device_id>/c/up and /c/dw) and
// provides CBOR encode/decode using the fxamacker/cbor/v2 TagSet mechanism.
//
// Every command is a CBOR-tagged array: tag(<n>)([field1, field2, ...]).
// The tag numbers match the C++ ArduinoIoTCloud library (src/cbor/CBOR.h).
//
// # Direction convention
//
//   - Up   = device → cloud  (published by this daemon)
//   - Down = cloud → device  (received by this daemon)
//
// # Wire encoding
//
// Each struct carries a blank `_` field with the `cbor:",toarray"` tag, which
// instructs fxamacker to marshal the struct as a CBOR array rather than a map.
// The TagSet registered in codec.go wraps each array in the protocol tag so
// the cloud can dispatch the message to the correct handler.
package command

import "fmt"

// ThingID is the opaque UUID string that the Arduino IoT Cloud assigns to a
// "thing" (a logical grouping of variables). It arrives from the cloud via
// ThingUpdateCmd and is used as a topic identifier for property subscriptions.
// The empty string is a valid zero value and means "no thing assigned yet".
type ThingID string

// String returns the ThingID as a plain string. Satisfies fmt.Stringer and
// provides a concise conversion for call sites that require a string (e.g.,
// MQTT topic builders).
func (t ThingID) String() string { return string(t) }

// DeviceBeginCmd (tag 0x10700, Up) announces the daemon to the cloud upon
// every new MQTT connection. Published once per connection, before Thing.begin.
type DeviceBeginCmd struct {
	_          struct{} `cbor:",toarray"`
	LibVersion string
}

// ThingBeginCmd (tag 0x10300, Up) requests the cloud to push the thing_id
// currently assigned to this device. An empty ThingID means "I don't know my
// thing yet". Retried with exponential back-off until the cloud replies.
type ThingBeginCmd struct {
	_       struct{} `cbor:",toarray"`
	ThingID ThingID
}

// ThingUpdateCmd (tag 0x10400, Down) carries the thing_id assigned to this
// device. An empty ThingID means the cloud registered the device but no thing
// is currently attached.
type ThingUpdateCmd struct {
	_       struct{} `cbor:",toarray"`
	ThingID ThingID
}

// ThingDetachCmd (tag 0x11000, Down) notifies the device that its previously
// assigned thing has been detached by an operator action in the Cloud UI.
// The broker connection is kept; the daemon returns to AwaitingThingID.
type ThingDetachCmd struct {
	_       struct{} `cbor:",toarray"`
	ThingID ThingID
}

// LastValuesBeginCmd (tag 0x10500, Up) requests the cloud to push the
// last-known values of all properties belonging to the assigned thing.
type LastValuesBeginCmd struct {
	_ struct{} `cbor:",toarray"`
}

// LastValuesUpdateCmd (tag 0x10600, Down) carries the last-known property
// values for the assigned thing as a SenML+CBOR blob. The daemon decodes
// Values using the internal/senml package and applies the result to the
// variable registry.
type LastValuesUpdateCmd struct {
	_      struct{} `cbor:",toarray"`
	Values []byte
}

// OTABeginCmd (tag 0x10000, Up) carries the digest of an installed artefact. On an
// MCU it is published on every connection to announce the running firmware; on this
// daemon it is published exactly once, right after an App bundle has been installed,
// as the success signal that closes a deploy job (RFC-14 §5.1). It is deliberately
// NOT sent at startup: a Linux board can hold and run several Apps, some installed
// by hand from App Lab, so there is no single installed digest to announce. See
// internal/ota, which implements it.
type OTABeginCmd struct {
	_      struct{} `cbor:",toarray"`
	SHA256 [32]byte
}

// OTAUpdateCmd (tag 0x10100, Down) is sent by the cloud to start a deploy. ID is
// the job id, URL the storage location of the App bundle, InitialSHA the digest
// the cloud believes is deployed and FinalSHA the digest the download must hash
// to. Handled by internal/ota.OTAFSM (RFC-14 §5.2).
type OTAUpdateCmd struct {
	_          struct{} `cbor:",toarray"`
	ID         [16]byte
	URL        string
	InitialSHA [32]byte
	FinalSHA   [32]byte
}

// OTAProgressCmd (tag 0x10200, Up) reports deploy progress device→cloud. State
// is the phase (see internal/ota.State) and StateData its payload: downloaded
// bytes during Fetch, a percentage during install, or a negative error code on
// failure.
//
// NOTE StateData is int32 to match the C++ library's wire format, so a byte count
// saturates just under 2 GiB. App bundles are expected to exceed that; see
// internal/ota.OTAFSM.progressData.
type OTAProgressCmd struct {
	_         struct{} `cbor:",toarray"`
	ID        [16]byte
	State     OTAState
	StateData int32
	Timestamp uint64
}

// OTAState is the phase carried by OTAProgressCmd. On the wire it is a CBOR simple
// value (major type 7), not an integer: the C++ library encodes it with
// cbor_encode_simple_value (src/cbor/IoTCloudMessageEncoder.cpp), so state 5 is the
// single byte E5 where a uint8 would give 05. It is encoded the same way here so the
// Cloud receives from this daemon exactly the bytes it receives from a firmware OTA.
//
// Only the one-byte form exists for it: 0…19 are the unassigned simple values, and
// 20…23 already mean false, true, null and undefined, so a state from 20 up cannot be
// sent as a simple value at all and is refused rather than turned into a boolean. The
// OTA states stop at 8.
type OTAState uint8

// maxOTAState is the largest state the one-byte simple-value form can carry.
const maxOTAState = 19

// MarshalCBOR implements cbor.Marshaler.
func (s OTAState) MarshalCBOR() ([]byte, error) {
	if s > maxOTAState {
		return nil, fmt.Errorf("command: OTA state %d cannot be encoded as a CBOR simple value", s)
	}
	return []byte{0xe0 | byte(s)}, nil
}

// UnmarshalCBOR implements cbor.Unmarshaler.
func (s *OTAState) UnmarshalCBOR(data []byte) error {
	if len(data) != 1 || data[0]&0xe0 != 0xe0 || data[0]&0x1f > maxOTAState {
		return fmt.Errorf("command: OTA state is not a one-byte CBOR simple value: % x", data)
	}
	*s = OTAState(data[0] & 0x1f)
	return nil
}

// TimezoneRequestCmd (tag 0x10800, Up) requests timezone info from the cloud.
// Unused on Linux where tzdata/chrony handles timezone; included for
// completeness.
type TimezoneRequestCmd struct {
	_ struct{} `cbor:",toarray"`
}

// TimezoneUpdateCmd (tag 0x10900, Down) carries timezone offset and expiry
// sent by the cloud in response to TimezoneRequestCmd.
type TimezoneUpdateCmd struct {
	_      struct{} `cbor:",toarray"`
	Offset int32
	Until  uint32
}
