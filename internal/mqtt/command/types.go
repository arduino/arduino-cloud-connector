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

// OTABeginCmd (tag 0x10000, Up) initiates an OTA update. Not implemented
// in this daemon; included so the TagSet covers all protocol tags and unknown
// commands can be decoded and logged by name rather than failing hard.
type OTABeginCmd struct {
	_      struct{} `cbor:",toarray"`
	SHA256 [32]byte
}

// OTAUpdateCmd (tag 0x10100, Down) is sent by the cloud to push OTA package
// metadata to the device. Not implemented.
type OTAUpdateCmd struct {
	_          struct{} `cbor:",toarray"`
	ID         [16]byte
	URL        string
	InitialSHA [32]byte
	FinalSHA   [32]byte
}

// OTAProgressCmd (tag 0x10200, Up) reports OTA progress device→cloud.
// Not implemented.
type OTAProgressCmd struct {
	_         struct{} `cbor:",toarray"`
	ID        [16]byte
	State     uint8
	StateData int32
	Timestamp uint64
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
