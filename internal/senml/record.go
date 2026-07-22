// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package senml implements a minimal subset of RFC 8428 (SenML) focused on
// the CBOR representation used by the Arduino IoT Cloud property channel
// (/a/t/<thing_id>/e/i and /e/o).
//
// Only CBOR encoding is supported (JSON and XML are out of scope). The key
// numbers in Record match the integer CBOR labels from RFC 8428 §6 and are
// compatible with the C++ ArduinoIoTCloud library.
//
// # Numeric values
//
// CBOR integers (positive and negative) and IEEE 754 floats are both valid
// SenML numeric values. fxamacker/cbor by default decodes all integers as
// uint64 unless IntDecConvertSigned is set; that option maps CBOR integers to
// int64. This package uses a custom Number type that preserves the distinction
// between int64 and float64 so that integer variables round-trip cleanly to
// the variable registry.
//
// # Subset implemented
//
//   - Single-value records: numeric (int64/float64), string, bool.
//   - BaseName, BaseTime, BaseValue.
//   - Multi-value properties (name "prop:field" colon semantics) are decoded
//     but flattened into separate Record entries — the registry layer handles
//     reassembly.
//
// OTA, RPC magic strings, timezone, and other protocol extensions are out of
// scope for this package.
package senml

import (
	"errors"
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// cborDecoder is shared; it converts CBOR integers to int64 (signed) so that
// arduino-style integers (-1, 0, 1…) survive the round-trip without becoming
// uint64.
var cborDecoder cbor.DecMode

func init() {
	var err error
	cborDecoder, err = cbor.DecOptions{IntDec: cbor.IntDecConvertSignedOrFail}.DecMode()
	if err != nil {
		panic(fmt.Errorf("senml: build CBOR decoder: %w", err))
	}
}

// ── Number ────────────────────────────────────────────────────────────────────

// Number is a union of int64 and float64, preserving the CBOR integer/float
// distinction for faithful round-tripping of Arduino IoT Cloud properties.
type Number struct {
	v interface{} // int64 | float64 | nil
}

// Int64Number returns a Number containing an int64.
func Int64Number(v int64) Number { return Number{v: v} }

// Float64Number returns a Number containing a float64.
func Float64Number(v float64) Number { return Number{v: v} }

// IsInt64 reports whether the underlying value is int64.
func (n Number) IsInt64() bool { _, ok := n.v.(int64); return ok }

// IsFloat64 reports whether the underlying value is float64.
func (n Number) IsFloat64() bool { _, ok := n.v.(float64); return ok }

// IsZero reports whether the number is zero (or nil, treated as zero).
func (n Number) IsZero() bool {
	switch v := n.v.(type) {
	case int64:
		return v == 0
	case float64:
		return v == 0
	}
	return true
}

// Int64 returns the int64 value, or 0 if the underlying type is not int64.
func (n Number) Int64() int64 {
	v, _ := n.v.(int64)
	return v
}

// Float64 returns the float64 value, or 0 if the underlying type is not float64.
func (n Number) Float64() float64 {
	v, _ := n.v.(float64)
	return v
}

// Value returns the underlying interface{} value (int64 or float64 or nil).
func (n Number) Value() interface{} { return n.v }

// Ptr returns a pointer to n, useful when assigning to *Number record fields.
func (n Number) Ptr() *Number { return &n }

// UnmarshalCBOR implements cbor.Unmarshaler.
// Using the shared decoder (IntDecConvertSigned), CBOR positive/negative
// integers decode to int64; IEEE 754 floats decode to float64.
func (n *Number) UnmarshalCBOR(data []byte) error {
	if err := cborDecoder.Unmarshal(data, &n.v); err != nil {
		return err
	}
	switch n.v.(type) {
	case int64, float64, nil:
		return nil
	default:
		return fmt.Errorf("senml: Number: unexpected type %T", n.v)
	}
}

// MarshalCBOR implements cbor.Marshaler.
func (n Number) MarshalCBOR() ([]byte, error) {
	return cbor.Marshal(n.v)
}

// ── Record ────────────────────────────────────────────────────────────────────

// Record is a single SenML measurement as specified by RFC 8428 §5.
// Integer CBOR keys (keyasint) are used in the CBOR encoding to minimise
// wire size; the key numbers match the RFC §6 label definitions.
type Record struct {
	BaseName    string  `cbor:"-2,keyasint,omitempty"`
	BaseTime    float64 `cbor:"-3,keyasint,omitempty"`
	BaseUnit    string  `cbor:"-4,keyasint,omitempty"`
	BaseVersion uint    `cbor:"-1,keyasint,omitempty"`
	BaseValue   *Number `cbor:"-5,keyasint,omitempty"`
	BaseSum     *Number `cbor:"-6,keyasint,omitempty"`
	Name        string  `cbor:"0,keyasint,omitempty"`
	Unit        string  `cbor:"1,keyasint,omitempty"`
	Time        float64 `cbor:"6,keyasint,omitempty"`
	UpdateTime  float64 `cbor:"7,keyasint,omitempty"`
	Value       *Number `cbor:"2,keyasint,omitempty"`
	StringValue *string `cbor:"3,keyasint,omitempty"`
	DataValue   *string `cbor:"8,keyasint,omitempty"`
	BoolValue   *bool   `cbor:"4,keyasint,omitempty"`
	Sum         *Number `cbor:"5,keyasint,omitempty"`
}

// validate checks invariants on a slice of records:
//   - at most one value field is set per record
//   - every value-bearing record has a non-empty resolved name (BaseName+Name)
//
// A record with no value field is a *base/context* record: per RFC 8428 it may
// carry BaseName/BaseTime/BaseValue/BaseSum/BaseVersion that apply to the
// records that follow, contributing no measurement of its own. Arduino Cloud
// emits such records in multi-value property messages (e.g. a ColoredLight
// change sends the swi/hue/sat/bri attributes preceded by base records), so
// they MUST be accepted — toVariables simply skips them. Rejecting them made
// the whole message fail to decode, dropping every attribute of the property.
// Only a value-bearing record needs a name (a value with no name is
// unaddressable and is still rejected).
var (
	errEmptyName     = errors.New("senml: value record has empty resolved name")
	errTooManyValues = errors.New("senml: more than one value field in record")
)

func validate(records []Record) error {
	var baseName string
	var outErr error

	for _, r := range records {
		if r.BaseName != "" {
			baseName = r.BaseName
		}

		var n int
		if r.Value != nil {
			n++
		}
		if r.StringValue != nil {
			n++
		}
		if r.BoolValue != nil {
			n++
		}
		if r.DataValue != nil {
			n++
		}
		if n > 1 {
			outErr = errors.Join(outErr, errTooManyValues)
		}

		hasValue := n > 0 || r.Sum != nil || (r.BaseSum != nil && !r.BaseSum.IsZero())
		if hasValue && baseName+r.Name == "" {
			outErr = errors.Join(outErr, errEmptyName)
		}
	}
	return outErr
}
