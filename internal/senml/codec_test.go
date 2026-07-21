// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package senml_test

import (
	"testing"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/stretchr/testify/require"

	"github.com/arduino/arduino-cloud-connector/internal/senml"
)

// Golden-byte test vectors are derived from the Arduino IoT Cloud C++ library:
//   extras/test/src/test_decode.cpp
//
// SenML CBOR format: array of maps with integer keys (RFC 8428 §6):
//   key -3 = BaseTime (float64)
//   key  0 = Name     (string)
//   key  2 = Value    (Number: int64 or float64)
//   key  3 = StringValue
//   key  4 = BoolValue
//   key  6 = Time     (float64, offset from BaseTime)

// ── Decode — inbound from cloud ───────────────────────────────────────────────

func TestDecode_Bool(t *testing.T) {
	// [{0: "test", 4: false}]
	// = 81 A2 00 64 74 65 73 74 04 F4
	payload := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x04, 0xF4}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, "test", vars[0].Name)
	require.Equal(t, senml.TypeBool, vars[0].Type)
	require.Equal(t, false, vars[0].Value)
}

func TestDecode_BoolTrue(t *testing.T) {
	// [{0: "test", 4: true}]
	// = 81 A2 00 64 74 65 73 74 04 F5
	payload := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x04, 0xF5}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, true, vars[0].Value)
}

func TestDecode_IntPositive(t *testing.T) {
	// [{0: "test", 2: 7}]
	// = 81 A2 00 64 74 65 73 74 02 07
	payload := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x02, 0x07}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, "test", vars[0].Name)
	require.Equal(t, senml.TypeInt, vars[0].Type)
	require.Equal(t, int64(7), vars[0].Value)
}

func TestDecode_IntNegative(t *testing.T) {
	// [{0: "test", 2: -7}]
	// = 81 A2 00 64 74 65 73 74 02 26
	// CBOR major type 1 (negative): 0x26 = -(0x06+1) = -7
	payload := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x02, 0x26}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, senml.TypeInt, vars[0].Type)
	require.Equal(t, int64(-7), vars[0].Value)
}

func TestDecode_IntZero(t *testing.T) {
	// [{0: "test", 2: 0}]
	// = 81 A2 00 64 74 65 73 74 02 00
	payload := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x02, 0x00}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, senml.TypeInt, vars[0].Type)
	require.Equal(t, int64(0), vars[0].Value)
}

func TestDecode_Float(t *testing.T) {
	// [{0: "test", 2: 3.1459}]
	// = 81 A2 00 64 74 65 73 74 02 FB 40 09 2A CD 9E 83 E4 26
	// FB = CBOR float64
	payload := []byte{
		0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74,
		0x02, 0xFB, 0x40, 0x09, 0x2A, 0xCD, 0x9E, 0x83, 0xE4, 0x26,
	}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, "test", vars[0].Name)
	require.Equal(t, senml.TypeFloat, vars[0].Type)
	got, ok := vars[0].Value.(float64)
	require.True(t, ok, "expected float64, got %T", vars[0].Value)
	require.InDelta(t, 3.1459, got, 0.0001)
}

func TestDecode_String(t *testing.T) {
	// [{0: "test", 3: "testtt"}]
	// = 81 A2 00 64 74 65 73 74 03 66 74 65 73 74 74 74
	payload := []byte{
		0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74,
		0x03, 0x66, 0x74, 0x65, 0x73, 0x74, 0x74, 0x74,
	}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 1)
	require.Equal(t, senml.TypeString, vars[0].Type)
	require.Equal(t, "testtt", vars[0].Value)
}

func TestDecode_MultipleVariables(t *testing.T) {
	// Two records: bool + int
	// [{0:"flag",4:true},{0:"count",2:42}]
	// 82
	//   A2 00 64 66 6C 61 67 04 F5     {0:"flag",4:true}
	//   A2 00 65 63 6F 75 6E 74 02 18 2A  {0:"count",2:42}
	payload := []byte{
		0x82,
		0xA2, 0x00, 0x64, 0x66, 0x6C, 0x61, 0x67, 0x04, 0xF5,
		0xA2, 0x00, 0x65, 0x63, 0x6F, 0x75, 0x6E, 0x74, 0x02, 0x18, 0x2A,
	}

	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, vars, 2)

	byName := make(map[string]senml.Variable)
	for _, v := range vars {
		byName[v.Name] = v
	}

	flag, ok := byName["flag"]
	require.True(t, ok)
	require.Equal(t, true, flag.Value)

	count, ok := byName["count"]
	require.True(t, ok)
	require.Equal(t, int64(42), count.Value)
}

func TestDecode_WithBaseTime(t *testing.T) {
	// Record with BaseName + BaseTime + Time offset.
	// [{-2:"sensor",-3:1631707445.0,0:"temp",2:42.0,6:0.0}]
	// Decoded timestamp should be Unix(1631707445, 0).
	//
	// Payload: 81 A5 21 66 73 65 6E 73 6F 72 22 FB 41 D8 4C 4A F5 00 00 00 00 64 74 65 6D 70 02 FA 42 28 00 00 06 00
	// Build it programmatically to avoid transcription errors.
	baseTime := time.Date(2021, 9, 15, 12, 4, 5, 0, time.UTC)

	// Encode a variable with timestamp, then decode and verify.
	vars := []senml.Variable{
		{Name: "temp", Type: senml.TypeFloat, Value: float64(42.0), Timestamp: baseTime},
	}
	payload, err := senml.Encode(vars)
	require.NoError(t, err)

	decoded, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, decoded, 1)
	require.Equal(t, "temp", decoded[0].Name)
	require.InDelta(t, 42.0, decoded[0].Value.(float64), 0.001)
	require.True(t, decoded[0].Timestamp.Equal(baseTime),
		"timestamp mismatch: got %v, want %v", decoded[0].Timestamp, baseTime)
}

// ── Encode — outbound to cloud ────────────────────────────────────────────────

func TestEncode_Bool(t *testing.T) {
	// [{0: "test", 4: false}] = 81 A2 00 64 74 65 73 74 04 F4
	want := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x04, 0xF4}

	got, err := senml.Encode([]senml.Variable{
		{Name: "test", Type: senml.TypeBool, Value: false},
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_IntPositive(t *testing.T) {
	// [{0: "test", 2: 7}] = 81 A2 00 64 74 65 73 74 02 07
	want := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x02, 0x07}

	got, err := senml.Encode([]senml.Variable{
		{Name: "test", Type: senml.TypeInt, Value: int64(7)},
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_String(t *testing.T) {
	// [{0: "test", 3: "testtt"}] = 81 A2 00 64 74 65 73 74 03 66 74 65 73 74 74 74
	want := []byte{
		0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74,
		0x03, 0x66, 0x74, 0x65, 0x73, 0x74, 0x74, 0x74,
	}

	got, err := senml.Encode([]senml.Variable{
		{Name: "test", Type: senml.TypeString, Value: "testtt"},
	})
	require.NoError(t, err)
	require.Equal(t, want, got)
}

func TestEncode_EmptySlice(t *testing.T) {
	got, err := senml.Encode(nil)
	require.NoError(t, err)
	require.Equal(t, []byte{}, got)
}

func TestEncode_UnsupportedType(t *testing.T) {
	_, err := senml.Encode([]senml.Variable{
		{Name: "bad", Value: struct{ X int }{42}},
	})
	require.Error(t, err)
}

// ── Round-trip ────────────────────────────────────────────────────────────────

func TestRoundTrip_Bool(t *testing.T) {
	original := []senml.Variable{{Name: "flag", Type: senml.TypeBool, Value: true}}
	roundTrip(t, original)
}

func TestRoundTrip_Int(t *testing.T) {
	original := []senml.Variable{{Name: "counter", Type: senml.TypeInt, Value: int64(-1234)}}
	roundTrip(t, original)
}

func TestRoundTrip_Float(t *testing.T) {
	original := []senml.Variable{{Name: "temp", Type: senml.TypeFloat, Value: float64(23.5)}}
	vars := roundTrip(t, original)
	require.InDelta(t, 23.5, vars[0].Value.(float64), 0.0001)
}

func TestRoundTrip_String(t *testing.T) {
	original := []senml.Variable{{Name: "label", Type: senml.TypeString, Value: "hello"}}
	roundTrip(t, original)
}

func TestRoundTrip_MultipleVariables(t *testing.T) {
	ts := time.Unix(1700000000, 0).UTC()
	original := []senml.Variable{
		{Name: "temp", Type: senml.TypeFloat, Value: float64(22.3), Timestamp: ts},
		{Name: "hum", Type: senml.TypeFloat, Value: float64(60.0), Timestamp: ts},
		{Name: "on", Type: senml.TypeBool, Value: true, Timestamp: ts},
	}

	payload, err := senml.Encode(original)
	require.NoError(t, err)

	decoded, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, decoded, len(original))

	byName := make(map[string]senml.Variable)
	for _, v := range decoded {
		byName[v.Name] = v
	}

	require.InDelta(t, 22.3, byName["temp"].Value.(float64), 0.0001)
	require.InDelta(t, 60.0, byName["hum"].Value.(float64), 0.0001)
	require.Equal(t, true, byName["on"].Value)

	// All timestamps should survive the round-trip within 1s (BaseTime offsets
	// use float64 seconds, so sub-second precision can be lost for large unix epochs).
	for _, orig := range original {
		got := byName[orig.Name].Timestamp
		require.WithinDuration(t, orig.Timestamp, got, time.Second,
			"timestamp mismatch for variable %q", orig.Name)
	}
}

// ── Number type ───────────────────────────────────────────────────────────────

func TestNumber_IntPreservedThroughCBOR(t *testing.T) {
	// Verify that integers decoded from CBOR come back as int64, not float64.
	// Arduino IoT Cloud distinguishes int and float properties — confusing them
	// would cause silent type errors in the variable registry.
	payload := []byte{0x81, 0xA2, 0x00, 0x64, 0x74, 0x65, 0x73, 0x74, 0x02, 0x07} // 7
	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	_, isInt := vars[0].Value.(int64)
	require.True(t, isInt, "CBOR integer should decode as int64, got %T", vars[0].Value)
}

func TestNumber_FloatPreservedThroughCBOR(t *testing.T) {
	// Verify that floats decoded from CBOR come back as float64, not int64.
	// FB = CBOR float64; value = 1.5
	// [{0:"x",2:1.5}] = 81 A2 00 61 78 02 F9 3E 00
	// F9 = half-float; 3E 00 = 1.5 in IEEE 754 half
	payload := []byte{0x81, 0xA2, 0x00, 0x61, 0x78, 0x02, 0xF9, 0x3E, 0x00}
	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	_, isFloat := vars[0].Value.(float64)
	require.True(t, isFloat, "CBOR float should decode as float64, got %T", vars[0].Value)
	require.InDelta(t, 1.5, vars[0].Value.(float64), 0.001)
}

// ── Error cases ───────────────────────────────────────────────────────────────

func TestDecode_InvalidCBOR(t *testing.T) {
	_, err := senml.Decode([]byte{0xFF, 0xFE})
	require.Error(t, err)
}

func TestDecode_EmptyName(t *testing.T) {
	// A VALUE-bearing record with no Name and no BaseName — should fail
	// validation (a value with no name is unaddressable).
	// [{2: 42}] = 81 A1 02 18 2A
	payload := []byte{0x81, 0xA1, 0x02, 0x18, 0x2A}
	_, err := senml.Decode(payload)
	require.Error(t, err)
}

// TestDecode_ColoredLightWithBaseRecords is the regression for the daemon
// silently dropping every ColoredLight update: a multi-value property message
// may carry value-less BASE records (BaseName/BaseTime), which are valid SenML
// (RFC 8428). They must be skipped, not cause the whole message to be rejected
// with "record has no value or sum field". Symptom before the fix: on_write
// never fired for a ColoredLight while it worked for a scalar variable.
func TestDecode_ColoredLightWithBaseRecords(t *testing.T) {
	want := []string{"clight:swi", "clight:hue", "clight:sat", "clight:bri"}

	// Layout A: a value-less BaseTime record, then full-name attribute records.
	varsA := decodeRecords(t, []senml.Record{
		{BaseTime: 1_700_000_000},
		{Name: "clight:swi", BoolValue: boolPtr(true)},
		{Name: "clight:hue", Value: senml.Float64Number(30).Ptr()},
		{Name: "clight:sat", Value: senml.Float64Number(50).Ptr()},
		{Name: "clight:bri", Value: senml.Float64Number(70).Ptr()},
	})
	require.Equal(t, want, varNames(varsA))

	// Layout B: a value-less BaseName+BaseTime record, then suffix-name records
	// resolved against the base name (clight: + swi = clight:swi).
	varsB := decodeRecords(t, []senml.Record{
		{BaseName: "clight:", BaseTime: 1_700_000_000},
		{Name: "swi", BoolValue: boolPtr(false)},
		{Name: "hue", Value: senml.Float64Number(30).Ptr()},
		{Name: "sat", Value: senml.Float64Number(50).Ptr()},
		{Name: "bri", Value: senml.Float64Number(70).Ptr()},
	})
	require.Equal(t, want, varNames(varsB))
}

// TestDecode_BaseOnlyRecordsProduceNoVariables: a message consisting only of
// base/context records is valid and simply yields no variables (not an error).
func TestDecode_BaseOnlyRecordsProduceNoVariables(t *testing.T) {
	vars := decodeRecords(t, []senml.Record{
		{BaseTime: 1_700_000_000},
		{BaseName: "clight:"},
	})
	require.Empty(t, vars)
}

// ── helpers ───────────────────────────────────────────────────────────────────

func boolPtr(v bool) *bool { return &v }

// decodeRecords marshals hand-built SenML records to CBOR (using the same
// keyasint tags the cloud uses on the wire) and decodes them via senml.Decode.
func decodeRecords(t *testing.T, recs []senml.Record) []senml.Variable {
	t.Helper()
	payload, err := cbor.Marshal(recs)
	require.NoError(t, err)
	vars, err := senml.Decode(payload)
	require.NoError(t, err)
	return vars
}

func varNames(vars []senml.Variable) []string {
	names := make([]string, len(vars))
	for i, v := range vars {
		names[i] = v.Name
	}
	return names
}

// roundTrip encodes vars and decodes them back, asserting no errors and that
// the name, type and value survive unchanged. Returns decoded vars for further
// assertions by the caller.
func roundTrip(t *testing.T, vars []senml.Variable) []senml.Variable {
	t.Helper()
	payload, err := senml.Encode(vars)
	require.NoError(t, err)

	decoded, err := senml.Decode(payload)
	require.NoError(t, err)
	require.Len(t, decoded, len(vars))

	for i, orig := range vars {
		require.Equal(t, orig.Name, decoded[i].Name, "name mismatch at index %d", i)
		require.Equal(t, orig.Type, decoded[i].Type, "type mismatch at index %d", i)
		require.Equal(t, orig.Value, decoded[i].Value, "value mismatch at index %d", i)
	}
	return decoded
}
