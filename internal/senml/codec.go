// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package senml

import (
	"cmp"
	"fmt"
	"slices"
	"time"

	"github.com/fxamacker/cbor/v2"
)

// ── Variable ─────────────────────────────────────────────────────────────────

// ValueType classifies the Go type of a decoded property value.
type ValueType string

const (
	TypeFloat  ValueType = "float"
	TypeInt    ValueType = "int"
	TypeString ValueType = "string"
	TypeBool   ValueType = "bool"
)

// Variable is a single decoded SenML property value with its name, typed Go
// value, and optional timestamp. It is the unit exchanged with the variable
// registry.
type Variable struct {
	Name      string
	Type      ValueType
	Value     interface{} // int64 | float64 | string | bool
	Timestamp time.Time
}

// ── Decode ────────────────────────────────────────────────────────────────────

// Decode parses a SenML+CBOR payload from the property channel and returns the
// contained variables. The payload is the raw []byte value of
// LastValuesUpdateCmd.Values or a property-topic message.
//
// Multi-value properties (name "property:field") are returned as separate
// Variable entries with their colon-delimited names intact; the caller
// (variable registry) is responsible for reassembly into composite values if
// needed.
func Decode(payload []byte) ([]Variable, error) {
	var records []Record
	if err := cborDecoder.Unmarshal(payload, &records); err != nil {
		return nil, fmt.Errorf("senml decode: %w", err)
	}
	if err := validate(records); err != nil {
		return nil, fmt.Errorf("senml decode: %w", err)
	}
	return toVariables(records), nil
}

// toVariables converts a validated slice of Records into Variables, resolving
// BaseName and BaseTime.
func toVariables(records []Record) []Variable {
	var baseName string
	var baseTime float64

	for _, r := range records {
		if r.BaseName != "" {
			baseName = r.BaseName
		}
		if r.BaseTime != 0 {
			baseTime = r.BaseTime
		}
	}

	vars := make([]Variable, 0, len(records))
	for _, r := range records {
		name := baseName + r.Name
		if name == "" {
			continue
		}
		ts := resolveTime(r.Time + baseTime)
		v := variableFromRecord(name, r, ts)
		if v != nil {
			vars = append(vars, *v)
		}
	}
	return vars
}

func variableFromRecord(name string, r Record, ts time.Time) *Variable {
	switch {
	case r.Value != nil:
		if r.Value.IsInt64() {
			return &Variable{Name: name, Type: TypeInt, Value: r.Value.Int64(), Timestamp: ts}
		}
		return &Variable{Name: name, Type: TypeFloat, Value: r.Value.Float64(), Timestamp: ts}
	case r.StringValue != nil:
		return &Variable{Name: name, Type: TypeString, Value: *r.StringValue, Timestamp: ts}
	case r.BoolValue != nil:
		return &Variable{Name: name, Type: TypeBool, Value: *r.BoolValue, Timestamp: ts}
	}
	return nil
}

// resolveTime converts a SenML time value (seconds since Unix epoch, possibly
// with fractional part; or zero meaning "no timestamp") to time.Time.
// Sub-second precision is preserved: values >1e11 are treated as ms, >1e14 μs,
// >1e17 ns (matches the Arduino IoT Cloud conventions used in mariquita).
func resolveTime(senmlTime float64) time.Time {
	if senmlTime == 0 {
		return time.Time{}
	}
	switch {
	case senmlTime > 1e17:
		return time.Unix(0, int64(senmlTime)).UTC()
	case senmlTime > 1e14:
		return time.UnixMicro(int64(senmlTime)).UTC()
	case senmlTime > 1e11:
		return time.UnixMilli(int64(senmlTime)).UTC()
	default:
		sec := int64(senmlTime)
		nsec := int64((senmlTime - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC()
	}
}

// ── Encode ────────────────────────────────────────────────────────────────────

// Encode serialises a slice of Variables to SenML+CBOR for publishing on the
// property outbound topic (/a/t/<thing_id>/e/o).
//
// BaseTime is set to the earliest timestamp in the batch; individual record
// Time fields become offsets from BaseTime, which reduces wire size.
func Encode(vars []Variable) ([]byte, error) {
	if len(vars) == 0 {
		return []byte{}, nil
	}

	records, err := toRecords(vars)
	if err != nil {
		return nil, err
	}

	// Compute BaseTime = min(record.Time). Records with zero Time (no
	// timestamp) are not affected.
	minTime := records[0].Time
	for _, r := range records[1:] {
		if r.Time != 0 && (minTime == 0 || r.Time < minTime) {
			minTime = r.Time
		}
	}
	if minTime != 0 {
		records[0].BaseTime = minTime
		for i := range records {
			records[i].Time -= minTime
		}
	}

	b, err := cbor.Marshal(records)
	if err != nil {
		return nil, fmt.Errorf("senml encode: %w", err)
	}
	return b, nil
}

// toRecords converts Variables to SenML Records, sorting multi-value fields
// for stable output.
func toRecords(vars []Variable) ([]Record, error) {
	records := make([]Record, 0, len(vars))
	for _, v := range vars {
		r, err := recordFromVariable(v)
		if err != nil {
			return nil, err
		}
		records = append(records, r)
	}

	// Sort by name for deterministic wire output.
	slices.SortFunc(records, func(a, b Record) int {
		return cmp.Compare(a.Name, b.Name)
	})
	return records, nil
}

func recordFromVariable(v Variable) (Record, error) {
	t := encodeTime(v.Timestamp)
	switch val := v.Value.(type) {
	case int64:
		n := Int64Number(val)
		return Record{Name: v.Name, Value: n.Ptr(), Time: t}, nil
	case int:
		n := Int64Number(int64(val))
		return Record{Name: v.Name, Value: n.Ptr(), Time: t}, nil
	case float64:
		n := Float64Number(val)
		return Record{Name: v.Name, Value: n.Ptr(), Time: t}, nil
	case string:
		return Record{Name: v.Name, StringValue: &val, Time: t}, nil
	case bool:
		return Record{Name: v.Name, BoolValue: &val, Time: t}, nil
	default:
		return Record{}, fmt.Errorf("senml: unsupported value type %T for variable %q", v.Value, v.Name)
	}
}

// encodeTime converts time.Time to the SenML float64 time representation.
// Zero time encodes as 0.0 (omitted from the wire by omitempty).
// Sub-second precision is preserved using the same strategy as resolveTime.
func encodeTime(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	sec := t.Unix()
	nsec := t.Nanosecond()
	switch {
	case nsec == 0:
		return float64(sec)
	case nsec%1_000_000 == 0:
		return float64(t.UnixMilli())
	case nsec%1_000 == 0:
		return float64(t.UnixMicro())
	default:
		return float64(sec)*1e9 + float64(nsec)
	}
}
