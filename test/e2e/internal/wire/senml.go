// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package wire

import (
	"fmt"
	"sort"
)

// SenML integer labels (RFC 8428 section 6), the CBOR representation the
// property channel uses (/a/t/<thing_id>/e/i and /e/o). Written out for the
// same reason as the command tags: a mislabelled field must break the suite,
// not be hidden behind a shared constant.
const (
	labelBaseVersion int64 = -1
	labelBaseName    int64 = -2
	labelBaseTime    int64 = -3
	labelBaseUnit    int64 = -4
	labelBaseValue   int64 = -5
	labelBaseSum     int64 = -6

	labelName        int64 = 0
	labelUnit        int64 = 1
	labelValue       int64 = 2
	labelStringValue int64 = 3
	labelBoolValue   int64 = 4
	labelSum         int64 = 5
	labelTime        int64 = 6
	labelUpdateTime  int64 = 7
	labelDataValue   int64 = 8
)

// Value is one property value: the unit a scenario asserts on and the unit it
// injects.
type Value struct {
	Name string
	// Value is int64, float64, string or bool -- the four types the Arduino
	// property channel carries.
	Value any
	// Time is the raw SenML time, in the units the payload used (seconds,
	// milliseconds and microseconds all occur in practice, distinguished by
	// magnitude). Zero means the record carried no timestamp. It stays raw
	// because converting would mean guessing the unit, and a scenario that
	// cares can compare the number it injected.
	Time float64
}

// DecodeSenML parses a SenML+CBOR payload into its values.
//
// Base fields are resolved the way RFC 8428 specifies: a base applies to the
// record that carries it and to those that follow, until another base replaces
// it. Note that the daemon's own decoder instead lets the LAST base name in
// the array win for every record; the two agree for everything the daemon
// emits (it only ever puts a base time on the first record) and this is the
// side that has to be right about the format.
//
// Records with no value field are context records: per the RFC they carry base
// fields for the records that follow and contribute no measurement, so they
// are skipped rather than rejected. Rejecting them once made an entire
// multi-attribute property message fail to decode.
func DecodeSenML(payload []byte) ([]Value, error) {
	v, err := Decode(payload)
	if err != nil {
		return nil, fmt.Errorf("wire: decode senml: %w", err)
	}
	records, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("wire: senml payload is %T, want an array of records", v)
	}

	var (
		baseName string
		baseTime float64
		out      []Value
	)
	for i, raw := range records {
		record, ok := raw.(map[any]any)
		if !ok {
			return nil, fmt.Errorf("wire: senml record %d is %T, want a map", i, raw)
		}
		if s, present, err := recordString(record, labelBaseName, "base name", i); err != nil {
			return nil, err
		} else if present {
			baseName = s
		}
		if f, present, err := recordFloat(record, labelBaseTime, "base time", i); err != nil {
			return nil, err
		} else if present {
			baseTime = f
		}

		name, _, err := recordString(record, labelName, "name", i)
		if err != nil {
			return nil, err
		}
		value, hasValue, err := recordValue(record, i)
		if err != nil {
			return nil, err
		}
		if !hasValue {
			continue // a context record
		}
		fullName := baseName + name
		if fullName == "" {
			return nil, fmt.Errorf("wire: senml record %d carries a value with no name", i)
		}
		t, _, err := recordFloat(record, labelTime, "time", i)
		if err != nil {
			return nil, err
		}
		out = append(out, Value{Name: fullName, Value: value, Time: t + baseTime})
	}
	return out, nil
}

// recordValue reads whichever value field the record carries, and refuses a
// record carrying more than one: per RFC 8428 that is invalid, and picking one
// silently would make the harness report a value the daemon never saw.
func recordValue(record map[any]any, index int) (any, bool, error) {
	var (
		found any
		count int
	)
	if v, ok := record[labelValue]; ok {
		count++
		switch n := v.(type) {
		case int64, float64:
			found = n
		case uint64:
			return nil, false, fmt.Errorf("wire: senml record %d value %d is too large for an int64", index, n)
		default:
			return nil, false, fmt.Errorf("wire: senml record %d value is %T, want a number", index, v)
		}
	}
	if v, ok := record[labelStringValue]; ok {
		count++
		s, ok := v.(string)
		if !ok {
			return nil, false, fmt.Errorf("wire: senml record %d string value is %T", index, v)
		}
		found = s
	}
	if v, ok := record[labelBoolValue]; ok {
		count++
		b, ok := v.(bool)
		if !ok {
			return nil, false, fmt.Errorf("wire: senml record %d boolean value is %T", index, v)
		}
		found = b
	}
	if v, ok := record[labelDataValue]; ok {
		count++
		s, ok := v.(string)
		if !ok {
			return nil, false, fmt.Errorf("wire: senml record %d data value is %T", index, v)
		}
		found = s
	}
	if count > 1 {
		return nil, false, fmt.Errorf("wire: senml record %d carries %d value fields, want at most 1", index, count)
	}
	return found, count == 1, nil
}

func recordString(record map[any]any, label int64, what string, index int) (string, bool, error) {
	v, ok := record[label]
	if !ok {
		return "", false, nil
	}
	s, ok := v.(string)
	if !ok {
		return "", false, fmt.Errorf("wire: senml record %d %s is %T, want a text string", index, what, v)
	}
	return s, true, nil
}

func recordFloat(record map[any]any, label int64, what string, index int) (float64, bool, error) {
	v, ok := record[label]
	if !ok {
		return 0, false, nil
	}
	switch n := v.(type) {
	case int64:
		return float64(n), true, nil
	case float64:
		return n, true, nil
	default:
		return 0, false, fmt.Errorf("wire: senml record %d %s is %T, want a number", index, what, v)
	}
}

// EncodeSenML writes values as a SenML+CBOR array, one record each.
//
// Deliberately flat: no base name, no base time, every record complete on its
// own. Base fields would shrink the payload and are what the daemon emits, but
// as the sending side the harness gains nothing from them and loses something
// real -- the daemon resolves a base name differently from the RFC, so a
// payload relying on one would be testing an agreement rather than the format.
//
// Records are sorted by name so a scenario's payload is byte-identical between
// runs, which keeps a failure diff readable.
func EncodeSenML(values []Value) ([]byte, error) {
	sorted := make([]Value, len(values))
	copy(sorted, values)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })

	b := appendArrayHeader(nil, len(sorted))
	for _, v := range sorted {
		if v.Name == "" {
			return nil, fmt.Errorf("wire: senml value with no name (%v)", v.Value)
		}
		label, err := valueLabel(v.Value)
		if err != nil {
			return nil, fmt.Errorf("wire: senml value %q: %w", v.Name, err)
		}

		pairs := 2 // name and value
		if v.Time != 0 {
			pairs++
		}
		b = appendMapHeader(b, pairs)
		b = appendInt(b, labelName)
		b = appendText(b, v.Name)
		b = appendInt(b, label)
		if b, err = appendValue(b, normalizeValue(v.Value)); err != nil {
			return nil, fmt.Errorf("wire: senml value %q: %w", v.Name, err)
		}
		if v.Time != 0 {
			b = appendInt(b, labelTime)
			b = appendFloat64(b, v.Time)
		}
	}
	return b, nil
}

// valueLabel picks the SenML label for a Go value: numbers are v, strings vs,
// booleans vb.
func valueLabel(v any) (int64, error) {
	switch v.(type) {
	case int, int64, float64:
		return labelValue, nil
	case string:
		return labelStringValue, nil
	case bool:
		return labelBoolValue, nil
	default:
		return 0, fmt.Errorf("unsupported type %T, want an integer, float, string or bool", v)
	}
}

// normalizeValue maps the types a scenario can produce onto the types the CBOR
// writer takes. YAML yields int for a whole number, and the integer/float
// distinction is preserved rather than flattened: an integer property that
// arrives as a float is a real difference to the variable registry.
func normalizeValue(v any) any {
	if n, ok := v.(int); ok {
		return int64(n)
	}
	return v
}
