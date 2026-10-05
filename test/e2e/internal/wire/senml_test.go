// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package wire

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"
)

// Golden bytes, assembled by hand from RFC 8428 section 6: the labels are
// integers (0 = name, 2 = value), so the record is a CBOR map keyed by number.
// A mislabelled field is the failure this pins down, and it is invisible to a
// round-trip test.
func TestEncodeSenMLGoldenBytes(t *testing.T) {
	got, err := EncodeSenML([]Value{{Name: "temp", Value: 21.5}})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	// [ { 0: "temp", 2: 21.5 } ]
	want := strings.ReplaceAll("81 a2 00 6474656d70 02 fb4035800000000000", " ", "")
	if hex.EncodeToString(got) != want {
		t.Errorf("got  %x\nwant %s", got, want)
	}
}

func TestSenMLRoundTrip(t *testing.T) {
	values := []Value{
		{Name: "temp", Value: 21.5},
		{Name: "count", Value: int64(7)},
		{Name: "label", Value: "on"},
		{Name: "switch", Value: true},
		{Name: "stamped", Value: 1.25, Time: 1726300000},
	}

	payload, err := EncodeSenML(values)
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	got, err := DecodeSenML(payload)
	if err != nil {
		t.Fatalf("DecodeSenML: %v", err)
	}

	// Encoding sorts by name, so compare as a map.
	byName := map[string]Value{}
	for _, v := range got {
		byName[v.Name] = v
	}
	for _, want := range values {
		g, ok := byName[want.Name]
		if !ok {
			t.Errorf("%q is missing from the decoded payload", want.Name)
			continue
		}
		if !reflect.DeepEqual(g, want) {
			t.Errorf("%q = %#v, want %#v", want.Name, g, want)
		}
	}
}

// The integer/float distinction has to survive: an integer property that comes
// back as a float is a real difference to the daemon's variable registry, not a
// formatting detail.
func TestSenMLKeepsIntegersAndFloatsApart(t *testing.T) {
	payload, err := EncodeSenML([]Value{
		{Name: "i", Value: 7},
		{Name: "f", Value: 7.0},
	})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	got, err := DecodeSenML(payload)
	if err != nil {
		t.Fatalf("DecodeSenML: %v", err)
	}
	for _, v := range got {
		switch v.Name {
		case "i":
			if _, ok := v.Value.(int64); !ok {
				t.Errorf("i = %#v (%T), want an int64", v.Value, v.Value)
			}
		case "f":
			if _, ok := v.Value.(float64); !ok {
				t.Errorf("f = %#v (%T), want a float64", v.Value, v.Value)
			}
		}
	}
}

// Base name and base time are resolved the RFC way: they apply from the record
// that carries them onward. This is also the shape the daemon's encoder
// produces (a base time on the first record), which is the case that matters.
func TestDecodeSenMLResolvesBaseFields(t *testing.T) {
	// [ { -2: "thing:", -3: 1000, 0: "a", 2: 1 },
	//   { 0: "b", 2: 2, 6: 5 } ]
	payload := mustHex(t, "82"+
		"a4 21 6674 68696e673a 22 1903e8 00 6161 02 01"+
		"a3 00 6162 02 02 06 05")

	got, err := DecodeSenML(payload)
	if err != nil {
		t.Fatalf("DecodeSenML: %v", err)
	}
	want := []Value{
		{Name: "thing:a", Value: int64(1), Time: 1000},
		{Name: "thing:b", Value: int64(2), Time: 1005},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %#v\nwant %#v", got, want)
	}
}

// A record with base fields and no value is a context record: it configures
// the records that follow and contributes no measurement. Rejecting those once
// made a whole multi-attribute property message fail to decode, dropping every
// attribute of the property.
func TestDecodeSenMLSkipsContextRecords(t *testing.T) {
	// [ { -2: "light:" }, { 0: "swi", 4: true }, { 0: "bri", 2: 50 } ]
	payload := mustHex(t, "83"+
		"a1 21 66 6c696768743a"+
		"a2 00 63 737769 04 f5"+
		"a2 00 63 627269 02 1832")

	got, err := DecodeSenML(payload)
	if err != nil {
		t.Fatalf("DecodeSenML: %v", err)
	}
	want := []Value{
		{Name: "light:swi", Value: true},
		{Name: "light:bri", Value: int64(50)},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %#v\nwant %#v", got, want)
	}
}

func TestDecodeSenMLRejectsMalformedRecords(t *testing.T) {
	tests := []struct {
		name    string
		payload string
	}{
		{"not an array", "a0"},
		{"record is not a map", "8101"},
		// { 0: "a", 2: 1, 3: "x" } -- both a numeric and a string value
		{"two value fields", "81 a3 00 6161 02 01 03 6178"},
		// { 2: 1 } -- a value with no name
		{"value with no name", "81 a1 02 01"},
		// { 0: 1, 2: 1 } -- name is not a string
		{"name is not a string", "81 a2 00 01 02 01"},
		// { 0: "a", 2: "x" } -- numeric value slot holding a string
		{"numeric value slot holds a string", "81 a2 00 6161 02 6178"},
		// { 0: "a", 4: 1 } -- boolean value slot holding a number
		{"boolean value slot holds a number", "81 a2 00 6161 04 01"},
		// { 0: "a", 2: 1, 6: "x" } -- time is not a number
		{"time is not a number", "81 a3 00 6161 02 01 06 6178"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if v, err := DecodeSenML(mustHex(t, tc.payload)); err == nil {
				t.Errorf("DecodeSenML succeeded: %#v", v)
			}
		})
	}
}

func TestEncodeSenMLRejectsWhatItCannotRepresent(t *testing.T) {
	if _, err := EncodeSenML([]Value{{Name: "", Value: 1}}); err == nil {
		t.Error("a value with no name was encoded, want an error")
	}
	if _, err := EncodeSenML([]Value{{Name: "x", Value: []int{1}}}); err == nil {
		t.Error("a slice value was encoded, want an error")
	}
}

// An empty batch is a valid empty array, not an error: a scenario may inject
// "no last values", which is what the cloud sends for a thing with no
// properties.
func TestEncodeSenMLEmptyBatch(t *testing.T) {
	payload, err := EncodeSenML(nil)
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	if hex.EncodeToString(payload) != "80" {
		t.Errorf("got %x, want 80 (an empty array)", payload)
	}
	got, err := DecodeSenML(payload)
	if err != nil {
		t.Fatalf("DecodeSenML: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %#v, want no values", got)
	}
}

// The labels themselves, restated so a change has to be deliberate. They are
// the RFC 8428 section 6 numbers.
func TestSenMLLabelNumbers(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  int64
		want int64
	}{
		{"base version", labelBaseVersion, -1},
		{"base name", labelBaseName, -2},
		{"base time", labelBaseTime, -3},
		{"base unit", labelBaseUnit, -4},
		{"base value", labelBaseValue, -5},
		{"base sum", labelBaseSum, -6},
		{"name", labelName, 0},
		{"unit", labelUnit, 1},
		{"value", labelValue, 2},
		{"string value", labelStringValue, 3},
		{"bool value", labelBoolValue, 4},
		{"sum", labelSum, 5},
		{"time", labelTime, 6},
		{"update time", labelUpdateTime, 7},
		{"data value", labelDataValue, 8},
	} {
		if tc.got != tc.want {
			t.Errorf("%s label = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}
