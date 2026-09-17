// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package wire

import (
	"encoding/hex"
	"math"
	"reflect"
	"strings"
	"testing"
)

// mustHex decodes a hex literal from a test table. Spaces are ignored, so a
// payload can be grouped by CBOR item and stay readable.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.ReplaceAll(s, " ", ""))
	if err != nil {
		t.Fatalf("bad hex %q in the test table: %v", s, err)
	}
	return b
}

// Decoding pinned against the example vectors in RFC 8949 Appendix A. These
// are the anchor for the reader: they come from the specification, not from
// either implementation, so they are what says which side is right when the
// harness and the daemon disagree.
func TestDecodeRFC8949Vectors(t *testing.T) {
	tests := []struct {
		hex  string
		want any
	}{
		{"00", int64(0)},
		{"01", int64(1)},
		{"0a", int64(10)},
		{"17", int64(23)},
		{"1818", int64(24)},
		{"1903e8", int64(1000)},
		{"1a000f4240", int64(1000000)},
		{"1b000000e8d4a51000", int64(1000000000000)},
		{"20", int64(-1)},
		{"29", int64(-10)},
		{"3903e7", int64(-1000)},
		{"f4", false},
		{"f5", true},
		{"f6", nil},
		{"fb3ff8000000000000", 1.5},
		{"fb7e37e43c8800759c", 1.0e+300},
		{"fbc010666666666666", -4.1},
		{"60", ""},
		{"6161", "a"},
		{"6449455446", "IETF"},
		{"62225c", `"\`},
		{"40", []byte{}},
		{"4401020304", []byte{1, 2, 3, 4}},
		{"80", []any{}},
		{"83010203", []any{int64(1), int64(2), int64(3)}},
		{"8301820203820405", []any{int64(1), []any{int64(2), int64(3)}, []any{int64(4), int64(5)}}},
		{"a0", map[any]any{}},
		{"a201020304", map[any]any{int64(1): int64(2), int64(3): int64(4)}},
		{"c11a514b67b0", Tag{Number: 1, Content: int64(1363896240)}},
		// The three float widths. The daemon emits doubles; the other two exist
		// because the firmware shortens floats.
		{"f93e00", 1.5},
		{"f9c400", -4.0},
		{"fa47c35000", 100000.0},
		{"f90001", math.Exp2(-24)}, // smallest half-precision subnormal
	}
	for _, tc := range tests {
		t.Run(tc.hex, func(t *testing.T) {
			got, err := Decode(mustHex(t, tc.hex))
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("Decode(%s) = %#v (%T), want %#v (%T)", tc.hex, got, got, tc.want, tc.want)
			}
		})
	}
}

// Infinities and NaN survive the half-precision path, which is the one place
// the conversion is written by hand rather than delegated to math.Float*frombits.
func TestDecodeHalfPrecisionSpecials(t *testing.T) {
	if got, err := Decode(mustHex(t, "f97c00")); err != nil || got != math.Inf(1) {
		t.Errorf("+Inf: got %v, %v", got, err)
	}
	if got, err := Decode(mustHex(t, "f9fc00")); err != nil || got != math.Inf(-1) {
		t.Errorf("-Inf: got %v, %v", got, err)
	}
	got, err := Decode(mustHex(t, "f97e00"))
	if err != nil {
		t.Fatalf("NaN: %v", err)
	}
	f, ok := got.(float64)
	if !ok || !math.IsNaN(f) {
		t.Errorf("NaN: got %#v, want NaN", got)
	}
	// Negative zero must keep its sign, or a round-trip silently changes value.
	got, err = Decode(mustHex(t, "f98000"))
	if err != nil {
		t.Fatalf("-0.0: %v", err)
	}
	if f, ok := got.(float64); !ok || !math.Signbit(f) || f != 0 {
		t.Errorf("-0.0: got %#v, want negative zero", got)
	}
}

// Every write path, checked against the same vectors from the other direction.
func TestAppendProducesRFC8949Vectors(t *testing.T) {
	tests := []struct {
		name string
		got  []byte
		want string
	}{
		{"uint 0", appendUint(nil, 0), "00"},
		{"uint 24", appendUint(nil, 24), "1818"},
		{"uint 1000", appendUint(nil, 1000), "1903e8"},
		{"uint 1e6", appendUint(nil, 1000000), "1a000f4240"},
		{"uint 1e12", appendUint(nil, 1000000000000), "1b000000e8d4a51000"},
		{"int -1", appendInt(nil, -1), "20"},
		{"int -10", appendInt(nil, -10), "29"},
		{"int -1000", appendInt(nil, -1000), "3903e7"},
		{"int 23", appendInt(nil, 23), "17"},
		{"float 1.5", appendFloat64(nil, 1.5), "fb3ff8000000000000"},
		{"false", appendBool(nil, false), "f4"},
		{"true", appendBool(nil, true), "f5"},
		{"text IETF", appendText(nil, "IETF"), "6449455446"},
		{"empty text", appendText(nil, ""), "60"},
		{"bytes", appendBytes(nil, []byte{1, 2, 3, 4}), "4401020304"},
		{"array header 3", appendArrayHeader(nil, 3), "83"},
		{"map header 1", appendMapHeader(nil, 1), "a1"},
		{"tag 1", appendTag(nil, 1), "c1"},
		// The command tags need a four-byte argument; getting the shortest-form
		// rule wrong here would change every command on the wire.
		{"tag 0x10700", appendTag(nil, TagDeviceBegin), "da00010700"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := hex.EncodeToString(tc.got); got != tc.want {
				t.Errorf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestAppendValueRoundTrip(t *testing.T) {
	values := []any{
		int64(0), int64(-1), int64(1000), int64(-1000),
		1.5, -4.1, 0.0,
		"", "hello", true, false, nil,
		[]byte{}, []byte{0xde, 0xad},
		[]any{int64(1), "two", 3.0, []any{true}},
		Tag{Number: TagThingUpdate, Content: []any{"thing-id"}},
	}
	for _, want := range values {
		encoded, err := appendValue(nil, want)
		if err != nil {
			t.Fatalf("appendValue(%#v): %v", want, err)
		}
		got, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode(%x): %v", encoded, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("round trip changed the value\n  got:  %#v\n  want: %#v", got, want)
		}
	}
}

func TestAppendValueRejectsUnsupportedTypes(t *testing.T) {
	if _, err := appendValue(nil, map[string]int{"a": 1}); err == nil {
		t.Error("a Go map was encoded, want an error")
	}
	if _, err := appendValue(nil, struct{}{}); err == nil {
		t.Error("a struct was encoded, want an error")
	}
}

// Malformed input must be named, not absorbed. A reader that quietly stops
// early would make a truncated payload look like a missing field.
func TestDecodeRejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name string
		hex  string
	}{
		{"empty", ""},
		{"trailing bytes", "0000"},
		{"truncated 1-byte argument", "18"},
		{"truncated 2-byte argument", "1901"},
		{"truncated 4-byte argument", "1a0102"},
		{"truncated 8-byte argument", "1b01020304"},
		{"string shorter than its length", "64494554"},
		{"string length beyond input", "7828"},
		{"array shorter than its count", "8301"},
		{"map missing a value", "a101"},
		{"indefinite-length byte string", "5f42010243030405ff"},
		{"indefinite-length array", "9f018202039f0405ffff"},
		{"reserved additional information", "1c"},
		{"unsupported simple value", "f0"},
		{"truncated half float", "f93e"},
		{"truncated double float", "fb3ff80000"},
		{"tag with no content", "c1"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if v, err := Decode(mustHex(t, tc.hex)); err == nil {
				t.Errorf("Decode(%s) = %#v, want an error", tc.hex, v)
			}
		})
	}
}

// An integer past int64 stays a uint64 rather than wrapping to a negative
// number, because a silently negative length or timestamp is worse than a type
// the caller has to notice.
func TestDecodeHugeUnsignedStaysUnsigned(t *testing.T) {
	got, err := Decode(mustHex(t, "1bffffffffffffffff"))
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != uint64(math.MaxUint64) {
		t.Errorf("got %#v (%T), want uint64 max", got, got)
	}
	if _, err := Decode(mustHex(t, "3bffffffffffffffff")); err == nil {
		t.Error("a negative integer below int64 min was accepted, want an error")
	}
}
