// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package wire reads and writes the MQTT payload formats of the Arduino IoT
// Cloud protocol -- the CBOR-tagged commands and the SenML property records --
// INDEPENDENTLY of the daemon.
//
// # Why not import the daemon's codec
//
// Because then the suite would only prove that the code agrees with itself. A
// wrong tag number, a reordered array field or a mislabelled SenML key would be
// encoded and decoded identically on both sides, the test would pass, and the
// real peer -- the C++ ArduinoIoTCloud library on one end, the cloud on the
// other -- would still reject the bytes. These formats are a contract with
// external parties, not an internal detail.
//
// So this package speaks CBOR through its own minimal reader and writer rather
// than through fxamacker/cbor with a TagSet, which is what the daemon uses. The
// technique differs on purpose: a copy of the same approach catches later drift
// but not a mistake already present in both.
//
// # Where the independence is anchored
//
// Two implementations that agree prove nothing on their own, so crosscheck_test.go
// (the file depguard exempts) encodes with one side and decodes with the other,
// both ways, for every command and for SenML. A divergence there says which
// side changed instead of leaving a scenario to time out.
//
// # This file: the CBOR subset
//
// RFC 8949, restricted to what the protocol uses: unsigned and negative
// integers, byte and text strings, arrays, maps, tags, booleans, null and the
// three float widths. Indefinite-length items are rejected rather than
// half-supported -- the daemon never emits them, and a silent partial parse
// would be worse than a named error.
package wire

import (
	"encoding/binary"
	"fmt"
	"math"
)

// CBOR major types (RFC 8949 section 3.1), spelled out so the encoder reads
// like the specification.
const (
	majorUint   byte = 0
	majorNegInt byte = 1
	majorBytes  byte = 2
	majorText   byte = 3
	majorArray  byte = 4
	majorMap    byte = 5
	majorTag    byte = 6
	majorSimple byte = 7
)

// Additional-information values with a meaning of their own.
const (
	aiOneByte    = 24
	aiTwoBytes   = 25
	aiFourBytes  = 26
	aiEightBytes = 27
	aiIndefinite = 31

	simpleFalse = 20
	simpleTrue  = 21
	simpleNull  = 22
)

// Tag is a CBOR tagged value: the protocol wraps every command in one.
type Tag struct {
	Number  uint64
	Content any
}

// ── writing ──────────────────────────────────────────────────────────────────

// appendHead writes an initial byte plus its argument in the shortest form,
// which is what "canonical" means for the lengths and integers here and what
// the daemon's encoder also produces.
func appendHead(b []byte, major byte, arg uint64) []byte {
	switch {
	case arg < aiOneByte:
		return append(b, major<<5|byte(arg))
	case arg <= math.MaxUint8:
		return append(b, major<<5|aiOneByte, byte(arg))
	case arg <= math.MaxUint16:
		b = append(b, major<<5|aiTwoBytes)
		return binary.BigEndian.AppendUint16(b, uint16(arg))
	case arg <= math.MaxUint32:
		b = append(b, major<<5|aiFourBytes)
		return binary.BigEndian.AppendUint32(b, uint32(arg))
	default:
		b = append(b, major<<5|aiEightBytes)
		return binary.BigEndian.AppendUint64(b, arg)
	}
}

func appendUint(b []byte, v uint64) []byte { return appendHead(b, majorUint, v) }

func appendInt(b []byte, v int64) []byte {
	if v < 0 {
		// A negative integer encodes -1-n, so -1 is argument 0.
		return appendHead(b, majorNegInt, uint64(-(v + 1)))
	}
	return appendHead(b, majorUint, uint64(v))
}

func appendFloat64(b []byte, v float64) []byte {
	// Always eight bytes. Shortening to float32 or float16 would be legal CBOR
	// and is what the firmware does, but the harness is the peer that has to be
	// predictable: a scenario asserting on bytes should not depend on whether a
	// value happened to fit.
	b = append(b, majorSimple<<5|aiEightBytes)
	return binary.BigEndian.AppendUint64(b, math.Float64bits(v))
}

func appendBool(b []byte, v bool) []byte {
	if v {
		return append(b, majorSimple<<5|simpleTrue)
	}
	return append(b, majorSimple<<5|simpleFalse)
}

func appendText(b []byte, s string) []byte {
	b = appendHead(b, majorText, uint64(len(s)))
	return append(b, s...)
}

func appendBytes(b, v []byte) []byte {
	b = appendHead(b, majorBytes, uint64(len(v)))
	return append(b, v...)
}

func appendArrayHeader(b []byte, n int) []byte { return appendHead(b, majorArray, uint64(n)) }
func appendMapHeader(b []byte, n int) []byte   { return appendHead(b, majorMap, uint64(n)) }
func appendTag(b []byte, number uint64) []byte { return appendHead(b, majorTag, number) }

// appendValue writes one of the Go types the protocol uses. It exists so array
// and map contents can be written generically; everything else in this package
// uses the typed helpers above.
func appendValue(b []byte, v any) ([]byte, error) {
	switch x := v.(type) {
	case nil:
		return append(b, majorSimple<<5|simpleNull), nil
	case bool:
		return appendBool(b, x), nil
	case int:
		return appendInt(b, int64(x)), nil
	case int64:
		return appendInt(b, x), nil
	case uint64:
		return appendUint(b, x), nil
	case float64:
		return appendFloat64(b, x), nil
	case string:
		return appendText(b, x), nil
	case []byte:
		return appendBytes(b, x), nil
	case Tag:
		b = appendTag(b, x.Number)
		return appendValue(b, x.Content)
	case []any:
		b = appendArrayHeader(b, len(x))
		for _, item := range x {
			var err error
			if b, err = appendValue(b, item); err != nil {
				return nil, err
			}
		}
		return b, nil
	default:
		return nil, fmt.Errorf("wire: cannot encode %T as CBOR", v)
	}
}

// ── reading ──────────────────────────────────────────────────────────────────

// Decode reads exactly one CBOR item and reports trailing bytes as an error:
// a payload with something after the command is malformed, and ignoring the
// remainder would hide it.
func Decode(data []byte) (any, error) {
	v, rest, err := decodeValue(data)
	if err != nil {
		return nil, err
	}
	if len(rest) != 0 {
		return nil, fmt.Errorf("wire: %d trailing bytes after the CBOR item", len(rest))
	}
	return v, nil
}

// decodeValue reads one item and returns it with whatever follows.
//
// Integers come back as int64 whenever they fit, and as uint64 only when they
// do not. Keeping them apart matters: SenML labels are negative, and a
// protocol field read as a huge positive number instead of a small negative one
// is the kind of mistake that looks like a missing field.
func decodeValue(data []byte) (any, []byte, error) {
	if len(data) == 0 {
		return nil, nil, fmt.Errorf("wire: unexpected end of CBOR input")
	}
	major := data[0] >> 5
	ai := data[0] & 0x1f

	if ai == aiIndefinite {
		return nil, nil, fmt.Errorf("wire: indefinite-length item (major type %d) is not supported", major)
	}
	if ai > aiEightBytes && ai < aiIndefinite && major != majorSimple {
		return nil, nil, fmt.Errorf("wire: reserved additional information %d for major type %d", ai, major)
	}

	switch major {
	case majorSimple:
		return decodeSimple(data)
	case majorUint:
		arg, rest, err := decodeArg(data)
		if err != nil {
			return nil, nil, err
		}
		if arg > math.MaxInt64 {
			return arg, rest, nil
		}
		return int64(arg), rest, nil
	case majorNegInt:
		arg, rest, err := decodeArg(data)
		if err != nil {
			return nil, nil, err
		}
		if arg > math.MaxInt64 {
			return nil, nil, fmt.Errorf("wire: negative integer -%d-1 does not fit in an int64", arg)
		}
		return -1 - int64(arg), rest, nil
	case majorBytes, majorText:
		arg, rest, err := decodeArg(data)
		if err != nil {
			return nil, nil, err
		}
		if arg > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("wire: string claims %d bytes, %d available", arg, len(rest))
		}
		content, rest := rest[:arg], rest[arg:]
		if major == majorText {
			return string(content), rest, nil
		}
		// Copied, and never nil: the caller keeps the value while the read
		// buffer may be reused, and an empty byte string (an unset Ethernet
		// address, say) should read as empty rather than as absent.
		out := make([]byte, arg)
		copy(out, content)
		return out, rest, nil
	case majorArray:
		arg, rest, err := decodeArg(data)
		if err != nil {
			return nil, nil, err
		}
		if arg > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("wire: array claims %d items, only %d bytes left", arg, len(rest))
		}
		out := make([]any, 0, arg)
		for range arg {
			var item any
			if item, rest, err = decodeValue(rest); err != nil {
				return nil, nil, err
			}
			out = append(out, item)
		}
		return out, rest, nil
	case majorMap:
		arg, rest, err := decodeArg(data)
		if err != nil {
			return nil, nil, err
		}
		if arg > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("wire: map claims %d pairs, only %d bytes left", arg, len(rest))
		}
		out := make(map[any]any, arg)
		for range arg {
			var k, v any
			if k, rest, err = decodeValue(rest); err != nil {
				return nil, nil, err
			}
			if v, rest, err = decodeValue(rest); err != nil {
				return nil, nil, err
			}
			out[k] = v
		}
		return out, rest, nil
	default: // majorTag
		arg, rest, err := decodeArg(data)
		if err != nil {
			return nil, nil, err
		}
		content, rest, err := decodeValue(rest)
		if err != nil {
			return nil, nil, err
		}
		return Tag{Number: arg, Content: content}, rest, nil
	}
}

// decodeArg reads the initial byte and its argument, returning the argument and
// the bytes after it.
func decodeArg(data []byte) (uint64, []byte, error) {
	ai := data[0] & 0x1f
	rest := data[1:]
	switch {
	case ai < aiOneByte:
		return uint64(ai), rest, nil
	case ai == aiOneByte:
		if len(rest) < 1 {
			return 0, nil, fmt.Errorf("wire: truncated 1-byte CBOR argument")
		}
		return uint64(rest[0]), rest[1:], nil
	case ai == aiTwoBytes:
		if len(rest) < 2 {
			return 0, nil, fmt.Errorf("wire: truncated 2-byte CBOR argument")
		}
		return uint64(binary.BigEndian.Uint16(rest)), rest[2:], nil
	case ai == aiFourBytes:
		if len(rest) < 4 {
			return 0, nil, fmt.Errorf("wire: truncated 4-byte CBOR argument")
		}
		return uint64(binary.BigEndian.Uint32(rest)), rest[4:], nil
	default: // aiEightBytes
		if len(rest) < 8 {
			return 0, nil, fmt.Errorf("wire: truncated 8-byte CBOR argument")
		}
		return binary.BigEndian.Uint64(rest), rest[8:], nil
	}
}

// decodeSimple handles major type 7: the booleans, null, and the floats.
//
// All three float widths are read even though the daemon emits only doubles.
// The firmware shortens floats, so the day a device-side change reaches this
// path the harness should read the value rather than fail to parse it.
func decodeSimple(data []byte) (any, []byte, error) {
	ai := data[0] & 0x1f
	rest := data[1:]
	switch ai {
	case simpleFalse:
		return false, rest, nil
	case simpleTrue:
		return true, rest, nil
	case simpleNull:
		return nil, rest, nil
	case aiTwoBytes: // half precision
		if len(rest) < 2 {
			return nil, nil, fmt.Errorf("wire: truncated half-precision float")
		}
		return float16to64(binary.BigEndian.Uint16(rest)), rest[2:], nil
	case aiFourBytes: // single precision
		if len(rest) < 4 {
			return nil, nil, fmt.Errorf("wire: truncated single-precision float")
		}
		return float64(math.Float32frombits(binary.BigEndian.Uint32(rest))), rest[4:], nil
	case aiEightBytes: // double precision
		if len(rest) < 8 {
			return nil, nil, fmt.Errorf("wire: truncated double-precision float")
		}
		return math.Float64frombits(binary.BigEndian.Uint64(rest)), rest[8:], nil
	default:
		return nil, nil, fmt.Errorf("wire: unsupported simple value %d", ai)
	}
}

// float16to64 converts an IEEE 754 binary16 to a float64, by the definition
// rather than by bit-shuffling: 5 exponent bits with a bias of 15, 10 mantissa
// bits, and the usual zero/subnormal/infinity/NaN special cases.
func float16to64(u uint16) float64 {
	negative := u&0x8000 != 0
	exponent := int(u>>10) & 0x1f
	mantissa := int(u & 0x03ff)

	var value float64
	switch exponent {
	case 0:
		if mantissa == 0 {
			if negative {
				return math.Copysign(0, -1)
			}
			return 0
		}
		value = float64(mantissa) * math.Exp2(-24) // subnormal
	case 0x1f:
		if mantissa != 0 {
			return math.NaN()
		}
		if negative {
			return math.Inf(-1)
		}
		return math.Inf(1)
	default:
		value = (1 + float64(mantissa)/1024) * math.Exp2(float64(exponent-15))
	}
	if negative {
		return -value
	}
	return value
}
