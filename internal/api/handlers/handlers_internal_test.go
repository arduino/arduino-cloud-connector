// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package handlers

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// errWriter is an io.Writer whose Write always fails, used to exercise the
// write-error path of writeSSE.
type errWriter struct{ err error }

func (e errWriter) Write([]byte) (int, error) { return 0, e.err }

// writeSSE must emit exactly "event: <name>\ndata: <json>\n\n" with the JSON
// payload copied in verbatim, and report no error on a normal write.
func TestWriteSSEFrameFormat(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSSE(&buf, variables.EventLastValue, map[string]string{"name": "temp"}); err != nil {
		t.Fatalf("writeSSE: %v", err)
	}

	want := "event: lastvalue\ndata: {\"name\":\"temp\"}\n\n"
	if got := buf.String(); got != want {
		t.Errorf("frame = %q, want %q", got, want)
	}
}

// The payload is appended as raw bytes (never used as a printf format string),
// so JSON containing verb-like characters (%s, %d, %) is copied literally with
// no format injection. Guards against a regression to an fmt.Fprintf form that
// concatenates data into the format string.
func TestWriteSSEDoesNotInterpretPayloadAsFormat(t *testing.T) {
	var buf bytes.Buffer
	if err := writeSSE(&buf, variables.EventUpdate, map[string]string{"note": "50% off, %s and %d"}); err != nil {
		t.Fatalf("writeSSE: %v", err)
	}

	want := "event: update\ndata: {\"note\":\"50% off, %s and %d\"}\n\n"
	if got := buf.String(); got != want {
		t.Errorf("frame = %q, want %q", got, want)
	}
}

// A payload whose JSON exceeds maxSSEPayload is rejected with an error and
// nothing is written — the guard that keeps the frame-size computation from
// overflowing int. Nothing is half-emitted onto the stream.
func TestWriteSSERejectsOversizedPayload(t *testing.T) {
	var buf bytes.Buffer
	// The JSON wrapper ({"v":"..."}) adds a few bytes, so a value of exactly
	// maxSSEPayload bytes already pushes the marshalled payload over the bound.
	big := strings.Repeat("x", maxSSEPayload)
	err := writeSSE(&buf, variables.EventUpdate, map[string]string{"v": big})
	if err == nil {
		t.Fatal("expected an error for an oversized payload, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("nothing should be written on rejection, wrote %d bytes", buf.Len())
	}
}

// When json.Marshal fails, writeSSE returns the wrapped error and writes
// nothing (the previous implementation silently swallowed it).
func TestWriteSSEReturnsMarshalError(t *testing.T) {
	var buf bytes.Buffer
	err := writeSSE(&buf, variables.EventThingUnavailable, make(chan int)) // channels are not JSON-marshalable
	if err == nil {
		t.Fatal("expected a marshal error, got nil")
	}
	if buf.Len() != 0 {
		t.Errorf("nothing should be written on marshal failure, wrote %d bytes", buf.Len())
	}
}

// A failing write is surfaced (wrapped) to the caller so the SSE handler can
// end the stream instead of ignoring the error.
func TestWriteSSEReturnsWriteError(t *testing.T) {
	sentinel := errors.New("connection reset")
	err := writeSSE(errWriter{err: sentinel}, variables.EventLastValueMissing, map[string]string{"name": "temp"})
	if !errors.Is(err, sentinel) {
		t.Fatalf("err = %v, want it to wrap %v", err, sentinel)
	}
}
