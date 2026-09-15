// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"strings"
	"testing"
)

func TestBagInterpolate(t *testing.T) {
	bag := NewBag()
	bag.Set("device_id", "9f1c2d3e")
	bag.Set("thing_id", "b2c3d4e5")

	tests := []struct {
		in   string
		want string
	}{
		{"", ""},
		{"nothing to do", "nothing to do"},
		{"{device_id}", "9f1c2d3e"},
		{"/a/d/{device_id}/c/dw", "/a/d/9f1c2d3e/c/dw"},
		{"/a/t/{thing_id}/e/i", "/a/t/b2c3d4e5/e/i"},
		{"{device_id}-{thing_id}", "9f1c2d3e-b2c3d4e5"},
	}
	for _, tc := range tests {
		got, err := bag.Interpolate(tc.in)
		if err != nil {
			t.Errorf("Interpolate(%q): %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("Interpolate(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// An empty value is a value: the first Thing.begin carries thing_id="" and a
// scenario has to be able to say so.
func TestBagInterpolateEmptyValue(t *testing.T) {
	bag := NewBag()
	bag.Set("thing_id", "")

	got, err := bag.Interpolate("[{thing_id}]")
	if err != nil {
		t.Fatalf("Interpolate: %v", err)
	}
	if got != "[]" {
		t.Errorf("got %q, want %q", got, "[]")
	}
}

// An unknown placeholder must fail loudly. Left as a literal it would build a
// topic that silently never matches, and the report would blame the
// expectation instead of the scenario.
func TestBagInterpolateRejectsUnknownPlaceholders(t *testing.T) {
	bag := NewBag()
	bag.Set("device_id", "9f1c2d3e")

	_, err := bag.Interpolate("/a/t/{thing_id}/e/i")
	if err == nil {
		t.Fatal("an unknown placeholder was accepted")
	}
	if !strings.Contains(err.Error(), "thing_id") {
		t.Errorf("the error should name the placeholder: %v", err)
	}
	// And it should say what IS available, which is what makes the message
	// actionable.
	if !strings.Contains(err.Error(), "device_id") {
		t.Errorf("the error should list the keys the bag holds: %v", err)
	}
}

func TestBagInterpolateRejectsMalformedPlaceholders(t *testing.T) {
	bag := NewBag()
	for _, in := range []string{"{unclosed", "{}", "a {b"} {
		if got, err := bag.Interpolate(in); err == nil {
			t.Errorf("Interpolate(%q) = %q, want an error", in, got)
		}
	}
}

func TestBagGetSetKeys(t *testing.T) {
	bag := NewBag()
	if _, ok := bag.Get("nope"); ok {
		t.Error("an empty bag returned a value")
	}
	bag.Set("b", "2")
	bag.Set("a", "1")
	bag.Set("a", "1-updated")

	if got, _ := bag.Get("a"); got != "1-updated" {
		t.Errorf("Get(a) = %q, want the updated value", got)
	}
	if got := bag.Keys(); len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Errorf("Keys() = %v, want a sorted [a b]", got)
	}
	if got := bag.MustGet("b"); got != "2" {
		t.Errorf("MustGet(b) = %q", got)
	}

	snapshot := bag.Snapshot()
	snapshot["a"] = "tampered"
	if got, _ := bag.Get("a"); got == "tampered" {
		t.Error("Snapshot returned a live map, not a copy")
	}
}

func TestBagMustGetPanicsOnAMissingKey(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("MustGet on a missing key did not panic")
		}
	}()
	NewBag().MustGet("device_id")
}
