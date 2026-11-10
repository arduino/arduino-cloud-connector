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

// The RFC-16 vectors are written out by hand from the RFC's tables: there is no C++
// reference for these messages, and the daemon's encoder is the thing under test.

func TestAppCommandGoldenBytes(t *testing.T) {
	one := int64(1)
	tests := []struct {
		name string
		got  []byte
		want string
	}{
		{
			// tag(0x13001) [ "app1:0.1.0", 1, "op-1", 1 ]
			name: "start with a parameter",
			got:  EncodeAppCommand("app1:0.1.0", 1, "op-1", &one),
			want: "da00013001 84 6a617070313a302e312e30 01 646f702d31 01",
		},
		{
			// tag(0x13001) [ "app1:0.1.0", 0, "op-1" ] -- no parameter, three elements
			name: "stop without a parameter",
			got:  EncodeAppCommand("app1:0.1.0", 0, "op-1", nil),
			want: "da00013001 83 6a617070313a302e312e30 00 646f702d31",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			want := strings.ReplaceAll(tc.want, " ", "")
			if got := hex.EncodeToString(tc.got); got != want {
				t.Errorf("got  %s\nwant %s", got, want)
			}
		})
	}
}

func TestDecodeAppUplinks(t *testing.T) {
	tests := []struct {
		name       string
		hex        string
		wantName   string
		wantFields map[string]any
	}{
		{
			// [ "op-1", 0 ] -- success carries no details
			name:     "succeeded",
			hex:      "da00013002 82 646f702d31 00",
			wantName: CmdAppCommandResponse,
			wantFields: map[string]any{
				"op_id": "op-1", "status": "ok", "elements": 2,
			},
		},
		{
			// [ "op-1", 2, 50 ]
			name:     "in progress",
			hex:      "da00013002 83 646f702d31 02 1832",
			wantName: CmdAppCommandResponse,
			wantFields: map[string]any{
				"op_id": "op-1", "status": "progress", "elements": 3, "details": int64(50),
			},
		},
		{
			// [ "op-1", 1, -2 ]
			name:     "failed",
			hex:      "da00013002 83 646f702d31 01 21",
			wantName: CmdAppCommandResponse,
			wantFields: map[string]any{
				"op_id": "op-1", "status": "fail", "elements": 3, "details": int64(-2),
			},
		},
		{
			// [ "app1:0.1.0", 1, true, "app2:1.0.0", -1, false ]
			name:     "apps list",
			hex:      "da00013000 86 6a617070313a302e312e30 01 f5 6a617070323a312e302e30 20 f4",
			wantName: CmdAppsList,
			wantFields: map[string]any{
				"apps": "app1:0.1.0=running*,app2:1.0.0=failure", "count": 2,
			},
		},
		{
			name:       "empty apps list",
			hex:        "da00013000 80",
			wantName:   CmdAppsList,
			wantFields: map[string]any{"apps": "", "count": 0},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := hex.DecodeString(strings.ReplaceAll(tc.hex, " ", ""))
			if err != nil {
				t.Fatal(err)
			}
			cmd, err := DecodeCommand(payload)
			if err != nil {
				t.Fatalf("DecodeCommand: %v", err)
			}
			if cmd.Name != tc.wantName {
				t.Errorf("name = %q, want %q", cmd.Name, tc.wantName)
			}
			if !reflect.DeepEqual(cmd.Fields, tc.wantFields) {
				t.Errorf("fields =\n  %#v\nwant\n  %#v", cmd.Fields, tc.wantFields)
			}
		})
	}
}

func TestDecodeAppUplinksRejectsAWrongShape(t *testing.T) {
	for name, h := range map[string]string{
		"response with one element":    "da00013002 81 646f702d31",
		"response with four elements":  "da00013002 84 646f702d31 01 21 00",
		"apps list with a partial app": "da00013000 82 6a617070313a302e312e30 01",
		"apps list default not a bool": "da00013000 83 6a617070313a302e312e30 01 01",
	} {
		t.Run(name, func(t *testing.T) {
			payload, err := hex.DecodeString(strings.ReplaceAll(h, " ", ""))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := DecodeCommand(payload); err == nil {
				t.Errorf("DecodeCommand accepted %s", h)
			}
		})
	}
}

func TestAppCommandCodeIsTheInverseOfItsName(t *testing.T) {
	for code, name := range appCommandNames {
		got, ok := AppCommandCode(name)
		if !ok || got != code {
			t.Errorf("AppCommandCode(%q) = %d, %v; want %d", name, got, ok, code)
		}
	}
	if _, ok := AppCommandCode("restart"); ok {
		t.Error("AppCommandCode accepted a command RFC-16 does not have")
	}
}
