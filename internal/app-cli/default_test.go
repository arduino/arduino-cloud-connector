// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// The request is what arduino-app-cli's HandleAppDetailsEdits decodes: a PATCH on the
// App, with only the default field set — a release accepts no other field.
func TestSetDefaultPatchesOnlyTheDefaultField(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPatch || r.URL.EscapedPath() != "/v1/apps/cmVsZWFzZTph-_" {
			t.Errorf("request: got %s %s", r.Method, r.URL.EscapedPath())
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatalf("body: %v", err)
		}
		if len(body) != 1 || body["default"] != true {
			t.Errorf("body: got %v want {default: true} alone", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"id":"cmVsZWFzZTph-_","name":"a","default":true}`)
	}))
	defer srv.Close()

	if err := New(srv.URL).SetDefault(context.Background(), "cmVsZWFzZTph-_"); err != nil {
		t.Fatalf("SetDefault: %v", err)
	}
}

// Every refusal is a failed command (RFC-16 §5: any error is -3), except no answer
// at all.
func TestSetDefaultRefusalsAreFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		status  int
		details string
	}{
		"invalid id":      {http.StatusPreconditionFailed, "invalid id"},
		"unknown app":     {http.StatusInternalServerError, "unable to find the app"},
		"invalid request": {http.StatusBadRequest, "invalid request"},
		"cannot edit":     {http.StatusInternalServerError, "unable to edit the app"},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprintf(w, "{\"details\":%q}\n", tc.details)
			}))
			defer srv.Close()
			err := New(srv.URL).SetDefault(context.Background(), "x")
			if !errors.Is(err, ErrFailed) {
				t.Fatalf("got %v, want ErrFailed", err)
			}
			assertOnlySpecific(t, err, ErrFailed)
		})
	}
}

func TestSetDefaultUnreachableIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	if err := New(url).SetDefault(context.Background(), "x"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}
