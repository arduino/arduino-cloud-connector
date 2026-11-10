// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
)

// The streams are written out by hand, as arduino-app-cli's render/sse.go writes
// them; see install_test.go for why.

// lifecycleServer answers one POST on path with stream.
func lifecycleServer(t *testing.T, path, stream string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.EscapedPath() != path {
			t.Errorf("request: got %s %s want POST %s", r.Method, r.URL.EscapedPath(), path)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, stream)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A start has no done event: it succeeded when the stream closes with no error.
// Shaped like a real one (RFC-16 §5): the sketch and python phases, compose lines,
// then 100 and the closing pair.
func TestStartSucceedsWhenTheStreamClosesWithoutAnError(t *testing.T) {
	stream := "event: message\ndata: {\"message\":\"Starting app \\\"blink\\\"\"}\n\n" +
		"event: progress\ndata: {\"name\":\"preparing\",\"progress\":0}\n\n" +
		"event: progress\ndata: {\"name\":\"uploading sketch\",\"progress\":0}\n\n" +
		"event: progress\ndata: {\"name\":\"sketch updated\",\"progress\":10}\n\n" +
		"event: progress\ndata: {\"name\":\"python provisioning\",\"progress\":10}\n\n" +
		"event: message\ndata: {\"message\":\"Container blink-main-1 Started\"}\n\n" +
		"event: progress\ndata: {\"name\":\"\",\"progress\":100}\n\n" +
		streamClosed
	// The id is base64url, so it can carry '-' and '_', and must reach the path as is.
	srv := lifecycleServer(t, "/v1/apps/dXNlcjpibGluaw-_/start", stream)

	var seen []int32
	if err := New(srv.URL).Start(context.Background(), "dXNlcjpibGluaw-_", func(p int32) { seen = append(seen, p) }); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if want := []int32{0, 0, 10, 10, 100}; !slices.Equal(seen, want) {
		t.Errorf("progress: got %v want %v", seen, want)
	}
}

// Every start failure of RFC-16 §5, as arduino-app-cli sends it once the prefixes are
// in.
func TestStartClassifiesStreamErrors(t *testing.T) {
	cases := []struct {
		name, code, message string
		want                error
	}{
		{"container missing", "not_prepared",
			"app is not prepared: the container \"img:tag\" is not on the board", ErrNotPrepared},
		{"model missing", "not_prepared",
			"app is not prepared: model \"m\" for brick \"b\" is not installed", ErrNotPrepared},
		{"another app running", "INTERNAL_SERVER_ERROR",
			"AnotherAppRunning: app \"other\" is running", ErrAnotherAppRunning},
		{"brick validation", "INTERNAL_SERVER_ERROR",
			"AppNotCompatible: brick \"b\" not found", ErrNotCompatible},
		{"no camera", "INTERNAL_SERVER_ERROR",
			"AppNotCompatible: no camera device found", ErrNotCompatible},
		{"pull out of space", "INTERNAL_SERVER_ERROR",
			"NoDiskSpace: not enough disk space to pull the docker image", ErrNoSpace},
		{"port conflict", "INTERNAL_SERVER_ERROR",
			"port 8080 is declared by multiple sources: x", ErrFailed},
		{"compose up", "INTERNAL_SERVER_ERROR",
			"failed to create the network of the app: x", ErrFailed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := "event: progress\ndata: {\"progress\":0}\n\n" +
				fmt.Sprintf("event: error\ndata: {\"code\":%q,\"message\":%q}\n\n", tc.code, tc.message) +
				streamClosed
			err := New(lifecycleServer(t, "/v1/apps/x/start", stream).URL).Start(context.Background(), "x", func(int32) {})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			assertOnlySpecific(t, err, tc.want)
		})
	}
}

// Without the closing pair the start's outcome is unknown: the connection dropped.
func TestStartCutBeforeTheCloseIsUnavailable(t *testing.T) {
	stream := "event: progress\ndata: {\"progress\":10}\n\n"
	err := New(lifecycleServer(t, "/v1/apps/x/start", stream).URL).Start(context.Background(), "x", func(int32) {})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}

// Should arduino-app-cli add a done event to the start, it is a success too.
func TestStartAcceptsADoneEvent(t *testing.T) {
	stream := "event: done\ndata: {}\n\n" + streamClosed
	if err := New(lifecycleServer(t, "/v1/apps/x/start", stream).URL).Start(context.Background(), "x", func(int32) {}); err != nil {
		t.Fatalf("Start: %v", err)
	}
}

// A start refused before the stream: an unknown App is a 500 "unable to find the app"
// — a failed command, not an unreachable arduino-app-cli, unlike the install's
// unprefixed 500.
func TestStartRefusalsAreFailures(t *testing.T) {
	for name, tc := range map[string]struct {
		status  int
		details string
		want    error
	}{
		"unknown app": {http.StatusInternalServerError, "unable to find the app", ErrFailed},
		"invalid id":  {http.StatusPreconditionFailed, "invalid id", ErrFailed},
		"no endpoint": {http.StatusNotFound, "", ErrNotCompatible},
	} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(tc.status)
				_, _ = fmt.Fprintf(w, "{\"details\":%q}\n", tc.details)
			}))
			defer srv.Close()
			err := New(srv.URL).Start(context.Background(), "x", func(int32) {})
			if !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			assertOnlySpecific(t, err, tc.want)
		})
	}
}

func TestStartUnreachableIsUnavailable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	if err := New(url).Start(context.Background(), "x", func(int32) {}); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("got %v, want ErrUnavailable", err)
	}
}
