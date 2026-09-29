// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package handlers_test

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/api/handlers"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// These tests cover the wiring between the HTTP layer and the registry's origin
// exclusion: that the X-App-Client-ID sent on the SSE subscription actually
// reaches Registry.Subscribe. The exclusion rules themselves are pinned in
// internal/variables/echo_test.go.

type sseFrame struct {
	event string
	data  string
}

// openSSE subscribes to a variable's event stream, optionally identifying the
// caller, and returns the decoded frames as they arrive. The returned func
// closes the stream.
func openSSE(t *testing.T, srvURL, name, clientID string) (<-chan sseFrame, func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srvURL+"/v1/variables/"+name+"/events", nil)
	if err != nil {
		cancel()
		t.Fatalf("NewRequest: %v", err)
	}
	if clientID != "" {
		req.Header.Set("X-App-Client-ID", clientID)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		t.Fatalf("Do: %v", err)
	}

	frames := make(chan sseFrame, 8)
	go func() {
		defer close(frames)
		defer resp.Body.Close() //nolint:errcheck
		reader := bufio.NewReader(resp.Body)
		var f sseFrame
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			switch s := strings.TrimRight(line, "\n"); {
			case s == "": // blank line terminates a frame
				select {
				case frames <- f:
				case <-ctx.Done():
					return
				}
				f = sseFrame{}
			case strings.HasPrefix(s, "event:"):
				f.event = strings.TrimSpace(strings.TrimPrefix(s, "event:"))
			case strings.HasPrefix(s, "data:"):
				f.data = strings.TrimSpace(strings.TrimPrefix(s, "data:"))
			}
		}
	}()
	return frames, cancel
}

func wantFrame(t *testing.T, frames <-chan sseFrame, what string) sseFrame {
	t.Helper()
	select {
	case f, ok := <-frames:
		if !ok {
			t.Fatalf("stream closed while waiting for %s", what)
		}
		return f
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return sseFrame{}
	}
}

func wantNoFrame(t *testing.T, frames <-chan sseFrame, d time.Duration) {
	t.Helper()
	select {
	case f, ok := <-frames:
		if !ok {
			t.Fatal("stream closed unexpectedly")
		}
		t.Fatalf("received %s frame %s, want none", f.event, f.data)
	case <-time.After(d):
	}
}

func frameValue(t *testing.T, f sseFrame) any {
	t.Helper()
	var payload map[string]any
	if err := json.Unmarshal([]byte(f.data), &payload); err != nil {
		t.Fatalf("unmarshal %q: %v", f.data, err)
	}
	return payload["value"]
}

func eventsServer(t *testing.T, reg *variables.Registry) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle("GET /v1/variables/{name}/events", handlers.HandleVariableEvents(reg, fakeSteady{true}))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// The id on the subscription request must reach the registry, so a value this
// same app wrote is not streamed back to it — while another app's write still
// is. Without the header reaching Subscribe there is nothing to match on and
// the app is handed back its own write.
func TestVariableEventsSuppressesTheSubscribersOwnWrite(t *testing.T) {
	reg := variables.NewRegistry()
	srv := eventsServer(t, reg)

	frames, closeStream := openSSE(t, srv.URL, "counter", "app-1")
	defer closeStream()
	if first := wantFrame(t, frames, "the first sync frame"); first.event != string(variables.EventLastValueMissing) {
		t.Fatalf("first frame = %q, want %q", first.event, variables.EventLastValueMissing)
	}

	reg.SetValue("counter", float64(1), time.Now().UTC(), "app-1")
	wantNoFrame(t, frames, 300*time.Millisecond)

	reg.SetValue("counter", float64(2), time.Now().UTC(), "app-2")
	got := wantFrame(t, frames, "the other app's update")
	if got.event != string(variables.EventUpdate) || frameValue(t, got) != float64(2) {
		t.Errorf("frame = %s %v, want %s 2", got.event, frameValue(t, got), variables.EventUpdate)
	}
}

// An id longer than the cap is treated as absent, so the subscriber keeps the
// pre-header behaviour and still receives everything. If the cap is ever
// raised above this length the test fails loudly (the frame gets suppressed)
// rather than quietly testing nothing.
func TestVariableEventsIgnoresAnOverlongClientID(t *testing.T) {
	reg := variables.NewRegistry()
	srv := eventsServer(t, reg)

	overlong := strings.Repeat("a", 129) // one past maxClientIDLen in handlers.go
	frames, closeStream := openSSE(t, srv.URL, "counter", overlong)
	defer closeStream()
	wantFrame(t, frames, "the first sync frame")

	reg.SetValue("counter", float64(3), time.Now().UTC(), overlong)
	got := wantFrame(t, frames, "the update an ignored id must not suppress")
	if frameValue(t, got) != float64(3) {
		t.Errorf("frame value = %v, want 3", frameValue(t, got))
	}
}
