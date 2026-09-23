// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package ntp

import (
	"net"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

func newTestServer(t *testing.T) (*Server, *eventlog.Log) {
	t.Helper()
	log := eventlog.New()
	srv, err := Start(log)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srv, log
}

// probe does exactly what the daemon does: dial UDP, write 48 bytes, read 48
// bytes, validate nothing.
func probe(t *testing.T, addr string, timeout time.Duration) (int, error) {
	t.Helper()
	conn, err := net.Dial("udp", addr)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close() //nolint:errcheck
	if err := conn.SetDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatalf("set deadline: %v", err)
	}
	if _, err := conn.Write(make([]byte, probeSize)); err != nil {
		t.Fatalf("write: %v", err)
	}
	return conn.Read(make([]byte, 64))
}

// The whole contract: a 48-byte datagram comes back, and the probe is on the
// timeline so a daemon stuck in Disconnected can be told apart from one that
// never checked.
func TestProbeIsAnsweredAndRecorded(t *testing.T) {
	srv, log := newTestServer(t)

	n, err := probe(t, srv.Addr(), 5*time.Second)
	if err != nil {
		t.Fatalf("no reply: %v", err)
	}
	if n != probeSize {
		t.Errorf("reply is %d bytes, want %d", n, probeSize)
	}
	if got := srv.Probes(); got != 1 {
		t.Errorf("Probes() = %d, want 1", got)
	}

	events := log.Events()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(events))
	}
	e := events[0]
	if e.Source != eventlog.SourceNTP || e.Kind != eventlog.KindNTPProbe {
		t.Errorf("event is %s/%s, want ntp/ntp_probe", e.Source, e.Kind)
	}
	// Connectivity probes are context, not claims: they must never take part
	// in the strict unconsumed sweep, or every scenario would have to tolerate
	// however many times the daemon happened to poll.
	if e.Significant() {
		t.Error("an NTP probe must not be significant")
	}
	for name, want := range map[string]any{"occurrence": 1, "bytes": probeSize, "replied": true} {
		if got, ok := e.Attrs[name]; !ok || got != want {
			t.Errorf("attrs[%q] = %v, want %v", name, got, want)
		}
	}
}

// Silent mode is how a scenario takes connectivity away: the daemon reads the
// same "no internet" verdict it would get from a dead uplink, and nothing has
// to touch its process or its configuration.
func TestSilentModeStopsAnsweringAndCanBeRestored(t *testing.T) {
	srv, log := newTestServer(t)
	srv.SetSilent(true)

	if _, err := probe(t, srv.Addr(), 300*time.Millisecond); err == nil {
		t.Fatal("the silent responder answered")
	}
	// The datagram still arrived, and the row says it went unanswered.
	if got := srv.Probes(); got != 1 {
		t.Errorf("Probes() = %d, want 1: the datagram was received even though it was not answered", got)
	}
	events := log.Events()
	if len(events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(events))
	}
	if got := events[0].Attrs["replied"]; got != false {
		t.Errorf("attrs[replied] = %v, want false", got)
	}

	srv.SetSilent(false)
	if _, err := probe(t, srv.Addr(), 5*time.Second); err != nil {
		t.Fatalf("connectivity was not restored: %v", err)
	}
}

// The reply carries a plausible NTP header even though the daemon never looks
// at it: nothing depends on it, and it costs one byte to keep a real client
// from seeing garbage.
func TestReplyLooksLikeAnNTPServerResponse(t *testing.T) {
	r := reply()
	if len(r) != probeSize {
		t.Fatalf("reply is %d bytes, want %d", len(r), probeSize)
	}
	// leap 0, version 3, mode 4 (server)
	if r[0] != 0x1C {
		t.Errorf("first byte = %#x, want 0x1c", r[0])
	}
}

func TestAddrIsLoopbackAndStable(t *testing.T) {
	srv, _ := newTestServer(t)

	host, port, err := net.SplitHostPort(srv.Addr())
	if err != nil {
		t.Fatalf("Addr() = %q: %v", srv.Addr(), err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host = %q, want 127.0.0.1", host)
	}
	if port == "0" {
		t.Error("port is 0, want the bound port")
	}
}

func TestStartRequiresALog(t *testing.T) {
	if srv, err := Start(nil); err == nil {
		_ = srv.Close()
		t.Fatal("Start(nil) succeeded, want an error")
	}
}

// Teardown must be idempotent: the harness closes fakes on the way out of
// every scenario, and a double close must not panic or report a failure.
func TestCloseIsIdempotent(t *testing.T) {
	log := eventlog.New()
	srv, err := Start(log)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
