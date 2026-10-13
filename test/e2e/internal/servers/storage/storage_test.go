// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package storage

import (
	"bytes"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
)

type fixture struct {
	log    *eventlog.Log
	srv    *Server
	client *http.Client
}

func newFixture(t *testing.T) fixture {
	t.Helper()
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	log := eventlog.New()
	srv, err := Start(Options{Log: log, CA: ca})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{
			RootCAs: ca.Pool(), MinVersion: tls.VersionTLS12,
		}},
	}
	return fixture{log: log, srv: srv, client: client}
}

func (f fixture) get(t *testing.T, url, rng string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rng != "" {
		req.Header.Set("Range", rng)
	}
	resp, err := f.client.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp, body
}

func (f fixture) lastEvent(t *testing.T) eventlog.Event {
	t.Helper()
	evs := f.log.Events()
	if len(evs) == 0 {
		t.Fatal("no event recorded")
	}
	return evs[len(evs)-1]
}

// The daemon's request, as the downloader sends it: a bounded range that is
// longer than the object. The answer must be a 206 for the whole object, with a
// Content-Range the daemon can parse, and the digest must be of these bytes.
func TestServesARangedRequestAndItsDigestMatches(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.Add("app1", Bundle{App: "app1:0.1.0", Size: 1000}); err != nil {
		t.Fatal(err)
	}
	resp, body := f.get(t, f.srv.URL("app1"), "bytes=0-67108863")
	if resp.StatusCode != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", resp.StatusCode)
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 0-999/1000" {
		t.Errorf("Content-Range = %q", got)
	}
	sum := sha256.Sum256(body)
	want, _ := f.srv.SHA256("app1")
	if hex.EncodeToString(sum[:]) != want || len(body) != 1000 {
		t.Errorf("served %d bytes hashing to %x, want 1000 bytes hashing to %s", len(body), sum, want)
	}

	ev := f.lastEvent(t)
	if ev.Source != eventlog.SourceStorage || ev.Attrs["bundle"] != "app1" ||
		ev.Attrs["status"] != http.StatusPartialContent || ev.Attrs["occurrence"] != 1 ||
		ev.Attrs["respond"] != "ok" {
		t.Errorf("event = %v", ev)
	}
	// No client certificate was presented, and the fake says so instead of
	// refusing.
	if ev.Attrs["client_cert_cn"] != "" || ev.Attrs["client_cert_valid"] != false {
		t.Errorf("client certificate attributes = %v", ev.Attrs)
	}
}

func TestServesAMiddleRangeAndTheWholeObject(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.Add("app1", Bundle{Size: 100}); err != nil {
		t.Fatal(err)
	}
	_, whole := f.get(t, f.srv.URL("app1"), "")
	resp, part := f.get(t, f.srv.URL("app1"), "bytes=10-19")
	if resp.StatusCode != http.StatusPartialContent || !bytes.Equal(part, whole[10:20]) {
		t.Errorf("range 10-19: status %d, %x; want 206, %x", resp.StatusCode, part, whole[10:20])
	}
	if got := resp.Header.Get("Content-Range"); got != "bytes 10-19/100" {
		t.Errorf("Content-Range = %q", got)
	}
}

func TestDirectivesAreConsumedInOrderThenTheBundleIsServed(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.Add("app1", Bundle{Size: 64, Responses: []Directive{
		{Respond: RespondStatus, Status: http.StatusForbidden},
		{Respond: RespondCorrupt},
	}}); err != nil {
		t.Fatal(err)
	}

	resp, _ := f.get(t, f.srv.URL("app1"), "bytes=0-63")
	if resp.StatusCode != http.StatusForbidden {
		t.Errorf("first answer = %d, want 403", resp.StatusCode)
	}

	_, corrupt := f.get(t, f.srv.URL("app1"), "bytes=0-63")
	_, good := f.get(t, f.srv.URL("app1"), "bytes=0-63")
	if len(corrupt) != 64 || bytes.Equal(corrupt, good) {
		t.Errorf("corrupt answer is %d bytes, equal to the good one: %v", len(corrupt), bytes.Equal(corrupt, good))
	}
	if got := f.lastEvent(t).Attrs["occurrence"]; got != 3 {
		t.Errorf("occurrence = %v, want 3", got)
	}
}

func TestAnUndeclaredBundleIs404(t *testing.T) {
	f := newFixture(t)
	resp, _ := f.get(t, f.srv.URL("missing"), "bytes=0-10")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
	if got := f.lastEvent(t).Attrs["respond"]; got != "unknown" {
		t.Errorf("respond = %v", got)
	}
}

func TestBundlesDifferAndResolveBackFromTheirDigest(t *testing.T) {
	f := newFixture(t)
	for name, app := range map[string]string{"a": "a:1", "b": "b:1"} {
		if err := f.srv.Add(name, Bundle{App: app}); err != nil {
			t.Fatal(err)
		}
	}
	shaA, _ := f.srv.SHA256("a")
	shaB, _ := f.srv.SHA256("b")
	if shaA == shaB {
		t.Fatal("two bundles have the same digest")
	}
	if name, app, ok := f.srv.Lookup(shaB); !ok || name != "b" || app != "b:1" {
		t.Errorf("Lookup(b) = %q %q %v", name, app, ok)
	}
	if _, _, ok := f.srv.Lookup(hex.EncodeToString(make([]byte, 32))); ok {
		t.Error("Lookup resolved a digest nobody declared")
	}
}

func TestAddRefusesMistakes(t *testing.T) {
	f := newFixture(t)
	if err := f.srv.Add("app1", Bundle{}); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		name string
		b    Bundle
	}{
		"duplicate":      {"app1", Bundle{}},
		"path separator": {"a/b", Bundle{}},
		"bad directive":  {"app2", Bundle{Responses: []Directive{{Respond: "nope"}}}},
		"status no code": {"app3", Bundle{Responses: []Directive{{Respond: RespondStatus}}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := f.srv.Add(tc.name, tc.b); err == nil {
				t.Error("accepted")
			}
		})
	}
}
