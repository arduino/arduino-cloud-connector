// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package steps

import (
	"strings"
	"testing"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/harness"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/storage"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

// publish records an uplink the way the broker would, so an expectation has
// something to match without a daemon.
func publish(w *harness.World, attrs map[string]any) {
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, attrs, nil)
}

func TestOTAJobIDs(t *testing.T) {
	a, _ := otaJobIDHex("job1")
	b, _ := otaJobIDHex("job2")
	if len(a) != 32 || a == b {
		t.Errorf("job ids %q and %q: want two distinct 16-byte ids", a, b)
	}
	pinned := "00112233445566778899aabbccddeeff"
	if got, _ := otaJobIDHex(pinned); got != pinned {
		t.Errorf("a 32-hex id is taken verbatim: got %s", got)
	}
	if _, err := otaJobID(""); err == nil {
		t.Error("an empty id was accepted")
	}
}

// cloud_ota must publish the URL and the digest of the bundle it names, under
// the id the expectations will look for.
func TestCloudOTAPublishesTheBundle(t *testing.T) {
	w, _ := newWorld(t)
	if err := w.Storage.Add("app1", storage.Bundle{App: "app1:0.1.0"}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := run(t, w, "cloud_ota", "{id: job1, bundle: app1}", 0); err != nil {
		t.Fatalf("cloud_ota: %v", err)
	}
	ev, _ := awaitNote(t, w, 0, wire.CmdOTAUpdate)
	id, _ := otaJobIDHex("job1")
	sha, _ := w.Storage.SHA256("app1")
	for k, want := range map[string]any{
		"id": id, "url": w.Storage.URL("app1"), "final_sha": sha,
		"initial_sha": strings.Repeat("00", 32),
	} {
		if ev.Attrs[k] != want {
			t.Errorf("%s = %v, want %v", k, ev.Attrs[k], want)
		}
	}

	// The overrides replace exactly one thing each.
	if _, _, err := run(t, w, "cloud_ota", "{id: job2, bundle: app1, url: 'http://insecure.invalid/b'}", 0); err != nil {
		t.Fatalf("cloud_ota with url: %v", err)
	}
	ev, _ = awaitNote(t, w, eventlog.Cursor(ev.Seq), wire.CmdOTAUpdate)
	if ev.Attrs["url"] != "http://insecure.invalid/b" || ev.Attrs["final_sha"] != sha {
		t.Errorf("url override: %v", ev.Attrs)
	}
}

func TestCloudOTARefusesMistakes(t *testing.T) {
	w, _ := newWorld(t)
	for _, y := range []string{
		"{bundle: app1}",              // no id
		"{id: job1, bundle: missing}", // not declared
		"{id: job1}",                  // nothing to deploy
		"{id: job1, url: 'https://x/b', sha256: abc}", // not a digest
		"{id: job1, bundel: app1}",                    // a typo
	} {
		if _, _, err := run(t, w, "cloud_ota", y, 0); err == nil {
			t.Errorf("cloud_ota %s: accepted", y)
		}
	}
}

func TestCloudAppCommands(t *testing.T) {
	w, _ := newWorld(t)
	cursor := eventlog.Cursor(0)
	for _, tc := range []struct {
		step, y, command string
		parameter        any
	}{
		{"cloud_app_start", "{app: 'a:1', op_id: op1, parameter: 0}", "start", int64(0)},
		{"cloud_app_start", "{app: 'a:1', op_id: op2}", "start", nil},
		{"cloud_app_stop", "{app: 'a:1', op_id: op3}", "stop", nil},
		{"cloud_app_set_default", "{app: 'a:1', op_id: op4}", "set_default", nil},
		{"cloud_app_delete", "{app: 'a:1', op_id: op5}", "delete", nil},
	} {
		if _, _, err := run(t, w, tc.step, tc.y, 0); err != nil {
			t.Fatalf("%s: %v", tc.step, err)
		}
		ev, next := awaitNote(t, w, cursor, wire.CmdAppCommand)
		if ev.Attrs["command"] != tc.command || ev.Attrs["app"] != "a:1" || ev.Attrs["parameter"] != tc.parameter {
			t.Errorf("%s published %v", tc.step, ev.Attrs)
		}
		cursor = next
	}
	if _, _, err := run(t, w, "cloud_app_start", "{app: 'a:1'}", 0); err == nil {
		t.Error("a command without op_id was accepted")
	}
}

// A list is a run of consecutive reports, each consumed: one too few fails.
func TestExpectOTAProgressAssertsASequence(t *testing.T) {
	w, _ := newWorld(t)
	id, _ := otaJobIDHex("job1")
	for _, v := range []int64{0, 50, 100} {
		publish(w, map[string]any{"cmd": wire.CmdOTAProgress, "id": id, "state": int64(6), "state_data": v})
	}
	_, next, err := run(t, w, "expect_ota_progress", "{id: job1, state: FlashOTA, state_data: [0, 50, 100], timeout: 1s}", 0)
	if err != nil {
		t.Fatalf("expect_ota_progress: %v", err)
	}
	for _, ev := range w.Log.Events() {
		if _, consumed := w.Log.ConsumedBy(ev.Seq); !consumed {
			t.Errorf("event %d was not consumed", ev.Seq)
		}
	}
	if _, _, err := run(t, w, "expect_ota_progress", "{id: job1, state: FlashOTA, state_data: 100, timeout: 200ms}", next); err == nil {
		t.Error("matched a fourth report that was never published")
	}
	// The state is a name from the table or a number; anything else is a mistake.
	if _, _, err := run(t, w, "expect_ota_progress", "{id: job1, state: Flashing, timeout: 1s}", 0); err == nil {
		t.Error("an unknown state name was accepted")
	}
}

// ok asserts the absence of details by itself, through the element count.
func TestExpectAppCommandResponse(t *testing.T) {
	w, _ := newWorld(t)
	publish(w, map[string]any{"cmd": wire.CmdAppCommandResponse, "op_id": "op1", "status": "ok", "elements": 3, "details": int64(0)})
	if _, _, err := run(t, w, "expect_app_command_response", "{op_id: op1, status: ok, timeout: 200ms}", 0); err == nil {
		t.Error("a success carrying details was accepted")
	}
	publish(w, map[string]any{"cmd": wire.CmdAppCommandResponse, "op_id": "op1", "status": "ok", "elements": 2})
	if _, _, err := run(t, w, "expect_app_command_response", "{op_id: op1, status: ok, timeout: 1s}", 0); err != nil {
		t.Errorf("the bare success: %v", err)
	}

	for _, y := range []string{
		"{op_id: op1, status: ok, details: 0}",
		"{op_id: op1, status: fail}",
		"{op_id: op1, status: done, details: 1}",
		"{status: ok}",
		"{op_id: op1, status: progress, details: []}",
	} {
		if _, _, err := run(t, w, "expect_app_command_response", y, 0); err == nil {
			t.Errorf("%s: accepted", y)
		}
	}
}

func TestExpectOTABeginNamesTheBundle(t *testing.T) {
	w, _ := newWorld(t)
	if err := w.Storage.Add("app1", storage.Bundle{}); err != nil {
		t.Fatal(err)
	}
	sha, _ := w.Storage.SHA256("app1")
	publish(w, map[string]any{"cmd": wire.CmdOTABegin, "sha256": sha})
	if _, _, err := run(t, w, "expect_ota_begin", "{bundle: app1, timeout: 1s}", 0); err != nil {
		t.Errorf("expect_ota_begin: %v", err)
	}
	if _, _, err := run(t, w, "expect_ota_begin", "{bundle: nope}", 0); err == nil {
		t.Error("an undeclared bundle was accepted")
	}
}
