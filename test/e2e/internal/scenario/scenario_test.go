// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package scenario

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/harness"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/provapi"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/steps"
)

// write puts a scenario file in a temp dir and returns its path.
func write(t *testing.T, name, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func newWorld(t *testing.T) *harness.World {
	t.Helper()
	w, err := harness.SetupServers(harness.Config{
		DeviceID: "9f1c2d3e-4567-89ab-cdef-0123456789ab",
		ThingID:  "b2c3d4e5-6789-4abc-8def-0123456789ab",
	})
	if err != nil {
		t.Fatalf("SetupServers: %v", err)
	}
	t.Cleanup(func() { _ = w.Teardown(context.Background()) })
	return w
}

func TestLoad(t *testing.T) {
	path := write(t, "full.yaml", `
name: full-lifecycle
strict_events: true
tolerate:
  - { source: mqtt, cmd: Thing.begin }
fakes:
  provisioning_api:
    csr:
      - { respond: status, status: 503 }
      - { respond: issue_cert }
steps:
  - await_daemon_state: { state: Provisioning, timeout: 10s }
  - app_post: { path: /v1/provisioning/start }
  - expect_mqtt_publish: { cmd: Thing.begin, thing_id: "" }
`)

	sc, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if sc.Name != "full-lifecycle" {
		t.Errorf("name = %q", sc.Name)
	}
	if !sc.StrictEvents {
		t.Error("strict_events was not read")
	}
	if len(sc.Tolerate) != 1 {
		t.Errorf("got %d tolerate entries, want 1", len(sc.Tolerate))
	}
	if len(sc.Steps) != 3 {
		t.Fatalf("got %d steps, want 3", len(sc.Steps))
	}
	if sc.Steps[0].Name != "await_daemon_state" || sc.Steps[2].Name != "expect_mqtt_publish" {
		t.Errorf("step names = %q, %q, %q", sc.Steps[0].Name, sc.Steps[1].Name, sc.Steps[2].Name)
	}
	if got := sc.Fakes.ProvisioningAPI["csr"]; len(got) != 2 || got[0].Status != 503 ||
		got[1].Respond != provapi.RespondIssueCert {
		t.Errorf("csr directives = %+v", got)
	}
}

// The name falls back to the file name, so a scenario file needs no ceremony.
func TestLoadDefaultsTheNameToTheFileName(t *testing.T) {
	path := write(t, "reconnect.yaml", "steps:\n  - expect_mqtt_publish: {}\n")
	sc, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if sc.Name != "reconnect" {
		t.Errorf("name = %q, want reconnect", sc.Name)
	}
}

// Every one of these has to be an error rather than a quietly different
// scenario: a file that parses but does not mean what it says is the worst
// outcome for a suite nobody watches closely.
func TestLoadRejectsMalformedScenarios(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "unknown top-level key",
			body: "name: x\nstrict_event: true\nsteps:\n  - expect_mqtt_publish: {}\n",
			want: "strict_event",
		},
		{
			name: "step is not a mapping",
			body: "steps:\n  - expect_mqtt_publish\n",
			want: "single-key mapping",
		},
		{
			name: "step with two keys",
			body: "steps:\n  - {expect_mqtt_publish: {}, app_var_write: {}}\n",
			want: "single-key mapping",
		},
		{
			name: "no steps",
			body: "name: empty\n",
			want: "no steps",
		},
		{
			name: "not yaml at all",
			body: "\tthis is not yaml\n",
			want: "parse",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(write(t, "s.yaml", tc.body))
			if err == nil {
				t.Fatal("Load succeeded")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error should mention %q: %v", tc.want, err)
			}
		})
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Error("Load succeeded on a missing file")
	}
}

func TestLoadDirIsSorted(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"b.yaml", "a.yaml", "ignored.txt"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("steps:\n  - expect_mqtt_publish: {}\n"), 0o600); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	scenarios, err := LoadDir(dir)
	if err != nil {
		t.Fatalf("LoadDir: %v", err)
	}
	if len(scenarios) != 2 || scenarios[0].Name != "a" || scenarios[1].Name != "b" {
		t.Errorf("got %d scenarios in the wrong order: %+v", len(scenarios), scenarios)
	}
}

// A typo in a step name must fail before anything starts, not after eighteen
// steps and a daemon launch.
func TestValidateNamesTheUnknownSteps(t *testing.T) {
	sc, err := Load(write(t, "s.yaml", "steps:\n  - expect_mqtt_publish: {}\n  - expect_pubish: {}\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	err = sc.Validate(steps.Default())
	if err == nil {
		t.Fatal("Validate accepted an unknown step")
	}
	if !strings.Contains(err.Error(), "expect_pubish") {
		t.Errorf("the error should name the step: %v", err)
	}
	// And list the vocabulary, which is what makes the message actionable.
	if !strings.Contains(err.Error(), "expect_mqtt_publish") {
		t.Errorf("the error should list the known steps: %v", err)
	}
}

func TestInterpolateResolvesAndCopies(t *testing.T) {
	bag := harness.NewBag()
	bag.Set("thing_id", "b2c3d4e5")

	var node yaml.Node
	if err := yaml.Unmarshal([]byte(`{topic: "/a/t/{thing_id}/e/i", values: [{name: temp, value: 21.5}]}`), &node); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	original := node.Content[0]

	resolved, err := interpolate(original, bag)
	if err != nil {
		t.Fatalf("interpolate: %v", err)
	}
	var got map[string]any
	if err := resolved.Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got["topic"] != "/a/t/b2c3d4e5/e/i" {
		t.Errorf("topic = %v", got["topic"])
	}
	// The nested value keeps its type: a float must not come back as a string.
	values, _ := got["values"].([]any)
	if len(values) != 1 {
		t.Fatalf("values = %v", got["values"])
	}
	if first, _ := values[0].(map[string]any); first["value"] != 21.5 {
		t.Errorf("nested value = %v (%T), want 21.5", first["value"], first["value"])
	}

	// The scenario may be run again, so the original must be untouched.
	var before map[string]any
	if err := original.Decode(&before); err != nil {
		t.Fatalf("decode the original: %v", err)
	}
	if before["topic"] != "/a/t/{thing_id}/e/i" {
		t.Errorf("interpolation mutated the loaded scenario: %v", before["topic"])
	}

	if _, err := interpolate(original, harness.NewBag()); err == nil {
		t.Error("interpolate accepted an unknown placeholder")
	}
}

// Run is the whole loop: interpolate, dispatch, advance the cursor, record.
func TestRunPassesAndRecordsEachStep(t *testing.T) {
	w := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{
		"cmd": "Device.begin", "lib_version": "0.0.0-e2e",
	}, nil)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTSubscribe, map[string]any{
		"topic": "/a/t/b2c3d4e5-6789-4abc-8def-0123456789ab/e/i",
	}, nil)

	sc, err := Load(write(t, "s.yaml", `
name: two-steps
strict_events: true
steps:
  - expect_mqtt_publish: { cmd: Device.begin, lib_version: 0.0.0-e2e, timeout: 2s }
  - expect_mqtt_subscribe: { topic: "/a/t/{thing_id}/e/i", timeout: 2s }
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	result := Run(context.Background(), w, sc, steps.Default())
	if result.Failed() {
		t.Fatalf("the scenario failed:\n%s", result.Format())
	}
	if len(result.Steps) != 2 {
		t.Fatalf("got %d step results, want 2", len(result.Steps))
	}
	for i, s := range result.Steps {
		if s.Status != eventlog.StepPassed {
			t.Errorf("step %d (%s) = %s", i+1, s.Name, s.Status)
		}
		if s.MatchedSeq != i+1 {
			t.Errorf("step %d matched seq %d, want %d", i+1, s.MatchedSeq, i+1)
		}
		if s.Detail == "" {
			t.Errorf("step %d has no detail for the report", i+1)
		}
	}
}

// A failed step stops the run and the rest are reported as skipped, not as
// passed and not as failed: a step that never ran has no verdict.
func TestRunSkipsTheStepsAfterAFailure(t *testing.T) {
	w := newWorld(t)

	sc, err := Load(write(t, "s.yaml", `
name: fails-first
steps:
  - expect_mqtt_publish: { cmd: Device.begin, timeout: 100ms }
  - expect_mqtt_subscribe: { timeout: 100ms }
  - app_var_write: { variable: temp, value: 1 }
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	result := Run(context.Background(), w, sc, steps.Default())
	if !result.Failed() {
		t.Fatal("the scenario passed with no events at all")
	}
	if result.Steps[0].Status != eventlog.StepFailed {
		t.Errorf("step 1 = %s, want failed", result.Steps[0].Status)
	}
	if result.Steps[0].Err == nil {
		t.Error("the failed step carries no error, so the report has nothing to explain")
	}
	for _, s := range result.Steps[1:] {
		if s.Status != eventlog.StepSkipped {
			t.Errorf("step %d (%s) = %s, want skipped", s.Index, s.Name, s.Status)
		}
	}
	// The report has to name the failing expectation.
	if !strings.Contains(result.Format(), "Device.begin") {
		t.Errorf("the report does not name the expectation:\n%s", result.Format())
	}
}

// The strict sweep is the user's requirement: traffic no step accounted for
// fails the run, so a message the daemon should not have sent cannot pass
// unnoticed.
func TestStrictEventsFailsOnUnconsumedTraffic(t *testing.T) {
	w := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Device.begin"}, nil)
	// Nobody expects this one.
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Thing.begin"}, nil)

	body := `
name: strict
strict_events: true
steps:
  - expect_mqtt_publish: { cmd: Device.begin, timeout: 2s }
`
	sc, err := Load(write(t, "strict.yaml", body))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	result := Run(context.Background(), w, sc, steps.Default())
	if !result.Failed() {
		t.Fatal("an unconsumed publish did not fail the run")
	}
	if len(result.Unconsumed) != 1 || result.Unconsumed[0].Attrs["cmd"] != "Thing.begin" {
		t.Errorf("unconsumed = %v, want the Thing.begin publish", result.Unconsumed)
	}
}

// Some unclaimed traffic is correct behaviour -- Thing.begin is retried with
// back-off -- so a scenario can forgive it by pattern.
func TestTolerateForgivesExpectedTraffic(t *testing.T) {
	w := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Device.begin"}, nil)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Thing.begin"}, nil)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Thing.begin"}, nil)

	sc, err := Load(write(t, "tolerant.yaml", `
name: tolerant
strict_events: true
tolerate:
  - { source: mqtt, cmd: Thing.begin }
steps:
  - expect_mqtt_publish: { cmd: Device.begin, timeout: 2s }
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	result := Run(context.Background(), w, sc, steps.Default())
	if result.Failed() {
		t.Fatalf("the tolerated retries failed the run:\n%s", result.Format())
	}
}

// Without strict_events the sweep is off, so a scenario can be written before
// its tolerate list is understood.
func TestNonStrictScenarioIgnoresUnconsumedTraffic(t *testing.T) {
	w := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Thing.begin"}, nil)

	sc, err := Load(write(t, "loose.yaml", `
name: loose
steps:
  - expect_mqtt_publish: { cmd: Thing.begin, timeout: 2s }
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Consume nothing else: the Device.begin is absent and no sweep runs.
	result := Run(context.Background(), w, sc, steps.Default())
	if result.Failed() {
		t.Fatalf("a non-strict scenario failed:\n%s", result.Format())
	}
	if len(result.Unconsumed) != 0 {
		t.Errorf("unconsumed = %v, want none reported when the sweep is off", result.Unconsumed)
	}
}

func TestRunRejectsAnUnknownStepBeforeRunningAnything(t *testing.T) {
	w := newWorld(t)
	sc, err := Load(write(t, "s.yaml", "steps:\n  - nope: {}\n"))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	result := Run(context.Background(), w, sc, steps.Default())
	if !result.Failed() {
		t.Fatal("an unknown step did not fail the run")
	}
	if len(result.Steps) != 1 || result.Steps[0].Name != "validate" {
		t.Errorf("steps = %+v, want a single validation failure", result.Steps)
	}
}

func TestApplyFakesQueuesDirectives(t *testing.T) {
	w := newWorld(t)

	err := applyFakes(w, Fakes{ProvisioningAPI: map[string][]provapi.Directive{
		"csr":      {{Respond: provapi.RespondStatus, Status: 503}},
		"complete": {{Respond: provapi.RespondOK}},
	}})
	if err != nil {
		t.Fatalf("applyFakes: %v", err)
	}

	// The queued 503 must be what the next call actually gets.
	status, _, err := postCSR(t, w)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	if status != 503 {
		t.Errorf("status = %d, want the queued 503", status)
	}

	if err := applyFakes(w, Fakes{ProvisioningAPI: map[string][]provapi.Directive{
		"nope": {{Respond: provapi.RespondOK}},
	}}); err == nil {
		t.Error("an unknown endpoint was accepted")
	}
	if err := applyFakes(w, Fakes{ProvisioningAPI: map[string][]provapi.Directive{
		"csr": {{Respond: "teleport"}},
	}}); err == nil {
		t.Error("an unknown respond was accepted")
	}
}

// postCSR calls the fake provisioning API the way the daemon would, to see
// which directive it actually gets.
func postCSR(t *testing.T, w *harness.World) (int, string, error) {
	t.Helper()
	resp, err := http.Post(w.ProvAPI.URL()+"/v1/onboarding/provision/csr",
		"application/json", strings.NewReader(`{"csr":"not a real csr"}`))
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close() //nolint:errcheck
	body, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body), err
}

func TestEndpointFor(t *testing.T) {
	for _, short := range []string{"csr", "provision/csr"} {
		if got, err := endpointFor(short); err != nil || got != provapi.EndpointCSR {
			t.Errorf("endpointFor(%q) = %q, %v", short, got, err)
		}
	}
	for _, short := range []string{"complete", "provision/complete"} {
		if got, err := endpointFor(short); err != nil || got != provapi.EndpointComplete {
			t.Errorf("endpointFor(%q) = %q, %v", short, got, err)
		}
	}
	if _, err := endpointFor("claim"); err == nil {
		t.Error("endpointFor accepted an endpoint the fake does not serve")
	}
}

// Both artifacts are written for every run: CI uploads the directory when a
// job fails, and the JSON is what a tool reads.
func TestWriteArtifacts(t *testing.T) {
	w := newWorld(t)
	w.Log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, map[string]any{"cmd": "Device.begin"}, nil)
	result := w.Log.Result("full lifecycle/1", []eventlog.StepResult{{
		Index: 1, Name: "expect_mqtt_publish", Status: eventlog.StepPassed, MatchedSeq: 1,
	}}, nil)

	dir := filepath.Join(t.TempDir(), "_artifacts")
	textPath, jsonPath, err := WriteArtifacts(dir, result)
	if err != nil {
		t.Fatalf("WriteArtifacts: %v", err)
	}
	// The scenario name reaches the file system, so it has to be sanitised.
	if filepath.Base(textPath) != "full-lifecycle-1.txt" {
		t.Errorf("text artifact = %q", filepath.Base(textPath))
	}
	for _, path := range []string{textPath, jsonPath} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if info.Size() == 0 {
			t.Errorf("%s is empty", path)
		}
	}
	text, err := os.ReadFile(textPath)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(text), "expect_mqtt_publish") {
		t.Errorf("the report does not mention the step:\n%s", text)
	}

	// No directory means no artifacts, which is how the go test driver runs
	// when nobody asked for them.
	if a, b, err := WriteArtifacts("", result); err != nil || a != "" || b != "" {
		t.Errorf("WriteArtifacts(\"\") = %q, %q, %v", a, b, err)
	}
}

func TestSanitize(t *testing.T) {
	for in, want := range map[string]string{
		"full-lifecycle": "full-lifecycle",
		"a b":            "a-b",
		"a/b":            "a-b",
		"a:b*c":          "a-b-c",
		"":               "scenario",
	} {
		if got := sanitize(in); got != want {
			t.Errorf("sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}
