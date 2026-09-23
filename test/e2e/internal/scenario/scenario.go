// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package scenario loads the YAML files and runs them.
//
// A scenario is an ordered list of named steps, and nothing else: no
// conditionals, no loops, no expression language. The only dynamic part is
// {placeholder} substitution from the variable bag, resolved at the moment each
// step runs so a value the daemon only learns later can still be referred to.
//
// Read as a state machine -- which is how it was designed -- the steps are the
// states and the file is the ordered list of transitions.
package scenario

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/harness"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/provapi"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/steps"
)

// Scenario is one loaded file.
type Scenario struct {
	Name string `yaml:"name"`
	// StrictEvents makes any significant event no step consumed fail the run.
	// Off by default, so a new scenario can be written before its tolerate
	// list is understood.
	StrictEvents bool `yaml:"strict_events"`
	// Tolerate lists traffic that is correct but unclaimed. The retried
	// Thing.begin is the standing example: the daemon resends it with back-off,
	// so a delayed Thing.update legitimately leaves extra publishes behind.
	Tolerate []yaml.Node `yaml:"tolerate"`
	Fakes    Fakes       `yaml:"fakes"`
	Steps    []Step      `yaml:"steps"`

	// Path is where it was loaded from, for an error that has to name the file.
	Path string `yaml:"-"`
}

// Fakes is the per-scenario fault injection.
type Fakes struct {
	// ProvisioningAPI queues responses per endpoint, keyed by the short names
	// the API uses: "csr" and "complete".
	ProvisioningAPI map[string][]provapi.Directive `yaml:"provisioning_api"`
}

// Step is one entry of the steps list: a single-key mapping whose key is the
// primitive name and whose value is its parameters.
type Step struct {
	Name   string
	Params yaml.Node
}

// UnmarshalYAML accepts `- expect_mqtt_publish: {cmd: Device.begin}` and
// `- app_post:` with no parameters at all.
func (s *Step) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.MappingNode || len(node.Content) != 2 {
		return fmt.Errorf("line %d: a step must be a single-key mapping like "+
			"`- expect_mqtt_publish: {cmd: Device.begin}`", node.Line)
	}
	s.Name = node.Content[0].Value
	s.Params = *node.Content[1]
	return nil
}

// Load reads one scenario file.
func Load(path string) (Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf("scenario: read %s: %w", path, err)
	}
	var sc Scenario
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	// Strict: a misspelled top-level key would otherwise be ignored and the
	// scenario would quietly not do what it says.
	dec.KnownFields(true)
	if err := dec.Decode(&sc); err != nil {
		return Scenario{}, fmt.Errorf("scenario: parse %s: %w", path, err)
	}
	sc.Path = path
	if sc.Name == "" {
		sc.Name = strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	}
	if len(sc.Steps) == 0 {
		return Scenario{}, fmt.Errorf("scenario %s: no steps", path)
	}
	return sc, nil
}

// LoadDir reads every *.yaml in a directory, sorted by name so a run is
// reproducible.
func LoadDir(dir string) ([]Scenario, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
	if err != nil {
		return nil, fmt.Errorf("scenario: scan %s: %w", dir, err)
	}
	sort.Strings(paths)
	out := make([]Scenario, 0, len(paths))
	for _, path := range paths {
		sc, err := Load(path)
		if err != nil {
			return nil, err
		}
		out = append(out, sc)
	}
	return out, nil
}

// Validate checks every step name against the registry BEFORE anything runs.
//
// Fail-fast matters here: a typo in step 19 would otherwise be found after
// eighteen steps and a daemon launch, and the report would be about a scenario
// that was never runnable.
func (sc Scenario) Validate(reg steps.Registry) error {
	var unknown []string
	for i, st := range sc.Steps {
		if _, ok := reg[st.Name]; !ok {
			unknown = append(unknown, fmt.Sprintf("step %d: %q", i+1, st.Name))
		}
	}
	if len(unknown) > 0 {
		known := make([]string, 0, len(reg))
		for name := range reg {
			known = append(known, name)
		}
		sort.Strings(known)
		return fmt.Errorf("scenario %s: unknown step(s) %s; the vocabulary is: %s",
			sc.Name, strings.Join(unknown, ", "), strings.Join(known, ", "))
	}
	return nil
}

// Run executes the scenario against a prepared World.
//
// The cursor is shared and only moves forward: each expectation resumes
// scanning where the previous one stopped. That is what makes ordering
// assertions free -- "Thing.begin after Device.begin" needs no syntax, it is
// simply the order the steps are written in.
func Run(ctx context.Context, w *harness.World, sc Scenario, reg steps.Registry) eventlog.Result {
	results := make([]eventlog.StepResult, 0, len(sc.Steps))

	if err := sc.Validate(reg); err != nil {
		return w.Log.Result(sc.Name, []eventlog.StepResult{{
			Index:  1,
			Name:   "validate",
			Status: eventlog.StepFailed,
			Err:    err,
		}}, tolerations(sc))
	}
	if err := applyFakes(w, sc.Fakes); err != nil {
		return w.Log.Result(sc.Name, []eventlog.StepResult{{
			Index:  1,
			Name:   "fakes",
			Status: eventlog.StepFailed,
			Err:    err,
		}}, tolerations(sc))
	}

	cursor := eventlog.Cursor(0)
	failed := false
	for i, st := range sc.Steps {
		if failed {
			results = append(results, eventlog.StepResult{
				Index:  i + 1,
				Name:   st.Name,
				Status: eventlog.StepSkipped,
			})
			continue
		}

		params, err := interpolate(&st.Params, w.Vars)
		if err != nil {
			results = append(results, eventlog.StepResult{
				Index:  i + 1,
				Name:   st.Name,
				Status: eventlog.StepFailed,
				Err:    fmt.Errorf("step %d (%s): %w", i+1, st.Name, err),
			})
			failed = true
			continue
		}

		sctx := &steps.Context{World: w, Cursor: cursor, Name: st.Name, Index: i + 1}
		started := time.Now()
		next, err := reg[st.Name](ctx, sctx, params)
		result := eventlog.StepResult{
			Index:  i + 1,
			Name:   st.Name,
			Detail: sctx.Detail,
			Status: eventlog.StepPassed,
			Since:  time.Since(started),
		}
		if err != nil {
			result.Status = eventlog.StepFailed
			result.Err = err
			failed = true
		} else if next > cursor {
			// An expectation matched: the cursor lands just past the event it
			// claimed, so the cursor IS that event's Seq.
			result.MatchedSeq = int(next)
			cursor = next
		}
		results = append(results, result)
	}

	return w.Log.Result(sc.Name, results, tolerations(sc))
}

// tolerations builds the predicates the final sweep forgives.
//
// When strict_events is off, the sweep is disabled by tolerating everything: a
// predicate with no constraints matches every event, because "all constraints
// satisfied" is trivially true for none. That keeps the opt-in in the scenario
// file rather than in two places in the code.
func tolerations(sc Scenario) []eventlog.Predicate {
	if !sc.StrictEvents {
		return []eventlog.Predicate{{Label: "strict_events is off"}}
	}
	out := make([]eventlog.Predicate, 0, len(sc.Tolerate))
	for i := range sc.Tolerate {
		params := map[string]any{}
		if err := sc.Tolerate[i].Decode(&params); err != nil {
			// A malformed tolerate entry must not silently forgive nothing (or
			// everything): it becomes a predicate that matches nothing, and the
			// unconsumed events it should have covered then fail the run.
			continue
		}
		out = append(out, steps.BuildPredicate(nil, params, nil))
	}
	return out
}

// applyFakes queues the scenario's fault directives before the daemon has a
// chance to call anything.
func applyFakes(w *harness.World, f Fakes) error {
	for short, directives := range f.ProvisioningAPI {
		endpoint, err := endpointFor(short)
		if err != nil {
			return err
		}
		if err := w.ProvAPI.Queue(endpoint, directives...); err != nil {
			return err
		}
	}
	return nil
}

// endpointFor maps the short names a scenario writes onto the endpoints the
// fake serves.
func endpointFor(short string) (provapi.Endpoint, error) {
	switch short {
	case "csr", string(provapi.EndpointCSR):
		return provapi.EndpointCSR, nil
	case "complete", string(provapi.EndpointComplete):
		return provapi.EndpointComplete, nil
	default:
		return "", fmt.Errorf("scenario: unknown provisioning_api endpoint %q (want csr or complete)", short)
	}
}

// interpolate returns a copy of the parameters with every {placeholder}
// resolved.
//
// A copy, because the scenario may be run more than once (a retry, or the same
// file driven by both the binary and the go test driver) and substituting in
// place would bake the first run's values into the second.
func interpolate(node *yaml.Node, bag *harness.Bag) (*yaml.Node, error) {
	if node == nil || node.Kind == 0 {
		return node, nil
	}
	out := *node
	if node.Kind == yaml.ScalarNode {
		value, err := bag.Interpolate(node.Value)
		if err != nil {
			return nil, err
		}
		out.Value = value
		return &out, nil
	}
	out.Content = make([]*yaml.Node, 0, len(node.Content))
	for _, child := range node.Content {
		resolved, err := interpolate(child, bag)
		if err != nil {
			return nil, err
		}
		out.Content = append(out.Content, resolved)
	}
	return &out, nil
}

// WriteArtifacts writes the human report and the JSON dump for one run.
//
// Both, always: the text is what a person reads and the JSON is what a tool
// reads, and CI uploads the directory only when a job fails, so writing them
// unconditionally costs nothing.
func WriteArtifacts(dir string, r eventlog.Result) (textPath, jsonPath string, err error) {
	if dir == "" {
		return "", "", nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", "", fmt.Errorf("scenario: artifacts dir: %w", err)
	}
	name := sanitize(r.Scenario)
	textPath = filepath.Join(dir, name+".txt")
	jsonPath = filepath.Join(dir, name+".json")

	if err := os.WriteFile(textPath, []byte(r.Format()), 0o644); err != nil {
		return "", "", fmt.Errorf("scenario: write %s: %w", textPath, err)
	}
	if err := r.WriteJSON(jsonPath); err != nil {
		return "", "", err
	}
	return textPath, jsonPath, nil
}

// sanitize keeps an artifact name usable as a file name on every platform the
// suite runs on.
func sanitize(name string) string {
	if name == "" {
		return "scenario"
	}
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			return r
		case r == '-', r == '_', r == '.':
			return r
		default:
			return '-'
		}
	}, name)
}
