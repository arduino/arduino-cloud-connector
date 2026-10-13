// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package steps

// The App deploy (RFC-14) and App lifecycle (RFC-16) vocabulary.
//
// The cloud side is one action per command, so a scenario reads as what the
// operator did: cloud_ota, cloud_app_start, cloud_app_stop… The expectations on
// what comes back are dedicated too, rather than spelled through
// expect_mqtt_publish, for three reasons that each remove a way to write a wrong
// scenario: a job is named by the word the scenario chose ("job1") instead of
// the 32 hex digits on the wire; an OTA state is named (FlashOTA) instead of
// numbered; and a progress sequence is ONE step with a list, in order, instead
// of a run of near-identical steps that is easy to miscount.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

// otaStates are the OTA states of OTAProgressCmd, restated from the C++
// library's ota::OTACloudProcessInterface::State (and RFC-14) for the same
// reason as the wire tags: the numbers are a contract, not a convenience.
var otaStates = map[string]int64{
	"Resume":       0,
	"OtaBegin":     1,
	"Idle":         2,
	"OtaAvailable": 3,
	"StartOTA":     4,
	"Fetch":        5,
	"FlashOTA":     6,
	"Reboot":       7,
	"Fail":         8,
}

// otaJobID is the 16-byte job id a scenario's job name stands for. A name that
// already is 32 hex digits is taken verbatim, so a scenario can also pin an id.
func otaJobID(name string) ([]byte, error) {
	if name == "" {
		return nil, fmt.Errorf("an OTA job needs an id")
	}
	if len(name) == 32 {
		if b, err := hex.DecodeString(name); err == nil {
			return b, nil
		}
	}
	sum := sha256.Sum256([]byte("e2e-ota-job:" + name))
	return sum[:16], nil
}

func otaJobIDHex(name string) (string, error) {
	b, err := otaJobID(name)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

// ── actions ──────────────────────────────────────────────────────────────────

type cloudOTAParams struct {
	// ID names the job; see otaJobID.
	ID string `yaml:"id"`
	// Bundle is a bundle declared under fakes.storage. It gives the URL and the
	// digest, either of which can be overridden.
	Bundle string `yaml:"bundle"`
	// URL replaces the bundle's URL, for an unusable one (http://, a host that
	// does not answer).
	URL string `yaml:"url"`
	// SHA256 replaces the bundle's digest, 64 hex digits.
	SHA256 string `yaml:"sha256"`
}

// cloudOTA is the cloud starting an App deploy: OTA.update with the job id, the
// bundle URL, an all-zero "currently installed" digest and the bundle's digest.
func cloudOTA(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p cloudOTAParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	id, err := otaJobID(p.ID)
	if err != nil {
		return sc.Cursor, fmt.Errorf("cloud_ota: %w", err)
	}

	url, digest := p.URL, p.SHA256
	if p.Bundle != "" {
		if url == "" {
			url = sc.World.Storage.URL(p.Bundle)
		}
		if digest == "" {
			sum, ok := sc.World.Storage.SHA256(p.Bundle)
			if !ok {
				return sc.Cursor, fmt.Errorf("cloud_ota: bundle %q is not declared under fakes.storage "+
					"(give sha256 to deploy one that does not exist)", p.Bundle)
			}
			digest = sum
		}
	}
	if url == "" || digest == "" {
		return sc.Cursor, fmt.Errorf("cloud_ota: needs a bundle, or both url and sha256")
	}
	final, err := hex.DecodeString(digest)
	if err != nil || len(final) != sha256.Size {
		return sc.Cursor, fmt.Errorf("cloud_ota: sha256 %q is not 64 hex digits", digest)
	}

	sc.Detail = fmt.Sprintf("cloud OTA.update %s (%s) %s", p.ID, hex.EncodeToString(id), url)
	payload := wire.EncodeOTAUpdate(id, url, make([]byte, sha256.Size), final)
	if err := sc.World.Broker.PublishCommand(sc.World.Vars.MustGet("device_id"), payload); err != nil {
		return sc.Cursor, fmt.Errorf("cloud_ota: %w", err)
	}
	return sc.Cursor, nil
}

type cloudAppCommandParams struct {
	// App is the App id, used verbatim (RFC-16 writes "<name>:<version>").
	App string `yaml:"app"`
	// OpID is the operation id every response to the command echoes.
	OpID string `yaml:"op_id"`
	// Parameter is the optional fourth element. For a start: 1 (or absent)
	// stops any running App first, 0 refuses if one is running.
	Parameter *int64 `yaml:"parameter"`
}

// cloudAppCommand builds the action for one RFC-16 command.
func cloudAppCommand(command string) Func {
	code, ok := wire.AppCommandCode(command)
	if !ok {
		panic("steps: unknown app command " + command)
	}
	step := "cloud_app_" + command
	return func(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
		var p cloudAppCommandParams
		if err := decodeInto(node, &p); err != nil {
			return sc.Cursor, err
		}
		if p.App == "" || p.OpID == "" {
			return sc.Cursor, fmt.Errorf("%s: needs app and op_id", step)
		}
		sc.Detail = fmt.Sprintf("cloud App.command %s %s op=%s", command, p.App, p.OpID)
		if p.Parameter != nil {
			sc.Detail += fmt.Sprintf(" parameter=%d", *p.Parameter)
		}
		payload := wire.EncodeAppCommand(p.App, code, p.OpID, p.Parameter)
		if err := sc.World.Broker.PublishCommand(sc.World.Vars.MustGet("device_id"), payload); err != nil {
			return sc.Cursor, fmt.Errorf("%s: %w", step, err)
		}
		return sc.Cursor, nil
	}
}

// ── expectations ─────────────────────────────────────────────────────────────

type expectOTAProgressParams struct {
	ID string `yaml:"id"`
	// State is a name from otaStates or its number.
	State string `yaml:"state"`
	// StateData is one value, or a list asserted as consecutive reports.
	StateData yaml.Node     `yaml:"state_data"`
	Timeout   time.Duration `yaml:"timeout"`
}

// expectOTAProgress waits for OTA.progress reports of one job in one state.
func expectOTAProgress(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p expectOTAProgressParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	id, err := otaJobIDHex(p.ID)
	if err != nil {
		return sc.Cursor, fmt.Errorf("expect_ota_progress: %w", err)
	}
	state, err := otaState(p.State)
	if err != nil {
		return sc.Cursor, fmt.Errorf("expect_ota_progress: %w", err)
	}
	data, err := intsOf(&p.StateData, "state_data")
	if err != nil {
		return sc.Cursor, fmt.Errorf("expect_ota_progress: %w", err)
	}
	base := map[string]any{"cmd": wire.CmdOTAProgress, "id": id, "state": state}
	label := fmt.Sprintf("OTA.progress %s %s", p.ID, p.State)
	return awaitSequence(ctx, sc, base, "state_data", data, label, p.Timeout)
}

type expectOTABeginParams struct {
	// Bundle names the bundle whose digest the board announces as installed.
	Bundle string `yaml:"bundle"`
	// SHA256 is the digest itself, for a scenario that does not name a bundle.
	SHA256  string        `yaml:"sha256"`
	Timeout time.Duration `yaml:"timeout"`
}

// expectOTABegin waits for the board's confirmation of a finished deploy:
// OTA.begin carrying the digest of the bundle it installed.
func expectOTABegin(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p expectOTABeginParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	digest := p.SHA256
	if p.Bundle != "" {
		sum, ok := sc.World.Storage.SHA256(p.Bundle)
		if !ok {
			return sc.Cursor, fmt.Errorf("expect_ota_begin: bundle %q is not declared under fakes.storage", p.Bundle)
		}
		digest = sum
	}
	if digest == "" {
		return sc.Cursor, fmt.Errorf("expect_ota_begin: needs a bundle or a sha256")
	}
	base := map[string]any{"cmd": wire.CmdOTABegin, "sha256": strings.ToLower(digest)}
	return awaitSequence(ctx, sc, base, "", nil, "OTA.begin "+p.Bundle, p.Timeout)
}

type expectAppCommandResponseParams struct {
	OpID string `yaml:"op_id"`
	// Status is ok, fail or progress.
	Status string `yaml:"status"`
	// Details is the error code of a fail, or the percentage of a progress —
	// one value, or a list asserted as consecutive responses. Never given for
	// ok: a success has no details, which the step asserts by itself.
	Details yaml.Node     `yaml:"details"`
	Timeout time.Duration `yaml:"timeout"`
}

// expectAppCommandResponse waits for App.commandResponse messages of one
// operation.
func expectAppCommandResponse(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p expectAppCommandResponseParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	if p.OpID == "" {
		return sc.Cursor, fmt.Errorf("expect_app_command_response: needs op_id")
	}
	details, err := intsOf(&p.Details, "details")
	if err != nil {
		return sc.Cursor, fmt.Errorf("expect_app_command_response: %w", err)
	}
	base := map[string]any{"cmd": wire.CmdAppCommandResponse, "op_id": p.OpID, "status": p.Status}
	switch p.Status {
	case "ok":
		if details != nil {
			return sc.Cursor, fmt.Errorf("expect_app_command_response: a success carries no details")
		}
		// RFC-16: [op_id, 0] and nothing else.
		base["elements"] = 2
	case "fail", "progress":
		if details == nil {
			return sc.Cursor, fmt.Errorf("expect_app_command_response: a %s needs details", p.Status)
		}
	default:
		return sc.Cursor, fmt.Errorf("expect_app_command_response: status %q is not ok, fail or progress", p.Status)
	}
	label := fmt.Sprintf("App.commandResponse %s %s", p.OpID, p.Status)
	return awaitSequence(ctx, sc, base, "details", details, label, p.Timeout)
}

// awaitSequence waits for one uplink matching base, or — when values is a list —
// for one per value, consecutively, each with key set to that value. Each event
// is consumed as it is matched, so the list asserts the exact run in order: a
// missing, extra or reordered report fails it.
func awaitSequence(ctx context.Context, sc *Context, base map[string]any, key string, values []int64,
	label string, timeout time.Duration,
) (eventlog.Cursor, error) {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	fixed := map[string]any{"source": string(eventlog.SourceMQTT), "kind": string(eventlog.KindMQTTPublish)}

	runs := []map[string]any{base}
	if values != nil {
		runs = runs[:0]
		for _, v := range values {
			params := make(map[string]any, len(base)+1)
			for k, bv := range base {
				params[k] = bv
			}
			params[key] = v
			runs = append(runs, params)
		}
	}

	cursor := sc.Cursor
	for i, params := range runs {
		pred := BuildPredicate(fixed, params, nil)
		sc.Predicate = pred
		sc.Detail = label
		if values != nil {
			sc.Detail = fmt.Sprintf("%s %s=%v", label, key, values)
		}
		ev, next, err := sc.World.Log.Await(ctx, cursor, pred, timeout)
		if err != nil {
			if len(runs) > 1 {
				return sc.Cursor, fmt.Errorf("%s=%d (%d of %d): %w", key, values[i], i+1, len(runs), err)
			}
			return sc.Cursor, err
		}
		sc.World.Log.Consume(ev.Seq, sc.Name)
		cursor = next
	}
	return cursor, nil
}

// otaState reads a state by name or number.
func otaState(s string) (int64, error) {
	if n, ok := otaStates[s]; ok {
		return n, nil
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("state %q is not one of OtaAvailable, StartOTA, Fetch, FlashOTA, Fail… nor a number", s)
	}
	return n, nil
}

// intsOf reads a parameter that is one integer or a list of them. nil means it
// was not given.
func intsOf(node *yaml.Node, field string) ([]int64, error) {
	switch node.Kind {
	case 0:
		return nil, nil
	case yaml.ScalarNode:
		var v int64
		if err := node.Decode(&v); err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		return []int64{v}, nil
	case yaml.SequenceNode:
		var vs []int64
		if err := node.Decode(&vs); err != nil {
			return nil, fmt.Errorf("%s: %w", field, err)
		}
		if len(vs) == 0 {
			return nil, fmt.Errorf("%s: an empty list asserts nothing", field)
		}
		return vs, nil
	default:
		return nil, fmt.Errorf("%s must be an integer or a list of integers", field)
	}
}
