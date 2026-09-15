// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package steps is the vocabulary a scenario is written in.
//
// The division is the point of the whole design: YAML says WHAT happens and in
// what order, Go says HOW to verify it. A step is a named Go primitive with
// typed parameters; the scenario file holds no conditionals, no loops and no
// expression language, only ordered steps and {placeholder} substitution. When
// a scenario needs an "if", it gets a new primitive here rather than new
// syntax there.
//
// # Expectations are one primitive, not twelve
//
// Every expect_* step is the same operation: build a predicate from a fixed
// source and kind plus one constraint per remaining parameter, then Await it
// from the current cursor. That is why `expect_publish: {cmd: Thing.begin,
// thing_id: ""}` needs no code of its own -- cmd and thing_id are simply
// attribute constraints, and the event log's predicates are data, so the
// failure report can say which of them did not hold.
//
// There is no sleep primitive, and there will not be one: every wait is a
// condition with a timeout, which is what keeps the suite from being flaky by
// construction.
package steps

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/harness"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/servers/broker"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

// DefaultTimeout is how long an expectation waits when the scenario does not
// say. Generous on purpose: the daemon's own back-offs are seconds long and a
// tight default would make the suite fail for reasons that are not bugs.
const DefaultTimeout = 15 * time.Second

// timeoutKey is the one parameter that is never an attribute constraint.
const timeoutKey = "timeout"

// Context is what the runner hands a step.
type Context struct {
	World *harness.World
	// Cursor is where this step starts scanning the log. Events before it
	// belong to earlier steps.
	Cursor eventlog.Cursor
	// Name is the step's name in the scenario, used to mark the event it
	// consumed so the timeline can say which step claimed what.
	Name string
	// Index is the 1-based position in the scenario.
	Index int
	// Detail is filled in by the step with a short summary of what it did, for
	// the step table in the report.
	Detail string
	// Predicate is filled in by an expectation with what it was waiting for, so
	// a failure can print the near-miss diff.
	Predicate eventlog.Predicate
}

// Func is one primitive. It returns the cursor the next step should start from:
// an expectation advances it past the event it matched, an action leaves it
// where it was.
type Func func(ctx context.Context, sc *Context, params *yaml.Node) (eventlog.Cursor, error)

// Registry maps scenario step names to primitives. The runner takes it
// explicitly rather than reading a package-level map filled by init(), so the
// dependency runs one way and a test can pass its own.
type Registry map[string]Func

// Default is the vocabulary the shipped scenarios use.
func Default() Registry {
	return Registry{
		// ── expectations ──────────────────────────────────────────────────
		"expect_api_call":     awaitEvent(eventlog.SourceProvisioningAPI, eventlog.KindHTTPRequest, nil),
		"expect_mqtt_connect": awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTConnect, nil),
		"expect_disconnect":   awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTDisconnect, nil),
		"expect_subscribe":    awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTSubscribe, nil),
		"expect_unsubscribe":  awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTUnsubscribe, nil),
		"expect_publish":      awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTPublish, nil),
		"expect_prop_publish": awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTPublish, nil),
		"expect_tls_error":    awaitEvent(eventlog.SourceMQTT, eventlog.KindMQTTTLSError, nil),
		"expect_sse":          awaitEvent(eventlog.SourceSSE, eventlog.KindSSEFrame, nil),
		"expect_daemon_exit":  awaitEvent(eventlog.SourceDaemonProcess, eventlog.KindProcessExit, nil),
		// The two state waits are the same primitive with the parameter the
		// scenario reads best: `state` rather than the attribute name.
		"await_daemon_state": awaitEvent(eventlog.SourceDaemonStatus, eventlog.KindStatusPoll,
			map[string]string{"state": "daemon"}),
		"await_cloud_state": awaitEvent(eventlog.SourceDaemonStatus, eventlog.KindStatusPoll,
			map[string]string{"state": "cloud_state"}),
		"await_provisioning_state": awaitEvent(eventlog.SourceDaemonStatus, eventlog.KindStatusPoll,
			map[string]string{"state": "provisioning"}),

		// ── actions ───────────────────────────────────────────────────────
		"app_get_identity":       appGetIdentity,
		"app_start_provisioning": appStartProvisioning,
		"app_post":               appPost,
		"app_put":                appPut,
		"app_sse_subscribe":      appSSESubscribe,
		"cloud_publish":          cloudPublish,
		"cloud_publish_prop":     cloudPublishProp,
		"stop_daemon":            stopDaemon,
	}
}

// ── expectations ─────────────────────────────────────────────────────────────

// awaitEvent is the one expectation primitive.
//
// rename maps a scenario-facing parameter name onto the attribute it
// constrains, for the cases where the natural word in YAML ("state") is not the
// attribute the producer recorded ("daemon").
func awaitEvent(src eventlog.Source, kind eventlog.Kind, rename map[string]string) Func {
	return func(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
		params, err := decodeMap(node)
		if err != nil {
			return sc.Cursor, err
		}
		timeout, err := takeTimeout(params)
		if err != nil {
			return sc.Cursor, err
		}

		pred := BuildPredicate(map[string]any{
			"source": string(src),
			"kind":   string(kind),
		}, params, rename)
		sc.Predicate = pred
		sc.Detail = pred.Label

		ev, cursor, err := sc.World.Log.Await(ctx, sc.Cursor, pred, timeout)
		if err != nil {
			return sc.Cursor, err
		}
		sc.World.Log.Consume(ev.Seq, sc.Name)
		return cursor, nil
	}
}

// BuildPredicate turns scenario parameters into a predicate.
//
// The convention is the whole scenario query language: "source" and "kind"
// constrain the event's own fields, and EVERY other key constrains the
// attribute of the same name. That is what lets `{cmd: Thing.begin, thing_id:
// ""}` work with no code per field, and it is shared with the tolerate list so
// the two cannot drift apart.
//
// fixed is applied first and cannot be overridden by the scenario; params is
// what the step or the tolerate entry carried; rename maps a scenario-facing
// name onto the attribute it actually constrains.
func BuildPredicate(fixed, params map[string]any, rename map[string]string) eventlog.Predicate {
	var (
		constraints []eventlog.Constraint
		label       string
	)
	for _, key := range sortedKeys(fixed) {
		constraints = append(constraints, eventlog.Eq(fieldFor(key, nil), fixed[key]))
		label += fmt.Sprintf("%v ", fixed[key])
	}
	for _, key := range sortedKeys(params) {
		field := fieldFor(key, rename)
		constraints = append(constraints, eventlog.Eq(field, params[key]))
		label += fmt.Sprintf("%s=%v ", strings.TrimPrefix(field, "attrs."), params[key])
	}
	return eventlog.Predicate{Label: strings.TrimSpace(label), Constraints: constraints}
}

// fieldFor maps a parameter name onto the predicate field it constrains.
func fieldFor(key string, rename map[string]string) string {
	if renamed, ok := rename[key]; ok {
		key = renamed
	}
	switch key {
	case "source", "kind", "seq":
		return key
	default:
		return "attrs." + key
	}
}

// ── actions ──────────────────────────────────────────────────────────────────

// appGetIdentityParams takes no parameters. Declaring the type empty is what
// makes a stray parameter an error rather than silence.
type appGetIdentityParams struct{}

// appGetIdentity reads the board identity and puts the uhwid in the bag.
//
// This is the one value only the daemon knows: it derives the uhwid from the
// hardware (a fixed one under -tags mock), and the harness has no way to guess
// it. Learning it here is what lets a later step assert on the CSR the daemon
// submits -- `expect_api_call: {endpoint: provision/csr, csr_subject: "CN={uhwid}"}`
// is the check that a stray subject attribute has not come back, which is the
// bug the fake CA reflects RDNs to catch.
//
// The board token comes back too and is deliberately NOT put in the bag: it is
// a credential, and the bag ends up in the artifact dump.
func appGetIdentity(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p appGetIdentityParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	id, err := sc.World.App.Identity(ctx)
	if err != nil {
		return sc.Cursor, fmt.Errorf("app_get_identity: %w", err)
	}
	if id.UHWID == "" {
		return sc.Cursor, fmt.Errorf("app_get_identity: the daemon answered with no uhwid")
	}
	// A missing board token is worth failing on here: it authenticates every
	// provisioning call, so without it the next step fails with an opaque 401
	// from the API instead of naming the cause.
	if id.BoardToken == "" {
		return sc.Cursor, fmt.Errorf("app_get_identity: the daemon answered with no board token")
	}
	sc.World.Vars.Set("uhwid", id.UHWID)
	sc.Detail = "identity uhwid=" + id.UHWID
	return sc.Cursor, nil
}

type appStartProvisioningParams struct {
	// OrganizationID is optional, exactly as in the API: a board can be
	// provisioned without one.
	OrganizationID string `yaml:"organization_id"`
}

// appStartProvisioning asks the daemon to provision, which is what App Lab
// does and the only way out of the Provisioning state.
//
// It is a named primitive rather than a generic POST because the endpoint has
// a contract worth holding the harness to: the body is optional, and the
// answer is 202 -- a 409 means a provisioning was already running, which is a
// different failure from an unreachable daemon and a scenario has to be able
// to tell them apart.
func appStartProvisioning(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p appStartProvisioningParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	if err := sc.World.App.StartProvisioning(ctx, p.OrganizationID); err != nil {
		return sc.Cursor, fmt.Errorf("app_start_provisioning: %w", err)
	}
	sc.Detail = "start provisioning"
	if p.OrganizationID != "" {
		sc.Detail += " org=" + p.OrganizationID
		sc.World.Vars.Set("organization_id", p.OrganizationID)
	}
	return sc.Cursor, nil
}

type appPostParams struct {
	Path   string         `yaml:"path"`
	Body   map[string]any `yaml:"body"`
	Status int            `yaml:"status"`
}

// appPost is the escape hatch for an endpoint with no named primitive yet. Use
// app_start_provisioning and app_get_identity for the two that have one: they
// check the status the API documents and learn what the scenario needs.
func appPost(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p appPostParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	if p.Path == "" {
		return sc.Cursor, fmt.Errorf("app_post: path is required")
	}
	sc.Detail = "POST " + p.Path

	var body any
	if p.Body != nil {
		body = p.Body
	}
	status, respBody, err := sc.World.App.Post(ctx, p.Path, body, p.Status)
	if err != nil {
		return sc.Cursor, fmt.Errorf("app_post %s: %w", p.Path, err)
	}
	sc.Detail = fmt.Sprintf("POST %s -> %d", p.Path, status)
	_ = respBody
	return sc.Cursor, nil
}

type appPutParams struct {
	Variable string `yaml:"variable"`
	Value    any    `yaml:"value"`
}

// appPut is the app writing a variable, the uplink half of the app role.
func appPut(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p appPutParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	if p.Variable == "" {
		return sc.Cursor, fmt.Errorf("app_put: variable is required")
	}
	sc.Detail = fmt.Sprintf("PUT %s=%v", p.Variable, p.Value)

	if err := sc.World.App.PutVariable(ctx, p.Variable, p.Value); err != nil {
		return sc.Cursor, fmt.Errorf("app_put %s: %w", p.Variable, err)
	}
	return sc.Cursor, nil
}

type appSubscribeParams struct {
	Variable string `yaml:"variable"`
}

// appSSESubscribe opens the variable's event stream. It returns only once the
// response headers are in, so a following injection cannot race it.
func appSSESubscribe(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p appSubscribeParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	if p.Variable == "" {
		return sc.Cursor, fmt.Errorf("app_sse_subscribe: variable is required")
	}
	sc.Detail = "subscribe " + p.Variable

	// The subscription outlives the step, so the World owns it and closes it at
	// teardown: a leaked stream would keep the daemon writing into a reader
	// nobody reads.
	stream, err := sc.World.App.SubscribeVariable(context.WithoutCancel(ctx), p.Variable)
	if err != nil {
		return sc.Cursor, fmt.Errorf("app_sse_subscribe %s: %w", p.Variable, err)
	}
	sc.World.AddStream(stream)
	return sc.Cursor, nil
}

type valueParams struct {
	Name  string  `yaml:"name"`
	Value any     `yaml:"value"`
	Time  float64 `yaml:"time"`
}

type cloudPublishParams struct {
	Cmd     string        `yaml:"cmd"`
	ThingID string        `yaml:"thing_id"`
	Values  []valueParams `yaml:"values"`
}

// cloudPublish is the cloud half of the handshake: the commands only the cloud
// sends.
func cloudPublish(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p cloudPublishParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	deviceID := sc.World.Vars.MustGet("device_id")
	sc.Detail = "cloud " + p.Cmd

	var payload []byte
	switch p.Cmd {
	case wire.CmdThingUpdate:
		payload = wire.EncodeThingUpdate(p.ThingID)
	case wire.CmdThingDetach:
		payload = wire.EncodeThingDetach(p.ThingID)
	case wire.CmdLastValuesUpdate:
		values, err := toWireValues(p.Values)
		if err != nil {
			return sc.Cursor, fmt.Errorf("cloud_publish %s: %w", p.Cmd, err)
		}
		senml, err := wire.EncodeSenML(values)
		if err != nil {
			return sc.Cursor, fmt.Errorf("cloud_publish %s: %w", p.Cmd, err)
		}
		payload = wire.EncodeLastValuesUpdate(senml)
	default:
		return sc.Cursor, fmt.Errorf("cloud_publish: %q is not a command the cloud sends "+
			"(have %s, %s, %s)", p.Cmd, wire.CmdThingUpdate, wire.CmdThingDetach, wire.CmdLastValuesUpdate)
	}

	if err := sc.World.Broker.PublishCommand(deviceID, payload); err != nil {
		return sc.Cursor, fmt.Errorf("cloud_publish %s: %w", p.Cmd, err)
	}
	return sc.Cursor, nil
}

type cloudPublishPropParams struct {
	ThingID  string        `yaml:"thing_id"`
	Variable string        `yaml:"variable"`
	Value    any           `yaml:"value"`
	Values   []valueParams `yaml:"values"`
}

// cloudPublishProp is the cloud changing a property value, which is what an
// operator does in the Cloud UI.
func cloudPublishProp(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p cloudPublishPropParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	thingID := p.ThingID
	if thingID == "" {
		thingID = sc.World.Vars.MustGet("thing_id")
	}

	values := p.Values
	if p.Variable != "" {
		values = append(values, valueParams{Name: p.Variable, Value: p.Value})
	}
	if len(values) == 0 {
		return sc.Cursor, fmt.Errorf("cloud_publish_prop: give either variable/value or values")
	}
	wireValues, err := toWireValues(values)
	if err != nil {
		return sc.Cursor, fmt.Errorf("cloud_publish_prop: %w", err)
	}
	payload, err := wire.EncodeSenML(wireValues)
	if err != nil {
		return sc.Cursor, fmt.Errorf("cloud_publish_prop: %w", err)
	}
	sc.Detail = fmt.Sprintf("cloud set %s on %s", describeValues(wireValues), broker.PropertyInTopic(thingID))

	if err := sc.World.Broker.PublishProperty(thingID, payload); err != nil {
		return sc.Cursor, fmt.Errorf("cloud_publish_prop: %w", err)
	}
	return sc.Cursor, nil
}

type stopDaemonParams struct {
	Timeout time.Duration `yaml:"timeout"`
}

// stopDaemon asks the daemon to shut down and waits for it to go.
//
// It is the counterpart of expect_daemon_exit, and the pair has to exist
// together: the process supervisor aborts the whole scenario on an exit nobody
// asked for, so an exit can only be expected if something announced it. This is
// what announces it.
//
// On a host without SIGTERM the process is killed instead, and the timeline
// says the graceful path was not exercised -- a shutdown scenario must check
// that rather than pass on a kill.
func stopDaemon(ctx context.Context, sc *Context, node *yaml.Node) (eventlog.Cursor, error) {
	var p stopDaemonParams
	if err := decodeInto(node, &p); err != nil {
		return sc.Cursor, err
	}
	if sc.World.Daemon == nil {
		return sc.Cursor, fmt.Errorf("stop_daemon: no daemon is running")
	}
	timeout := p.Timeout
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	stopCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	info, err := sc.World.Daemon.Stop(stopCtx)
	if err != nil {
		return sc.Cursor, fmt.Errorf("stop_daemon: %w", err)
	}
	sc.Detail = fmt.Sprintf("stopped (exit %d, graceful=%t)", info.Code, sc.World.Daemon.GracefulStopExercised())
	return sc.Cursor, nil
}

func toWireValues(params []valueParams) ([]wire.Value, error) {
	out := make([]wire.Value, 0, len(params))
	for _, v := range params {
		if v.Name == "" {
			return nil, fmt.Errorf("a value needs a name")
		}
		if v.Value == nil {
			return nil, fmt.Errorf("value %q has no value", v.Name)
		}
		out = append(out, wire.Value{Name: v.Name, Value: v.Value, Time: v.Time})
	}
	return out, nil
}

func describeValues(values []wire.Value) string {
	parts := make([]string, 0, len(values))
	for _, v := range values {
		parts = append(parts, fmt.Sprintf("%s=%v", v.Name, v.Value))
	}
	return fmt.Sprint(parts)
}

// ── parameter decoding ───────────────────────────────────────────────────────

// decodeMap reads the parameters as a plain map, for the expectation primitive
// where every unknown key is a constraint.
func decodeMap(node *yaml.Node) (map[string]any, error) {
	params := map[string]any{}
	if node == nil || node.Kind == 0 || node.Tag == "!!null" {
		return params, nil
	}
	if err := node.Decode(&params); err != nil {
		return nil, fmt.Errorf("parameters must be a mapping: %w", err)
	}
	return params, nil
}

// decodeInto reads the parameters into a typed struct, rejecting any key the
// struct does not declare.
//
// The strictness is the point: a mistyped parameter name would otherwise be
// silently ignored and the step would quietly do something other than what the
// scenario says. yaml.Node.Decode has no strict mode, so the node is
// re-marshalled through a decoder that does.
func decodeInto(node *yaml.Node, target any) error {
	if node == nil || node.Kind == 0 || node.Tag == "!!null" {
		return nil
	}
	raw, err := yaml.Marshal(node)
	if err != nil {
		return fmt.Errorf("parameters: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(target); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("parameters: %w", err)
	}
	return nil
}

func takeTimeout(params map[string]any) (time.Duration, error) {
	raw, ok := params[timeoutKey]
	if !ok {
		return DefaultTimeout, nil
	}
	delete(params, timeoutKey)
	switch v := raw.(type) {
	case string:
		d, err := time.ParseDuration(v)
		if err != nil {
			return 0, fmt.Errorf("timeout %q: %w", v, err)
		}
		return d, nil
	case int:
		return time.Duration(v) * time.Second, nil
	default:
		return 0, fmt.Errorf("timeout must be a duration string like \"10s\", got %v (%T)", raw, raw)
	}
}

func sortedKeys(params map[string]any) []string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
