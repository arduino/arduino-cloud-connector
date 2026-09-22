// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package cloud implements the Cloud FSM — the nested finite state machine
// that lives inside the daemon's macro `Run` state and owns the per-thing
// handshake protocol with Arduino IoT Cloud:
//
//	Disconnected → Reconnecting → Connecting → AnnouncingDevice
//	  (Device.begin → DeviceNetConfig → Thing.begin)
//	  → AwaitingThingID  (two-phase back-off; loops on empty ThingUpdateCmd)
//	    → SyncingLastValues  (subscribe property topic → LastValues.begin)
//	      → Steady
//	        ├─ ThingDetachCmd / ThingUpdateCmd(empty) → AwaitingThingID
//	        ├─ ThingUpdateCmd(new_id, different)      → handled in-place, stay in Steady
//	        └─ broker disconnect                      → Reconnecting
//
// The FSM is fully self-managed: on any broker connection loss (Paho callback
// or publish/subscribe failure) it autonomously transitions to Reconnecting,
// waits with exponential back-off, and re-runs the entire handshake. The
// daemon FSM only starts this FSM and cancels its context for graceful
// shutdown; it never drives transitions directly.
//
// # DNS invariant on reconnect
//
// Every transition from Reconnecting → Connecting calls mqtt.Client.Connect,
// which by contract performs a fresh, uncached DNS resolution of the broker
// hostname (see package internal/mqtt). The Arduino IoT Cloud broker IP can
// change between connection attempts, so the FSM intentionally does not
// retain any address information across the back-off interval: re-resolution
// is the broker driver's responsibility on every dial.
package cloud

import (
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"math/big"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
	"github.com/arduino/arduino-cloud-connector/internal/senml"
	"github.com/arduino/arduino-cloud-connector/internal/system"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// ── States ────────────────────────────────────────────────────────────────────

// State is one of the cloud FSM states. Exposed as a string for status reporting.
type State string

const (
	StateDisconnected      State = "Disconnected"
	StateReconnecting      State = "Reconnecting"
	StateConnecting        State = "Connecting"
	StateAnnouncingDevice  State = "AnnouncingDevice"
	StateAwaitingThingID   State = "AwaitingThingID"
	StateSyncingLastValues State = "SyncingLastValues"
	StateSteady            State = "Steady"
)

// ── Back-off parameters ──────────────────────────────────────────────────────

// Broker reconnection back-off — exponential with jitter, generic.
const (
	connBackoffBase       = 1 * time.Second
	connBackoffMax        = 5 * time.Minute
	connBackoffMultiplier = 2.0
	connBackoffJitter     = 0.10 // ±10%
)

// Thing.begin retry back-off — mirrors ArduinoIoTCloud C++ (src/AIoTC_Config.h:160-169).
// Phase 1: 2 → 4 → 8 → 16 → 32 → 32 s (cloud hasn't acknowledged us yet).
// Phase 2: 20 → 40 → 80 → 160 → 320 → 640 → 1280 s (cloud registered us but
// no thing assigned). Never disconnect; cap at 1280 s indefinitely.
const (
	thingIDPhase1Base = 2 * time.Second
	thingIDPhase1Max  = 32 * time.Second
	thingIDPhase2Base = 20 * time.Second
	thingIDPhase2Max  = 1280 * time.Second
)

// LastValues sync retry — diverges from C++ (src/AIoTC_Config.h:171-172):
// re-request every 15 s up to 10 attempts, then log an error and proceed to
// Steady instead of disconnecting. The broker stays connected, so a late
// LastValues.update or live property updates are still applied.
const (
	lastValuesRetryInterval = 15 * time.Second
	lastValuesMaxRetries    = 10
)

const (
	phasePending    = "pending"
	phaseRegistered = "registered"
)

// timers holds the FSM's configurable duration parameters. Initialised with
// production constants by New; tests may override individual fields to
// accelerate back-off intervals.
type timers struct {
	connBackoffBase   time.Duration
	thingBeginRetry   time.Duration
	lastValuesTimeout time.Duration
}

// ── Events ────────────────────────────────────────────────────────────────────

// event is the marker interface for any input the FSM consumes.
type event interface {
	String() string
}

type evCommandMessage struct{ msg mqtt.CommandMessage }
type evPropertyMessage struct{ msg mqtt.PropertyMessage }
type evConnectionLost struct{ err error }

func (e evCommandMessage) String() string {
	return fmt.Sprintf("CommandMessage(%s)", e.msg.Cmd)
}
func (e evPropertyMessage) String() string {
	return fmt.Sprintf("PropertyMessage(len=%d)", len(e.msg.Payload))
}
func (e evConnectionLost) String() string {
	return fmt.Sprintf("ConnectionLost(%v)", e.err)
}

// ── FSM ───────────────────────────────────────────────────────────────────────

// FSM is the cloud lifecycle state machine. Construct via New, drive via Run,
// observe via Snapshot.
type FSM struct {
	cfg      config.Config
	ks       *keystore.Keystore
	reg      *variables.Registry
	client   mqtt.Client
	deviceID string

	// netConfig reports the active network for the DeviceNetConfig announcement.
	// Defaults to system.NetConfig.
	netConfig func() (command.DeviceNetConfigCmd, bool)

	// run-goroutine-private state (no external access)
	state       State
	thingID     command.ThingID
	connBackoff time.Duration

	// shared state — published atomically via mu on every transition
	mu       sync.RWMutex
	snapshot Snapshot

	events chan event
	done   chan struct{}

	// timers holds configurable duration parameters; initialised with production
	// constants by New and overridden by tests to accelerate back-off intervals.
	timers timers
}

// Snapshot is an atomic view of the FSM's externally-observable state.
// ThingID is a plain string so callers do not need to import the command
// package just to read it.
type Snapshot struct {
	State   State
	ThingID string
}

// New creates a Cloud FSM bound to the given client and keystore. The
// keystore must already hold a valid device_id (the board must be provisioned).
func New(cfg config.Config, ks *keystore.Keystore, reg *variables.Registry, client mqtt.Client) (*FSM, error) {
	deviceID, err := ks.DeviceID()
	if err != nil {
		return nil, fmt.Errorf("cloud: read device_id: %w", err)
	}
	f := newFSM(cfg, deviceID, reg, client)
	f.ks = ks
	return f, nil
}

// newFSM builds an FSM from a known device_id, bypassing the keystore. Shared by
// New and tests (which inject a fake client without a provisioned keystore).
func newFSM(cfg config.Config, deviceID string, reg *variables.Registry, client mqtt.Client) *FSM {
	f := &FSM{
		cfg:       cfg,
		reg:       reg,
		client:    client,
		deviceID:  deviceID,
		netConfig: system.NetConfig,
		events:    make(chan event, 64),
		done:      make(chan struct{}),
		snapshot:  Snapshot{State: StateDisconnected},
		timers: timers{
			connBackoffBase:   connBackoffBase,
			thingBeginRetry:   thingIDPhase1Base,
			lastValuesTimeout: lastValuesRetryInterval,
		},
	}

	// Wire the broker-disconnect notification straight into the event channel.
	// The callback is invoked by a Paho goroutine — keep it cheap.
	client.OnConnectionLost(func(err error) {
		f.send(evConnectionLost{err: err})
	})

	return f
}

// Snapshot returns a copy of the current externally-observable state.
// Safe to call from any goroutine.
func (f *FSM) Snapshot() Snapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snapshot
}

// Done returns a channel that is closed when Run has returned (after the
// graceful shutdown sequence: unsubscribe + disconnect).
func (f *FSM) Done() <-chan struct{} { return f.done }

// Run drives the FSM. It blocks until ctx is cancelled, performing a clean
// shutdown (unsubscribe property topic, disconnect broker) before returning.
// Safe to call exactly once per FSM instance.
func (f *FSM) Run(ctx context.Context) {
	defer close(f.done)
	defer f.shutdown()

	next := f.runReconnecting
	for next != nil {
		next = next(ctx)
	}
}

// ── State functions ──────────────────────────────────────────────────────────

type stateFn func(ctx context.Context) stateFn

func (f *FSM) runReconnecting(ctx context.Context) stateFn {
	f.transition(StateReconnecting, "")
	slog.Debug("cloud: (re)connecting — draining stale events from previous connection")
	f.drainEvents()

	base := f.timers.connBackoffBase
	delay := f.connBackoff
	if delay == 0 {
		delay = base
	} else {
		delay = nextConnBackoff(delay)
	}
	f.connBackoff = delay

	slog.Info("cloud: waiting before next connection attempt", "delay", delay)
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(delay):
		return f.runConnecting
	}
}

func (f *FSM) runConnecting(ctx context.Context) stateFn {
	f.transition(StateConnecting, "")

	slog.Info("cloud: connecting to broker", "broker", f.cfg.MQTTBroker, "device_id", f.deviceID)
	if err := f.client.Connect(ctx); err != nil {
		if ctx.Err() != nil {
			slog.Debug("cloud: connect aborted by context cancel")
			return nil
		}
		slog.Error("cloud: connect failed", "error", err)
		return f.runReconnecting
	}
	f.connBackoff = 0 // reset on successful connect
	slog.Info("cloud: connected to broker", "device_id", f.deviceID)
	return f.runAnnouncingDevice
}

func (f *FSM) runAnnouncingDevice(ctx context.Context) stateFn {
	f.transition(StateAnnouncingDevice, "")

	if err := f.client.SubscribeCommandChannel(f.deviceID, func(msg mqtt.CommandMessage) {
		f.send(evCommandMessage{msg: msg})
	}); err != nil {
		slog.Error("cloud: subscribe command channel failed", "error", err)
		f.client.Disconnect()
		return f.runReconnecting
	}

	if err := f.client.PublishCommand(f.deviceID,
		command.From(command.DeviceBeginCmd{LibVersion: f.cfg.Version}),
	); err != nil {
		slog.Error("cloud: Device.begin failed", "error", err)
		f.client.Disconnect()
		return f.runReconnecting
	}

	// Announce the network config between Device.begin and Thing.begin (C++
	// handleSendCapabilities order). Best-effort: UI-only metadata, so a failure
	// here must not abort the handshake.
	f.publishNetConfig()

	return f.runAwaitingThingID
}

// publishNetConfig detects the active network connection and, if known, sends a
// DeviceNetConfig command. Errors are logged, never propagated.
func (f *FSM) publishNetConfig() {
	if f.netConfig == nil {
		return
	}
	netCfg, ok := f.netConfig()
	if !ok {
		slog.Debug("cloud: network configuration unknown, skipping DeviceNetConfig")
		return
	}
	if err := f.client.PublishCommand(f.deviceID, command.From(netCfg)); err != nil {
		slog.Warn("cloud: DeviceNetConfig publish failed", "error", err)
		return
	}
	slog.Info("cloud: published network configuration", "type", netCfg.Type, "ssid", netCfg.SSID)
}

func (f *FSM) runAwaitingThingID(ctx context.Context) stateFn {
	f.thingID = ""
	f.transition(StateAwaitingThingID, "")

	if err := f.client.PublishCommand(f.deviceID,
		command.From(command.ThingBeginCmd{ThingID: ""}),
	); err != nil {
		slog.Error("cloud: Thing.begin failed", "error", err)
		f.client.Disconnect()
		return f.runReconnecting
	}

	phase1Base := thingIDPhase1Base
	if f.timers.thingBeginRetry > 0 {
		phase1Base = f.timers.thingBeginRetry
	}
	delay, maxDelay := phase1Base, thingIDPhase1Max
	phase := phasePending
	timer := time.NewTimer(delay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if err := f.client.PublishCommand(f.deviceID,
				command.From(command.ThingBeginCmd{ThingID: ""}),
			); err != nil {
				slog.Error("cloud: Thing.begin retry failed", "error", err)
				f.client.Disconnect()
				return f.runReconnecting
			}
			delay = nextThingIDBackoff(delay, maxDelay)
			slog.Warn("cloud: no thing_id yet, resent Thing.begin",
				"phase", phase, "next_retry_in", delay)
			timer.Reset(delay)
		case ev := <-f.events:
			switch e := ev.(type) {
			case evConnectionLost:
				slog.Info("cloud: broker connection lost", "error", e.err)
				return f.runReconnecting
			case evCommandMessage:
				msg, ok := e.msg.Cmd.Inner().(command.ThingUpdateCmd)
				if !ok {
					slog.Debug("cloud: ignoring non-ThingUpdate command while awaiting thing_id", "cmd", e.msg.Cmd)
					continue
				}
				if msg.ThingID == "" {
					if phase == phasePending {
						slog.Info("cloud: registered device with no thing attached; switching to long back-off",
							"phase2_base", thingIDPhase2Base, "phase2_max", thingIDPhase2Max)
						phase = phaseRegistered
						delay, maxDelay = thingIDPhase2Base, thingIDPhase2Max
						timer.Reset(delay)
					}
					continue
				}
				slog.Info("cloud: thing_id received", "thing_id", msg.ThingID)
				f.thingID = msg.ThingID
				return f.runSyncingLastValues
			}
		}
	}
}

func (f *FSM) runSyncingLastValues(ctx context.Context) stateFn {
	f.transition(StateSyncingLastValues, f.thingID)

	if err := f.client.SubscribePropertyTopic(f.thingID.String(), func(msg mqtt.PropertyMessage) {
		f.send(evPropertyMessage{msg: msg})
	}); err != nil {
		slog.Error("cloud: subscribe property topic failed", "error", err)
		f.client.Disconnect()
		return f.runReconnecting
	}

	requestLastValues := func() error {
		return f.client.PublishCommand(f.deviceID, command.From(command.LastValuesBeginCmd{}))
	}

	attempts := 1
	if err := requestLastValues(); err != nil {
		slog.Error("cloud: LastValues.begin failed", "error", err)
		f.client.Disconnect()
		return f.runReconnecting
	}

	lvTimeout := lastValuesRetryInterval
	if f.timers.lastValuesTimeout > 0 {
		lvTimeout = f.timers.lastValuesTimeout
	}
	timer := time.NewTimer(lvTimeout)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			if attempts >= lastValuesMaxRetries {
				slog.Error("cloud: no LastValues.update after max attempts; entering steady state without initial sync",
					"attempts", attempts, "thing_id", f.thingID)
				// No answer is treated as "the cloud announced nothing": every
				// pending subscriber gets lastvalue_missing so apps stop waiting
				// and can publish. Leaving them pending would freeze them in both
				// directions, and the request pipeline makes a late answer
				// unlikely enough that waiting for one is the worse bet.
				f.reg.ApplyLastValues(nil)
				return f.runSteady
			}
			attempts++
			if err := requestLastValues(); err != nil {
				slog.Error("cloud: LastValues.begin retry failed", "error", err)
				f.client.Disconnect()
				return f.runReconnecting
			}
			slog.Warn("cloud: no LastValues.update yet, retrying",
				"attempt", attempts, "max", lastValuesMaxRetries, "retry_in", f.timers.lastValuesTimeout)
			timer.Reset(f.timers.lastValuesTimeout)
		case ev := <-f.events:
			switch e := ev.(type) {
			case evConnectionLost:
				slog.Info("cloud: broker connection lost during LastValues sync", "error", e.err)
				return f.runReconnecting
			case evCommandMessage:
				switch msg := e.msg.Cmd.Inner().(type) {
				case command.LastValuesUpdateCmd:
					slog.Info("cloud: received LastValues", "thing_id", f.thingID)
					if err := f.applyProperties(msg.Values, true); err != nil {
						slog.Warn("cloud: failed to decode LastValues payload", "error", err)
					}
					return f.runSteady
				case command.ThingDetachCmd:
					slog.Info("cloud: thing detached during LastValues sync")
					_ = f.client.UnsubscribePropertyTopic(f.thingID.String())
					return f.runAwaitingThingID
				}
			}
		}
	}
}

func (f *FSM) runSteady(ctx context.Context) stateFn {
	f.transition(StateSteady, f.thingID)
	slog.Info("cloud: entered steady state", "thing_id", f.thingID)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("cloud: steady state context cancelled, exiting")
			return nil
		case ev := <-f.events:
			switch e := ev.(type) {
			case evConnectionLost:
				slog.Info("cloud: broker connection lost in steady state", "error", e.err)
				return f.runReconnecting
			case evCommandMessage:
				if next := f.handleSteadyCommand(e.msg.Cmd); next != nil {
					return next
				}
			case evPropertyMessage:
				if err := f.applyProperties(e.msg.Payload, false); err != nil {
					slog.Warn("cloud: failed to decode inbound property update", "error", err)
				}
			}
		}
	}
}

// handleSteadyCommand processes a downlink command in steady state. Returns
// a non-nil stateFn if the message forces a state transition; nil if the
// command was handled in-place and the FSM stays in Steady.
func (f *FSM) handleSteadyCommand(cmd command.Cmd) stateFn {
	switch msg := cmd.Inner().(type) {
	case command.ThingDetachCmd:
		slog.Info("cloud: ThingDetach received", "thing_id", f.thingID)
		_ = f.client.UnsubscribePropertyTopic(f.thingID.String())
		return f.runAwaitingThingID

	case command.ThingUpdateCmd:
		switch msg.ThingID {
		case f.thingID:
			slog.Debug("cloud: ThingUpdate re-announce", "thing_id", msg.ThingID)
			return nil
		case "":
			slog.Info("cloud: ThingUpdate with empty id; treating as detach", "old_thing_id", f.thingID)
			_ = f.client.UnsubscribePropertyTopic(f.thingID.String())
			return f.runAwaitingThingID
		default:
			// Cloud reassigned us to a different thing. Detach+attach in-place,
			// stay in Steady — no broker reconnect, no exit from the command
			// channel dispatcher.
			slog.Info("cloud: thing reassigned by cloud",
				"old_thing_id", f.thingID, "new_thing_id", msg.ThingID)
			_ = f.client.UnsubscribePropertyTopic(f.thingID.String())
			f.thingID = msg.ThingID
			f.transition(StateSteady, msg.ThingID)
			if err := f.client.SubscribePropertyTopic(msg.ThingID.String(), func(m mqtt.PropertyMessage) {
				f.send(evPropertyMessage{msg: m})
			}); err != nil {
				slog.Error("cloud: re-attach subscribe failed", "error", err)
				f.client.Disconnect()
				return f.runReconnecting
			}
			if err := f.client.PublishCommand(f.deviceID,
				command.From(command.LastValuesBeginCmd{}),
			); err != nil {
				slog.Warn("cloud: LastValues.begin after re-attach failed", "error", err)
			}
			return nil
		}

	case command.LastValuesUpdateCmd:
		slog.Info("cloud: LastValuesUpdate in steady state", "thing_id", f.thingID)
		if err := f.applyProperties(msg.Values, true); err != nil {
			slog.Warn("cloud: failed to decode LastValuesUpdate payload", "error", err)
		}
		return nil

	default:
		slog.Debug("cloud: unhandled command in steady state", "cmd", cmd)
		return nil
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (f *FSM) transition(state State, thingID command.ThingID) {
	if state != f.state {
		slog.Info("cloud: state transition", "from", f.state, "to", state, "thing_id", thingID)
	}
	f.state = state
	f.mu.Lock()
	f.snapshot = Snapshot{State: state, ThingID: thingID.String()}
	f.mu.Unlock()
}

func (f *FSM) send(ev event) {
	select {
	case f.events <- ev:
	default:
		slog.Warn("cloud: event channel full, dropping event", "event", ev.String())
	}
}

// drainEvents discards any stale events left over from a previous connection.
// Called when entering Reconnecting so the next Connected state starts fresh.
func (f *FSM) drainEvents() {
	for {
		select {
		case <-f.events:
		default:
			return
		}
	}
}

// shutdown performs the graceful exit sequence. Called via defer from Run.
func (f *FSM) shutdown() {
	slog.Info("cloud: shutting down", "state", f.state, "thing_id", f.thingID)
	if f.thingID != "" {
		_ = f.client.UnsubscribePropertyTopic(f.thingID.String())
	}
	f.client.Disconnect()
	f.transition(StateDisconnected, "")
	slog.Info("cloud: shutdown complete")
}

// applyProperties decodes a SenML+CBOR payload and writes each variable into
// the registry (creating entries on demand, so values pushed by the cloud
// before any app subscribes are still stored as the board's value).
//
// isLastValues distinguishes the two kinds of inbound payload, and it decides
// which SSE event apps see — it is not just a logging switch:
//
//   - true (a LastValues.update, the reply to LastValues.begin): the payload is
//     handed to Registry.ApplyLastValues, which emits "lastvalue" sync frames
//     for what the cloud announced and "lastvalue_missing" to subscribers still
//     waiting for a verdict. Each decoded variable's NAME is logged (never its
//     value — see TestApplyProperties_NeverLogsVariableValue).
//   - false (a live property message on the thing's inbound topic): each value
//     goes through SetValue, which emits "update".
//
// On the last-values path exactly one ApplyLastValues call happens on every
// exit path, including an empty or undecodable payload: a pending subscriber
// must always get its verdict, and "we could not read the answer" is no
// different from "there was no answer".
func (f *FSM) applyProperties(payload []byte, isLastValues bool) error {
	var (
		vars []senml.Variable
		err  error
	)
	if len(payload) > 0 {
		vars, err = senml.Decode(payload)
	}

	if !isLastValues {
		if err != nil {
			return err
		}
		for _, v := range vars {
			// No origin id: this value came from the cloud, not from an app, so
			// it must reach every subscriber — including one that wrote the
			// same variable earlier and needs to learn the cloud changed it.
			f.reg.SetValue(v.Name, v.Value, v.Timestamp, "")
		}
		return nil
	}

	values := make([]variables.Variable, 0, len(vars))
	for _, v := range vars {
		slog.Info("cloud: last value received", "name", v.Name)
		values = append(values, variables.Variable{Name: v.Name, Value: v.Value, Timestamp: v.Timestamp})
	}
	f.reg.ApplyLastValues(values)
	return err
}

func nextConnBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * connBackoffMultiplier)
	if next > connBackoffMax {
		next = connBackoffMax
	}
	randomSeed, err := rand.Int(rand.Reader, big.NewInt(10))
	if err != nil {
		randomSeed = big.NewInt(0)
	}
	jitter := float64(next) * connBackoffJitter * (float64(randomSeed.Int64())*2 - 1)
	return next + time.Duration(jitter)
}

func nextThingIDBackoff(current, maxDelay time.Duration) time.Duration {
	next := current * 2
	if next > maxDelay {
		return maxDelay
	}
	return next
}
