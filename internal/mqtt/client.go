// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package mqtt provides the low-level Arduino IoT Cloud MQTT client.
//
// Topic architecture:
//
//	Command channel (CBOR-tagged, device-keyed):
//	  pub  /a/d/<device_id>/c/up    — device → cloud
//	  sub  /a/d/<device_id>/c/dw    — cloud → device
//
//	Telemetry/properties channel (SenML+CBOR, thing-keyed):
//	  pub  /a/t/<thing_id>/e/o      — outbound property values
//	  sub  /a/t/<thing_id>/e/i      — inbound property values
//
// The cloud lifecycle (Device.begin → Thing.begin → LastValues → Steady →
// Detach/Reattach → Reconnect) lives in package internal/cloud, not
// here. This package is stateless with respect to thing_id and handshake
// phase: it only knows how to dial the broker and ferry bytes.
//
// Command encoding/decoding is handled by the sub-package
// internal/mqtt/command, which registers a fxamacker/cbor/v2 TagSet so that
// each CBOR tag maps to a concrete typed struct. Callers use type switches on
// [CommandMessage.Cmd].Inner() to handle specific commands.
//
// # DNS invariant
//
// Every call to Connect() on the Paho-backed client performs a fresh DNS
// resolution of the broker hostname. The Arduino IoT Cloud broker sits behind
// a load balancer whose IP set can rotate between connection attempts; a
// cached IP is never safe to reuse. See client_paho.go for details.
//
// # Mock builds
//
// There is no MQTT-specific mock: the Paho-backed client is compiled into
// -tags mock builds too. Only the board-dependent pieces have mock variants
// (provisioning, UHWID, keystore permissions). Without real credentials and a
// registered board the broker connection never establishes, so the Cloud FSM
// stays in its pre-Steady states — the expected behaviour for front-end
// development without a real board or cloud account.
package mqtt

import (
	"context"
	"fmt"

	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// CommandMessage wraps a fully decoded, typed command received on the
// device-keyed downlink topic (/a/d/<device_id>/c/dw).
//
// Use Cmd.Inner() with a type switch to handle the concrete command:
//
//	switch msg := cmdMsg.Cmd.Inner().(type) {
//	case command.ThingUpdateCmd:
//	    id := msg.ThingID
//	case command.ThingDetachCmd:
//	    ...
//	}
type CommandMessage struct {
	Cmd command.Cmd
}

// PropertyMessage is a raw payload received on the thing-keyed inbound
// property topic (/a/t/<thing_id>/e/i). Decoding (SenML+CBOR → variable
// values) is the lifecycle FSM's responsibility, not the client's.
type PropertyMessage struct {
	Payload []byte
}

// Client is the low-level MQTT abstraction used by the thing lifecycle FSM.
// Implementations are stateless with respect to lifecycle/thing_id; they only
// manage the broker connection and topic-level subscriptions.
type Client interface {
	// Connect establishes the broker connection. Blocks until the handshake
	// completes or ctx is cancelled.
	Connect(ctx context.Context) error

	// Disconnect closes the broker connection. Safe to call on an already
	// disconnected client (no-op).
	Disconnect()

	// IsConnected reports the current connection state.
	IsConnected() bool

	// OnConnectionLost registers a callback that fires when the broker drops
	// the connection unexpectedly. Must be called BEFORE Connect to be
	// effective on the first connection.
	OnConnectionLost(handler func(err error))

	// SubscribeCommandChannel subscribes to /a/d/<deviceID>/c/dw and forwards
	// every decoded command to onMessage. The callback runs on a background
	// goroutine — keep it cheap (push to a channel and return).
	SubscribeCommandChannel(deviceID string, onMessage func(CommandMessage)) error

	// SubscribePropertyTopic subscribes to /a/t/<thingID>/e/i with the given
	// raw-payload callback. Same threading caveat as SubscribeCommandChannel.
	SubscribePropertyTopic(thingID string, onMessage func(PropertyMessage)) error

	// UnsubscribePropertyTopic unsubscribes from /a/t/<thingID>/e/i.
	UnsubscribePropertyTopic(thingID string) error

	// PublishCommand encodes cmd as a CBOR-tagged message and publishes it on
	// /a/d/<deviceID>/c/up.
	PublishCommand(deviceID string, cmd command.Cmd) error

	// PublishProperty publishes a raw SenML+CBOR payload on /a/t/<thingID>/e/o.
	PublishProperty(thingID string, payload []byte) error
}

// ── topic helpers ─────────────────────────────────────────────────────────────

func commandUpTopic(deviceID string) string   { return fmt.Sprintf("/a/d/%s/c/up", deviceID) }
func commandDownTopic(deviceID string) string { return fmt.Sprintf("/a/d/%s/c/dw", deviceID) }
func propertyInTopic(thingID string) string   { return fmt.Sprintf("/a/t/%s/e/i", thingID) }
func propertyOutTopic(thingID string) string  { return fmt.Sprintf("/a/t/%s/e/o", thingID) }
