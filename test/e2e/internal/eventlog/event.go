// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package eventlog is the backbone of the E2E harness: a single ordered log
// that every observation source writes into, and that every assertion reads
// from.
//
// The alternative — one channel per fake — was rejected for three reasons that
// matter in practice. A channel read consumes the event, so two assertions
// cannot both see it. Each source needs its own bespoke wait, so every one is
// a fresh opportunity to write a flaky sleep. And there is no way to ask a
// question that spans sources ("did the CSR arrive before the MQTT CONNECT?"),
// because nothing orders events across channels.
//
// One append-only log with a single monotonic Seq fixes all three: Await is
// non-destructive and cursor-based, every wait is a condition with an explicit
// timeout (no sleeps anywhere in the harness), ordering assertions are integer
// comparisons, and a failure can print the whole timeline instead of the word
// "timeout".
package eventlog

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// Source is where an observation came from.
type Source string

const (
	SourceProvisioningAPI Source = "provisioning_api"
	SourceNTP             Source = "ntp"
	SourceMQTT            Source = "mqtt"
	SourceSSE             Source = "sse"
	SourceDaemonStatus    Source = "daemon_status"
	SourceDaemonLog       Source = "daemon_log"
	SourceDaemonProcess   Source = "daemon_process"
)

// Kind is what kind of observation it is, within a Source.
type Kind string

const (
	KindHTTPRequest Kind = "http_request"
	KindNTPProbe    Kind = "ntp_probe"

	KindMQTTConnect     Kind = "mqtt_connect"
	KindMQTTDisconnect  Kind = "mqtt_disconnect"
	KindMQTTSubscribe   Kind = "mqtt_subscribe"
	KindMQTTUnsubscribe Kind = "mqtt_unsubscribe"
	KindMQTTPublish     Kind = "mqtt_publish"
	KindMQTTKeepalive   Kind = "mqtt_keepalive" // PINGREQ/PINGRESP
	KindMQTTAck         Kind = "mqtt_ack"       // PUBACK/SUBACK

	KindSSEFrame    Kind = "sse_frame"
	KindStatusPoll  Kind = "status_poll"
	KindDaemonLine  Kind = "daemon_line"
	KindHarnessNote Kind = "harness_note" // the harness narrating its own actions

	// KindProcessExit is the daemon under test terminating. It is significant:
	// an exit a scenario did not ask for is a crash, and a scenario that DOES
	// ask for one (a graceful-shutdown scenario) consumes it like any other
	// expectation.
	KindProcessExit Kind = "process_exit"
)

// Event is one observation. Attrs carries the decoded, queryable view (topic,
// command name, variable, value, status code…); Raw keeps the original bytes
// for the artifact dump when there are any.
//
// Seq is assigned by the Log and is monotonic across EVERY source, which is
// what makes cross-source ordering assertions possible.
type Event struct {
	Seq    int            `json:"seq"`
	At     time.Time      `json:"at"`
	Since  time.Duration  `json:"since"` // since the log was created; what the timeline prints
	Source Source         `json:"source"`
	Kind   Kind           `json:"kind"`
	Attrs  map[string]any `json:"attrs,omitempty"`
	Raw    []byte         `json:"raw,omitempty"`
}

// Significant reports whether this event participates in the strict
// unconsumed-event sweep at the end of a scenario.
//
// The distinction is not cosmetic: without it, "fail on any event no step
// consumed" would fail every scenario immediately, because the daemon emits
// log lines continuously, the status poller ticks, and MQTT keepalive runs
// every 30s. Only protocol-level facts count — the things a scenario is
// actually making claims about.
//
// Sources excluded entirely (ntp, daemon_status, daemon_log) are timeline
// context: useful when reading a failure, never assertable by absence.
func (e Event) Significant() bool {
	switch e.Source {
	case SourceProvisioningAPI, SourceSSE:
		return true
	case SourceDaemonProcess:
		return e.Kind == KindProcessExit
	case SourceMQTT:
		switch e.Kind {
		case KindMQTTConnect, KindMQTTDisconnect,
			KindMQTTSubscribe, KindMQTTUnsubscribe, KindMQTTPublish:
			return true
		default:
			return false
		}
	default:
		return false
	}
}

// String renders one timeline row: "#12 +1.12s  mqtt  SUBSCRIBE  /a/t/abc/e/i".
//
// NOTE: this prints property values, which is correct here — they are
// synthetic test data and seeing them is the point of the timeline. It is the
// opposite of what the daemon must do: production logging must never log cloud
// variable values, they are user data. Do not lift this formatter into the
// daemon.
func (e Event) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "#%-3d %7s  %-17s %-13s", e.Seq, formatSince(e.Since), e.Source, e.displayKind())
	if s := e.attrsString(); s != "" {
		b.WriteString("  " + s)
	}
	return b.String()
}

// displayKind drops the source prefix a Kind repeats, so a row reads
// "mqtt  subscribe" instead of "mqtt  mqtt_subscribe". Purely cosmetic: the
// Kind constant is unchanged, and predicates still match on the full name.
func (e Event) displayKind() string {
	if s, ok := strings.CutPrefix(string(e.Kind), string(e.Source)+"_"); ok {
		return s
	}
	return string(e.Kind)
}

// attrsString renders Attrs with stable ordering, so timelines diff cleanly
// between runs and golden tests stay meaningful.
func (e Event) attrsString() string {
	if len(e.Attrs) == 0 {
		return ""
	}
	keys := make([]string, 0, len(e.Attrs))
	for k := range e.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%v", k, e.Attrs[k]))
	}
	return strings.Join(parts, " ")
}

func formatSince(d time.Duration) string {
	return fmt.Sprintf("+%.2fs", d.Seconds())
}
