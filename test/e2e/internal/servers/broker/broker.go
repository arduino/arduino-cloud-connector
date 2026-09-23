// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package broker is the MQTT broker the daemon under test connects to: the
// harness playing the cloud.
//
// # Why an embedded broker rather than Mosquitto in a container
//
// Three reasons, in order of weight. Being the broker means seeing what a
// client cannot: SUBSCRIBE packets, the certificate presented at the
// handshake, and a handshake that was REFUSED -- which is the whole failure
// mode of a wrong device certificate. Docker as a test dependency means the
// suite only runs in CI, and a suite that only runs in CI stops being run.
// And Mosquitto is not VerneMQ either, so an external broker proves less than
// it appears to.
//
// What stays out of scope, deliberately: the real broker runs VerneMQ on
// Erlang, whose path validation is the public_key module rather than OpenSSL,
// so certificate acceptance by the REAL broker remains a staging and
// on-board matter. This package proves the daemon speaks the protocol, not
// that VerneMQ likes its certificate.
//
// # Every observation goes to the event log
//
// Connects, disconnects, subscribes, unsubscribes, publishes (decoded through
// internal/wire) and refused handshakes are all appended to the shared log,
// because an observation that is not in the log cannot be asserted on and
// cannot appear on a failure timeline. Injections the harness makes itself are
// recorded as harness notes rather than as MQTT traffic, so a step waiting for
// a device publish can never match the harness's own downlink.
package broker

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

// The topic architecture, restated rather than imported. The command channel
// is keyed by device, the property channel by thing.
//
//	pub  /a/d/<device_id>/c/up   device -> cloud, CBOR commands
//	sub  /a/d/<device_id>/c/dw   cloud -> device, CBOR commands
//	pub  /a/t/<thing_id>/e/o     device -> cloud, SenML properties
//	sub  /a/t/<thing_id>/e/i     cloud -> device, SenML properties
const (
	commandUpSuffix    = "/c/up"
	commandDownSuffix  = "/c/dw"
	propertyOutSuffix  = "/e/o"
	propertyInSuffix   = "/e/i"
	devicePrefix       = "/a/d/"
	thingPrefix        = "/a/t/"
	defaultInjectedQoS = 1
)

// CommandUpTopic is where the device publishes commands.
func CommandUpTopic(deviceID string) string { return devicePrefix + deviceID + commandUpSuffix }

// CommandDownTopic is where the cloud publishes commands.
func CommandDownTopic(deviceID string) string { return devicePrefix + deviceID + commandDownSuffix }

// PropertyOutTopic is where the device publishes property values.
func PropertyOutTopic(thingID string) string { return thingPrefix + thingID + propertyOutSuffix }

// PropertyInTopic is where the cloud publishes property values.
func PropertyInTopic(thingID string) string { return thingPrefix + thingID + propertyInSuffix }

// Options configures the broker.
type Options struct {
	// Log receives every observation. Required.
	Log *eventlog.Log
	// CA issues the broker's server certificate and is the root the presented
	// client certificate is verified against. Required.
	CA *pki.CA
}

// Server is a running broker.
type Server struct {
	log  *eventlog.Log
	ca   *pki.CA
	mqtt *mochi.Server
	addr string

	mu         sync.Mutex
	clientCert *x509.Certificate
}

// Start binds a TLS listener on loopback and serves until Close.
func Start(opts Options) (*Server, error) {
	if opts.Log == nil {
		return nil, fmt.Errorf("broker: no event log given")
	}
	if opts.CA == nil {
		return nil, fmt.Errorf("broker: no CA given")
	}
	s := &Server{log: opts.Log, ca: opts.CA}

	tlsCfg, err := s.tlsConfig()
	if err != nil {
		return nil, err
	}

	// InlineClient is what lets the harness publish downlink without opening a
	// second connection; mochi refuses Publish without it. The logger is
	// discarded because the event log is the observability channel here, and
	// anything worth reading is appended to it explicitly.
	s.mqtt = mochi.New(&mochi.Options{
		InlineClient: true,
		Logger:       slog.New(slog.DiscardHandler),
	})
	if err := s.mqtt.AddHook(&hook{server: s}, nil); err != nil {
		return nil, fmt.Errorf("broker: add hook: %w", err)
	}

	listener := listeners.NewTCP(listeners.Config{
		ID:        "e2e-mqtts",
		Address:   "127.0.0.1:0",
		TLSConfig: tlsCfg,
	})
	if err := s.mqtt.AddListener(listener); err != nil {
		return nil, fmt.Errorf("broker: add listener: %w", err)
	}
	// Addressed only after Init, which is what binds the socket and resolves
	// port 0 to the real one.
	s.addr = listener.Address()

	if err := s.mqtt.Serve(); err != nil {
		return nil, fmt.Errorf("broker: serve: %w", err)
	}
	return s, nil
}

// Addr is the bound host:port.
func (s *Server) Addr() string { return s.addr }

// URL is what ARDUINO_CLOUD_CONNECTOR__MQTT_BROKER must be set to.
//
// The mqtts scheme matters: it is what makes the daemon's client dial over
// TLS, and it is the scheme the production default uses.
func (s *Server) URL() string { return "mqtts://" + s.addr }

// Close stops the broker.
func (s *Server) Close() error {
	if err := s.mqtt.Close(); err != nil {
		return fmt.Errorf("broker: close: %w", err)
	}
	return nil
}

// ClientCert returns the certificate the last accepted client presented at the
// handshake, which is how a scenario checks that the device is using the
// certificate it was just issued.
func (s *Server) ClientCert() (*x509.Certificate, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.clientCert == nil {
		return nil, false
	}
	return s.clientCert, true
}

// ── TLS ──────────────────────────────────────────────────────────────────────

func (s *Server) tlsConfig() (*tls.Config, error) {
	serverCert, err := s.ca.ServerCert("localhost", "127.0.0.1")
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientCAs:    s.ca.Pool(),
		// RequestClientCert, not RequireAndVerifyClientCert, and the
		// enforcement moved into verifyClientCert below. The behaviour is the
		// same -- an unverifiable client is refused -- but this way every
		// rejection, including a client that presented nothing, lands on the
		// timeline with its reason. Left to crypto/tls, a refused handshake is
		// invisible to the harness and the scenario would simply time out
		// waiting for a CONNECT that was never going to arrive.
		ClientAuth:            tls.RequestClientCert,
		VerifyPeerCertificate: s.verifyClientCert,
		MinVersion:            tls.VersionTLS12,
		// The real broker is TLS 1.2-only, which is why the daemon caps itself
		// there too. Modelling that cap here keeps the harness from being more
		// permissive than the thing it stands in for.
		MaxVersion: tls.VersionTLS12,
	}, nil
}

// verifyClientCert is the broker's mTLS gate, and the place a wrong device
// certificate becomes a legible failure instead of a timeout.
func (s *Server) verifyClientCert(rawCerts [][]byte, _ [][]*x509.Certificate) error {
	reject := func(reason string, attrs map[string]any) error {
		if attrs == nil {
			attrs = map[string]any{}
		}
		attrs["reason"] = reason
		s.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTTLSError, attrs, nil)
		return fmt.Errorf("broker: client certificate rejected: %s", reason)
	}

	if len(rawCerts) == 0 {
		return reject("no certificate presented", nil)
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return reject(fmt.Sprintf("certificate does not parse: %v", err), nil)
	}
	attrs := map[string]any{
		"cert_cn":     leaf.Subject.CommonName,
		"cert_issuer": leaf.Issuer.String(),
	}

	intermediates := x509.NewCertPool()
	for _, raw := range rawCerts[1:] {
		if extra, perr := x509.ParseCertificate(raw); perr == nil {
			intermediates.AddCert(extra)
		}
	}
	if _, err := leaf.Verify(x509.VerifyOptions{
		Roots:         s.ca.Pool(),
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		return reject(err.Error(), attrs)
	}

	s.mu.Lock()
	s.clientCert = leaf
	s.mu.Unlock()
	return nil
}

// ── injection ────────────────────────────────────────────────────────────────

// PublishCommand sends a CBOR command on the device downlink, which is the
// cloud half of the handshake (Thing.update, LastValues.update, Thing.detach).
func (s *Server) PublishCommand(deviceID string, payload []byte) error {
	return s.publish(CommandDownTopic(deviceID), payload, defaultInjectedQoS)
}

// PublishProperty sends SenML property values on the thing inbound topic.
func (s *Server) PublishProperty(thingID string, payload []byte) error {
	return s.publish(PropertyInTopic(thingID), payload, defaultInjectedQoS)
}

// Publish sends a raw payload at a chosen QoS.
//
// The QoS is exposed because it is an open question with the cloud team, not
// because scenarios should be picky: the daemon subscribes at QoS 1 while the
// real cloud is believed to publish downlink at QoS 0, so a scenario that
// wants to pin that behaviour needs to say which one it is testing.
func (s *Server) Publish(topic string, payload []byte, qos byte) error {
	return s.publish(topic, payload, qos)
}

func (s *Server) publish(topic string, payload []byte, qos byte) error {
	if err := s.mqtt.Publish(topic, payload, false, qos); err != nil {
		return fmt.Errorf("broker: publish %s: %w", topic, err)
	}
	// Recorded as a harness note, NOT as an mqtt publish: notes are not
	// significant, so the harness's own downlink neither has to be consumed by
	// a step nor can be mistaken for something the device sent.
	attrs := map[string]any{
		"direction": "downlink",
		"topic":     topic,
		"qos":       qos,
		"size":      len(payload),
	}
	decodePayload(topic, payload, attrs)
	s.log.Append(eventlog.SourceMQTT, eventlog.KindHarnessNote, attrs, payload)
	return nil
}

// ── hooks ────────────────────────────────────────────────────────────────────

// hook turns mochi's callbacks into events.
type hook struct {
	mochi.HookBase
	server *Server
}

func (h *hook) ID() string { return "e2e-observer" }

func (h *hook) Provides(b byte) bool {
	switch b {
	case mochi.OnConnectAuthenticate,
		mochi.OnACLCheck,
		mochi.OnConnect,
		mochi.OnDisconnect,
		mochi.OnSubscribed,
		mochi.OnUnsubscribed,
		mochi.OnPublished,
		mochi.OnPacketRead:
		return true
	}
	return false
}

// OnConnectAuthenticate and OnACLCheck are deliberately wide open: the gate is
// the mutual-TLS handshake, which the real broker also relies on. Refusing
// here would test the harness, not the daemon.
func (h *hook) OnConnectAuthenticate(_ *mochi.Client, _ packets.Packet) bool { return true }
func (h *hook) OnACLCheck(_ *mochi.Client, _ string, _ bool) bool            { return true }

func (h *hook) OnConnect(cl *mochi.Client, pk packets.Packet) error {
	if cl.Net.Inline {
		return nil
	}
	attrs := map[string]any{
		"client_id":        cl.ID,
		"clean_session":    pk.Connect.Clean,
		"keepalive":        int(pk.Connect.Keepalive),
		"protocol_version": int(pk.ProtocolVersion),
	}
	// The identity check that only the broker can make: the device must
	// connect under the same id as the common name of the certificate it
	// presented. A mismatch is how a stale device_id or a certificate from a
	// previous provisioning shows up.
	if cert, ok := h.server.ClientCert(); ok {
		attrs["cert_cn"] = cert.Subject.CommonName
		attrs["client_id_matches_cert_cn"] = cert.Subject.CommonName == cl.ID
	}
	h.server.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTConnect, attrs, nil)
	return nil
}

func (h *hook) OnDisconnect(cl *mochi.Client, err error, expire bool) {
	if cl.Net.Inline {
		return
	}
	attrs := map[string]any{"client_id": cl.ID, "expire": expire}
	if err != nil {
		attrs["error"] = err.Error()
	}
	h.server.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTDisconnect, attrs, nil)
}

// OnSubscribed emits one event per filter, because one filter is the unit a
// scenario asserts on -- even though a single SUBSCRIBE packet may carry
// several.
func (h *hook) OnSubscribed(cl *mochi.Client, pk packets.Packet, reasonCodes []byte) {
	if cl.Net.Inline {
		return
	}
	for i, filter := range pk.Filters {
		attrs := map[string]any{
			"client_id": cl.ID,
			"topic":     filter.Filter,
			"qos":       filter.Qos,
		}
		if i < len(reasonCodes) {
			attrs["reason_code"] = reasonCodes[i]
		}
		h.server.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTSubscribe, attrs, nil)
	}
}

func (h *hook) OnUnsubscribed(cl *mochi.Client, pk packets.Packet) {
	if cl.Net.Inline {
		return
	}
	for _, filter := range pk.Filters {
		h.server.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTUnsubscribe, map[string]any{
			"client_id": cl.ID,
			"topic":     filter.Filter,
		}, nil)
	}
}

// OnPublished records what the device sent, decoded. The harness's own
// injections arrive here too -- they come from mochi's inline client -- and are
// skipped, because publish() already recorded them as notes.
func (h *hook) OnPublished(cl *mochi.Client, pk packets.Packet) {
	if cl.Net.Inline {
		return
	}
	attrs := map[string]any{
		"direction": "uplink",
		"client_id": cl.ID,
		"topic":     pk.TopicName,
		"qos":       pk.FixedHeader.Qos,
		"retain":    pk.FixedHeader.Retain,
		"size":      len(pk.Payload),
	}
	decodePayload(pk.TopicName, pk.Payload, attrs)
	h.server.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTPublish, attrs, pk.Payload)
}

// OnPacketRead records keepalives and nothing else. PINGREQ has no hook of its
// own, and it is worth having on the timeline: it is the difference between a
// daemon that is idle and one that is gone.
func (h *hook) OnPacketRead(cl *mochi.Client, pk packets.Packet) (packets.Packet, error) {
	if !cl.Net.Inline && pk.FixedHeader.Type == packets.Pingreq {
		h.server.log.Append(eventlog.SourceMQTT, eventlog.KindMQTTKeepalive,
			map[string]any{"client_id": cl.ID}, nil)
	}
	return pk, nil
}

// ── payload decoding ─────────────────────────────────────────────────────────

// decodePayload adds the decoded view of a payload to attrs, so a scenario can
// constrain the command name or a property value instead of raw bytes.
//
// A payload that does not decode still produces an event, with decode_error
// set: an unparsable publish is a real observation, and dropping it would turn
// a wire-format regression into a silent timeout.
func decodePayload(topic string, payload []byte, attrs map[string]any) {
	switch {
	case strings.HasSuffix(topic, commandUpSuffix), strings.HasSuffix(topic, commandDownSuffix):
		cmd, err := wire.DecodeCommand(payload)
		if err != nil {
			attrs["decode_error"] = err.Error()
			return
		}
		attrs["cmd"] = cmd.Name
		attrs["cmd_tag"] = fmt.Sprintf("%#05x", cmd.Tag)
		for k, v := range cmd.Fields {
			attrs[k] = v
		}
	case strings.HasSuffix(topic, propertyOutSuffix), strings.HasSuffix(topic, propertyInSuffix):
		values, err := wire.DecodeSenML(payload)
		if err != nil {
			attrs["decode_error"] = err.Error()
			return
		}
		attrs["values"] = len(values)
		for _, v := range values {
			// Flat keys so every value stays queryable as attrs.value.<name>,
			// which is what keeps predicates simple comparisons.
			attrs["value."+v.Name] = v.Value
		}
		// The single-value case is the common one (one property changed), and
		// it gets the ergonomic names a scenario reads best.
		if len(values) == 1 {
			attrs["variable"] = values[0].Name
			attrs["value"] = values[0].Value
		}
	}
}
