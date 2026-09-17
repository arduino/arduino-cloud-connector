// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package broker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	mochi "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/packets"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/pki"
	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/wire"
)

const (
	testUHWID    = "3a7bd3e2360a3d29eea436fcfb7e44c735d117c42d1c1835420b6b9942dd4f1b"
	testDeviceID = "9f1c2d3e-4567-89ab-cdef-0123456789ab"
	testThingID  = "b2c3d4e5-6789-4abc-8def-0123456789ab"
	awaitBudget  = 5 * time.Second
)

type fixture struct {
	log *eventlog.Log
	ca  *pki.CA
	srv *Server
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
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return fixture{log: log, ca: ca, srv: srv}
}

// newDeviceCert issues a certificate the way provisioning does: a CN-only CSR
// goes to the CA, and the daemon would end up holding this certificate and the
// key that signed the request.
func newDeviceCert(t *testing.T, ca *pki.CA, bad bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("device key: %v", err)
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader,
		&x509.CertificateRequest{Subject: pkix.Name{CommonName: testUHWID}}, key)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	csrPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER}))

	issue := ca.IssueDevice
	if bad {
		issue = ca.IssueDeviceBadSignature
	}
	issued, err := issue(pki.DeviceRequest{CSRPEM: csrPEM, DeviceID: testDeviceID})
	if err != nil {
		t.Fatalf("issue device certificate: %v", err)
	}
	return tls.Certificate{Certificate: [][]byte{issued.CertDER}, PrivateKey: key}
}

// clientTLS mirrors the daemon's own TLS configuration, including the TLS 1.2
// cap: the point is to connect the way the thing under test connects.
func clientTLS(ca *pki.CA, cert tls.Certificate) *tls.Config {
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      ca.Pool(),
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12,
	}
}

// connect dials the broker with paho, the same client library the daemon uses,
// with the same options: MQTT 3.1.1, CleanSession false, the device id as the
// client id.
func connect(t *testing.T, f fixture, cert tls.Certificate) paho.Client {
	t.Helper()
	opts := paho.NewClientOptions().
		AddBroker(f.srv.URL()).
		SetClientID(testDeviceID).
		SetTLSConfig(clientTLS(f.ca, cert)).
		SetCleanSession(false).
		SetAutoReconnect(false).
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(awaitBudget)

	client := paho.NewClient(opts)
	token := client.Connect()
	if !token.WaitTimeout(awaitBudget) {
		t.Fatal("CONNECT timed out")
	}
	if err := token.Error(); err != nil {
		t.Fatalf("CONNECT failed: %v", err)
	}
	t.Cleanup(func() { client.Disconnect(250) })
	return client
}

// await is how every wait in the harness is written: a condition with a
// timeout, never a sleep.
func await(t *testing.T, log *eventlog.Log, from eventlog.Cursor, p eventlog.Predicate) (eventlog.Event, eventlog.Cursor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), awaitBudget)
	defer cancel()
	ev, cursor, err := log.Await(ctx, from, p, awaitBudget)
	if err != nil {
		t.Fatalf("awaiting %s: %v", p, err)
	}
	return ev, cursor
}

func pred(label string, constraints ...eventlog.Constraint) eventlog.Predicate {
	return eventlog.Predicate{Label: label, Constraints: constraints}
}

// The handshake the daemon performs, and the two things only the broker can
// see: the client id it connected under and the certificate it presented.
func TestConnectRecordsTheClientIDAndTheCertificate(t *testing.T) {
	f := newFixture(t)
	connect(t, f, newDeviceCert(t, f.ca, false))

	ev, _ := await(t, f.log, 0, pred("mqtt CONNECT",
		eventlog.Eq("kind", string(eventlog.KindMQTTConnect)),
		eventlog.Eq("attrs.client_id", testDeviceID),
	))
	for name, want := range map[string]any{
		"cert_cn":                   testDeviceID,
		"client_id_matches_cert_cn": true,
		"clean_session":             false,
		"protocol_version":          4, // MQTT 3.1.1
	} {
		if got := ev.Attrs[name]; got != want {
			t.Errorf("attrs[%q] = %v (%T), want %v", name, got, got, want)
		}
	}
	if !ev.Significant() {
		t.Error("a CONNECT must be significant for the strict sweep")
	}

	cert, ok := f.srv.ClientCert()
	if !ok {
		t.Fatal("the broker kept no client certificate")
	}
	if cert.Subject.CommonName != testDeviceID {
		t.Errorf("certificate CN = %q, want the device id", cert.Subject.CommonName)
	}
}

// The fault the whole PKI design exists for, seen from the broker: a
// certificate signed by the wrong key is refused, and the refusal says why.
//
// Without the event this is the worst failure in the suite to debug -- the
// daemon simply never connects, and every later step times out blaming the
// message it was waiting for.
func TestARogueSignedCertificateIsRefusedWithAReason(t *testing.T) {
	f := newFixture(t)

	conn, err := tls.Dial("tcp", f.srv.Addr(), clientTLS(f.ca, newDeviceCert(t, f.ca, true)))
	if err == nil {
		_ = conn.Close()
		t.Fatal("the broker accepted a certificate the CA did not sign")
	}

	ev, _ := await(t, f.log, 0, pred("mqtt TLS error",
		eventlog.Eq("kind", string(eventlog.KindMQTTTLSError)),
	))
	if got := ev.Attrs["cert_cn"]; got != testDeviceID {
		t.Errorf("cert_cn = %v, want the device id", got)
	}
	reason, _ := ev.Attrs["reason"].(string)
	if reason == "" {
		t.Fatal("the rejection carries no reason")
	}
	if !ev.Significant() {
		t.Error("a refused handshake must be significant: it is what a certificate bug looks like")
	}
	t.Logf("recorded rejection: %s", reason)
}

func TestAClientWithNoCertificateIsRefused(t *testing.T) {
	f := newFixture(t)

	conn, err := tls.Dial("tcp", f.srv.Addr(), &tls.Config{
		RootCAs:    f.ca.Pool(),
		ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS12,
		MaxVersion: tls.VersionTLS12,
	})
	if err == nil {
		_ = conn.Close()
		t.Fatal("the broker accepted a client that presented no certificate")
	}

	ev, _ := await(t, f.log, 0, pred("mqtt TLS error",
		eventlog.Eq("kind", string(eventlog.KindMQTTTLSError)),
		eventlog.Eq("attrs.reason", "no certificate presented"),
	))
	if _, ok := ev.Attrs["cert_cn"]; ok {
		t.Error("a rejection with no certificate should carry no CN")
	}
}

// A certificate from a different CA is the shape of a device provisioned
// against another environment, and it must be refused for a named reason
// rather than silently.
func TestACertificateFromAnotherCAIsRefused(t *testing.T) {
	f := newFixture(t)
	other, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}

	conn, err := tls.Dial("tcp", f.srv.Addr(), clientTLS(f.ca, newDeviceCert(t, other, false)))
	if err == nil {
		_ = conn.Close()
		t.Fatal("the broker accepted a certificate from an unrelated CA")
	}
	await(t, f.log, 0, pred("mqtt TLS error",
		eventlog.Eq("kind", string(eventlog.KindMQTTTLSError)),
		eventlog.Constraint{Field: "attrs.reason", Op: eventlog.OpContains, Want: "unknown authority"},
	))
}

// Subscribes are the half of the protocol a client-side observer cannot see,
// and they are what the handshake assertions are built on.
func TestSubscribeAndUnsubscribeAreRecordedPerFilter(t *testing.T) {
	f := newFixture(t)
	client := connect(t, f, newDeviceCert(t, f.ca, false))

	topic := CommandDownTopic(testDeviceID)
	if token := client.Subscribe(topic, 1, func(paho.Client, paho.Message) {}); !token.WaitTimeout(awaitBudget) {
		t.Fatal("SUBSCRIBE timed out")
	} else if err := token.Error(); err != nil {
		t.Fatalf("SUBSCRIBE failed: %v", err)
	}

	ev, cursor := await(t, f.log, 0, pred("mqtt SUBSCRIBE",
		eventlog.Eq("kind", string(eventlog.KindMQTTSubscribe)),
		eventlog.Eq("attrs.topic", topic),
	))
	if got := ev.Attrs["qos"]; got != byte(1) {
		t.Errorf("qos = %v (%T), want 1: the daemon subscribes at QoS 1", got, got)
	}

	if token := client.Unsubscribe(topic); !token.WaitTimeout(awaitBudget) {
		t.Fatal("UNSUBSCRIBE timed out")
	}
	await(t, f.log, cursor, pred("mqtt UNSUBSCRIBE",
		eventlog.Eq("kind", string(eventlog.KindMQTTUnsubscribe)),
		eventlog.Eq("attrs.topic", topic),
	))
}

// A command publish is recorded decoded, which is what lets a scenario say
// `cmd: Device.begin` instead of comparing bytes.
func TestCommandPublishIsRecordedDecoded(t *testing.T) {
	f := newFixture(t)
	client := connect(t, f, newDeviceCert(t, f.ca, false))

	// What the daemon publishes first, encoded by the harness codec because
	// here the harness is standing in for the device.
	payload := deviceBeginPayload(t, "0.0.0-e2e")
	if token := client.Publish(CommandUpTopic(testDeviceID), 1, false, payload); !token.WaitTimeout(awaitBudget) {
		t.Fatal("PUBLISH timed out")
	}

	ev, _ := await(t, f.log, 0, pred("mqtt PUBLISH Device.begin",
		eventlog.Eq("kind", string(eventlog.KindMQTTPublish)),
		eventlog.Eq("attrs.cmd", wire.CmdDeviceBegin),
	))
	for name, want := range map[string]any{
		"lib_version": "0.0.0-e2e",
		"cmd_tag":     "0x10700",
		"direction":   "uplink",
		"topic":       CommandUpTopic(testDeviceID),
	} {
		if got := ev.Attrs[name]; got != want {
			t.Errorf("attrs[%q] = %v, want %v", name, got, want)
		}
	}
	if string(ev.Raw) != string(payload) {
		t.Error("the raw payload was not kept")
	}
}

// A property publish is decoded into per-variable attributes. The flat
// value.<name> keys are what keep predicates simple comparisons, and the
// single-value shorthand is what the common scenario reads.
func TestPropertyPublishIsRecordedPerVariable(t *testing.T) {
	f := newFixture(t)
	client := connect(t, f, newDeviceCert(t, f.ca, false))

	payload, err := wire.EncodeSenML([]wire.Value{{Name: "temp", Value: 42.0}})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	topic := PropertyOutTopic(testThingID)
	if token := client.Publish(topic, 1, false, payload); !token.WaitTimeout(awaitBudget) {
		t.Fatal("PUBLISH timed out")
	}

	ev, cursor := await(t, f.log, 0, pred("mqtt PUBLISH temp",
		eventlog.Eq("kind", string(eventlog.KindMQTTPublish)),
		eventlog.Eq("attrs.topic", topic),
	))
	for name, want := range map[string]any{
		"variable":   "temp",
		"value":      42.0,
		"value.temp": 42.0,
		"values":     1,
	} {
		if got := ev.Attrs[name]; got != want {
			t.Errorf("attrs[%q] = %v (%T), want %v", name, got, got, want)
		}
	}

	// With several values there is no single "variable", but each stays
	// addressable on its own key.
	multi, err := wire.EncodeSenML([]wire.Value{
		{Name: "temp", Value: 21.5},
		{Name: "switch", Value: true},
	})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}
	if token := client.Publish(topic, 1, false, multi); !token.WaitTimeout(awaitBudget) {
		t.Fatal("PUBLISH timed out")
	}
	ev, _ = await(t, f.log, cursor, pred("mqtt PUBLISH two values",
		eventlog.Eq("kind", string(eventlog.KindMQTTPublish)),
		eventlog.Eq("attrs.values", 2),
	))
	if got := ev.Attrs["value.temp"]; got != 21.5 {
		t.Errorf("value.temp = %v, want 21.5", got)
	}
	if got := ev.Attrs["value.switch"]; got != true {
		t.Errorf("value.switch = %v, want true", got)
	}
	if _, ok := ev.Attrs["variable"]; ok {
		t.Error("a multi-value payload must not claim a single variable")
	}
}

// An unparsable payload still produces an event. Dropping it would turn a
// wire-format regression into a timeout on the next step.
func TestAnUndecodablePayloadIsStillRecorded(t *testing.T) {
	f := newFixture(t)
	client := connect(t, f, newDeviceCert(t, f.ca, false))

	if token := client.Publish(CommandUpTopic(testDeviceID), 1, false, []byte{0xff, 0xff}); !token.WaitTimeout(awaitBudget) {
		t.Fatal("PUBLISH timed out")
	}
	ev, _ := await(t, f.log, 0, pred("mqtt PUBLISH undecodable",
		eventlog.Eq("kind", string(eventlog.KindMQTTPublish)),
	))
	if _, ok := ev.Attrs["decode_error"]; !ok {
		t.Errorf("no decode_error recorded: %v", ev.Attrs)
	}
	if _, ok := ev.Attrs["cmd"]; ok {
		t.Error("an undecodable payload must not claim a command name")
	}
}

// Injection: the cloud half of the handshake. It must reach the device, and it
// must NOT look like something the device sent -- otherwise a step waiting for
// a device publish could match the harness's own downlink.
func TestInjectionReachesTheClientAndIsRecordedAsANote(t *testing.T) {
	f := newFixture(t)
	client := connect(t, f, newDeviceCert(t, f.ca, false))

	received := make(chan []byte, 1)
	topic := CommandDownTopic(testDeviceID)
	if token := client.Subscribe(topic, 1, func(_ paho.Client, m paho.Message) {
		received <- m.Payload()
	}); !token.WaitTimeout(awaitBudget) {
		t.Fatal("SUBSCRIBE timed out")
	}

	payload := wire.EncodeThingUpdate(testThingID)
	if err := f.srv.PublishCommand(testDeviceID, payload); err != nil {
		t.Fatalf("PublishCommand: %v", err)
	}

	select {
	case got := <-received:
		if string(got) != string(payload) {
			t.Errorf("the client received %x, want %x", got, payload)
		}
	case <-time.After(awaitBudget):
		t.Fatal("the injected command never reached the client")
	}

	ev, _ := await(t, f.log, 0, pred("harness downlink",
		eventlog.Eq("kind", string(eventlog.KindHarnessNote)),
		eventlog.Eq("attrs.cmd", wire.CmdThingUpdate),
	))
	if got := ev.Attrs["direction"]; got != "downlink" {
		t.Errorf("direction = %v, want downlink", got)
	}
	if got := ev.Attrs["thing_id"]; got != testThingID {
		t.Errorf("thing_id = %v, want %v", got, testThingID)
	}
	if ev.Significant() {
		t.Error("the harness's own injection must not be significant, or every scenario would have to consume it")
	}
	// And no mqtt_publish was recorded for it: that kind belongs to the device.
	for _, e := range f.log.Events() {
		if e.Kind == eventlog.KindMQTTPublish {
			t.Errorf("the injection was recorded as a device publish: %s", e)
		}
	}
}

func TestPropertyInjectionIsDecodedOnTheTimeline(t *testing.T) {
	f := newFixture(t)
	payload, err := wire.EncodeSenML([]wire.Value{{Name: "temp", Value: 30.0}})
	if err != nil {
		t.Fatalf("EncodeSenML: %v", err)
	}

	if err := f.srv.PublishProperty(testThingID, payload); err != nil {
		t.Fatalf("PublishProperty: %v", err)
	}
	ev, _ := await(t, f.log, 0, pred("harness property downlink",
		eventlog.Eq("kind", string(eventlog.KindHarnessNote)),
		eventlog.Eq("attrs.topic", PropertyInTopic(testThingID)),
	))
	if got := ev.Attrs["value.temp"]; got != 30.0 {
		t.Errorf("value.temp = %v, want 30.0", got)
	}
	if got := ev.Attrs["qos"]; got != byte(1) {
		t.Errorf("qos = %v, want 1", got)
	}
}

// The QoS is settable because downlink QoS is an open question with the cloud
// team: the daemon subscribes at 1, the real cloud is believed to publish at 0.
func TestPublishAcceptsAChosenQoS(t *testing.T) {
	f := newFixture(t)

	if err := f.srv.Publish("/a/t/x/e/i", []byte{0x80}, 0); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ev, _ := await(t, f.log, 0, pred("harness note", eventlog.Eq("kind", string(eventlog.KindHarnessNote))))
	if got := ev.Attrs["qos"]; got != byte(0) {
		t.Errorf("qos = %v, want 0", got)
	}
}

// A keepalive on the timeline is the difference between a daemon that is idle
// and one that is gone. It must never be significant, or every scenario would
// have to tolerate however many pings happened to fit in its runtime.
//
// The hook is driven directly instead of through a real client: PINGREQ only
// happens after a whole keepalive interval, and the daemon's is 30 seconds.
// The full path (paho pings, mochi calls the hook, the event appears) was
// checked by hand; it cannot be automated at a one-second keepalive because
// mochi then drops the connection with a write timeout right after answering,
// which would make the test flaky for a reason that has nothing to do with the
// daemon.
func TestKeepaliveIsRecordedButNotSignificant(t *testing.T) {
	f := newFixture(t)
	h := &hook{server: f.srv}

	pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}}
	if _, err := h.OnPacketRead(&mochi.Client{ID: testDeviceID}, pk); err != nil {
		t.Fatalf("OnPacketRead: %v", err)
	}

	ev, _ := await(t, f.log, 0, pred("mqtt keepalive",
		eventlog.Eq("kind", string(eventlog.KindMQTTKeepalive)),
		eventlog.Eq("attrs.client_id", testDeviceID),
	))
	if ev.Significant() {
		t.Error("a keepalive must not be significant")
	}
}

// Every other packet type passes through OnPacketRead untouched and unlogged:
// the specific hooks are what record connects, subscribes and publishes, and a
// second event per packet would have to be tolerated by every scenario.
func TestOnPacketReadIgnoresEverythingButPings(t *testing.T) {
	f := newFixture(t)
	h := &hook{server: f.srv}

	for _, typ := range []byte{packets.Connect, packets.Publish, packets.Subscribe, packets.Puback} {
		pk := packets.Packet{FixedHeader: packets.FixedHeader{Type: typ}}
		if _, err := h.OnPacketRead(&mochi.Client{ID: testDeviceID}, pk); err != nil {
			t.Fatalf("OnPacketRead(%d): %v", typ, err)
		}
	}
	if events := f.log.Events(); len(events) != 0 {
		t.Errorf("recorded %d events for non-ping packets: %v", len(events), events)
	}
}

// mochi's inline client is how the harness injects, so its traffic reaches the
// same hooks. It has to be skipped everywhere, or the harness would observe
// itself: a step waiting for a device publish could match the cloud downlink
// the scenario itself sent.
func TestTheInlineClientIsNeverObserved(t *testing.T) {
	f := newFixture(t)
	h := &hook{server: f.srv}
	inline := &mochi.Client{ID: "inline"}
	inline.Net.Inline = true

	if err := h.OnConnect(inline, packets.Packet{}); err != nil {
		t.Fatalf("OnConnect: %v", err)
	}
	h.OnDisconnect(inline, nil, false)
	h.OnSubscribed(inline, packets.Packet{Filters: packets.Subscriptions{{Filter: "x"}}}, []byte{0})
	h.OnUnsubscribed(inline, packets.Packet{Filters: packets.Subscriptions{{Filter: "x"}}})
	h.OnPublished(inline, packets.Packet{TopicName: "x", Payload: []byte("y")})
	if _, err := h.OnPacketRead(inline, packets.Packet{FixedHeader: packets.FixedHeader{Type: packets.Pingreq}}); err != nil {
		t.Fatalf("OnPacketRead: %v", err)
	}

	if events := f.log.Events(); len(events) != 0 {
		t.Errorf("the inline client produced %d events: %v", len(events), events)
	}
}

func TestDisconnectIsRecorded(t *testing.T) {
	f := newFixture(t)
	client := connect(t, f, newDeviceCert(t, f.ca, false))
	client.Disconnect(250)

	ev, _ := await(t, f.log, 0, pred("mqtt DISCONNECT",
		eventlog.Eq("kind", string(eventlog.KindMQTTDisconnect)),
		eventlog.Eq("attrs.client_id", testDeviceID),
	))
	if !ev.Significant() {
		t.Error("a DISCONNECT must be significant: an unexpected one is a real failure")
	}
}

// The URL is what goes into the daemon's environment, so its shape is part of
// the contract with the rest of the harness.
func TestURLAndAddr(t *testing.T) {
	f := newFixture(t)

	host, port, err := net.SplitHostPort(f.srv.Addr())
	if err != nil {
		t.Fatalf("Addr() = %q: %v", f.srv.Addr(), err)
	}
	if host != "127.0.0.1" {
		t.Errorf("host = %q, want 127.0.0.1", host)
	}
	if port == "0" {
		t.Error("port is 0, want the bound port")
	}
	if want := "mqtts://" + f.srv.Addr(); f.srv.URL() != want {
		t.Errorf("URL() = %q, want %q", f.srv.URL(), want)
	}
}

func TestTopicHelpers(t *testing.T) {
	tests := map[string]string{
		CommandUpTopic(testDeviceID):   "/a/d/" + testDeviceID + "/c/up",
		CommandDownTopic(testDeviceID): "/a/d/" + testDeviceID + "/c/dw",
		PropertyOutTopic(testThingID):  "/a/t/" + testThingID + "/e/o",
		PropertyInTopic(testThingID):   "/a/t/" + testThingID + "/e/i",
	}
	for got, want := range tests {
		if got != want {
			t.Errorf("got %q, want %q", got, want)
		}
	}
}

func TestStartValidatesItsOptions(t *testing.T) {
	ca, err := pki.NewCA()
	if err != nil {
		t.Fatalf("new CA: %v", err)
	}
	if srv, err := Start(Options{CA: ca}); err == nil {
		_ = srv.Close()
		t.Error("Start with no log succeeded, want an error")
	}
	if srv, err := Start(Options{Log: eventlog.New()}); err == nil {
		_ = srv.Close()
		t.Error("Start with no CA succeeded, want an error")
	}
}

// deviceBeginPayload assembles tag(0x10700)[libVersion] byte by byte.
//
// The wire package has no uplink encoder on purpose -- the harness is the
// cloud, and uplink commands are what it READS -- so the one test that needs
// to speak as the device writes the bytes out. That it is hand-assembled is a
// bonus: it checks the decoder against bytes neither codec produced.
//
// Only valid for a version string shorter than 24 characters, which is all the
// short-form CBOR text header can carry.
func deviceBeginPayload(t *testing.T, libVersion string) []byte {
	t.Helper()
	if len(libVersion) >= 24 {
		t.Fatalf("version %q is too long for the short-form text header", libVersion)
	}
	payload := []byte{
		0xda, 0x00, 0x01, 0x07, 0x00, // tag(0x10700)
		0x81,                         // array of 1
		byte(0x60 | len(libVersion)), // text string
	}
	return append(payload, libVersion...)
}
