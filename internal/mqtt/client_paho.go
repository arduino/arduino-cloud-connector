// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package mqtt

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"sync"
	"time"

	pahomqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// DNS / dial tunables. Every Connect() builds fresh instances of these — the
// broker IP can rotate between attempts (load-balancer rebalance, region
// migration, failover), so neither the resolver nor the dialer is reused
// across reconnections.
const (
	dnsLookupTimeout   = 10 * time.Second
	tcpDialTimeout     = 15 * time.Second
	tlsHandshakeBudget = 15 * time.Second
	// brokerAckTimeout bounds every SUBACK/PUBACK wait. Without it, a publish or
	// subscribe on a half-open connection (broker silent at the MQTT layer while
	// TCP + keepalive stay alive, so Paho never reports the connection lost) blocks
	// forever — wedging the single outbound worker and back-pressuring the REST API
	// until the daemon is restarted. On timeout the call returns ErrBrokerAckTimeout;
	// PublishProperty additionally nudges the FSM to reconnect (notifyConnectionLost)
	// so a wedged connection self-heals without a restart.
	brokerAckTimeout = 10 * time.Second
)

// ErrBrokerAckTimeout is returned by the publish/subscribe helpers when the
// broker does not acknowledge within brokerAckTimeout — the tell-tale of a
// half-open connection.
var ErrBrokerAckTimeout = errors.New("mqtt: broker did not acknowledge within timeout")

// NewPahoClient returns a Client backed by github.com/eclipse/paho.mqtt.golang.
// Selected by the default (non-mock) build tag — see client_mock.go for the
// mock variant.
func NewPahoClient(cfg config.Config, ks *keystore.Keystore) Client {
	return &pahoClient{cfg: cfg, ks: ks}
}

type pahoClient struct {
	cfg config.Config
	ks  *keystore.Keystore

	// mu guards client and onConnLostFunc: they are read by the outbound worker
	// goroutine (PublishProperty) while the Cloud FSM goroutine may be replacing
	// them across a (re)connect.
	mu             sync.Mutex
	client         pahomqtt.Client
	onConnLostFunc func(error)
}

func (c *pahoClient) OnConnectionLost(handler func(err error)) {
	c.mu.Lock()
	c.onConnLostFunc = handler
	c.mu.Unlock()
}

// getClient returns the current Paho client under lock (may be nil).
func (c *pahoClient) getClient() pahomqtt.Client {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.client
}

// setClient replaces the current Paho client under lock.
func (c *pahoClient) setClient(client pahomqtt.Client) {
	c.mu.Lock()
	c.client = client
	c.mu.Unlock()
}

// notifyConnectionLost invokes the registered connection-lost handler, if any.
// Called both by Paho's own ConnectionLostHandler and by the publish path on an
// ack timeout, so a half-open connection that keepalive cannot detect still
// drives the FSM to tear down and reconnect.
func (c *pahoClient) notifyConnectionLost(err error) {
	c.mu.Lock()
	handler := c.onConnLostFunc
	c.mu.Unlock()
	if handler != nil {
		handler(err)
	}
}

func (c *pahoClient) Connect(ctx context.Context) error {
	// Ensure any previous client is fully torn down first. This matters when the
	// reconnect was triggered by a publish-ack timeout rather than a Paho-detected
	// drop: there the old client still believes it is connected, so without this it
	// would leak (goroutines + a duplicate broker session on the same ClientID).
	c.Disconnect()

	tlsCfg, err := c.buildTLSConfig()
	if err != nil {
		return fmt.Errorf("build TLS config: %w", err)
	}

	// Fresh resolver + dialer per Connect call. Resolver.PreferGo bypasses
	// glibc's getaddrinfo (and therefore nscd / systemd-resolved caches): Go
	// reads /etc/resolv.conf and queries the upstream DNS servers directly,
	// with no in-process caching of its own. Combined with CustomOpenConnectionFn
	// below (which does an explicit LookupHost on every dial), this guarantees
	// that every (re)connection attempt sees the broker's *current* IP set.
	resolver := &net.Resolver{PreferGo: true}
	dialer := &net.Dialer{
		Timeout:  tcpDialTimeout,
		Resolver: resolver,
	}

	opts := pahomqtt.NewClientOptions().
		AddBroker(c.cfg.MQTTBroker).
		SetTLSConfig(tlsCfg).
		SetCleanSession(false).
		SetAutoReconnect(false). // the lifecycle FSM owns reconnect logic
		SetKeepAlive(30 * time.Second).
		SetConnectTimeout(tcpDialTimeout + tlsHandshakeBudget + dnsLookupTimeout).
		SetDialer(dialer).
		SetCustomOpenConnectionFn(func(uri *url.URL, _ pahomqtt.ClientOptions) (net.Conn, error) {
			return openFreshConnection(ctx, uri, resolver, dialer, tlsCfg)
		}).
		SetConnectionLostHandler(func(_ pahomqtt.Client, err error) {
			slog.Warn("MQTT client: connection lost", "error", err)
			c.notifyConnectionLost(err)
		})

	deviceID, err := c.ks.DeviceID()
	if err == nil {
		opts.SetClientID(deviceID)
		slog.Debug("MQTT client: dialing broker (CONNECT)", "broker", c.cfg.MQTTBroker, "client_id", deviceID)
	} else {
		slog.Warn("MQTT client: connecting without a client ID (device_id unavailable)", "error", err)
	}

	client := pahomqtt.NewClient(opts)
	token := client.Connect()
	select {
	case <-ctx.Done():
		slog.Debug("MQTT client: connect aborted by context", "error", ctx.Err())
		return ctx.Err()
	case <-token.Done():
	}
	if err := token.Error(); err != nil {
		slog.Warn("MQTT client: CONNECT failed", "error", err)
		return err
	}
	slog.Info("MQTT client: CONNACK received — session established", "client_id", deviceID)
	c.setClient(client)
	return nil
}

// openFreshConnection establishes a TCP+TLS connection to the broker after
// performing an explicit, uncached DNS lookup. Invoked by Paho through
// CustomOpenConnectionFn on every (re)connection attempt — there is no path
// through which a cached IP can be reused, by design: the cloud team
// requires that every reconnect re-resolve the hostname because the broker's
// IP may have changed between attempts.
//
// On hosts with multiple A/AAAA records, IPs are tried in resolution order;
// the first successful TCP+TLS handshake wins. The TLS ServerName is forced
// to the original hostname so certificate validation succeeds even though we
// dial by IP literal.
func openFreshConnection(
	ctx context.Context,
	uri *url.URL,
	resolver *net.Resolver,
	dialer *net.Dialer,
	tlsCfg *tls.Config,
) (net.Conn, error) {
	host := uri.Hostname()
	port := uri.Port()
	if port == "" {
		// Sensible default for the schemes Paho supports over TLS.
		switch uri.Scheme {
		case "ssl", "tls", "mqtts", "wss":
			port = "8883"
		default:
			port = "1883"
		}
	}

	lookupCtx, cancel := context.WithTimeout(ctx, dnsLookupTimeout)
	defer cancel()
	ips, err := resolver.LookupHost(lookupCtx, host)
	if err != nil {
		return nil, fmt.Errorf("dns lookup %s: %w", host, err)
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("dns lookup %s: no addresses returned", host)
	}
	slog.Info("MQTT client: DNS resolved (uncached)", "host", host, "ips", ips)

	var lastErr error
	for _, ip := range ips {
		addr := net.JoinHostPort(ip, port)

		tcpConn, dialErr := dialer.DialContext(ctx, "tcp", addr)
		if dialErr != nil {
			lastErr = dialErr
			slog.Debug("MQTT client: tcp dial failed", "addr", addr, "error", dialErr)
			continue
		}

		// Clone the TLS config so we can pin ServerName for cert validation
		// without mutating the shared config (Paho retains a reference to it).
		cfg := tlsCfg.Clone()
		if cfg.ServerName == "" {
			cfg.ServerName = host
		}

		tlsConn := tls.Client(tcpConn, cfg)
		hsCtx, hsCancel := context.WithTimeout(ctx, tlsHandshakeBudget)
		hsErr := tlsConn.HandshakeContext(hsCtx)
		hsCancel()
		if hsErr != nil {
			_ = tcpConn.Close()
			lastErr = hsErr
			slog.Debug("MQTT client: TLS handshake failed", "addr", addr, "error", hsErr)
			continue
		}

		slog.Info("MQTT client: broker connection established",
			"host", host, "ip", ip, "port", port)
		return tlsConn, nil
	}

	return nil, fmt.Errorf("connect %s: all resolved IPs failed: %w", host, lastErr)
}

func (c *pahoClient) Disconnect() {
	// Drop the reference under lock (so the next Connect builds a brand-new Paho
	// client — fresh resolver, dialer, and CustomOpenConnectionFn closure that
	// re-captures the current ctx), then quiesce the old client outside the lock
	// so the up-to-500ms disconnect never blocks a concurrent getClient().
	c.mu.Lock()
	client := c.client
	c.client = nil
	c.mu.Unlock()
	if client != nil && client.IsConnected() {
		slog.Debug("MQTT client: disconnecting from broker (500ms quiesce)")
		client.Disconnect(500)
	} else {
		slog.Debug("MQTT client: Disconnect called but client already nil/not connected")
	}
}

func (c *pahoClient) IsConnected() bool {
	client := c.getClient()
	return client != nil && client.IsConnected()
}

func (c *pahoClient) SubscribeCommandChannel(deviceID string, onMessage func(CommandMessage)) error {
	client := c.getClient()
	if client == nil {
		return fmt.Errorf("client not connected")
	}
	topic := commandDownTopic(deviceID)
	token := client.Subscribe(topic, 1, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		cmd, err := command.Decode(msg.Payload())
		if err != nil {
			slog.Warn("MQTT client: invalid command on downlink channel",
				"topic", topic, "error", err)
			return
		}
		onMessage(CommandMessage{Cmd: cmd})
	})
	if !token.WaitTimeout(brokerAckTimeout) {
		slog.Warn("MQTT client: command-channel SUBACK timed out", "topic", topic, "timeout", brokerAckTimeout)
		return fmt.Errorf("subscribe %s: %w", topic, ErrBrokerAckTimeout)
	}
	if token.Error() != nil {
		return fmt.Errorf("subscribe %s: %w", topic, token.Error())
	}
	slog.Warn("MQTT client: SUBACK received for command channel", "topic", topic)
	return nil
}

func (c *pahoClient) SubscribePropertyTopic(thingID string, onMessage func(PropertyMessage)) error {
	client := c.getClient()
	if client == nil {
		return fmt.Errorf("client not connected")
	}
	topic := propertyInTopic(thingID)
	token := client.Subscribe(topic, 1, func(_ pahomqtt.Client, msg pahomqtt.Message) {
		onMessage(PropertyMessage{Payload: msg.Payload()})
	})
	if !token.WaitTimeout(brokerAckTimeout) {
		slog.Warn("MQTT client: property-topic SUBACK timed out", "topic", topic, "timeout", brokerAckTimeout)
		return fmt.Errorf("subscribe %s: %w", topic, ErrBrokerAckTimeout)
	}
	if token.Error() != nil {
		return fmt.Errorf("subscribe %s: %w", topic, token.Error())
	}
	slog.Warn("MQTT client: SUBACK received for property topic", "topic", topic)
	return nil
}

func (c *pahoClient) UnsubscribePropertyTopic(thingID string) error {
	client := c.getClient()
	if client == nil {
		return nil
	}
	topic := propertyInTopic(thingID)
	token := client.Unsubscribe(topic)
	if !token.WaitTimeout(brokerAckTimeout) {
		// Best-effort: never block shutdown / reprovision teardown on a wedged
		// connection. The client is torn down next anyway.
		slog.Warn("MQTT client: unsubscribe timed out; continuing teardown", "topic", topic, "timeout", brokerAckTimeout)
		return nil
	}
	if token.Error() != nil {
		return fmt.Errorf("unsubscribe %s: %w", topic, token.Error())
	}
	return nil
}

func (c *pahoClient) PublishCommand(deviceID string, cmd command.Cmd) error {
	client := c.getClient()
	if client == nil {
		return fmt.Errorf("client not connected")
	}
	payload, err := command.Encode(cmd)
	if err != nil {
		return fmt.Errorf("encode command %s: %w", cmd, err)
	}
	topic := commandUpTopic(deviceID)
	token := client.Publish(topic, 1, false, payload)
	if !token.WaitTimeout(brokerAckTimeout) {
		slog.Warn("MQTT client: command publish timed out (no broker ack)", "topic", topic, "cmd", cmd, "timeout", brokerAckTimeout)
		return fmt.Errorf("publish command %s: %w", topic, ErrBrokerAckTimeout)
	}
	if err := token.Error(); err != nil {
		slog.Warn("MQTT client: command publish failed", "topic", topic, "cmd", cmd, "error", err)
		return err
	}
	return nil
}

func (c *pahoClient) PublishProperty(thingID string, payload []byte) error {
	client := c.getClient()
	if client == nil {
		return fmt.Errorf("client not connected")
	}
	topic := propertyOutTopic(thingID)

	token := client.Publish(topic, 1, false, payload)
	if !token.WaitTimeout(brokerAckTimeout) {
		// Half-open connection: the broker isn't acking and Paho's keepalive
		// hasn't noticed. Signal the FSM (via the connection-lost handler) so it
		// tears down and reconnects, instead of letting this — the single
		// outbound worker — wedge here until a manual daemon restart.
		slog.Warn("MQTT client: property publish timed out — connection wedged, signalling reconnect",
			"topic", topic, "timeout", brokerAckTimeout)
		c.notifyConnectionLost(fmt.Errorf("property publish ack timeout on %s", topic))
		return fmt.Errorf("publish property %s: %w", topic, ErrBrokerAckTimeout)
	}
	if err := token.Error(); err != nil {
		slog.Warn("MQTT client: property publish failed", "topic", topic, "error", err)
		return err
	}
	return nil
}

// buildTLSConfig constructs the mTLS configuration from the device certificate
// and cloud private key stored in the keystore. RootCAs comes from
// config.CloudRootCAs: cfg.MQTTCAFile (the private CN=Arduino CA, absent from
// system roots) when set, else system roots.
func (c *pahoClient) buildTLSConfig() (*tls.Config, error) {
	certPEM, err := c.ks.DeviceCertPEM()
	if err != nil {
		return nil, fmt.Errorf("load device cert: %w", err)
	}

	privKey, err := c.ks.CloudPrivateKey()
	if err != nil {
		return nil, fmt.Errorf("load cloud private key: %w", err)
	}

	privKeyDER, err := x509.MarshalECPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("marshal cloud private key: %w", err)
	}
	privKeyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: privKeyDER})

	cert, err := tls.X509KeyPair([]byte(certPEM), privKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("build X.509 key pair: %w", err)
	}

	pool, err := c.cfg.CloudRootCAs()
	if err != nil {
		return nil, err
	}
	if pool != nil {
		slog.Info("MQTT client: pinned broker CA loaded", "ca_file", c.cfg.MQTTCAFile)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		MaxVersion:   tls.VersionTLS12, // broker is TLS 1.2-only; a TLS 1.3 ClientHello is rejected (alert 70)
		RootCAs:      pool,             // nil = system roots
	}, nil
}
