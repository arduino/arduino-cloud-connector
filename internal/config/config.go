// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import (
	"fmt"
	"os"
	"strconv"
)

const (
	defaultPort            = 5683
	defaultSocket          = "/run/arduino-cloud-connector/daemon.sock"
	defaultMQTTBroker      = "mqtts://iot.arduino.cc:8885"
	defaultProvisioningAPI = "https://api2.arduino.cc/provisioning"
	defaultDataDir         = "/var/lib/arduino-cloud-connector"
	defaultMQTTCAFile      = "" // empty means use system roots
	defaultLogLevel        = "info"
)

// Config holds all runtime configuration for the daemon, populated from
// environment variables with sensible defaults.
type Config struct {
	// DataDir is the directory used for persistent storage (keys, certs,
	// device ID, thing ID cache). Default: /var/lib/arduino-cloud-connector.
	DataDir string

	// Port is the localhost port for the REST API.
	// Env: ARDUINO_CLOUD_CONNECTOR__PORT. Default: 5683.
	Port int

	// Socket is the path of an additional UNIX-domain-socket listener for the
	// REST API, served alongside the 127.0.0.1 TCP listener. It is the channel
	// used by App Lab app containers (bind-mounted into the container, the same
	// pattern as arduino-router.sock), so the API need not be exposed on any
	// network interface. Empty disables the socket listener.
	// Env: ARDUINO_CLOUD_CONNECTOR__SOCKET. Default: /run/arduino-cloud-connector/daemon.sock.
	Socket string

	// MQTTBroker is the Arduino IoT Cloud VerneMQ broker URL.
	// Env: ARDUINO_CLOUD_CONNECTOR__MQTT_BROKER.
	MQTTBroker string

	// ProvisioningAPI is the base URL of the Arduino Provisioning API.
	// Env: ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API.
	ProvisioningAPI string

	// MQTTCAFile is the path to a PEM CA used to verify the broker's server
	// certificate. The Arduino broker uses the private CN=Arduino CA, absent
	// from the system store, so it must be pinned (as the boards do). When set
	// it becomes tls.Config.RootCAs; when empty the system roots are used.
	// Env: ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE. Default: unset (system roots).
	MQTTCAFile string

	// LogLevel controls log verbosity.
	// Env: ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL. Default: info.
	LogLevel string

	// Version is set from the binary build version (via -ldflags).
	// Not read from environment — injected by main after NewFromEnv.
	Version string
}

// NewFromEnv builds a Config from environment variables, applying defaults
// where variables are unset.
func NewFromEnv() (Config, error) {
	cfg := Config{
		DataDir:         envOr("ARDUINO_CLOUD_CONNECTOR__DATA_DIR", defaultDataDir),
		MQTTBroker:      envOr("ARDUINO_CLOUD_CONNECTOR__MQTT_BROKER", defaultMQTTBroker),
		ProvisioningAPI: envOr("ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API", defaultProvisioningAPI),
		MQTTCAFile:      envOr("ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE", defaultMQTTCAFile),
		LogLevel:        envOr("ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL", defaultLogLevel),
		Socket:          envOr("ARDUINO_CLOUD_CONNECTOR__SOCKET", defaultSocket),
		Port:            defaultPort,
	}

	if portStr := os.Getenv("ARDUINO_CLOUD_CONNECTOR__PORT"); portStr != "" {
		p, err := strconv.Atoi(portStr)
		if err != nil {
			return Config{}, fmt.Errorf("ARDUINO_CLOUD_CONNECTOR__PORT: %w", err)
		}
		cfg.Port = p
	}

	return cfg, nil
}

// DaemonVersion returns the version string injected at build time.
// Used in Device.begin MQTT messages. Set via the Version field after loading.
func (c *Config) DaemonVersion() string {
	if c.Version == "" {
		return "0.0.0-dev"
	}
	return c.Version
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
