// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import "testing"

// allEnvKeys are every variable NewFromEnv reads; tests clear them so the
// result never depends on the host environment.
var allEnvKeys = []string{
	"ARDUINO_CLOUD_CONNECTOR__DATA_DIR",
	"ARDUINO_CLOUD_CONNECTOR__MQTT_BROKER",
	"ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API",
	"ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE",
	"ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL",
	"ARDUINO_CLOUD_CONNECTOR__PORT",
	"ARDUINO_CLOUD_CONNECTOR__SOCKET",
	"ARDUINO_CLOUD_CONNECTOR__NTP_PROBE_HOST",
}

func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allEnvKeys {
		t.Setenv(k, "")
	}
}

func TestNewFromEnvDefaults(t *testing.T) {
	clearEnv(t)

	cfg, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv: %v", err)
	}

	if cfg.Port != defaultPort {
		t.Errorf("Port: got %d want %d", cfg.Port, defaultPort)
	}
	if cfg.Socket != defaultSocket {
		t.Errorf("Socket: got %q want %q", cfg.Socket, defaultSocket)
	}
	if cfg.DataDir != defaultDataDir {
		t.Errorf("DataDir: got %q want %q", cfg.DataDir, defaultDataDir)
	}
	if cfg.MQTTBroker != defaultMQTTBroker {
		t.Errorf("MQTTBroker: got %q want %q", cfg.MQTTBroker, defaultMQTTBroker)
	}
	if cfg.ProvisioningAPI != defaultProvisioningAPI {
		t.Errorf("ProvisioningAPI: got %q want %q", cfg.ProvisioningAPI, defaultProvisioningAPI)
	}
	if cfg.LogLevel != defaultLogLevel {
		t.Errorf("LogLevel: got %q want %q", cfg.LogLevel, defaultLogLevel)
	}
	if cfg.NTPProbeHost != defaultNTPProbeHost {
		t.Errorf("NTPProbeHost: got %q want %q", cfg.NTPProbeHost, defaultNTPProbeHost)
	}
	// Empty => caller falls back to system roots (RootCAs nil).
	if cfg.MQTTCAFile != "" {
		t.Errorf("MQTTCAFile: got %q want empty (system roots)", cfg.MQTTCAFile)
	}
}

func TestNewFromEnvOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__DATA_DIR", "/tmp/data")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__MQTT_BROKER", "mqtts://broker.example:8885")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__PROVISIONING_API", "https://prov.example/provisioning")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE", "/etc/arduino/ca.pem")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL", "debug")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__PORT", "9000")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__SOCKET", "/tmp/custom.sock")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__NTP_PROBE_HOST", "127.0.0.1:18123")

	cfg, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv: %v", err)
	}

	if cfg.Socket != "/tmp/custom.sock" {
		t.Errorf("Socket: got %q want /tmp/custom.sock", cfg.Socket)
	}

	if cfg.DataDir != "/tmp/data" {
		t.Errorf("DataDir: got %q", cfg.DataDir)
	}
	if cfg.MQTTBroker != "mqtts://broker.example:8885" {
		t.Errorf("MQTTBroker: got %q", cfg.MQTTBroker)
	}
	if cfg.ProvisioningAPI != "https://prov.example/provisioning" {
		t.Errorf("ProvisioningAPI: got %q", cfg.ProvisioningAPI)
	}
	if cfg.MQTTCAFile != "/etc/arduino/ca.pem" {
		t.Errorf("MQTTCAFile: got %q", cfg.MQTTCAFile)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel: got %q", cfg.LogLevel)
	}
	if cfg.Port != 9000 {
		t.Errorf("Port: got %d want 9000", cfg.Port)
	}
	if cfg.NTPProbeHost != "127.0.0.1:18123" {
		t.Errorf("NTPProbeHost: got %q want 127.0.0.1:18123", cfg.NTPProbeHost)
	}
}

func TestNewFromEnvInvalidPort(t *testing.T) {
	clearEnv(t)
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__PORT", "not-a-number")

	if _, err := NewFromEnv(); err == nil {
		t.Fatal("expected error for non-numeric PORT")
	}
}

func TestDaemonVersion(t *testing.T) {
	var c Config
	if got := c.DaemonVersion(); got != "0.0.0-dev" {
		t.Errorf("empty version: got %q want 0.0.0-dev", got)
	}
	c.Version = "1.4.2"
	if got := c.DaemonVersion(); got != "1.4.2" {
		t.Errorf("set version: got %q want 1.4.2", got)
	}
}
