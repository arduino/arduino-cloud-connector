// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import (
	"testing"
	"time"
)

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
	"ARDUINO_CLOUD_CONNECTOR__APP_CLI_URL",
	"ARDUINO_CLOUD_CONNECTOR__APP_DOWNLOAD_DIR",
	"ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE",
	"ARDUINO_CLOUD_CONNECTOR__DOWNLOAD_TIMEOUT",
	"ARDUINO_CLOUD_CONNECTOR__INSTALL_TIMEOUT",
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
	// Empty => caller falls back to system roots (RootCAs nil).
	if cfg.MQTTCAFile != "" {
		t.Errorf("MQTTCAFile: got %q want empty (system roots)", cfg.MQTTCAFile)
	}

	// App deploy / OTA (RFC-14 §5.6).
	if cfg.AppCLIURL != defaultAppCLIURL {
		t.Errorf("AppCLIURL: got %q want %q", cfg.AppCLIURL, defaultAppCLIURL)
	}
	if cfg.AppDownloadDir != defaultAppDownloadDir {
		t.Errorf("AppDownloadDir: got %q want %q", cfg.AppDownloadDir, defaultAppDownloadDir)
	}
	if cfg.MaxBundleSize != defaultMaxBundleSize {
		t.Errorf("MaxBundleSize: got %d want %d", cfg.MaxBundleSize, defaultMaxBundleSize)
	}
	if cfg.DownloadTimeout != defaultDownloadTimeout {
		t.Errorf("DownloadTimeout: got %s want %s", cfg.DownloadTimeout, defaultDownloadTimeout)
	}
	if cfg.InstallTimeout != defaultInstallTimeout {
		t.Errorf("InstallTimeout: got %s want %s", cfg.InstallTimeout, defaultInstallTimeout)
	}
	if cfg.MQTTCAFile != defaultMQTTCAFile {
		t.Errorf("MQTTCAFile: got %q want %q (system roots)", cfg.MQTTCAFile, defaultMQTTCAFile)
	}
}

// The download dir is independent of DataDir: relocating the data dir must NOT
// drag the bundle download area with it, since the two can live on different
// filesystems.
func TestAppDownloadDirIsIndependentOfDataDir(t *testing.T) {
	clearEnv(t)
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__DATA_DIR", "/srv/connector")

	cfg, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv: %v", err)
	}
	if cfg.AppDownloadDir != defaultAppDownloadDir {
		t.Errorf("AppDownloadDir: got %q want %q", cfg.AppDownloadDir, defaultAppDownloadDir)
	}
}

func TestOTAOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__APP_CLI_URL", "http://127.0.0.1:9999")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__APP_DOWNLOAD_DIR", "/data/bundles")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE", "5368709120") // 5 GiB > int32
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__DOWNLOAD_TIMEOUT", "45m")
	t.Setenv("ARDUINO_CLOUD_CONNECTOR__INSTALL_TIMEOUT", "90s")

	cfg, err := NewFromEnv()
	if err != nil {
		t.Fatalf("NewFromEnv: %v", err)
	}

	if cfg.AppCLIURL != "http://127.0.0.1:9999" {
		t.Errorf("AppCLIURL: got %q", cfg.AppCLIURL)
	}
	if cfg.AppDownloadDir != "/data/bundles" {
		t.Errorf("AppDownloadDir: got %q", cfg.AppDownloadDir)
	}
	if cfg.MaxBundleSize != 5*(1<<30) {
		t.Errorf("MaxBundleSize: got %d want %d", cfg.MaxBundleSize, 5*(1<<30))
	}
	if cfg.DownloadTimeout != 45*time.Minute {
		t.Errorf("DownloadTimeout: got %s", cfg.DownloadTimeout)
	}
	if cfg.InstallTimeout != 90*time.Second {
		t.Errorf("InstallTimeout: got %s", cfg.InstallTimeout)
	}
}

func TestOTAInvalidValues(t *testing.T) {
	cases := []struct{ key, val string }{
		{"ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE", "huge"},
		{"ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE", "0"},
		{"ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE", "-1"},
		{"ARDUINO_CLOUD_CONNECTOR__DOWNLOAD_TIMEOUT", "soon"},
		{"ARDUINO_CLOUD_CONNECTOR__DOWNLOAD_TIMEOUT", "0s"},
		{"ARDUINO_CLOUD_CONNECTOR__INSTALL_TIMEOUT", "-5m"},
	}
	for _, tc := range cases {
		t.Run(tc.key+"="+tc.val, func(t *testing.T) {
			clearEnv(t)
			t.Setenv(tc.key, tc.val)
			if _, err := NewFromEnv(); err == nil {
				t.Fatalf("expected error for %s=%q", tc.key, tc.val)
			}
		})
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
