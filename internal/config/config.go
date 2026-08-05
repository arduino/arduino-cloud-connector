// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const (
	defaultPort            = 5683
	defaultSocket          = "/run/arduino-cloud-connector/daemon.sock"
	defaultMQTTBroker      = "mqtts://iot.arduino.cc:8885"
	defaultProvisioningAPI = "https://api2.arduino.cc/provisioning"
	defaultDataDir         = "/var/lib/arduino-cloud-connector"
	defaultMQTTCAFile      = "" // empty means use system roots
	defaultLogLevel        = "info"
	defaultAppCLIURL       = "http://127.0.0.1:8080"
	defaultAppDownloadDir  = "/var/lib/arduino-cloud-connector/app-download"
	// defaultMaxBundleSize is 4 GiB. RFC-14 §5.6 tabulates 200 MiB, but App Lab
	// Apps that ship AI models or container layers are expected to exceed 2 GB,
	// so the guard is set high enough not to reject them while still bounding a
	// runaway/hostile Content-Length. Tighten it per-fleet via the env var.
	defaultMaxBundleSize   = int64(4) << 30
	defaultDownloadTimeout = 30 * time.Minute
	defaultInstallTimeout  = 10 * time.Minute
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

	// MQTTCAFile is the path to a PEM CA used to verify the server certificate of
	// every Arduino Cloud endpoint the daemon connects to — the MQTT broker and
	// the storage service that serves App bundles. The Arduino broker uses the
	// private CN=Arduino CA, absent from the system store, so it must be pinned
	// (as the boards do). When set it becomes the sole tls.Config.RootCAs; when
	// empty the system roots are used. See CloudRootCAs.
	//
	// The name is narrower than the effect for backwards compatibility: it was
	// introduced for the broker alone.
	// Env: ARDUINO_CLOUD_CONNECTOR__MQTT_CA_FILE. Default: unset (system roots).
	MQTTCAFile string

	// LogLevel controls log verbosity.
	// Env: ARDUINO_CLOUD_CONNECTOR__LOG_LEVEL. Default: info.
	LogLevel string

	// Version is set from the binary build version (via -ldflags).
	// Not read from environment — injected by main after NewFromEnv.
	Version string

	// ── App deploy / OTA (RFC-14 §5.6) ───────────────────────────────────────

	// AppCLIURL is the base URL of the local arduino-app-cli daemon, which owns
	// unpacking and activating a downloaded App bundle (RFC-14 §5.4). Always a
	// loopback address: the same trust boundary as every other app-cli endpoint.
	// Env: ARDUINO_CLOUD_CONNECTOR__APP_CLI_URL. Default: http://127.0.0.1:8080.
	AppCLIURL string

	// AppDownloadDir is where App bundles are downloaded to and where the
	// resume journal of an interrupted download lives. Shared with
	// arduino-app-cli, which reads the bundle by path. An absolute path in its
	// own right, independent of DataDir: bundles are large and short-lived, so a
	// deployment may well want them on a different filesystem.
	// Env: ARDUINO_CLOUD_CONNECTOR__APP_DOWNLOAD_DIR.
	// Default: /var/lib/arduino-cloud-connector/app-download.
	AppDownloadDir string

	// MaxBundleSize is the largest App bundle the daemon will download, in
	// bytes. A job whose advertised size exceeds it is rejected before any
	// byte is fetched. int64 because bundles are expected to exceed 2 GB.
	// Env: ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE. Default: 4 GiB.
	MaxBundleSize int64

	// DownloadTimeout bounds one whole bundle download, including all
	// chunk-level retries. Exceeding it fails the job (recoverably: the journal
	// survives, so a later job for the same bundle resumes).
	// Env: ARDUINO_CLOUD_CONNECTOR__DOWNLOAD_TIMEOUT. Default: 30m.
	DownloadTimeout time.Duration

	// InstallTimeout is the maximum silence tolerated on the arduino-app-cli
	// install SSE stream before the install is considered hung.
	// Env: ARDUINO_CLOUD_CONNECTOR__INSTALL_TIMEOUT. Default: 10m.
	InstallTimeout time.Duration
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

	// App deploy / OTA.
	cfg.AppCLIURL = envOr("ARDUINO_CLOUD_CONNECTOR__APP_CLI_URL", defaultAppCLIURL)
	cfg.AppDownloadDir = envOr("ARDUINO_CLOUD_CONNECTOR__APP_DOWNLOAD_DIR", defaultAppDownloadDir)

	var err error
	if cfg.MaxBundleSize, err = envBytes(
		"ARDUINO_CLOUD_CONNECTOR__MAX_BUNDLE_SIZE", defaultMaxBundleSize,
	); err != nil {
		return Config{}, err
	}
	if cfg.DownloadTimeout, err = envDuration(
		"ARDUINO_CLOUD_CONNECTOR__DOWNLOAD_TIMEOUT", defaultDownloadTimeout,
	); err != nil {
		return Config{}, err
	}
	if cfg.InstallTimeout, err = envDuration(
		"ARDUINO_CLOUD_CONNECTOR__INSTALL_TIMEOUT", defaultInstallTimeout,
	); err != nil {
		return Config{}, err
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

// envDuration parses key as a Go duration ("30m", "90s"). Unset/empty yields
// fallback; a non-positive or unparsable value is a hard configuration error
// rather than a silent fallback, because a zero timeout would make every
// download or install fail instantly.
func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s: must be positive, got %s", key, d)
	}
	return d, nil
}

// envBytes parses key as a plain byte count. int64 (not int) so a >2 GiB limit
// is representable on 32-bit boards.
func envBytes(key string, fallback int64) (int64, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	if n <= 0 {
		return 0, fmt.Errorf("%s: must be positive, got %d", key, n)
	}
	return n, nil
}
