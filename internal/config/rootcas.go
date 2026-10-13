// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package config

import (
	"crypto/x509"
	"fmt"
	"os"
)

// CloudRootCAs returns the certificate pool used to verify the server
// certificate of an Arduino Cloud endpoint. It lives here, on the config, rather
// than in one of the clients because the rule has to hold identically for every
// endpoint the daemon talks to — the MQTT broker (internal/mqtt) and the storage
// service that serves App bundles (internal/storage-api). Two copies of a trust
// decision are two chances to let them drift apart.
//
// When MQTTCAFile is empty it returns (nil, nil), which callers assign to
// tls.Config.RootCAs to mean "use the system root store". When set, the file must
// contain at least one PEM-encoded certificate, and that becomes the SOLE trusted
// root — pinning, the same way the boards do it.
//
// Pinning applies to storage too, and that is not a compromise but a requirement:
// the storage service presents a certificate signed by the private Arduino CA —
// the same CA that signs the device certificates and the broker's — so the two
// endpoints necessarily move together. A pin that covered only the broker would
// leave the bundle download verifying against roots that cannot vouch for it, and
// there is no configuration in which the broker is behind the pinned CA and
// storage is not.
//
// Leaving it unset also works on a board installed from the .deb, which puts the
// Arduino CAs into the system store (see debian/ postinst).
func (c Config) CloudRootCAs() (*x509.CertPool, error) {
	if c.MQTTCAFile == "" {
		return nil, nil
	}
	caPEM, err := os.ReadFile(c.MQTTCAFile)
	if err != nil {
		return nil, fmt.Errorf("read CA file %q: %w", c.MQTTCAFile, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("CA file %q: no certificates parsed", c.MQTTCAFile)
	}
	return pool, nil
}
