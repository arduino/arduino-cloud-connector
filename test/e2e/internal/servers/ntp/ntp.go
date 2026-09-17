// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package ntp is the fake connectivity probe the daemon checks before it tries
// to reach the cloud.
//
// The daemon's probe is NOT an HTTP call, which is the only thing that makes
// this package necessary: isInternetReachable opens a UDP socket to
// time.arduino.cc:123, writes 48 bytes, reads 48 bytes and validates nothing
// at all. So a responder that echoes any datagram of the right size satisfies
// it completely, and the daemon is pointed here with
// ARDUINO_CLOUD_CONNECTOR__NTP_PROBE_HOST.
//
// Redirecting the probe with /etc/hosts was tried and does not work: the
// daemon also shells out to `host -t A time.arduino.cc` when it inspects the
// default route, and `host` queries DNS directly, ignoring /etc/hosts. Making
// the host configurable was the change that let the probe be faked at all --
// and it avoids needing root to bind UDP port 123.
package ntp

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

// probeSize is the datagram size the daemon writes and expects back.
const probeSize = 48

// Server is a running fake probe target.
type Server struct {
	log  *eventlog.Log
	conn *net.UDPConn

	// silent makes the responder accept datagrams and never answer, which is
	// how a scenario takes connectivity away: the daemon then reads the same
	// "no internet" verdict it would get from a dead uplink, without anything
	// in the harness having to touch its process or its config.
	silent atomic.Bool

	probes atomic.Int64

	closeOnce sync.Once
}

// Start binds a UDP socket on loopback and answers probes until Close.
func Start(log *eventlog.Log) (*Server, error) {
	if log == nil {
		return nil, fmt.Errorf("ntp: no event log given")
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		return nil, fmt.Errorf("ntp: listen udp: %w", err)
	}
	s := &Server{log: log, conn: conn}
	go s.serve()
	return s, nil
}

// Addr is what ARDUINO_CLOUD_CONNECTOR__NTP_PROBE_HOST must be set to.
func (s *Server) Addr() string { return s.conn.LocalAddr().String() }

// SetSilent stops or resumes answering. Existing probes are unaffected; the
// next one decides.
func (s *Server) SetSilent(silent bool) { s.silent.Store(silent) }

// Probes is how many datagrams have been received.
func (s *Server) Probes() int64 { return s.probes.Load() }

// Close stops the responder.
func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		err = s.conn.Close()
	})
	if err != nil {
		return fmt.Errorf("ntp: close: %w", err)
	}
	return nil
}

func (s *Server) serve() {
	buf := make([]byte, 128)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			return // the socket was closed
		}
		silent := s.silent.Load()
		replied := false
		if !silent && n > 0 {
			if _, werr := s.conn.WriteToUDP(reply(), addr); werr == nil {
				replied = true
			}
		}
		// NTP events are timeline context only -- never significant, so they
		// need no tolerate entry -- but they are what explains a daemon that
		// sits in Disconnected: the rows show whether it ever probed at all.
		s.log.Append(eventlog.SourceNTP, eventlog.KindNTPProbe, map[string]any{
			"occurrence": int(s.probes.Add(1)),
			"bytes":      n,
			"replied":    replied,
		}, nil)
	}
}

// reply is a minimal NTPv3 server response.
//
// The daemon never parses it, so the contents are decoration -- but a
// plausible first byte (leap 0, version 3, mode 4 = server) costs nothing and
// means a real NTP client pointed at the harness does not see garbage.
func reply() []byte {
	b := make([]byte, probeSize)
	b[0] = 0x1C
	return b
}
