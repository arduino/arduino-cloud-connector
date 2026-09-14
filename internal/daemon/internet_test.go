// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package daemon

import (
	"context"
	"net"
	"testing"
	"time"
)

// newUDPResponder starts a UDP listener on 127.0.0.1 that replies to every
// datagram with replyLen bytes (0 = never reply), and returns its host:port.
func newUDPResponder(t *testing.T, replyLen int) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	go func() {
		buf := make([]byte, 64)
		for {
			n, addr, rerr := conn.ReadFromUDP(buf)
			if rerr != nil {
				return // listener closed by Cleanup
			}
			if replyLen > 0 && n > 0 {
				_, _ = conn.WriteToUDP(make([]byte, replyLen), addr)
			}
		}
	}()
	return conn.LocalAddr().String()
}

// The probe only needs the round-trip: the reply is never parsed, so a host
// that answers with 48 bytes of anything satisfies the check. This is what
// lets the connectivity probe be pointed at a local responder.
func TestIsInternetReachableAcceptsAnyNTPSizedReply(t *testing.T) {
	host := newUDPResponder(t, 48)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if !isInternetReachable(ctx, host) {
		t.Fatalf("isInternetReachable(%q) = false, want true", host)
	}
}

// A host that accepts the datagram but never answers must read as unreachable,
// and the context deadline must cap the wait rather than the full
// internetCheckTimeout — otherwise a stop signal arriving during the probe is
// absorbed for seconds.
func TestIsInternetReachableHonoursContextDeadline(t *testing.T) {
	host := newUDPResponder(t, 0) // accepts, never replies

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	start := time.Now()
	if isInternetReachable(ctx, host) {
		t.Fatalf("isInternetReachable(%q) = true, want false (no reply)", host)
	}
	if elapsed := time.Since(start); elapsed >= internetCheckTimeout {
		t.Fatalf("waited %v; the context deadline should have capped it well below %v",
			elapsed, internetCheckTimeout)
	}
}
