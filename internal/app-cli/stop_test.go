// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"errors"
	"slices"
	"testing"
)

// A stop reports one progress, 100, at its end (RFC-16 §5), and like the start has no
// done event.
func TestStopSucceedsWhenTheStreamClosesWithoutAnError(t *testing.T) {
	stream := "event: message\ndata: {\"message\":\"Stopping app \\\"blink\\\"\"}\n\n" +
		"event: heartbeat\n\n" +
		"event: message\ndata: {\"message\":\"Container blink-main-1 Stopped\"}\n\n" +
		"event: progress\ndata: {\"name\":\"\",\"progress\":100}\n\n" +
		streamClosed
	var seen []int32
	srv := lifecycleServer(t, "/v1/apps/blink/stop", stream)
	if err := New(srv.URL).Stop(context.Background(), "blink", func(p int32) { seen = append(seen, p) }); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if !slices.Equal(seen, []int32{100}) {
		t.Errorf("progress: got %v want [100]", seen)
	}
}

func TestStopFailureIsAFailedCommand(t *testing.T) {
	stream := "event: error\ndata: {\"code\":\"INTERNAL_SERVER_ERROR\",\"message\":\"compose stop failed\"}\n\n" + streamClosed
	err := New(lifecycleServer(t, "/v1/apps/blink/stop", stream).URL).Stop(context.Background(), "blink", func(int32) {})
	if !errors.Is(err, ErrFailed) {
		t.Fatalf("got %v, want ErrFailed", err)
	}
	assertOnlySpecific(t, err, ErrFailed)
}
