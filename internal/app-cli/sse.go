// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// maxEventLine bounds one line of an event stream. message events carry tool output
// (arduino-cli upload logs, model handlers, compose lines), so lines can be long, but
// a line past this is not a well-formed event.
const maxEventLine = 1 << 20

// streamClosedCode is the code of the error event arduino-app-cli sends as the last
// event of every stream, whatever the outcome — for backward compatibility on its
// side, so it is never a verdict on its own.
const streamClosedCode = "SERVER_CLOSED"

// verdict says what ends an operation successfully.
type verdict int

const (
	// untilDone: the operation reports its success with a done event (install).
	// The stream closing before it leaves the outcome unknown.
	untilDone verdict = iota
	// untilClosed: the operation has no done event (start, stop), so success is
	// the stream closing with no error before it. A done event, should
	// arduino-app-cli add one, is a success too. The connection ending without the
	// close still leaves the outcome unknown.
	untilClosed
)

// follow reads an operation's event stream until its verdict, on the caller's
// goroutine.
//
// The verdict is the first done or error event — or, for untilClosed, the close of
// the stream. Anything else that ends the stream means the outcome is unknown, and
// is ErrUnavailable.
func follow(ctx context.Context, body io.Reader, onProgress ProgressFunc, v verdict) error {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 4096), maxEventLine)

	var (
		event string
		data  strings.Builder
		last  float64
	)
	for sc.Scan() {
		line := sc.Text()
		if line != "" {
			field, value, _ := strings.Cut(line, ":")
			value = strings.TrimPrefix(value, " ")
			switch field {
			case "event":
				event = value
			case "data":
				if data.Len() > 0 {
					data.WriteByte('\n')
				}
				data.WriteString(value)
			}
			continue
		}

		// A blank line dispatches the event.
		name, payload := event, data.String()
		event = ""
		data.Reset()

		switch name {
		case "progress":
			var p struct {
				Progress float64 `json:"progress"`
			}
			if err := json.Unmarshal([]byte(payload), &p); err != nil {
				slog.Warn("appcli: unreadable progress event from arduino-app-cli", "error", err)
				continue
			}
			// Values can be fractional (66.66667 while three models download), and
			// must never go backwards on the Cloud's progress bar.
			last = max(last, min(p.Progress, 100))
			slog.Debug("appcli: arduino-app-cli progress",
				"raw", p.Progress, "reported", int32(last))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			onProgress(int32(last))

		case "message":
			slog.Debug("appcli: arduino-app-cli", "message", payload)

		case "done":
			var d struct {
				ID      string `json:"id"`
				Release string `json:"release"`
			}
			_ = json.Unmarshal([]byte(payload), &d)
			slog.Info("appcli: arduino-app-cli reports done", "app_id", d.ID, "release", d.Release)
			return nil

		case "error":
			var e struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			}
			if err := json.Unmarshal([]byte(payload), &e); err != nil {
				return fmt.Errorf("%w: unreadable error event: %v", ErrFailed, err)
			}
			if e.Code == streamClosedCode {
				return closed(v)
			}
			return streamError(e.Code, e.Message)

		case "close":
			return closed(v)
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("%w: reading the stream: %v", ErrUnavailable, err)
	}
	return fmt.Errorf("%w: the stream ended without a verdict", ErrUnavailable)
}

// closed is what the stream closing means for the operation.
func closed(v verdict) error {
	if v == untilClosed {
		return nil
	}
	return fmt.Errorf("%w: the stream closed without a verdict", ErrUnavailable)
}
