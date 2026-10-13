// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package appcli is the HTTP client for the local arduino-app-cli daemon, which owns
// the Apps installed on the board: it installs a bundle (RFC-14 §6.1), lists the
// Apps, and starts and stops one (RFC-16 §5). One file per endpoint.
//
// Every call that changes the board answers with a Server-Sent Events stream that
// reports the operation, and this package follows that stream to the operation's
// verdict on the caller's goroutine, so a progress callback is never called from
// anywhere else.
//
// # What this package deliberately does not know
//
// It knows no deploy job, no Cloud command and no wire code. It reports what
// arduino-app-cli said, as one of the sentinels in errors.go; turning that into what
// the Cloud is told is the caller's job — internal/ota for a deploy,
// internal/applifecycle for a lifecycle command.
package appcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"
)

const (
	// dialTimeout bounds the connection to arduino-app-cli. It is on the same
	// board, so anything slower than this means it is not listening.
	dialTimeout = 10 * time.Second
	// requestTimeout bounds a plain request/response call (the App list, setting the
	// default App). The streamed calls have no overall timeout: an install or a start
	// has no natural duration, and their silence is bounded by the caller's watchdog
	// instead.
	requestTimeout = 30 * time.Second
)

// ProgressFunc reports an operation's progress as a percentage (0–100). It is called
// for every progress event arduino-app-cli sends, repeats included, never goes
// backwards, and must not block.
type ProgressFunc func(percent int32)

// Client talks to one arduino-app-cli daemon.
type Client struct {
	baseURL string
	http    *http.Client
}

// New returns a client for the arduino-app-cli daemon at baseURL, e.g.
// http://127.0.0.1:8800.
func New(baseURL string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		http: &http.Client{
			// No proxy: this is a loopback call, and an environment proxy must never
			// see it.
			Transport: &http.Transport{
				Proxy:       nil,
				DialContext: (&net.Dialer{Timeout: dialTimeout}).DialContext,
			},
		},
	}
}

// send issues req. A failure to get any answer is ErrUnavailable, unless it is the
// caller's cancellation, which is returned as-is.
func (c *Client) send(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return resp, nil
}

// request issues a plain request/response call, bounded by requestTimeout. The
// returned done must be called once the response body has been read; it also closes
// the body.
//
// Running out of requestTimeout is ErrUnavailable — arduino-app-cli did not answer —
// while the caller's own cancellation is returned as-is.
func (c *Client) request(parent context.Context, method, path string, body io.Reader, contentType string) (
	resp *http.Response, done func(), err error,
) {
	ctx, cancel := context.WithTimeout(parent, requestTimeout)
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		cancel()
		return nil, nil, fmt.Errorf("%w: build the request: %v", ErrFailed, err)
	}
	req.Header.Set("Accept", "application/json")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err = c.send(ctx, req)
	if err != nil {
		cancel()
		if parent.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			return nil, nil, fmt.Errorf("%w: no answer within %s", ErrUnavailable, requestTimeout)
		}
		return nil, nil, err
	}
	return resp, func() { _ = resp.Body.Close(); cancel() }, nil
}

// readFailed classifies a failure to read a plain response's body.
func readFailed(parent context.Context, resp *http.Response, what string, err error) error {
	if parent.Err() != nil {
		return parent.Err()
	}
	if resp.Request.Context().Err() != nil {
		return fmt.Errorf("%w: reading %s: %v", ErrUnavailable, what, err)
	}
	return fmt.Errorf("%w: unreadable %s: %v", ErrFailed, what, err)
}

// stream issues a request answered by an event stream and follows that stream to the
// operation's verdict. refused classifies an answer that is not the stream.
func (c *Client) stream(ctx context.Context, req *http.Request, onProgress ProgressFunc,
	verdict verdict, refused func(*http.Response) error,
) error {
	req.Header.Set("Accept", "text/event-stream")
	resp, err := c.send(ctx, req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if err := checkStream(resp, refused); err != nil {
		return err
	}
	return follow(ctx, resp.Body, onProgress, verdict)
}

// checkStream tells whether resp is the event stream the operation answers with.
func checkStream(resp *http.Response, refused func(*http.Response) error) error {
	if resp.StatusCode != http.StatusOK {
		return refused(resp)
	}
	if mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mt != "text/event-stream" {
		return fmt.Errorf("%w: arduino-app-cli answered %q, not an event stream",
			ErrFailed, resp.Header.Get("Content-Type"))
	}
	return nil
}
