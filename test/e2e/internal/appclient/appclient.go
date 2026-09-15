// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package appclient is the harness playing an APP against the daemon's REST
// API: the other half of its job, next to playing the cloud.
//
// It matters because the interesting scenarios cross both roles. The cloud
// pushes a value down over MQTT and the app must see it arrive on its SSE
// stream; the app puts a value and the cloud must see it published. Neither
// half proves anything on its own.
//
// Paths, JSON field names and SSE event names are written out here rather than
// imported from the daemon. If someone renames a route or an event, the E2E
// suite must break -- that is the regression it exists to catch, and importing
// the constant would hide it.
package appclient

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/arduino/arduino-cloud-connector/test/e2e/internal/eventlog"
)

// The daemon's REST surface.
const (
	PathHealth            = "/v1/health"
	PathVersion           = "/v1/version"
	PathIdentity          = "/v1/identity"
	PathStatus            = "/v1/status"
	PathProvisioningStart = "/v1/provisioning/start"
)

// VariablePath is PUT /v1/variables/{name}.
func VariablePath(name string) string { return "/v1/variables/" + url.PathEscape(name) }

// VariableEventsPath is the SSE stream for one variable.
func VariableEventsPath(name string) string { return VariablePath(name) + "/events" }

// The SSE event names the daemon emits. The first frame is always one of the
// three sync kinds; every later change is an update.
const (
	EventUpdate           = "update"
	EventLastValue        = "lastvalue"
	EventLastValueMissing = "lastvalue_missing"
	EventThingUnavailable = "thing_unavailable"
)

// requestTimeout bounds one REST call. Generous, because a step's own timeout
// is the real budget; this only stops a wedged socket from hanging a scenario.
const requestTimeout = 15 * time.Second

// Cloud is the cloud section of the status response.
type Cloud struct {
	State   string `json:"state"`
	ThingID string `json:"thing_id"`
}

// Status is GET /v1/status.
type Status struct {
	Provisioning   string `json:"provisioning"`
	Daemon         string `json:"daemon"`
	Cloud          *Cloud `json:"cloud"`
	DeviceID       string `json:"device_id"`
	OrganizationID string `json:"organization_id"`
}

// Identity is GET /v1/identity.
type Identity struct {
	UHWID        string `json:"uhwid"`
	BoardToken   string `json:"board_token"`
	PublicKeyPEM string `json:"public_key_pem"`
}

// Client is the app-role client.
type Client struct {
	log     *eventlog.Log
	baseURL string
	http    *http.Client

	mu         sync.Mutex
	lastStatus string // rendered status, for change detection
}

// New returns a client for a daemon serving on baseURL (e.g.
// http://127.0.0.1:5683).
func New(log *eventlog.Log, baseURL string) (*Client, error) {
	if log == nil {
		return nil, fmt.Errorf("appclient: no event log given")
	}
	if baseURL == "" {
		return nil, fmt.Errorf("appclient: no base URL given")
	}
	return &Client{
		log:     log,
		baseURL: strings.TrimSuffix(baseURL, "/"),
		// No global timeout on the client: the SSE stream is a long-lived
		// response and a client-wide deadline would cut it off. Each one-shot
		// call sets its own via the request context.
		http: &http.Client{},
	}, nil
}

// BaseURL is where the daemon is being reached.
func (c *Client) BaseURL() string { return c.baseURL }

// WaitReady polls the health endpoint until the daemon answers.
//
// Needed because the process is up before its listener is: a REST call made in
// that window fails with a connection error that has nothing to do with what
// the scenario is testing. This is the one place the harness polls rather than
// waiting on the event log -- there is no event to wait for until the socket
// exists.
func (c *Client) WaitReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for {
		if status, _, err := c.do(ctx, http.MethodGet, PathHealth, nil, false); err == nil && status == http.StatusOK {
			c.log.Append(eventlog.SourceApp, eventlog.KindHarnessNote, map[string]any{
				"action": "daemon_ready",
				"path":   PathHealth,
			}, nil)
			return nil
		} else if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("status %d", status)
		}
		if !time.Now().Before(deadline) {
			return fmt.Errorf("appclient: daemon did not answer %s within %s: %w", PathHealth, timeout, lastErr)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// Status fetches the daemon status and records it when it has changed.
func (c *Client) Status(ctx context.Context) (Status, error) {
	_, body, err := c.expect(ctx, http.MethodGet, PathStatus, nil, http.StatusOK)
	if err != nil {
		return Status{}, err
	}
	var s Status
	if err := json.Unmarshal(body, &s); err != nil {
		return Status{}, fmt.Errorf("appclient: decode status: %w", err)
	}
	c.recordStatus(s)
	return s, nil
}

// recordStatus appends a status event only when something changed.
//
// The poller runs several times a second and the daemon sits in one state for
// most of a scenario; appending every poll would bury the timeline in identical
// rows. On change, the row is exactly the transition a reader is looking for.
func (c *Client) recordStatus(s Status) {
	attrs := map[string]any{
		"provisioning": s.Provisioning,
		"daemon":       s.Daemon,
	}
	if s.Cloud != nil {
		attrs["cloud_state"] = s.Cloud.State
		if s.Cloud.ThingID != "" {
			attrs["thing_id"] = s.Cloud.ThingID
		}
	}
	if s.DeviceID != "" {
		attrs["device_id"] = s.DeviceID
	}
	if s.OrganizationID != "" {
		attrs["organization_id"] = s.OrganizationID
	}

	rendered := fmt.Sprint(attrs)
	c.mu.Lock()
	changed := rendered != c.lastStatus
	c.lastStatus = rendered
	c.mu.Unlock()
	if changed {
		c.log.Append(eventlog.SourceDaemonStatus, eventlog.KindStatusPoll, attrs, nil)
	}
}

// StartStatusPoller polls the status in the background until the returned stop
// function is called. Every transition lands on the timeline, which is what
// await_daemon_state waits on.
func (c *Client) StartStatusPoller(interval time.Duration) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			// A failed poll is expected around startup and shutdown and says
			// nothing on its own, so it is not recorded: the daemon's own exit
			// event is what explains a daemon that stopped answering.
			_, _ = c.Status(ctx)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

// Identity fetches the board identity, which is where the uhwid comes from.
func (c *Client) Identity(ctx context.Context) (Identity, error) {
	_, body, err := c.expect(ctx, http.MethodGet, PathIdentity, nil, http.StatusOK)
	if err != nil {
		return Identity{}, err
	}
	var id Identity
	if err := json.Unmarshal(body, &id); err != nil {
		return Identity{}, fmt.Errorf("appclient: decode identity: %w", err)
	}
	// The board token is a credential: recorded as present, never as a value.
	c.log.Append(eventlog.SourceApp, eventlog.KindHarnessNote, map[string]any{
		"action":            "identity",
		"uhwid":             id.UHWID,
		"board_token_bytes": len(id.BoardToken),
	}, nil)
	return id, nil
}

// StartProvisioning asks the daemon to provision. The organization id is
// optional, exactly as in the API.
func (c *Client) StartProvisioning(ctx context.Context, organizationID string) error {
	var body any
	if organizationID != "" {
		body = map[string]string{"organization_id": organizationID}
	}
	_, _, err := c.expect(ctx, http.MethodPost, PathProvisioningStart, body, http.StatusAccepted)
	return err
}

// PutVariable sends a value the way a cloud brick app does.
//
// A 409 is a real answer here, not a transport failure: the daemon refuses to
// queue a value while no thing is assigned. It is returned as an error naming
// the status so a scenario can tell the two apart.
func (c *Client) PutVariable(ctx context.Context, name string, value any) error {
	_, _, err := c.expect(ctx, http.MethodPut, VariablePath(name),
		map[string]any{"value": value}, http.StatusNoContent)
	return err
}

// Post is the generic action a scenario uses for an endpoint with no dedicated
// primitive yet. wantStatus of 0 accepts any 2xx.
func (c *Client) Post(ctx context.Context, path string, body any, wantStatus int) (int, []byte, error) {
	if wantStatus != 0 {
		return c.expect(ctx, http.MethodPost, path, body, wantStatus)
	}
	status, respBody, err := c.do(ctx, http.MethodPost, path, body, true)
	if err != nil {
		return status, respBody, err
	}
	if status < 200 || status > 299 {
		return status, respBody, fmt.Errorf("appclient: POST %s: status %d: %s", path, status, respBody)
	}
	return status, respBody, nil
}

// ── SSE ──────────────────────────────────────────────────────────────────────

// Stream is an open SSE subscription for one variable.
type Stream struct {
	Variable string

	cancel context.CancelFunc
	done   chan struct{}
}

// SubscribeVariable opens the SSE stream for a variable and pumps its frames
// into the log.
//
// It returns once the response headers are in, which is what makes ordering
// meaningful: the daemon flushes them before the first frame, so a scenario
// that subscribes and then injects a value cannot race its own subscription.
func (c *Client) SubscribeVariable(ctx context.Context, name string) (*Stream, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	path := VariableEventsPath(name)
	req, err := http.NewRequestWithContext(streamCtx, http.MethodGet, c.baseURL+path, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("appclient: new request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("appclient: GET %s: %w", path, err)
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("appclient: GET %s: status %d: %s", path, resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		_ = resp.Body.Close()
		cancel()
		return nil, fmt.Errorf("appclient: GET %s: content type %q, want text/event-stream", path, ct)
	}

	c.log.Append(eventlog.SourceApp, eventlog.KindHarnessNote, map[string]any{
		"action":   "sse_subscribe",
		"variable": name,
		"path":     path,
	}, nil)

	s := &Stream{Variable: name, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(s.done)
		defer resp.Body.Close() //nolint:errcheck
		c.pumpSSE(streamCtx, name, resp.Body)
	}()
	return s, nil
}

// Close ends the subscription.
func (s *Stream) Close() {
	s.cancel()
	<-s.done
}

// pumpSSE parses the frames and appends one event each.
//
// The framing is the daemon's: "event: <name>\ndata: <json>\n\n". Parsing it
// by hand rather than with an SSE library is deliberate -- the frame shape is
// part of the contract with the app, and a library that tolerated a malformed
// frame would hide exactly that.
func (c *Client) pumpSSE(ctx context.Context, variable string, body io.Reader) {
	scanner := bufio.NewScanner(body)
	var eventName string
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "event: "):
			eventName = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			c.appendSSEFrame(variable, eventName, strings.TrimPrefix(line, "data: "))
			eventName = ""
		case line == "":
			// Frame boundary.
		default:
			c.log.Append(eventlog.SourceSSE, eventlog.KindHarnessNote, map[string]any{
				"variable": variable,
				"note":     "unrecognised SSE line",
				"line":     line,
			}, []byte(line))
		}
	}
	// A stream that ends is worth a note: the daemon closed it, or the harness
	// did. It is not an event a scenario asserts on, so it stays a note.
	attrs := map[string]any{"action": "sse_closed", "variable": variable}
	if err := scanner.Err(); err != nil && ctx.Err() == nil {
		attrs["error"] = err.Error()
	}
	c.log.Append(eventlog.SourceSSE, eventlog.KindHarnessNote, attrs, nil)
}

// appendSSEFrame records one frame, with the payload fields flattened into
// attributes so a step can constrain them.
func (c *Client) appendSSEFrame(variable, eventName, data string) {
	attrs := map[string]any{
		"variable": variable,
		"event":    eventName,
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(data), &payload); err != nil {
		attrs["decode_error"] = err.Error()
	} else {
		for k, v := range payload {
			attrs[k] = v
		}
	}
	c.log.Append(eventlog.SourceSSE, eventlog.KindSSEFrame, attrs, []byte(data))
}

// ── plumbing ─────────────────────────────────────────────────────────────────

// expect performs a call and requires an exact status.
func (c *Client) expect(ctx context.Context, method, path string, body any, wantStatus int) (int, []byte, error) {
	status, respBody, err := c.do(ctx, method, path, body, true)
	if err != nil {
		return status, respBody, err
	}
	if status != wantStatus {
		return status, respBody, fmt.Errorf("appclient: %s %s: status %d, want %d: %s",
			method, path, status, wantStatus, respBody)
	}
	return status, respBody, nil
}

// do performs one call. record is false for the readiness poll, which would
// otherwise fill the timeline with connection failures before the daemon is up.
func (c *Client) do(ctx context.Context, method, path string, body any, record bool) (int, []byte, error) {
	var reader io.Reader
	var encoded []byte
	if body != nil {
		var err error
		if encoded, err = json.Marshal(body); err != nil {
			return 0, nil, fmt.Errorf("appclient: encode request: %w", err)
		}
		reader = bytes.NewReader(encoded)
	}

	callCtx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, method, c.baseURL+path, reader)
	if err != nil {
		return 0, nil, fmt.Errorf("appclient: new request: %w", err)
	}
	if encoded != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := c.http.Do(req)
	if err != nil {
		if record {
			c.log.Append(eventlog.SourceApp, eventlog.KindHarnessNote, map[string]any{
				"action": strings.ToLower(method),
				"path":   path,
				"error":  err.Error(),
			}, encoded)
		}
		return 0, nil, fmt.Errorf("appclient: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("appclient: %s %s: read response: %w", method, path, err)
	}
	if record && path != PathStatus {
		// The status endpoint is excluded: the poller hits it constantly and
		// recordStatus already puts every transition on the timeline.
		c.log.Append(eventlog.SourceApp, eventlog.KindHarnessNote, map[string]any{
			"action": strings.ToLower(method),
			"path":   path,
			"status": resp.StatusCode,
		}, encoded)
	}
	return resp.StatusCode, respBody, nil
}
