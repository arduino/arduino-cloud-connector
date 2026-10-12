// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appinstaller

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// installPath is arduino-app-cli's install endpoint (RFC-14 §6.1). prepare=true makes
// it also download the containers and models the release needs to run, which is
// where a Cloud deploy ends: the App is installed and ready, but not started. The
// parameter defaults to false, so it has to be passed.
const installPath = "/v1/apps/install?prepare=true"

const (
	// dialTimeout bounds the connection to arduino-app-cli. It is on the same
	// board, so anything slower than this means it is not listening.
	dialTimeout = 10 * time.Second
	// maxEventLine bounds one line of the event stream. message events carry tool
	// output (arduino-cli upload logs, model handlers), so lines can be long, but a
	// line past this is not a well-formed event.
	maxEventLine = 1 << 20
	// maxErrorBody bounds how much of a non-200 response is read for its error.
	maxErrorBody = 4 << 10
)

// AppCLI installs bundles through arduino-app-cli's HTTP API.
//
// The bundle is uploaded as the multipart field "file", and the response is a
// Server-Sent Events stream that reports the install: progress events while the
// containers and models are downloaded, then exactly one done or error event. The
// stream also ends with an error event whose code is SERVER_CLOSED — on success too,
// for backward compatibility on app-cli's side — so that one is not a verdict: it only
// means the stream is over.
//
// Install reads the stream on the caller's goroutine, so onProgress is always called
// from there, as Installer requires.
type AppCLI struct {
	baseURL string
	client  *http.Client
}

// NewAppCLI returns an installer for the arduino-app-cli daemon at baseURL, e.g.
// http://127.0.0.1:8800.
func NewAppCLI(baseURL string) *AppCLI {
	return &AppCLI{
		baseURL: strings.TrimRight(baseURL, "/"),
		client: &http.Client{
			// No overall timeout: an install has no natural duration, and silence is
			// bounded by the caller's stall watchdog instead. No proxy either: this is
			// a loopback call, and an environment proxy must never see it.
			Transport: &http.Transport{
				Proxy:       nil,
				DialContext: (&net.Dialer{Timeout: dialTimeout}).DialContext,
			},
		},
	}
}

// Install uploads the bundle at req.BundlePath and follows the install to its end.
func (a *AppCLI) Install(ctx context.Context, req Request, onProgress ProgressFunc) error {
	f, err := os.Open(req.BundlePath)
	if err != nil {
		return fmt.Errorf("%w: open the bundle: %v", ErrFailed, err)
	}
	defer f.Close()

	// The body is streamed rather than buffered: a bundle can be several GB.
	bundle := &readTracker{r: f}
	pr, pw := io.Pipe()
	mw := multipart.NewWriter(pw)
	uploaded := make(chan struct{})
	go func() {
		defer close(uploaded)
		_ = pw.CloseWithError(writeBundle(mw, bundle, filepath.Base(req.BundlePath)))
	}()
	// Closing the reader unblocks the upload goroutine if the request ended before it
	// did; waiting for it means nothing this call started outlives it.
	defer func() {
		_ = pr.Close()
		<-uploaded
	}()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPut, a.baseURL+installPath, pr)
	if err != nil {
		return fmt.Errorf("%w: build the request: %v", ErrUnavailable, err)
	}
	httpReq.Header.Set("Content-Type", mw.FormDataContentType())
	httpReq.Header.Set("Accept", "text/event-stream")

	resp, err := a.client.Do(httpReq)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A failure to read our own bundle aborts the upload too, and must not be
		// reported as arduino-app-cli being unreachable.
		_ = pr.Close()
		<-uploaded
		if bundle.err != nil {
			return fmt.Errorf("%w: read the bundle: %v", ErrFailed, bundle.err)
		}
		return fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}
	if mt, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type")); mt != "text/event-stream" {
		return fmt.Errorf("%w: arduino-app-cli answered %q, not an event stream",
			ErrFailed, resp.Header.Get("Content-Type"))
	}
	return follow(ctx, resp.Body, onProgress)
}

// writeBundle writes the multipart body: the bundle as the field "file".
func writeBundle(mw *multipart.Writer, bundle io.Reader, name string) error {
	part, err := mw.CreateFormFile("file", name)
	if err != nil {
		return err
	}
	if _, err := io.Copy(part, bundle); err != nil {
		return err
	}
	return mw.Close()
}

// readTracker remembers a failure of the reader it wraps, so a broken upload can be
// told apart from a broken connection. Read only after the upload goroutine is done.
type readTracker struct {
	r   io.Reader
	err error
}

func (t *readTracker) Read(p []byte) (int, error) {
	n, err := t.r.Read(p)
	if err != nil && !errors.Is(err, io.EOF) {
		t.err = err
	}
	return n, err
}

// ── the event stream ─────────────────────────────────────────────────────────

// streamClosedCode is the code of the error event arduino-app-cli sends as the last
// event of every stream, whatever the outcome.
const streamClosedCode = "SERVER_CLOSED"

// follow reads the event stream until the install's verdict.
//
// The verdict is the first done or error event; the stream is abandoned as soon as it
// arrives. An end of stream before it — the SERVER_CLOSED marker, the close event, or
// the connection simply ending — means the install's outcome is unknown, and is
// ErrUnavailable (RFC-14 §5.10 -41).
func follow(ctx context.Context, body io.Reader, onProgress ProgressFunc) error {
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
				slog.Warn("appinstaller: unreadable progress event from arduino-app-cli", "error", err)
				continue
			}
			// Values are fractional while the models download (66.66667 with three),
			// and must never go backwards on the Cloud's progress bar.
			last = max(last, min(p.Progress, 100))
			if ctx.Err() != nil {
				return ctx.Err()
			}
			onProgress(int32(last))

		case "message":
			slog.Debug("appinstaller: arduino-app-cli", "message", payload)

		case "done":
			var d struct {
				ID      string `json:"id"`
				Release string `json:"release"`
			}
			_ = json.Unmarshal([]byte(payload), &d)
			slog.Info("appinstaller: arduino-app-cli installed the release",
				"app_id", d.ID, "release", d.Release)
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
				return fmt.Errorf("%w: the install stream closed without a verdict", ErrUnavailable)
			}
			return streamError(e.Code, e.Message)

		case "close":
			return fmt.Errorf("%w: the install stream closed without a verdict", ErrUnavailable)
		}
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("%w: reading the install stream: %v", ErrUnavailable, err)
	}
	return fmt.Errorf("%w: the install stream ended without a verdict", ErrUnavailable)
}

// ── arduino-app-cli errors → sentinels ───────────────────────────────────────

// prefixes are the error prefixes arduino-app-cli puts in front of an error message
// to say which failure it is (RFC-14 §6.1, "Error prefix proposal"): the four codes
// its events carry are too coarse — bad_request alone covers three different Cloud
// codes. The message reads "<prefix>: <text>".
//
// A prefix is matched as a whole word, never as a string prefix:
// AdditionalResourceDownloadError is itself a prefix of
// AdditionalResourceDownloadErrorNoDiskSpace.
var prefixes = map[string]error{
	"GenericError":       ErrFailed,
	"NoDiskSpace":        ErrNoSpace,
	"AppArchiveRejected": ErrArchiveRejected,
	"AppYamlInvalid":     ErrInvalidAppYaml,
	"AppNotCompatible":   ErrNotCompatible,
	"ExtractionError":    ErrExtraction,
	"AdditionalResourceDownloadErrorNoDiskSpace": ErrResourceNoSpace,
	"AdditionalResourceDownloadError":            ErrResourceDownload,
}

// prefixed returns the sentinel named by msg's prefix, if it has a known one.
func prefixed(msg string) (error, bool) {
	word, _, found := strings.Cut(msg, ":")
	if !found {
		return nil, false
	}
	sentinel, ok := prefixes[strings.TrimSpace(word)]
	return sentinel, ok
}

// streamError classifies an error event from the install stream.
func streamError(code, msg string) error {
	if sentinel, ok := prefixed(msg); ok {
		return fmt.Errorf("%w: %s", sentinel, msg)
	}
	if code == "app_already_exists" {
		return fmt.Errorf("%w: %s", ErrAlreadyExists, msg)
	}
	// bad_request, INTERNAL_SERVER_ERROR or anything new, with no prefix to say more.
	return fmt.Errorf("%w: %s (%s)", ErrFailed, msg, code)
}

// statusError classifies a response that is not the event stream: arduino-app-cli
// refused the request before the install began, with {"details": "..."}.
func statusError(resp *http.Response) error {
	var body struct {
		Details string `json:"details"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, maxErrorBody)).Decode(&body)
	detail := fmt.Sprintf("arduino-app-cli answered %d: %s", resp.StatusCode, body.Details)

	if sentinel, ok := prefixed(body.Details); ok {
		return fmt.Errorf("%w: %s", sentinel, detail)
	}
	switch {
	case resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusMethodNotAllowed:
		// No install endpoint: an arduino-app-cli too old to deploy the App, which
		// RFC-14 §5.10 files under -44.
		return fmt.Errorf("%w: no install endpoint, %s", ErrNotCompatible, detail)
	case resp.StatusCode >= http.StatusInternalServerError:
		// The two 500s that are a failed install carry a prefix (GenericError,
		// NoDiskSpace); the one left is arduino-app-cli failing to open the stream at
		// all, which RFC-14 §6.1 reports as unreachable.
		return fmt.Errorf("%w: %s", ErrUnavailable, detail)
	default:
		return fmt.Errorf("%w: %s", ErrFailed, detail)
	}
}
