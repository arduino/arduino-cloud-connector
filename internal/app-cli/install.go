// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
)

// installPath is arduino-app-cli's install endpoint (RFC-14 §6.1). prepare=true makes
// it also download the containers and models the release needs to run, which is
// where a Cloud deploy ends: the App is installed and ready, but not started. The
// parameter defaults to false, so it has to be passed.
const installPath = "/v1/apps/install?prepare=true"

// Install uploads the bundle at bundlePath and follows the install to its verdict.
//
// The bundle is uploaded as the multipart field "file", and the stream reports the
// install: progress events while the containers and models are downloaded, then
// exactly one done or error event.
func (c *Client) Install(ctx context.Context, bundlePath string, onProgress ProgressFunc) error {
	f, err := os.Open(bundlePath)
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
		_ = pw.CloseWithError(writeBundle(mw, bundle, filepath.Base(bundlePath)))
	}()
	// Closing the reader unblocks the upload goroutine if the request ended before it
	// did; waiting for it means nothing this call started outlives it.
	defer func() {
		_ = pr.Close()
		<-uploaded
	}()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.baseURL+installPath, pr)
	if err != nil {
		return fmt.Errorf("%w: build the request: %v", ErrUnavailable, err)
	}
	req.Header.Set("Content-Type", mw.FormDataContentType())
	req.Header.Set("Accept", "text/event-stream")

	resp, err := c.send(ctx, req)
	if err != nil {
		if errors.Is(err, ErrUnavailable) {
			// A failure to read our own bundle aborts the upload too, and must not be
			// reported as arduino-app-cli being unreachable.
			_ = pr.Close()
			<-uploaded
			if bundle.err != nil {
				return fmt.Errorf("%w: read the bundle: %v", ErrFailed, bundle.err)
			}
		}
		return err
	}
	defer resp.Body.Close()

	if err := checkStream(resp, installRefused); err != nil {
		return err
	}
	return follow(ctx, resp.Body, onProgress, untilDone)
}

// installRefused classifies a refusal of the install. The two server errors that are
// a failed install carry a prefix (GenericError, NoDiskSpace); the one left is
// arduino-app-cli failing to open the stream at all, which RFC-14 §6.1 reports as
// unreachable.
func installRefused(resp *http.Response) error { return statusError(resp, ErrUnavailable) }

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
