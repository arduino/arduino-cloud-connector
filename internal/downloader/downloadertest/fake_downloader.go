// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package downloadertest provides a controllable, in-memory implementation of
// downloader.Downloader for automated tests.
//
// It never touches the network or a real transfer. Callers script the outcome they
// want — content to deliver, an error to return, a download that blocks until
// released — which makes it possible to drive the App-deploy state machine through
// every branch without standing up a server. The transfer-level edge cases (ranged
// chunking, resume, retry budgets, verification) are covered once, in downloader's
// own tests.
package downloadertest

import (
	"context"
	"os"
	"sync"

	"github.com/arduino/arduino-cloud-connector/internal/downloader"
)

// FakeDownloader is a scriptable downloader.Downloader.
//
// The zero value is usable: it writes Content (empty by default) to the requested
// destination and returns success.
type FakeDownloader struct {
	mu sync.Mutex

	// Content is written to Request.DestPath on a successful download.
	Content []byte
	// Err, when non-nil, is returned instead of performing the download. Use the
	// downloader.Err* or storageapi.Err* sentinels so the caller's error mapping is
	// exercised.
	Err error
	// Started, when non-nil, is closed the first time Download is called — a signal
	// that a transfer is under way.
	Started chan struct{}
	// Block, when non-nil, holds Download until it is closed (or the context is
	// cancelled), so a test can act while a transfer is in flight.
	Block <-chan struct{}
	// Progress values are reported to onProgress before the download completes.
	Progress []Progress

	// Recorded observations.
	requests  []downloader.Request
	discarded []string
}

// Progress is one onProgress callback to emit.
type Progress struct{ Written, Total int64 }

// Download implements downloader.Downloader.
func (f *FakeDownloader) Download(ctx context.Context, req downloader.Request, onProgress downloader.ProgressFunc) error {
	f.mu.Lock()
	f.requests = append(f.requests, req)
	content, err, started, block := f.Content, f.Err, f.Started, f.Block
	progress := append([]Progress(nil), f.Progress...)
	f.mu.Unlock()

	if started != nil {
		// Closed at most once even across repeated downloads.
		select {
		case <-started:
		default:
			close(started)
		}
	}

	for _, p := range progress {
		if onProgress != nil {
			onProgress(p.Written, p.Total)
		}
	}

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if err != nil {
		return err
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return os.WriteFile(req.DestPath, content, 0o600)
}

// Discard implements downloader.Downloader.
func (f *FakeDownloader) Discard(destPath string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.discarded = append(f.discarded, destPath)
}

// Requests returns every download request received, in order.
func (f *FakeDownloader) Requests() []downloader.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]downloader.Request(nil), f.requests...)
}

// Discarded returns every destination Discard was called for, in order.
func (f *FakeDownloader) Discarded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.discarded...)
}

// SetErr changes the scripted error. Safe to call while a download is blocked.
func (f *FakeDownloader) SetErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Err = err
}

// Compile-time proof that the fake satisfies the interface it stands in for.
var _ downloader.Downloader = (*FakeDownloader)(nil)
