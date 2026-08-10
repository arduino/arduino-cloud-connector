// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package storageapitest provides a controllable, in-memory implementation of
// storageapi.Client for automated tests.
//
// It serves byte ranges out of a []byte and never touches the network, which is
// what lets internal/downloader test its transfer machinery — chunking, resume,
// checkpointing, retry budgets — without standing up a TLS server, and script
// conditions a real server makes awkward: a body that dies at byte N, a server that
// ignores Range, a size that changes mid-transfer. The HTTP-level semantics those
// conditions stand in for (206 vs 200, Content-Range parsing, status
// classification) are covered once, in storageapi's own tests against a real
// httptest server.
package storageapitest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
)

// FakeClient is a scriptable storageapi.Client.
//
// The zero value serves an empty artefact, which is not useful; set Content.
type FakeClient struct {
	mu sync.Mutex

	// Content is the artefact served.
	Content []byte

	// Err, when non-nil, is returned instead of serving anything.
	Err error
	// FailNext makes the next N calls fail with FailErr, then serve normally.
	FailNext int
	// FailErr is the error used by FailNext. Defaults to a retryable
	// storageapi.ErrConnect, i.e. a transient network failure.
	FailErr error
	// IgnoreRange makes the fake answer like a server with no Range support: the
	// whole artefact from byte 0, with Ranged false.
	IgnoreRange bool
	// ServeToEnd makes the fake honour the range START but ignore its END, running
	// the body on to the end of the artefact — still a 206, still Ranged, still
	// reporting the requested start. Real CDNs and object stores do this, and it is
	// the one shape the plain slice above cannot produce: the caller receives more
	// bytes than it asked for and has to stop itself at the chunk boundary without
	// mistaking the surplus for misplaced data.
	ServeToEnd bool
	// TruncateAt, when > 0, cuts every served body to this many bytes — a
	// connection that dies mid-body.
	TruncateAt int
	// ReadSize, when > 0, caps how many bytes each Read of the body returns.
	//
	// It matters more than it looks: a bytes.Reader hands over the whole body in one
	// Read, so a caller that means to stop at a chunk boundary sails past it and the
	// transfer completes in a single pass no matter what it intended. Capping the
	// read size is what makes a chunk boundary observable — without it, a test for
	// chunk-boundary behaviour passes whether the code is right or not.
	ReadSize int
	// Total, when non-zero, overrides the advertised total size instead of using
	// len(Content). Used to drive the size-inconsistency paths.
	Total int64
	// Hook, when non-nil, runs at the start of every call with the 1-based call
	// number, so a test can change the script mid-transfer.
	Hook func(call int)

	// Recorded observations.
	calls  int
	ranges []string
	served int64
}

// FetchAppRelease implements storageapi.Client.
func (f *FakeClient) FetchAppRelease(ctx context.Context, url string, start, end int64) (*storageapi.Response, error) {
	f.mu.Lock()
	f.calls++
	call := f.calls
	f.ranges = append(f.ranges, fmt.Sprintf("bytes=%d-%d", start, end))
	hook := f.Hook
	f.mu.Unlock()

	if hook != nil {
		hook(call)
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if f.Err != nil {
		return nil, f.Err
	}
	if f.FailNext > 0 {
		f.FailNext--
		if f.FailErr != nil {
			return nil, f.FailErr
		}
		return nil, storageapi.Retryable(storageapi.ErrConnect, "fake transient failure")
	}

	total := f.Total
	if total == 0 {
		total = int64(len(f.Content))
	}

	// A real server rejects a range that starts past the end of the object; the
	// downloader should never ask for one, so make it loud if it does.
	if start >= int64(len(f.Content)) && len(f.Content) > 0 {
		return nil, storageapi.Terminal(storageapi.ErrBadResponse,
			"fake: range starts at %d, past the end of %d bytes", start, len(f.Content))
	}

	body, ranged := f.slice(start, end)
	if f.TruncateAt > 0 && f.TruncateAt < len(body) {
		body = body[:f.TruncateAt]
	}
	f.served += int64(len(body))

	var r io.Reader = bytes.NewReader(body)
	if f.ReadSize > 0 {
		r = &cappedReader{r: r, max: f.ReadSize}
	}

	return &storageapi.Response{
		Body:   io.NopCloser(r),
		Start:  f.startOf(ranged, start),
		Total:  total,
		Ranged: ranged,
	}, nil
}

// cappedReader hands over at most max bytes per Read, the way a network does.
type cappedReader struct {
	r   io.Reader
	max int
}

func (c *cappedReader) Read(p []byte) (int, error) {
	if len(p) > c.max {
		p = p[:c.max]
	}
	return c.r.Read(p)
}

// slice picks the bytes this call serves, honouring IgnoreRange and ServeToEnd.
func (f *FakeClient) slice(start, end int64) (body []byte, ranged bool) {
	if f.IgnoreRange {
		return f.Content, false
	}
	if f.ServeToEnd {
		if start >= int64(len(f.Content)) {
			return nil, true
		}
		return f.Content[start:], true
	}
	last := end
	if last > int64(len(f.Content))-1 {
		last = int64(len(f.Content)) - 1
	}
	if start > last {
		return nil, true
	}
	return f.Content[start : last+1], true
}

func (f *FakeClient) startOf(ranged bool, start int64) int64 {
	if !ranged {
		return 0
	}
	return start
}

// Calls returns how many times FetchAppRelease was called.
func (f *FakeClient) Calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Ranges returns the Range spec of every call, in order.
func (f *FakeClient) Ranges() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.ranges...)
}

// Served returns how many artefact bytes were handed out in total — the number a
// resume test checks to prove bytes already on disk were not re-fetched.
func (f *FakeClient) Served() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.served
}

// SetIgnoreRange flips IgnoreRange while a transfer is in flight.
func (f *FakeClient) SetIgnoreRange(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.IgnoreRange = v
}

// SetTotal overrides the advertised total while a transfer is in flight.
func (f *FakeClient) SetTotal(v int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Total = v
}

// SetErr changes the scripted error while a transfer is in flight. Safe to call
// from Hook, which runs without the lock held.
func (f *FakeClient) SetErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Err = err
}

// Compile-time proof that the fake satisfies the interface it stands in for.
var _ storageapi.Client = (*FakeClient)(nil)
