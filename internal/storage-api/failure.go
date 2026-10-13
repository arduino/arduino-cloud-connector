// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package storageapi

import "fmt"

// Failure is the error carrier used by this package, and by whoever drives a
// transfer with it — internal/downloader builds its own disk-side failures with
// the same type, so one classification serves the whole download loop.
//
// It pairs one of the exported sentinels with a specific message, the underlying
// cause, and whether the attempt is worth repeating.
//
// # Retry is for the download loop, not for callers
//
// Retry answers one question, asked once per attempt by the loop that is fetching
// chunks: try this chunk again, or stop? By the time an error escapes a completed
// download, that decision has already been made and the outcome is final — "503,
// will retry" and "503, gave up" both surface as ErrBadResponse. So code outside
// the loop (internal/ota, anything reporting to the Cloud) must classify on the
// sentinel with errors.Is and must NOT branch on Retry: doing so would read "this
// chunk was worth another go" as "this deploy is worth another go", which is a
// different question with a different answer.
type Failure struct {
	// Sentinel is the exported Err* value callers match with errors.Is.
	Sentinel error
	// Msg describes this particular occurrence.
	Msg string
	// Cause is the underlying net/os error, when there is one.
	Cause error
	// Retry reports whether the download loop may repeat the attempt. See the note
	// above: meaningful only while the loop is running.
	Retry bool
}

func (f *Failure) Error() string {
	if f.Cause != nil {
		return fmt.Sprintf("%v: %s: %v", f.Sentinel, f.Msg, f.Cause)
	}
	return fmt.Sprintf("%v: %s", f.Sentinel, f.Msg)
}

// Unwrap exposes both the sentinel and the cause, so errors.Is matches the
// sentinel (how callers classify the outcome) and also any wrapped net/os error
// (useful when debugging).
func (f *Failure) Unwrap() []error {
	if f.Cause == nil {
		return []error{f.Sentinel}
	}
	return []error{f.Sentinel, f.Cause}
}

// Retryable builds an error the download loop may retry.
func Retryable(sentinel error, format string, args ...any) *Failure {
	return &Failure{Sentinel: sentinel, Msg: fmt.Sprintf(format, args...), Retry: true}
}

// Terminal builds an error that ends the download outright — retrying it would
// only delay the inevitable (an expired grant, an oversized artefact, a digest
// that will never match).
func Terminal(sentinel error, format string, args ...any) *Failure {
	return &Failure{Sentinel: sentinel, Msg: fmt.Sprintf(format, args...)}
}

// Wrap attaches the underlying cause and returns f, so it chains onto a
// constructor call.
func (f *Failure) Wrap(err error) *Failure {
	f.Cause = err
	return f
}
