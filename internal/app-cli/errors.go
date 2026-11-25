// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package appcli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// Failure sentinels. Every failure this package reports matches exactly one of
// ErrUnavailable or ErrFailed through errors.Is, and at most one of the specific
// verdicts, which all wrap ErrFailed.
//
// # Why the failure is typed rather than just "it failed"
//
// Each specific verdict reaches the operator as a different Arduino IoT Cloud error
// code — RFC-14 §5.10 for a deploy, RFC-16 §3 for a lifecycle command — and only
// arduino-app-cli can tell them apart. A caller that only wants to know whether the
// operation succeeded matches ErrFailed, which is also the right fallback for a
// failure nobody classified.
var (
	// ErrUnavailable: arduino-app-cli could not be reached, or its stream ended
	// before the operation's verdict, so the outcome is unknown.
	ErrUnavailable = errors.New("appcli: arduino-app-cli unavailable")
	// ErrFailed: the operation failed and the reason is not one of those below.
	ErrFailed = errors.New("appcli: operation failed")

	// ErrNoSpace: arduino-app-cli ran out of disk space — taking a bundle in, or
	// pulling the images an App needs to start.
	ErrNoSpace = fmt.Errorf("%w: no disk space", ErrFailed)
	// ErrNotCompatible: the App does not fit this board — the release targets
	// another board, needs a newer arduino-app-cli, or needs a brick, model or
	// peripheral the board does not have. Also an arduino-app-cli without the
	// endpoint at all.
	ErrNotCompatible = fmt.Errorf("%w: app not compatible with this board", ErrFailed)

	// Install verdicts (RFC-14 §6.1).

	// ErrArchiveRejected: the archive is not a valid bundle.
	ErrArchiveRejected = fmt.Errorf("%w: archive rejected", ErrFailed)
	// ErrInvalidAppYaml: the release manifest or app.yaml is missing, unparsable or
	// fails validation.
	ErrInvalidAppYaml = fmt.Errorf("%w: invalid app.yaml", ErrFailed)
	// ErrAlreadyExists: a release of the same name is already installed.
	ErrAlreadyExists = fmt.Errorf("%w: app already installed", ErrFailed)
	// ErrExtraction: the bundle could not be extracted onto the board's disk.
	ErrExtraction = fmt.Errorf("%w: extraction failed", ErrFailed)
	// ErrResourceNoSpace: the App is installed, but there is no disk space left for
	// the containers and models it needs to run. The release stays on the board.
	ErrResourceNoSpace = fmt.Errorf("%w: no disk space for the app resources", ErrFailed)
	// ErrResourceDownload: the App is installed, but downloading the containers and
	// models it needs to run failed. The release stays on the board.
	ErrResourceDownload = fmt.Errorf("%w: app resources download failed", ErrFailed)

	// Start verdicts (RFC-16 §5).

	// ErrNotPrepared: a container or model the App needs is not on the board.
	ErrNotPrepared = fmt.Errorf("%w: app not prepared", ErrFailed)
	// ErrAnotherAppRunning: another App is running or starting.
	ErrAnotherAppRunning = fmt.Errorf("%w: another app is running", ErrFailed)
)

// prefixes are the error prefixes arduino-app-cli puts in front of an error message
// to say which failure it is (RFC-14 §6.1 and RFC-16 §5, "Error prefix proposal"):
// the codes its events carry are too coarse — INTERNAL_SERVER_ERROR alone covers most
// of the failures. The message reads "<prefix>: <text>".
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
	"AnotherAppRunning":  ErrAnotherAppRunning,
	"AdditionalResourceDownloadErrorNoDiskSpace": ErrResourceNoSpace,
	"AdditionalResourceDownloadError":            ErrResourceDownload,
}

// codes are the event error codes that are a verdict on their own, with no prefix
// needed.
var codes = map[string]error{
	"app_already_exists": ErrAlreadyExists,
	"not_prepared":       ErrNotPrepared,
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

// streamError classifies an error event of an operation's stream.
func streamError(code, msg string) error {
	if sentinel, ok := prefixed(msg); ok {
		return fmt.Errorf("%w: %s", sentinel, msg)
	}
	if sentinel, ok := codes[code]; ok {
		return fmt.Errorf("%w: %s", sentinel, msg)
	}
	// bad_request, INTERNAL_SERVER_ERROR or anything new, with no prefix to say more.
	return fmt.Errorf("%w: %s (%s)", ErrFailed, msg, code)
}

// maxErrorBody bounds how much of a non-200 response is read for its error.
const maxErrorBody = 4 << 10

// statusError classifies a response that is not the event stream: arduino-app-cli
// refused the request before the operation began, with {"details": "..."}.
//
// unprefixed5xx is what a server error with no prefix means, which differs by
// endpoint.
func statusError(resp *http.Response, unprefixed5xx error) error {
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
		// No such endpoint: an arduino-app-cli too old for the operation, which
		// RFC-14 §5.10 files under -44.
		return fmt.Errorf("%w: no such endpoint, %s", ErrNotCompatible, detail)
	case resp.StatusCode >= http.StatusInternalServerError:
		return fmt.Errorf("%w: %s", unprefixed5xx, detail)
	default:
		return fmt.Errorf("%w: %s", ErrFailed, detail)
	}
}
