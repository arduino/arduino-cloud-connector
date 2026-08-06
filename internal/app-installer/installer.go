// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package appinstaller hands a verified App bundle to whatever installs it on the
// board, and follows the install until it succeeds or fails.
//
// The split of responsibilities is the point of the package: internal/ota owns the
// deploy protocol — the state machine, the Cloud messages and the wire error codes —
// while everything about talking to the installing service lives here. The
// production implementation is an arduino-app-cli client (RFC-14 §5.4/§5.5): a POST
// that starts the install, and a stream that reports it. That endpoint does not
// exist yet, so for now the only implementations are Unavailable and test doubles.
//
// It has no notion of an OTA job, a state machine or a wire code. Callers hand it a
// path and a digest; it hands back progress and a typed error. Mapping those errors
// onto anything user-visible (for the App-deploy flow, the OTAProgressCmd codes) is
// the caller's job — see internal/ota.
//
// # An install is asynchronous, like a download
//
// Install is not a plain request/response call, and the shape of the real
// implementation is worth stating up front: it starts a job in another process and
// then follows it, which means holding a Server-Sent Events stream open for minutes,
// and very likely re-subscribing to an install already in progress when that stream
// drops. That is the same class of problem internal/downloader solves for the
// transfer, and the reason this is a package rather than a function on the state
// machine: reconnection and idempotency are real code with state of their own.
//
// None of that shows from the outside. Install blocks until the install is finished,
// exactly like Download does. The contract that makes that safe is documented on
// Installer and every implementation has to honour it.
package appinstaller

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Installer installs a downloaded App bundle on the board.
type Installer interface {
	// Install hands req over and returns when the install has finished, has failed,
	// or ctx has been cancelled.
	//
	// onProgress reports an install percentage (0–100) and must be called
	// monotonically: the caller forwards each value straight to the Cloud, which
	// renders it as deploy progress.
	//
	// # Concurrency contract — required, not advisory
	//
	// An implementation may use goroutines internally, and the SSE client will have
	// to, but:
	//
	//   - onProgress must be called ONLY from the goroutine that called Install.
	//     Feed values from an internal goroutine through a channel and pump the
	//     callback from Install's own frame.
	//   - Install must not return while a goroutine it started can still call
	//     onProgress, and must not call onProgress once ctx is done.
	//
	// This is not a style preference. The caller (internal/ota) runs its state
	// machine on a single goroutine and touches its job and reporting state without
	// locks, so a callback arriving on an unexpected goroutine — or arriving after
	// the state machine has moved on — is a data race, and one that would surface
	// only under a real install.
	//
	// Errors must match one of the sentinels below through errors.Is, so the caller
	// can classify the outcome without knowing anything about the transport — and
	// must be as specific as the service allows, because each sentinel reaches the
	// operator as a different Cloud error code. A context cancellation is returned
	// as-is.
	Install(ctx context.Context, req Request, onProgress ProgressFunc) error
}

// Request is the handover payload for arduino-app-cli's POST /v1/apps/deploy
// (RFC-14 §5.4). Only a path is handed over — the daemon never opens the archive,
// so path-traversal and zip-bomb defence stay entirely inside arduino-app-cli,
// which already implements both (RFC-14 §5.9).
//
// # Why there is no App identity in here
//
// The RFC draft originally also carried cloud_app_id (an idempotency key) and name.
// Neither is available to the device: the OTA command channel delivers only
// (id, url, initialSha, finalSha) — verified against the C++ OtaUpdateCmdDown, whose
// schema is shared with the MCU firmware-OTA path and is the thing RFC-14 reuses
// unchanged. Deriving cloud_app_id from the storage URL's key layout was tried and
// dropped: it coupled the daemon to an S3 path shape it has no business knowing. The
// App identity lives inside the bundle, where arduino-app-cli — which unpacks it
// anyway — can read it directly.
type Request struct {
	// BundlePath is the absolute path of the verified archive.
	BundlePath string `json:"bundle_path"`
	// SHA256 is the hex digest arduino-app-cli re-verifies before unpacking.
	SHA256 string `json:"sha256"`
}

// ProgressFunc reports install progress as a percentage (0–100). It must not block.
type ProgressFunc func(percent int32)

// Failure sentinels. Every failure this package reports is one of these: either no
// installer could be reached at all, or the install did not end with the App running.
//
// # Why the failure is typed rather than just "it failed"
//
// Each of these reaches the operator as a different Arduino IoT Cloud error code
// (RFC-14 §5.10 -41…-48, mapped in internal/ota.decodeError), and that is the whole
// reason the distinctions exist: "the archive is malformed", "app.yaml does not
// validate", "this App does not fit this board" and "it installed but does not stay
// up" are four different things for the person looking at a failed deploy, and only
// arduino-app-cli is in a position to tell them apart.
//
// That makes them a requirement on the deploy stream, not just a Go detail: every
// stream must end in exactly one terminal outcome — running (success), or one of the
// specific failures below. If the handover degrades to "non-zero exit", four codes
// collapse back into ErrFailed and the operator is told nothing useful.
//
// The four specific verdicts wrap ErrFailed, so a caller that only wants to know
// whether the install succeeded still matches with errors.Is(err, ErrFailed) — which
// also makes ErrFailed the right fallback for an install failure nobody classified.
var (
	// ErrUnavailable: the installing service could not be reached, so the install
	// never started — or its stream closed without a terminal event, which is the
	// same thing seen from the other end. The bundle on disk is untouched and still
	// valid.
	ErrUnavailable = errors.New("appinstaller: no app installer available")
	// ErrFailed: the install did not succeed and the reason is not one of the four
	// below.
	ErrFailed = errors.New("appinstaller: install failed")

	// ErrArchiveRejected: the archive is not a valid bundle. The digest matched
	// before the handover, so it is malformed at origin, not corrupted in transit.
	ErrArchiveRejected = fmt.Errorf("%w: archive rejected", ErrFailed)
	// ErrInvalidAppYaml: app.yaml is missing, unparsable, or fails validation.
	ErrInvalidAppYaml = fmt.Errorf("%w: invalid app.yaml", ErrFailed)
	// ErrNotCompatible: the App does not fit this board — the installed bricks
	// version, the hardware, or a runtime too old to deploy it.
	ErrNotCompatible = fmt.Errorf("%w: app not compatible with this board", ErrFailed)
	// ErrRunFailed: the App was installed but does not stay up. A deploy succeeds
	// only if the App ends up running, so this is a failure however the board
	// recovers afterwards.
	ErrRunFailed = fmt.Errorf("%w: app does not stay running", ErrFailed)
)

// Func adapts a function to Installer, for tests and for wiring a trivial
// implementation. The concurrency contract on Installer.Install applies to it too.
type Func func(ctx context.Context, req Request, onProgress ProgressFunc) error

func (f Func) Install(ctx context.Context, req Request, onProgress ProgressFunc) error {
	return f(ctx, req, onProgress)
}

// Unavailable is the placeholder used until the arduino-app-cli endpoint ships. It
// fails every install with ErrUnavailable, which the Cloud renders as a failed
// deploy — the honest outcome, and far better than reporting success for an App that
// was downloaded but never installed.
func Unavailable() Installer {
	return Func(func(_ context.Context, req Request, _ ProgressFunc) error {
		slog.Error("appinstaller: no installer wired — bundle downloaded but cannot be installed",
			"bundle", req.BundlePath,
			"detail", "arduino-app-cli POST /v1/apps/deploy (RFC-14 §5.4) is not implemented yet")
		return fmt.Errorf("%w: arduino-app-cli deploy endpoint not implemented yet", ErrUnavailable)
	})
}
