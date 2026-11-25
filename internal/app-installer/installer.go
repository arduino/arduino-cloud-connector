// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package appinstaller hands a verified App bundle to whatever installs it on the
// board, and follows the install until it succeeds or fails.
//
// The split of responsibilities is the point of the package: internal/ota owns the
// deploy protocol — the state machine, the Cloud messages and the wire error codes —
// while the installing service sits behind the Installer interface. The production
// implementation is AppCLI, which installs through the arduino-app-cli client in
// internal/app-cli (RFC-14 §5.4, §6.1): one request that uploads the bundle and
// starts the install, answered by a stream that reports it.
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
// then follows it, which means holding a Server-Sent Events stream open for minutes.
// arduino-app-cli offers no way to re-attach to an install whose stream dropped, so a
// dropped stream is a failed install (appcli.ErrUnavailable), not something to retry.
//
// None of that shows from the outside. Install blocks until the install is finished,
// exactly like Download does. The contract that makes that safe is documented on
// Installer and every implementation has to honour it.
package appinstaller

import (
	"context"
	"fmt"
	"log/slog"

	appcli "github.com/arduino/arduino-cloud-connector/internal/app-cli"
)

// Installer installs a downloaded App bundle on the board.
type Installer interface {
	// Install hands req over and returns when the install has finished, has failed,
	// or ctx has been cancelled.
	//
	// onProgress reports an install percentage (0–100) and must be called
	// monotonically. It may repeat a value, and an implementation should call it on
	// every progress report the service makes: the caller forwards each call to the
	// Cloud, and counts it as a sign of life for its stall watchdog.
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
	// Errors must match one of the internal/app-cli sentinels through errors.Is, so
	// the caller can classify the outcome without knowing anything about the
	// transport — and must be as specific as the service allows, because each
	// sentinel reaches the operator as a different Cloud error code. A context
	// cancellation is returned as-is.
	Install(ctx context.Context, req Request, onProgress ProgressFunc) error
}

// Request describes the bundle to install. The daemon never opens the archive, so
// path-traversal and zip-bomb defence stay entirely inside arduino-app-cli, which
// already implements both (RFC-14 §5.9).
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
	BundlePath string
	// SHA256 is the hex digest the archive was verified against. arduino-app-cli's
	// install endpoint takes no digest today, so AppCLI does not send it.
	SHA256 string
}

// ProgressFunc reports install progress as a percentage (0–100). It must not block.
type ProgressFunc func(percent int32)

// Func adapts a function to Installer, for tests and for wiring a trivial
// implementation. The concurrency contract on Installer.Install applies to it too.
type Func func(ctx context.Context, req Request, onProgress ProgressFunc) error

func (f Func) Install(ctx context.Context, req Request, onProgress ProgressFunc) error {
	return f(ctx, req, onProgress)
}

// Unavailable fails every install with ErrUnavailable, which the Cloud renders as a
// failed deploy — the honest outcome for a process wired without an installer, and
// far better than reporting success for an App that was downloaded but never
// installed.
func Unavailable() Installer {
	return Func(func(_ context.Context, req Request, _ ProgressFunc) error {
		slog.Error("appinstaller: no installer wired — bundle downloaded but cannot be installed",
			"bundle", req.BundlePath,
			"detail", "no arduino-app-cli client configured")
		return fmt.Errorf("%w: no installer configured", appcli.ErrUnavailable)
	})
}

// AppCLI installs through arduino-app-cli. Its Install reads the install stream on
// the caller's goroutine, so the concurrency contract on Installer holds by
// construction.
func AppCLI(c *appcli.Client) Installer {
	return Func(func(ctx context.Context, req Request, onProgress ProgressFunc) error {
		return c.Install(ctx, req.BundlePath, appcli.ProgressFunc(onProgress))
	})
}
