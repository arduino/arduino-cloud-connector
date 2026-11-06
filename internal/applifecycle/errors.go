// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package applifecycle

import (
	"errors"

	appcli "github.com/arduino/arduino-cloud-connector/internal/app-cli"
)

// Error is the negative code a failed command reports in AppCommandResponseCmd's
// details (RFC-16 §3, "Error codes"). It is a code space of its own, not the OTA
// one, although the shared meanings keep the same numbers (-40, -41, -44, -49).
type Error int32

const (
	// ErrAppNotPrepared: a container or model the App needs is not on the board
	// (start).
	ErrAppNotPrepared Error = -1
	// ErrAnotherAppRunning: another App is running and the command asked not to stop
	// it (start, parameter 0).
	ErrAnotherAppRunning Error = -2
	// ErrGenericFailure: any other failure, and the fallback for anything
	// unclassified — including a command that went silent past the watchdog, for
	// which RFC-16 has no code of its own.
	ErrGenericFailure Error = -3
	// ErrNoDiskSpace: no disk space for what the App needs (start).
	ErrNoDiskSpace Error = -40
	// ErrAppCLIUnreachable: arduino-app-cli cannot be reached, or its stream ended
	// before the command's outcome.
	ErrAppCLIUnreachable Error = -41
	// ErrAppNotCompatible: the App does not fit this board (start).
	ErrAppNotCompatible Error = -44
	// ErrAnotherCommandInProgress: the board is already carrying out a command — a
	// lifecycle command or an App deploy.
	ErrAnotherCommandInProgress Error = -49
)

// String returns the RFC-16 name of the code.
func (e Error) String() string {
	switch e {
	case ErrAppNotPrepared:
		return "AppNotPrepared"
	case ErrAnotherAppRunning:
		return "AnotherAppRunning"
	case ErrGenericFailure:
		return "GenericFailure"
	case ErrNoDiskSpace:
		return "NoDiskSpace"
	case ErrAppCLIUnreachable:
		return "AppCLINotReachable"
	case ErrAppNotCompatible:
		return "AppNotCompatible"
	case ErrAnotherCommandInProgress:
		return "AnotherCommandInProgress"
	default:
		return "Unknown"
	}
}

// decodeError is the single place where a failure becomes an RFC-16 code.
func decodeError(err error) Error {
	switch {
	case errors.Is(err, appcli.ErrUnavailable):
		return ErrAppCLIUnreachable
	case errors.Is(err, appcli.ErrNotPrepared):
		return ErrAppNotPrepared
	case errors.Is(err, appcli.ErrAnotherAppRunning):
		return ErrAnotherAppRunning
	case errors.Is(err, appcli.ErrNoSpace):
		return ErrNoDiskSpace
	case errors.Is(err, appcli.ErrNotCompatible):
		return ErrAppNotCompatible
	default:
		return ErrGenericFailure
	}
}
