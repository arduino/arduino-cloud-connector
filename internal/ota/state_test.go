// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package ota

import (
	"errors"
	"fmt"
	"testing"

	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
	"github.com/arduino/arduino-cloud-connector/internal/downloader"
	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
)

// TestDecodeError guards the one place where an outcome from a lower layer becomes a
// value the Cloud renders. A wrong mapping shows the operator the wrong reason for a
// failed deploy, which is worse than showing none.
func TestDecodeError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want Error
	}{
		// Network / storage service.
		{"url invalid", storageapi.ErrURLInvalid, ErrURLParse},
		{"connect", storageapi.ErrConnect, ErrServerConnect},
		{"bad response", storageapi.ErrBadResponse, ErrHTTPResponse},
		{"bad headers", storageapi.ErrBadHeaders, ErrHTTPHeader},

		// Transfer / local disk.
		{"too large", downloader.ErrTooLarge, ErrBundleTooLarge},
		{"no space", downloader.ErrNoSpace, ErrNoOtaStorage},
		{"bad size", downloader.ErrBadSize, ErrHTTPHeader},
		{"transfer", downloader.ErrTransfer, ErrDownload},
		{"digest mismatch", downloader.ErrDigestMismatch, ErrDigestMismatch},
		{"timeout", downloader.ErrTimeout, ErrDownloadTimeout},
		{"open file", downloader.ErrOpenFile, ErrOpenFile},
		{"write file", downloader.ErrWriteFile, ErrWriteFile},

		// Handover to the installing service.
		{"installer unavailable", appinstaller.ErrUnavailable, ErrInstallerUnavailable},
		{"install failed", appinstaller.ErrFailed, ErrInstallFailed},

		// Raised here.
		{"install stalled", errInstallStalled, ErrInstallTimeout},
		{"install stalled, wrapped", fmt.Errorf("%w: none for 10m", errInstallStalled), ErrInstallTimeout},

		// Anything unclassified must surface as an internal error, never as success.
		{"unknown", errors.New("something else"), ErrInternal},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeError(tc.err); got != tc.want {
				t.Errorf("decodeError(%v): got %s want %s", tc.err, got, tc.want)
			}
		})
	}
}

// A wrapped error must still map correctly: the FSM sees whatever the layer below
// chose to wrap around the sentinel, and every layer wraps.
func TestDecodeErrorSeesThroughWrapping(t *testing.T) {
	cases := map[string]struct {
		err  error
		want Error
	}{
		"joined": {errors.Join(errors.New("context"), downloader.ErrNoSpace), ErrNoOtaStorage},
		// The shape appinstaller.Unavailable actually returns.
		"formatted": {fmt.Errorf("%w: no endpoint", appinstaller.ErrUnavailable), ErrInstallerUnavailable},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := decodeError(tc.err); got != tc.want {
				t.Errorf("decodeError on a wrapped sentinel: got %s want %s", got, tc.want)
			}
		})
	}
}

// Every code decodeError can return must have a name, so logs never print "Unknown"
// for a failure the daemon itself produced.
func TestEveryMappedCodeHasAName(t *testing.T) {
	sentinels := []error{
		storageapi.ErrURLInvalid, storageapi.ErrConnect,
		storageapi.ErrBadResponse, storageapi.ErrBadHeaders,
		downloader.ErrTooLarge, downloader.ErrNoSpace, downloader.ErrBadSize,
		downloader.ErrTransfer, downloader.ErrDigestMismatch, downloader.ErrTimeout,
		downloader.ErrOpenFile, downloader.ErrWriteFile,
		appinstaller.ErrUnavailable, appinstaller.ErrFailed,
		errInstallStalled,
		errors.New("unclassified"),
	}
	for _, s := range sentinels {
		code := decodeError(s)
		if code.String() == "Unknown" {
			t.Errorf("%v mapped to a code with no name (%d)", s, int32(code))
		}
	}
}

func TestStateAndErrorWireValuesMatchTheCppReference(t *testing.T) {
	// These numbers are the contract with the existing Cloud OTA pipeline: if any of
	// them drifts, the Cloud renders the wrong phase or the wrong reason.
	states := map[State]int16{
		StateResume: 0, StateOtaBegin: 1, StateIdle: 2, StateOtaAvailable: 3,
		StateStartOTA: 4, StateFetch: 5, StateFlashOTA: 6, StateReboot: 7, StateFail: 8,
	}
	for s, want := range states {
		if int16(s) != want {
			t.Errorf("state %s: got wire value %d want %d", s, int16(s), want)
		}
	}
	// Only phases from OtaAvailable upwards are reported, matching the C++
	// dispatcher's `state >= OtaAvailable` guard.
	for s, reportable := range map[State]bool{
		StateResume: false, StateOtaBegin: false, StateIdle: false,
		StateOtaAvailable: true, StateStartOTA: true, StateFetch: true,
		StateFlashOTA: true, StateFail: true,
	} {
		if s.reportable() != reportable {
			t.Errorf("state %s reportable: got %v want %v", s, s.reportable(), reportable)
		}
	}
	// Reused firmware-OTA codes must keep their C++ ota::OTAError values.
	for code, want := range map[Error]int32{
		ErrNoOtaStorage: -2, ErrDigestMismatch: -6, ErrURLParse: -9,
		ErrServerConnect: -10, ErrHTTPHeader: -11, ErrDownload: -12,
		ErrHTTPResponse: -14, ErrOpenFile: -19, ErrWriteFile: -20,
	} {
		if int32(code) != want {
			t.Errorf("error %s: got %d want %d", code, int32(code), want)
		}
	}
	// Every code must be negative, since the Cloud distinguishes failure from
	// progress by sign.
	for _, code := range []Error{
		ErrNoOtaStorage, ErrDigestMismatch, ErrURLParse, ErrServerConnect,
		ErrHTTPHeader, ErrDownload, ErrHTTPResponse, ErrOpenFile, ErrWriteFile,
		ErrBundleTooLarge, ErrDownloadTimeout,
		ErrInstallerUnavailable, ErrInstallFailed, ErrInstallTimeout, ErrInternal,
	} {
		if code >= 0 {
			t.Errorf("error %s has non-negative value %d", code, int32(code))
		}
	}
}
