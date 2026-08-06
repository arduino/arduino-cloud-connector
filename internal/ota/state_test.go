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

// everySentinel is every failure decodeError is expected to recognise, from all three
// layers below plus this package's own.
var everySentinel = []error{
	storageapi.ErrURLInvalid, storageapi.ErrConnect,
	storageapi.ErrBadResponse, storageapi.ErrBadHeaders,
	downloader.ErrTooLarge, downloader.ErrNoSpace, downloader.ErrBadSize,
	downloader.ErrTransfer, downloader.ErrDigestMismatch, downloader.ErrTimeout,
	downloader.ErrOpenFile, downloader.ErrWriteFile,
	appinstaller.ErrUnavailable, appinstaller.ErrFailed,
	appinstaller.ErrArchiveRejected, appinstaller.ErrInvalidAppYaml,
	appinstaller.ErrNotCompatible, appinstaller.ErrRunFailed,
	errInstallStalled,
}

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
		{"no space", downloader.ErrNoSpace, ErrNoDiskSpace},
		{"bad size", downloader.ErrBadSize, ErrSizeMismatch},
		{"transfer", downloader.ErrTransfer, ErrDownload},
		{"digest mismatch", downloader.ErrDigestMismatch, ErrDigestMismatch},
		{"timeout", downloader.ErrTimeout, ErrDownloadTimeout},
		// No wire code of its own: the destination and its sidecars live in the
		// download dir, so failing to open them means that directory is unusable.
		{"open file", downloader.ErrOpenFile, ErrNoOtaStorage},
		{"write file", downloader.ErrWriteFile, ErrWriteFile},

		// Handover to the installing service. The four specific verdicts must win over
		// the generic one they wrap — that ordering IS the difference between telling
		// the operator "app.yaml does not validate" and telling them "install failed".
		{"installer unavailable", appinstaller.ErrUnavailable, ErrInstallerUnavailable},
		{"install failed", appinstaller.ErrFailed, ErrInstallFailed},
		{"archive rejected", appinstaller.ErrArchiveRejected, ErrArchiveRejected},
		{"invalid app.yaml", appinstaller.ErrInvalidAppYaml, ErrInvalidAppYaml},
		{"not compatible", appinstaller.ErrNotCompatible, ErrAppNotCompatible},
		{"app does not run", appinstaller.ErrRunFailed, ErrAppRunFailed},

		// Raised here.
		{"install stalled", errInstallStalled, ErrInstallTimeout},
		{"install stalled, wrapped", fmt.Errorf("%w: none for 10m", errInstallStalled), ErrInstallTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// The fallback is deliberately a code none of these should reach: a case
			// that silently fell through to it would otherwise look like a pass.
			if got := decodeError(tc.err, ErrDeployInterrupted); got != tc.want {
				t.Errorf("decodeError(%v): got %s want %s", tc.err, got, tc.want)
			}
		})
	}
}

// An unrecognised failure takes the caller's fallback, and the caller picks it from
// the phase the deploy was in (RFC-14 §5.10). There is no catch-all code: "the
// download failed" and "the install failed" are both actionable, "internal error" is
// not.
func TestDecodeErrorFallsBackToTheCallersPhaseCode(t *testing.T) {
	unclassified := errors.New("something else")
	for _, fallback := range []Error{ErrDownload, ErrInstallFailed} {
		if got := decodeError(unclassified, fallback); got != fallback {
			t.Errorf("unclassified error with fallback %s: got %s", fallback, got)
		}
	}
	// A recognised failure must never be overridden by the fallback.
	if got := decodeError(downloader.ErrDigestMismatch, ErrInstallFailed); got != ErrDigestMismatch {
		t.Errorf("a classified failure took the fallback: got %s", got)
	}
}

// A wrapped error must still map correctly: the FSM sees whatever the layer below
// chose to wrap around the sentinel, and every layer wraps.
func TestDecodeErrorSeesThroughWrapping(t *testing.T) {
	cases := map[string]struct {
		err  error
		want Error
	}{
		"joined": {errors.Join(errors.New("context"), downloader.ErrNoSpace), ErrNoDiskSpace},
		// The shape appinstaller.Unavailable actually returns.
		"formatted": {fmt.Errorf("%w: no endpoint", appinstaller.ErrUnavailable), ErrInstallerUnavailable},
		// The shape a real arduino-app-cli client would return: its own message
		// wrapped around a specific verdict, which itself wraps ErrFailed.
		"specific verdict, wrapped": {
			fmt.Errorf("%w: app.yaml line 3", appinstaller.ErrInvalidAppYaml), ErrInvalidAppYaml,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := decodeError(tc.err, ErrDownload); got != tc.want {
				t.Errorf("decodeError on a wrapped sentinel: got %s want %s", got, tc.want)
			}
		})
	}
}

// Every code decodeError can return must have a name, so logs never print "Unknown"
// for a failure the daemon itself produced.
func TestEveryMappedCodeHasAName(t *testing.T) {
	for _, s := range append(append([]error(nil), everySentinel...), errors.New("unclassified")) {
		code := decodeError(s, ErrDownload)
		if code.String() == "Unknown" {
			t.Errorf("%v mapped to a code with no name (%d)", s, int32(code))
		}
	}
}

// Every sentinel must map to a code of its own. A duplicate would mean two distinct
// failures reaching the operator as the same reason — the exact thing the taxonomy
// exists to prevent, and the way it degrades silently if a new sentinel is added
// without a case in decodeError.
func TestEverySentinelMapsToADistinctCode(t *testing.T) {
	// ErrFailed is the one legitimate overlap: it is both a sentinel and the fallback
	// for the install phase, so it is checked by TestDecodeError instead.
	seen := map[Error]error{}
	for _, s := range everySentinel {
		code := decodeError(s, ErrDeployInterrupted)
		if code == ErrDeployInterrupted {
			t.Errorf("%v is not recognised by decodeError", s)
			continue
		}
		if prev, dup := seen[code]; dup {
			t.Errorf("%v and %v both map to %s (%d)", prev, s, code, int32(code))
		}
		seen[code] = s
	}
}

func TestStateWireValuesMatchTheCppReference(t *testing.T) {
	// These numbers are the contract with the existing Cloud OTA pipeline: if any of
	// them drifts, the Cloud renders the wrong deploy phase. The error codes are
	// pinned separately, in TestErrorCodesMatchRFC14.
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
}

// TestErrorCodesMatchRFC14 transcribes RFC-14 §5.10 — every row, value and name —
// because the table is the contract with the Cloud: the value decides which reason the
// operator is shown, and a drift here is invisible until someone reads a wrong error
// message off a board they cannot reach.
//
// The reused half must additionally keep the numbers of the C++ ota::OTAError, which
// is what lets the existing Cloud-side mapping apply to an App deploy unchanged.
func TestErrorCodesMatchRFC14(t *testing.T) {
	// Reused firmware-OTA codes (C++ src/ota/OTATypes.h).
	reused := map[Error]struct {
		value int32
		name  string
	}{
		ErrNoOtaStorage:    {-2, "NoOtaStorage"},
		ErrSizeMismatch:    {-5, "OtaHeaderLength"},
		ErrDigestMismatch:  {-6, "OtaHeaderCrc"},
		ErrURLParse:        {-9, "UrlParseError"},
		ErrServerConnect:   {-10, "ServerConnectError"},
		ErrHTTPHeader:      {-11, "HttpHeaderError"},
		ErrDownload:        {-12, "OtaDownload"},
		ErrDownloadTimeout: {-13, "OtaHeaderTimeout"},
		ErrHTTPResponse:    {-14, "HttpResponse"},
		ErrWriteFile:       {-20, "ErrorWriteUpdateFile"},
	}
	// App-deploy codes, new in RFC-14.
	appSpecific := map[Error]struct {
		value int32
		name  string
	}{
		ErrNoDiskSpace:          {-40, "NoDiskSpace"},
		ErrInstallerUnavailable: {-41, "AppCLIUnreachable"},
		ErrArchiveRejected:      {-42, "AppArchiveRejected"},
		ErrInvalidAppYaml:       {-43, "AppInvalidYaml"},
		ErrAppNotCompatible:     {-44, "AppNotCompatible"},
		ErrBundleTooLarge:       {-45, "AppBundleTooLarge"},
		ErrInstallFailed:        {-46, "AppInstallationFailed"},
		ErrInstallTimeout:       {-47, "AppInstallationTimeout"},
		ErrAppRunFailed:         {-48, "AppRunFailed"},
		ErrDeployInterrupted:    {-49, "DeployInterrupted"},
		ErrDeployInProgress:     {-50, "DeployAlreadyInProgress"},
	}

	for _, table := range []map[Error]struct {
		value int32
		name  string
	}{reused, appSpecific} {
		for code, want := range table {
			if int32(code) != want.value {
				t.Errorf("%s: got wire value %d want %d", want.name, int32(code), want.value)
			}
			if code.String() != want.name {
				t.Errorf("code %d: got name %q want %q", int32(code), code.String(), want.name)
			}
			// The Cloud tells a failure from progress by the sign of state_data.
			if code >= 0 {
				t.Errorf("%s has non-negative value %d", want.name, int32(code))
			}
		}
	}

	// The App range starts at -40, leaving -26…-39 as a buffer. ota::OTAError already
	// reaches -25 and both enums grow at the tail, so an App code above -40 would
	// eventually collide with a firmware code the Cloud renders differently.
	for code := range appSpecific {
		if code > -40 {
			t.Errorf("app-deploy code %s is %d, inside the range reserved for ota::OTAError growth",
				code, int32(code))
		}
	}
}
