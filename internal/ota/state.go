// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package ota

import (
	"errors"

	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
	"github.com/arduino/arduino-cloud-connector/internal/downloader"
	storageapi "github.com/arduino/arduino-cloud-connector/internal/storage-api"
)

// State is one state of the OTA process. The numeric values are wire values:
// they are published to Arduino IoT Cloud in OTAProgressCmd.State and the Cloud
// UI renders the deploy phase from them, so they MUST stay identical to
// OTACloudProcessInterface::State in the C++ ArduinoIoTCloud library
// (src/ota/interface/OTAInterface.h) — that is what the existing firmware-OTA
// pipeline already emits, and reusing it verbatim is why no cloud-side change is
// needed (RFC-14 §5.1).
type State int16

const (
	// StateResume asks: did a previous run leave a job unfinished? Entered once
	// at process start. Never reported to the cloud.
	StateResume State = 0
	// StateOtaBegin is never entered by the App-deploy path, and is kept only so
	// this enum stays a faithful mirror of the C++ one.
	//
	// On an MCU it is a state: the process passes through it on every connection to
	// publish OTABeginCmd with the installed firmware digest. Here OTABeginCmd is
	// published once, straight after a successful install (announceInstalled), and
	// never at startup — a Linux board can hold and run several Apps, so there is no
	// single installed digest to announce. See the package comment.
	StateOtaBegin State = 1
	// StateIdle waits for an OTAUpdateCmd. Never reported to the cloud.
	StateIdle State = 2
	// StateOtaAvailable means a job was received and validated.
	StateOtaAvailable State = 3
	// StateStartOTA means the free-space check passed and the HTTPS request is
	// being issued.
	StateStartOTA State = 4
	// StateFetch means the bundle is downloading; state_data carries the byte
	// count, republished every progressInterval.
	StateFetch State = 5
	// StateFlashOTA means arduino-app-cli is installing; state_data carries a
	// percentage.
	StateFlashOTA State = 6
	// StateReboot exists only to keep the numbering aligned with the C++ enum.
	// An App deploy never reboots the board: on success the process confirms the
	// install to the Cloud and returns to Idle (RFC-14 §5.1).
	StateReboot State = 7
	// StateFail reports a failure; state_data carries a negative Error code.
	StateFail State = 8
)

// String returns the state name used in logs. The names match the C++
// STATE_NAMES table so daemon logs and firmware-OTA logs read the same.
func (s State) String() string {
	switch s {
	case StateResume:
		return "Resume"
	case StateOtaBegin:
		return "OtaBegin"
	case StateIdle:
		return "Idle"
	case StateOtaAvailable:
		return "OtaAvailable"
	case StateStartOTA:
		return "StartOTA"
	case StateFetch:
		return "Fetch"
	case StateFlashOTA:
		return "FlashOTA"
	case StateReboot:
		return "Reboot"
	case StateFail:
		return "Fail"
	default:
		return "Unknown"
	}
}

// reportable tells whether entering this state produces an OTAProgressCmd.
// Mirrors the C++ dispatcher, which reports only from OtaAvailable upwards
// (OTAInterface.cpp: `state >= OtaAvailable || state < 0`): Resume, OtaBegin and
// Idle are local bookkeeping the cloud job has no phase for.
func (s State) reportable() bool { return s >= StateOtaAvailable }

// Error is the negative code published in OTAProgressCmd.StateData when the
// process enters StateFail. The Cloud renders a failure reason from it.
//
// Codes -1…-25 are the firmware-OTA codes from the C++ library
// (src/ota/OTATypes.h, ota::OTAError) and keep their exact meaning, so the
// existing Cloud-side mapping applies unchanged. Codes from -100 down are new
// and specific to an App deploy — the Cloud has no text for them yet (see the
// open point in the session notes).
type Error int32

const (
	ErrNone Error = 0

	// ── Reused firmware-OTA codes (C++ ota::OTAError) ────────────────────────

	// ErrNoOtaStorage: not enough free space in the download directory.
	ErrNoOtaStorage Error = -2
	// ErrDigestMismatch: the downloaded bytes do not hash to the expected
	// digest. Reuses the C++ CRC-mismatch code, whose Cloud-side meaning
	// ("corrupted download") is exactly right; the algorithm differs (SHA-256
	// here, CRC-32 on MCUs) but the operator-visible cause does not.
	ErrDigestMismatch Error = -6
	// ErrURLParse: the URL in OTAUpdateCmd is malformed or not https.
	ErrURLParse Error = -9
	// ErrServerConnect: could not establish the TLS connection to storage.
	ErrServerConnect Error = -10
	// ErrHTTPHeader: the response is missing or contradicts the headers the
	// download needs (no Content-Length, bad Content-Range).
	ErrHTTPHeader Error = -11
	// ErrDownload: the transfer failed or was truncated after all retries.
	ErrDownload Error = -12
	// ErrHTTPResponse: unexpected HTTP status.
	ErrHTTPResponse Error = -14
	// ErrOpenFile: could not create/open the bundle file or its journal.
	ErrOpenFile Error = -19
	// ErrWriteFile: a write to the bundle file failed.
	ErrWriteFile Error = -20

	// ── App-deploy specific codes ────────────────────────────────────────────

	// ErrBundleTooLarge: the advertised bundle size exceeds MaxBundleSize.
	ErrBundleTooLarge Error = -100
	// -101 is retired. It was ErrHostNotAllowed, raised when the download URL's
	// host did not match a configured storage host. That check is gone: the cloud
	// picks the URL and the C++ reference consumes it verbatim, so pinning a host
	// only ever broke deploys the MCUs completed fine. The number is left unused
	// rather than reassigned, in case the cloud already maps it.
	//
	// ErrDownloadTimeout: DownloadTimeout elapsed before the transfer finished.
	ErrDownloadTimeout Error = -102
	// ErrInstallerUnavailable: arduino-app-cli could not be reached.
	ErrInstallerUnavailable Error = -103
	// ErrInstallFailed: arduino-app-cli reported the install as failed.
	ErrInstallFailed Error = -104
	// ErrInstallTimeout: the install SSE stream went silent for longer than
	// InstallTimeout.
	ErrInstallTimeout Error = -105
	// ErrInternal: an unclassified local failure.
	ErrInternal Error = -106
)

func (e Error) String() string {
	switch e {
	case ErrNone:
		return "None"
	case ErrNoOtaStorage:
		return "NoOtaStorage"
	case ErrDigestMismatch:
		return "DigestMismatch"
	case ErrURLParse:
		return "UrlParseError"
	case ErrServerConnect:
		return "ServerConnectError"
	case ErrHTTPHeader:
		return "HttpHeaderError"
	case ErrDownload:
		return "OtaDownload"
	case ErrHTTPResponse:
		return "HttpResponse"
	case ErrOpenFile:
		return "ErrorOpenUpdateFile"
	case ErrWriteFile:
		return "ErrorWriteUpdateFile"
	case ErrBundleTooLarge:
		return "BundleTooLarge"
	case ErrDownloadTimeout:
		return "DownloadTimeout"
	case ErrInstallerUnavailable:
		return "InstallerUnavailable"
	case ErrInstallFailed:
		return "InstallFailed"
	case ErrInstallTimeout:
		return "InstallTimeout"
	case ErrInternal:
		return "Internal"
	default:
		return "Unknown"
	}
}

// ── Go errors → wire codes ───────────────────────────────────────────────────

// errInstallStalled is the one failure this package decides for itself: the install
// went silent for longer than InstallTimeout and the stall watchdog cancelled it.
//
// It is a sentinel, like downloader's and app-installer's, so decodeError classifies
// every failure the same way regardless of which layer produced it. Unlike theirs it
// is unexported, because nothing outside this package raises or reads it.
var errInstallStalled = errors.New("ota: install produced no progress")

// decodeError is the single place where a failure from a lower layer becomes an
// Arduino IoT Cloud error code.
//
// The split is deliberate: each layer below describes what went wrong in its own
// terms and knows nothing about the OTA protocol, while the wire values — copied from
// the C++ ota::OTAError so the existing Cloud-side mapping applies unchanged — live
// here, with the rest of the protocol. Anything unrecognised becomes ErrInternal
// rather than silently reporting success.
//
// Three packages are matched because three layers can fail, and which one did is
// informative: storage-api reports what the network and the storage service said,
// downloader reports what happened to the bytes on this disk, and app-installer
// reports whether the handover to arduino-app-cli started at all and how it ended.
func decodeError(err error) Error {
	switch {
	// Network / storage service.
	case errors.Is(err, storageapi.ErrURLInvalid):
		return ErrURLParse
	case errors.Is(err, storageapi.ErrConnect):
		return ErrServerConnect
	case errors.Is(err, storageapi.ErrBadResponse):
		return ErrHTTPResponse
	case errors.Is(err, storageapi.ErrBadHeaders):
		return ErrHTTPHeader

	// Transfer / local disk.
	case errors.Is(err, downloader.ErrTooLarge):
		return ErrBundleTooLarge
	case errors.Is(err, downloader.ErrNoSpace):
		return ErrNoOtaStorage
	case errors.Is(err, downloader.ErrBadSize):
		// A size the transfer cannot reconcile is the same class of problem as a
		// contradictory header, so it reuses that code rather than introducing one the
		// Cloud has never been told about.
		return ErrHTTPHeader
	case errors.Is(err, downloader.ErrTransfer):
		return ErrDownload
	case errors.Is(err, downloader.ErrDigestMismatch):
		return ErrDigestMismatch
	case errors.Is(err, downloader.ErrTimeout):
		return ErrDownloadTimeout
	case errors.Is(err, downloader.ErrOpenFile):
		return ErrOpenFile
	case errors.Is(err, downloader.ErrWriteFile):
		return ErrWriteFile

	// Handover to the installing service.
	case errors.Is(err, appinstaller.ErrUnavailable):
		return ErrInstallerUnavailable
	case errors.Is(err, appinstaller.ErrFailed):
		return ErrInstallFailed

	// Raised here.
	case errors.Is(err, errInstallStalled):
		return ErrInstallTimeout

	default:
		return ErrInternal
	}
}
