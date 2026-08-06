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
// The set is RFC-14 §5.10 in full, and it has two halves.
//
// Codes -2…-20 are firmware-OTA codes borrowed from the C++ library
// (src/ota/OTATypes.h, ota::OTAError) and keep their exact numeric value, so the
// Cloud's existing mapping applies unchanged. What they are *called* there is often
// misleading, and RFC-14 reuses them for what they actually mean at their emission
// site rather than for their name: -6 OtaHeaderCrc is the integrity check, -13
// OtaHeaderTimeout is the download timeout, -11 HttpHeaderError is specifically a
// missing Content-Length. The names below are the C++ ones, kept so a daemon log line
// and the Cloud UI say the same word about the same failure; the doc comment says what
// it means here. The Cloud renders these ten with App-deploy wording chosen per job
// type — a non-writable download dir must not reach the operator as "No OTA storage".
//
// Codes -40…-50 are new and specific to an App deploy. -26…-39 are deliberately left
// free: ota::OTAError already reaches -25, the same team owns both enums and both grow
// at the tail, so the App range starts far enough away that neither can walk into the
// other.
type Error int32

const (
	ErrNone Error = 0

	// ── Reused firmware-OTA codes (C++ ota::OTAError) ────────────────────────

	// ErrNoOtaStorage: the download directory is unusable — missing, not a
	// directory, or not writable. This is what a failure to create or open the
	// bundle file means in practice: on a healthy board there is no other reason
	// for it, and "out of space" has its own code (ErrNoDiskSpace).
	ErrNoOtaStorage Error = -2
	// ErrSizeMismatch: the bytes received do not add up to the advertised
	// Content-Length — checked per ranged chunk and over the whole transfer.
	ErrSizeMismatch Error = -5
	// ErrDigestMismatch: the downloaded bytes do not hash to the digest carried in
	// OTAUpdateCmd. The C++ name says CRC and the algorithm differs (SHA-256 here),
	// but the code IS the integrity check and the operator-visible cause — a
	// corrupted download — is the same.
	ErrDigestMismatch Error = -6
	// ErrURLParse: the URL in OTAUpdateCmd is malformed or not https.
	ErrURLParse Error = -9
	// ErrServerConnect: could not establish the connection to the storage service,
	// including a failed mTLS handshake.
	ErrServerConnect Error = -10
	// ErrHTTPHeader: the response does not reveal the artefact size (no
	// Content-Length), so neither the free-space check nor the byte accounting can
	// proceed.
	ErrHTTPHeader Error = -11
	// ErrDownload: generic/terminal failure of the bundle download, and the
	// fallback for anything unclassified in that phase.
	ErrDownload Error = -12
	// ErrDownloadTimeout: DownloadTimeout elapsed before the transfer finished.
	ErrDownloadTimeout Error = -13
	// ErrHTTPResponse: unexpected HTTP status (≠ 200/206).
	ErrHTTPResponse Error = -14
	// ErrWriteFile: I/O error writing the bundle to disk. Out of space is reported
	// as ErrNoDiskSpace, not here.
	ErrWriteFile Error = -20

	// ── App-deploy specific codes ────────────────────────────────────────────

	// ErrNoDiskSpace: not enough free disk space for the bundle.
	ErrNoDiskSpace Error = -40
	// ErrInstallerUnavailable: arduino-app-cli cannot be reached, or its stream
	// closed without emitting a terminal event.
	ErrInstallerUnavailable Error = -41
	// ErrArchiveRejected: arduino-app-cli rejected the archive as invalid. The
	// digest already matched, so the bundle is malformed at origin rather than
	// corrupted in transit.
	ErrArchiveRejected Error = -42
	// ErrInvalidAppYaml: arduino-app-cli found app.yaml missing, unparsable or
	// failing validation.
	ErrInvalidAppYaml Error = -43
	// ErrAppNotCompatible: the App does not fit this board — the installed bricks
	// version, the hardware, or an arduino-app-cli too old to deploy it.
	ErrAppNotCompatible Error = -44
	// ErrBundleTooLarge: the advertised bundle size exceeds MaxBundleSize.
	ErrBundleTooLarge Error = -45
	// ErrInstallFailed: arduino-app-cli reported a terminal install failure, and
	// the fallback for anything unclassified in the install phase.
	ErrInstallFailed Error = -46
	// ErrInstallTimeout: no event from arduino-app-cli for InstallTimeout. The
	// watchdog spans the install and the wait for the running verdict, and every
	// event received resets it.
	ErrInstallTimeout Error = -47
	// ErrAppRunFailed: arduino-app-cli reports the App does not stay up. A deploy
	// succeeds only if the App ends up running, so this fails the job whatever the
	// board does next.
	ErrAppRunFailed Error = -48
	// ErrDeployInterrupted: a daemon restart (crash, upgrade or reboot) interrupted
	// the deploy and it could not be resumed automatically.
	ErrDeployInterrupted Error = -49
	// ErrDeployInProgress: a second job arrived while one was already in flight.
	ErrDeployInProgress Error = -50
)

// String returns the RFC-14 §5.10 name of the code, which is also the C++
// ota::OTAError name for the reused half. Logs and the Cloud UI then name a failure
// the same way, which is the point of reusing the numbers at all.
func (e Error) String() string {
	switch e {
	case ErrNone:
		return "None"
	case ErrNoOtaStorage:
		return "NoOtaStorage"
	case ErrSizeMismatch:
		return "OtaHeaderLength"
	case ErrDigestMismatch:
		return "OtaHeaderCrc"
	case ErrURLParse:
		return "UrlParseError"
	case ErrServerConnect:
		return "ServerConnectError"
	case ErrHTTPHeader:
		return "HttpHeaderError"
	case ErrDownload:
		return "OtaDownload"
	case ErrDownloadTimeout:
		return "OtaHeaderTimeout"
	case ErrHTTPResponse:
		return "HttpResponse"
	case ErrWriteFile:
		return "ErrorWriteUpdateFile"
	case ErrNoDiskSpace:
		return "NoDiskSpace"
	case ErrInstallerUnavailable:
		return "AppCLIUnreachable"
	case ErrArchiveRejected:
		return "AppArchiveRejected"
	case ErrInvalidAppYaml:
		return "AppInvalidYaml"
	case ErrAppNotCompatible:
		return "AppNotCompatible"
	case ErrBundleTooLarge:
		return "AppBundleTooLarge"
	case ErrInstallFailed:
		return "AppInstallationFailed"
	case ErrInstallTimeout:
		return "AppInstallationTimeout"
	case ErrAppRunFailed:
		return "AppRunFailed"
	case ErrDeployInterrupted:
		return "DeployInterrupted"
	case ErrDeployInProgress:
		return "DeployAlreadyInProgress"
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
// terms and knows nothing about the OTA protocol, while the wire values — RFC-14
// §5.10, half of them copied from the C++ ota::OTAError so the existing Cloud-side
// mapping applies unchanged — live here, with the rest of the protocol.
//
// Three packages are matched because three layers can fail, and which one did is
// informative: storage-api reports what the network and the storage service said,
// downloader reports what happened to the bytes on this disk, and app-installer
// reports whether the handover to arduino-app-cli started at all and how it ended.
//
// fallback is what an unrecognised error becomes, and it is the caller's to choose
// because the honest answer depends on where the deploy was when it failed: RFC-14
// §5.10 collapses an unmapped download failure to ErrDownload and an unmapped install
// failure to ErrInstallFailed. There is deliberately no catch-all "internal error"
// code — which phase died is something the operator can act on, "something went wrong"
// is not.
func decodeError(err error, fallback Error) Error {
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
		return ErrNoDiskSpace
	case errors.Is(err, downloader.ErrBadSize):
		return ErrSizeMismatch
	case errors.Is(err, downloader.ErrTransfer):
		return ErrDownload
	case errors.Is(err, downloader.ErrDigestMismatch):
		return ErrDigestMismatch
	case errors.Is(err, downloader.ErrTimeout):
		return ErrDownloadTimeout
	case errors.Is(err, downloader.ErrOpenFile):
		// Not its own code: RFC-14 §5.10 has no slot for "could not open the
		// destination", and it does not need one. The destination and its sidecars
		// live in the download dir, so failing to create or open them means that
		// directory is missing or not writable — which is exactly ErrNoOtaStorage.
		return ErrNoOtaStorage
	case errors.Is(err, downloader.ErrWriteFile):
		return ErrWriteFile

	// Handover to the installing service. The last four are verdicts only
	// arduino-app-cli can give, and they exist so that "app-cli said no" reaches the
	// operator as a reason rather than as a generic install failure.
	case errors.Is(err, appinstaller.ErrUnavailable):
		return ErrInstallerUnavailable
	case errors.Is(err, appinstaller.ErrArchiveRejected):
		return ErrArchiveRejected
	case errors.Is(err, appinstaller.ErrInvalidAppYaml):
		return ErrInvalidAppYaml
	case errors.Is(err, appinstaller.ErrNotCompatible):
		return ErrAppNotCompatible
	case errors.Is(err, appinstaller.ErrRunFailed):
		return ErrAppRunFailed
	case errors.Is(err, appinstaller.ErrFailed):
		return ErrInstallFailed

	// Raised here.
	case errors.Is(err, errInstallStalled):
		return ErrInstallTimeout

	default:
		return fallback
	}
}
