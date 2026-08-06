// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package ota implements the App-deploy process: the state machine that receives
// an OTA job from Arduino IoT Cloud, downloads an App Lab App bundle, hands it to
// arduino-app-cli for installation, and reports progress and outcome back to the
// Cloud (RFC-14 §5).
//
// # Why it looks like the firmware OTA
//
// Arduino IoT Cloud already runs a complete OTA pipeline for microcontrollers:
// the job lifecycle, the Web UI, the storage service and the CBOR command
// channel all exist. RFC-14's central bet is that only the *artefact* changes, not
// the transport — so this FSM is modelled state-for-state on
// OTACloudProcessInterface in the C++ ArduinoIoTCloud library, and the wire
// values it publishes (OTABeginCmd 0x10000, OTAProgressCmd 0x10200, the phase
// numbers, the 10-second progress cadence) are the ones the Cloud already
// understands. No new device protocol, no new credential, no cloud-side change.
//
//	Resume ──▶ Idle ──▶ OtaAvailable ──▶ StartOTA ──▶ Fetch ──▶ FlashOTA
//	            ▲  ▲                                    │           │
//	            │  │                                    ▼           │
//	            │  └───────────────────────── Fail ◀────┘           │
//	            └──── (success: publish OTABeginCmd once) ◀──────────┘
//
// Three messages carry the whole contract with the Cloud:
//
//   - Fail publishes OTAProgressCmd(8, <negative code>) — the failure the Cloud
//     renders in the UI. The codes are RFC-14 §5.10 and live in state.go.
//   - A successful install publishes OTABeginCmd(<installed digest>) exactly once,
//     which is what closes the job: the Cloud matches the digest against the one it
//     asked for (RFC-14 §5.1). An App deploy never reboots the board, so unlike the
//     MCU flow there is no Reboot state in the path.
//   - A job arriving while another is in flight is refused on the spot with
//     OTAProgressCmd(8, -50), keyed by the refused job. That one is published by
//     Deliver rather than by the state machine — see there for why.
//
// Of the three, only the failure is held when the broker is down (see the outbox
// field); the other two are published or dropped.
//
// Every one of them is keyed by a job id, which is also why a job that cannot be
// named produces no message at all: there would be nothing for the Cloud to attach it
// to. The C++ reference returns early on a null context for the same reason.
//
// # Why the digest is announced once, and never at startup
//
// The MCU flow publishes OTABeginCmd on every connection, because an MCU runs
// exactly one firmware: "the installed artefact" is a well-defined thing, and
// re-announcing it lets the Cloud's view converge for free.
//
// A Linux board is not like that. It can hold and run several Apps at once, and the
// user can install them by hand from App Lab without the Cloud being involved at
// all. There is no single "the deployed App" to announce, so announcing one at every
// boot would state something false — and the more Apps the board carries, the more
// misleading it gets. So the digest goes out once, when this process has just
// installed that bundle and can vouch for it, and nothing is persisted afterwards.
//
// The cost is accepted deliberately: if that one publish does not reach the Cloud
// (broker down at that instant), the job is not closed and nothing retries it. A
// wrong claim repeated on every boot is worse than a lost confirmation.
//
// # What the process is gated on: the broker, NOT a thing
//
// A deploy job can arrive at any moment from the instant the board reaches the
// broker, whether or not a thing is attached. Nothing in the flow needs one: the
// job arrives on the device-keyed downlink topic (/a/d/<device_id>/c/dw) and every
// reply goes out on /c/up, so thing_id never enters the picture. Cloud Variables
// need a thing; deploying an App does not.
//
// This matches the C++ reference, which gates the OTA FSM on the MQTT connection
// alone (ArduinoIoTCloudTCP::update: bail out below State::ConnectPhy, then drive
// _ota.update()) and dispatches OtaUpdateCmdDownId from the generic command loop,
// with no reference to thing state. RFC-14 §5.1's "only active while the FSM is in
// Steady" is narrower than both, and would have made a board with no thing
// undeployable.
//
// The C++ is more precise still, and worth reproducing: from Idle onwards the
// process is allowed to "run independently from the mqttClient". So a download is
// NOT torn down when the broker drops — losing gigabytes of transfer to an MQTT blip
// would be absurd. Only publishing is gated: progress messages that fall in the gap
// are dropped, and the next one carries the same cumulative byte count. See
// SetConnected.
//
// # Where this differs from the C++ reference, and why
//
//   - No approval policy. The MCU library can hold a job in OtaAvailable waiting
//     for approveOta(); an App deploy is always operator-initiated from the Cloud
//     UI, so consent has already been given and the FSM proceeds directly.
//   - Resume is a real state, not a boot check. On the MCU, Resume asks "did I
//     just reboot from an OTA?". Here it asks "did a previous daemon run leave a
//     download unfinished?" and picks the transfer up at the byte it stopped at.
package ota

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appinstaller "github.com/arduino/arduino-cloud-connector/internal/app-installer"
	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/downloader"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt/command"
)

// Publisher is the subset of mqtt.Client the process needs: OTA messages travel
// on the device-keyed command channel (/a/d/<device_id>/c/up), not on a thing
// topic, so no thing_id is involved.
type Publisher interface {
	PublishCommand(deviceID string, cmd command.Cmd) error
}

// ── Events ───────────────────────────────────────────────────────────────────

type event interface{ String() string }

// evUpdate carries an OTAUpdateCmd received on the downlink command channel. It is
// the only event: connectivity changes are a flag (see SetConnected), not an event,
// because nothing in the flow has to happen at the moment a connection comes up.
type evUpdate struct{ cmd command.OTAUpdateCmd }

func (evUpdate) String() string { return "OTAUpdate" }

// eventQueueSize is small on purpose: the Cloud rejects a second deploy while one
// is in flight (RFC-14 §3), so more than a couple of queued commands means
// something upstream is retrying and the extras are of no use.
const eventQueueSize = 4

// ── Snapshot ─────────────────────────────────────────────────────────────────

// Snapshot is an atomic view of the process for status reporting.
type Snapshot struct {
	State State  `json:"state"`
	JobID string `json:"job_id,omitempty"`
	// Downloaded/Total are meaningful during Fetch.
	Downloaded int64 `json:"downloaded,omitempty"`
	Total      int64 `json:"total,omitempty"`
}

// ── OTAFSM ──────────────────────────────────────────────────────────────────

// OTAFSM is the App-deploy state machine. Construct with New, drive with Run,
// feed with Deliver/SetConnected, observe with Snapshot.
type OTAFSM struct {
	deviceID       string
	pub            Publisher
	storage        downloader.Downloader
	inst           appinstaller.Installer
	downloadDir    string
	installTimeout time.Duration

	events chan event
	done   chan struct{}

	// connected gates publishing: false means nothing published now would reach
	// the Cloud. It tracks the BROKER connection, not the thing handshake.
	connected atomic.Bool
	// connUp carries a signal each time the broker connection comes up, so a waiting
	// Idle can flush the outbox without polling. Buffered, so SetConnected never
	// blocks and a signal is not lost when nobody is listening yet.
	connUp chan struct{}

	// busy claims the single deploy slot. Deliver takes it before queueing a job and
	// only the end of that job releases it, so a second OTAUpdateCmd arriving in
	// between is answered with ErrDeployInProgress rather than queued behind a deploy
	// the Cloud stopped waiting for minutes ago. Resume takes it too, for the job it
	// recovers from disk.
	busy atomic.Bool

	mu       sync.RWMutex
	snapshot Snapshot
	// reportLastSec/reportCounter make progress timestamps strictly increasing
	// even within the same second, mirroring OTACloudProcessInterface::reportStatus.
	// Under mu, not run-goroutine-private: Deliver publishes the -50 rejection from
	// the caller's goroutine and needs a timestamp too.
	reportLastSec uint64
	reportCounter uint64

	// run-goroutine-private state — no locking, only Run's goroutine touches it.
	state       State
	job         *Job
	bundle      string
	failCode    Error
	failCause   error
	clampWarned bool

	// outbox holds terminal reports composed while the broker was down, to be sent
	// when it comes back.
	//
	// Only terminal ones. A progress report lost in a disconnected gap costs nothing —
	// the next one carries the same cumulative byte count — but the Fail report is sent
	// once and is what closes the Cloud job with a reason, so dropping it leaves the
	// job open until the Cloud's own timeout, with nothing to show the operator. The
	// case that makes this necessary rather than nice is ErrDeployInterrupted: it is
	// decided in Resume, milliseconds after start-up, when the broker is essentially
	// never connected yet.
	//
	// Holding rather than waiting for the broker is the point. The state machine
	// returns to Idle at once, so a job arriving during the outage is accepted like any
	// other — whereas parking in Fail until the broker returned would keep the deploy
	// slot claimed and answer that job with ErrDeployInProgress, a refusal that could
	// not be published either.
	outbox []command.OTAProgressCmd

	now func() time.Time
}

// New builds the App-deploy process. storage fetches bundles (see
// internal/downloader, which drives internal/storage-api for the bytes); inst may
// be appinstaller.Unavailable() until the arduino-app-cli endpoint exists.
func New(cfg config.Config, deviceID string, pub Publisher, storage downloader.Downloader, inst appinstaller.Installer) *OTAFSM {
	return &OTAFSM{
		deviceID:       deviceID,
		pub:            pub,
		storage:        storage,
		inst:           inst,
		downloadDir:    cfg.AppDownloadDir,
		installTimeout: cfg.InstallTimeout,
		events:         make(chan event, eventQueueSize),
		done:           make(chan struct{}),
		connUp:         make(chan struct{}, 1),
		snapshot:       Snapshot{State: StateResume},
		now:            time.Now,
	}
}

// Done returns a channel closed when Run has returned.
func (f *OTAFSM) Done() <-chan struct{} { return f.done }

// Snapshot returns the current externally-observable state. Safe from any
// goroutine.
func (f *OTAFSM) Snapshot() Snapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.snapshot
}

// Deliver hands an OTAUpdateCmd to the process. Non-blocking: called from the
// Cloud FSM's event loop, which must never stall behind a download.
//
// A job that arrives while another deploy is in flight is refused here rather than
// queued, and answered on the spot with ErrDeployInProgress (RFC-14 §5.10 -50).
// Queueing it would be worse than useless: it would start minutes or hours later, for
// a job the Cloud has long since timed out, while the Cloud heard nothing at all in
// the meantime. The C++ reference simply ignores the command outside Idle, which has
// the same silence problem.
func (f *OTAFSM) Deliver(cmd command.OTAUpdateCmd) {
	jobID := hex.EncodeToString(cmd.ID[:])

	if !f.busy.CompareAndSwap(false, true) {
		// The slot is taken, and two very different things look like this.
		if jobID == f.Snapshot().JobID {
			// The same job again — an MQTT redelivery, or the Cloud repeating itself.
			// Rejecting it would fail the deploy that is running right now, using the
			// id of the very job it is running.
			slog.Info("ota: OTAUpdateCmd repeats the deploy already in flight, ignoring",
				"job_id", jobID)
			return
		}
		slog.Warn("ota: a deploy is already in flight, refusing the new job",
			"job_id", jobID, "in_flight", f.Snapshot().JobID)
		f.rejectBusy(cmd.ID)
		return
	}

	select {
	case f.events <- evUpdate{cmd: cmd}:
	default:
		// Unreachable while the slot admits one job at a time. Releasing it matters
		// anyway: a slot claimed for a command that was never queued would refuse
		// every future deploy.
		f.busy.Store(false)
		slog.Warn("ota: event queue full, dropping OTAUpdateCmd",
			"job_id", jobID, "state", f.Snapshot().State)
	}
}

// releaseSlot lets the next job in. Called when a deploy is over — successfully or
// not — and never before its cleanup, so a job accepted right afterwards cannot find
// the previous one's bundle or record still on disk.
func (f *OTAFSM) releaseSlot() { f.busy.Store(false) }

// rejectBusy answers a refused job with OTAProgressCmd(Fail, ErrDeployInProgress),
// keyed by the REFUSED job's id so the Cloud closes that job and leaves the running
// one alone.
//
// This is the only report published from outside the run goroutine, and deliberately:
// the answer is worth having only if it is prompt, and the run goroutine is blocked
// inside a download or an install for as long as those take. It touches nothing that
// goroutine owns — the id comes from the command, and the timestamp counter sits
// behind mu for exactly this reason.
func (f *OTAFSM) rejectBusy(id [16]byte) {
	if !f.isConnected() {
		slog.Debug("ota: broker not connected, the refusal could not be sent",
			"job_id", hex.EncodeToString(id[:]))
		return
	}
	cmd := command.OTAProgressCmd{
		ID:        id,
		State:     uint8(StateFail),
		StateData: int32(ErrDeployInProgress),
		Timestamp: f.reportTimestamp(),
	}
	if err := f.publish(cmd); err != nil {
		slog.Warn("ota: publishing the refusal failed",
			"job_id", hex.EncodeToString(id[:]), "error", err)
	}
}

// SetConnected tells the process whether the broker connection is live, i.e.
// whether a message published now would actually reach the Cloud. Called as soon
// as the device is announced on the command channel — NOT when a thing is
// attached, which an App deploy does not need.
//
// It gates publishing only, and nothing more: a connection coming up does not make
// the process do anything. Nothing is announced at connection time — see the package
// comment on why the installed digest is published once, after an install, rather
// than on every connection.
//
// An in-flight download deliberately keeps running while disconnected: the transfer
// does not need the broker, and abandoning gigabytes because MQTT blipped would be a
// poor trade. This is also what the C++ does — from Idle onwards its OTA FSM "run[s]
// independently from the mqttClient". Progress messages that fall in the gap are
// dropped; the next one carries the current cumulative byte count anyway.
func (f *OTAFSM) SetConnected(v bool) {
	if f.connected.Swap(v) == v {
		return
	}
	slog.Debug("ota: broker connectivity changed", "connected", v)
	if v {
		select {
		case f.connUp <- struct{}{}:
		default: // a signal is already pending; one is enough to wake the waiter
		}
	}
}

func (f *OTAFSM) isConnected() bool { return f.connected.Load() }

// ── The outbox ───────────────────────────────────────────────────────────────

// outboxLimit caps the number of held reports.
//
// One failed job produces one report and only one job runs at a time, so in practice
// the outbox holds one. The cap is there so that a pathological sequence — a long
// outage with the Cloud timing jobs out and reissuing them — cannot grow it without
// bound; the oldest goes first, since a newer failure describes a more recent state of
// the board.
const outboxLimit = 4

// hold puts a report that could not be published where the next connection will find
// it. See the outbox field for why only terminal reports get this treatment.
func (f *OTAFSM) hold(cmd command.OTAProgressCmd) {
	if len(f.outbox) >= outboxLimit {
		slog.Warn("ota: outbox full, dropping the oldest held report",
			"job_id", hex.EncodeToString(f.outbox[0].ID[:]), "limit", outboxLimit)
		f.outbox = f.outbox[1:]
	}
	f.outbox = append(f.outbox, cmd)
	slog.Info("ota: broker down, holding the failure report until it reconnects",
		"job_id", hex.EncodeToString(cmd.ID[:]), "code", Error(cmd.StateData), "held", len(f.outbox))
}

// flushOutbox sends everything held, oldest first.
//
// It runs before every publish, which is what keeps the order right: a held report
// always leaves before any message composed after it, so the Cloud never sees a
// failure for one job arrive after progress for the next. Held reports are lost if the
// daemon stops first — the outbox is memory, not a journal, and a deploy interrupted
// that way is what ErrDeployInterrupted reports on the following start.
func (f *OTAFSM) flushOutbox() {
	if len(f.outbox) == 0 || !f.isConnected() {
		return
	}
	held := f.outbox
	f.outbox = nil
	for _, cmd := range held {
		if err := f.publish(cmd); err != nil {
			// Not re-held: failing to publish while connected is a different problem
			// from having nowhere to publish, and one this loop cannot fix by repeating.
			slog.Warn("ota: a held failure report could not be delivered, dropping it",
				"job_id", hex.EncodeToString(cmd.ID[:]), "error", err)
			continue
		}
		slog.Info("ota: held failure report delivered",
			"job_id", hex.EncodeToString(cmd.ID[:]), "code", Error(cmd.StateData))
	}
}

// Run drives the FSM until ctx is cancelled. Call exactly once per OTAFSM.
func (f *OTAFSM) Run(ctx context.Context) {
	defer close(f.done)
	slog.Info("ota: app deploy process starting", "device_id", f.deviceID)

	next := f.runResume
	for next != nil {
		next = next(ctx)
	}
	slog.Info("ota: app deploy process stopped", "state", f.state)
}

type stateFn func(ctx context.Context) stateFn

// ── State handlers ───────────────────────────────────────────────────────────

// runResume answers "did a previous run leave work behind?".
//
// A job record on disk means a job was accepted but never finished — a crash, a
// package upgrade, or a reboot landed in the middle of it. The job is rebuilt from the
// record and re-entered at StartOTA, so the Cloud sees the deploy continue rather than
// restart. How much of the bundle is already on disk is not this layer's concern:
// internal/downloader resumes the transfer from its own state.
//
// When the record names a job but cannot be continued, the deploy is closed with
// ErrDeployInterrupted (RFC-14 §5.10 -49) instead: the Cloud is still holding that job
// open, and a reason it can render beats a silent timeout. A record too damaged to
// yield even a job id gets no message at all — there is nothing to key one to.
func (f *OTAFSM) runResume(ctx context.Context) stateFn {
	f.transition(StateResume)

	job, lost := f.loadJob()
	switch {
	case lost != nil:
		slog.Warn("ota: an interrupted deploy cannot be resumed", "job_id", lost.IDHex())
		f.job = lost
		return f.fail(ErrDeployInterrupted,
			errors.New("ota: the deploy job record does not describe a job that can be continued"))

	case job == nil:
		slog.Debug("ota: no interrupted deploy to resume")
		// Nothing is in flight, so any bundle still on disk belongs to a job whose
		// install never completed. Nobody is waiting for it and at multiple GB it is
		// worth reclaiming.
		f.discardOrphanBundles()
		return f.runIdle
	}

	slog.Info("ota: interrupted deploy found, resuming", "job_id", job.IDHex())
	// The recovered job holds the deploy slot exactly like a freshly delivered one
	// would, so a job arriving now is refused with -50 instead of queueing up behind it.
	f.busy.Store(true)
	f.job = job
	return f.runStartOTA
}

// bundleExt is the suffix every bundle path ends in. It is this package's own
// naming convention, and the only part of a file name in the download directory that
// this package is entitled to recognise.
const bundleExt = ".zip"

// bundlePath is where a job's verified bundle lands. Named after the job id, so a
// stray file is traceable to the deploy that produced it.
func (f *OTAFSM) bundlePath(job Job) string {
	return filepath.Join(f.downloadDir, job.IDHex()+bundleExt)
}

// discardOrphanBundles reclaims everything left in the download directory when no
// deploy is in flight. Called from Resume, i.e. when nothing is using those files.
//
// The two glob patterns matter, and the second one is the point: a bundle is
// "<job-id>.zip", but an interrupted transfer leaves companions named after it —
// the downloader's partial and resume record, plus any temp file from an atomic
// write that did not finish. Matching only "*.zip" would find the finished bundle
// and miss a multi-gigabyte partial, which nothing else ever reclaims: this runs
// precisely when the in-flight record is gone, so there is no second chance.
func (f *OTAFSM) discardOrphanBundles() {
	var matches []string
	// "*.zip" finds finished bundles; "*.zip.*" finds anything named after one.
	for _, pattern := range []string{"*" + bundleExt, "*" + bundleExt + ".*"} {
		found, err := filepath.Glob(filepath.Join(f.downloadDir, pattern))
		if err != nil {
			slog.Warn("ota: could not scan the download dir for orphan bundles",
				"dir", f.downloadDir, "pattern", pattern, "error", err)
			return
		}
		matches = append(matches, found...)
	}
	if len(matches) == 0 {
		return
	}

	// Log once per bundle rather than once per file, and let the downloader clean up
	// through its own API as well — it owns its sidecars, and anything it keeps that
	// is not named after the bundle would not have matched above.
	bundles := make(map[string]struct{}, len(matches))
	for _, path := range matches {
		if base, ok := bundleBase(path); ok {
			bundles[base] = struct{}{}
		}
	}
	for base := range bundles {
		slog.Warn("ota: removing orphan app bundle left by a previous run", "bundle", base)
		f.storage.Discard(base)
	}
	removeQuietly(matches...)
}

// bundleBase maps any file that belongs to a bundle back to the bundle path itself:
// the bundle, a partial, a resume record, or a temp file from an interrupted atomic
// write (which is prefixed with a dot). It recognises only bundleExt — what the
// downloader calls its own sidecars stays the downloader's business.
func bundleBase(path string) (string, bool) {
	dir, name := filepath.Split(path)
	name = strings.TrimPrefix(name, ".")
	i := strings.Index(name, bundleExt)
	if i < 0 {
		return "", false
	}
	// The suffix has to end the name or begin a sidecar suffix. Without this,
	// "abcd.zipped" would be read as the bundle "abcd.zip" and an unrelated file
	// would be deleted along with it.
	if rest := name[i+len(bundleExt):]; rest != "" && !strings.HasPrefix(rest, ".") {
		return "", false
	}
	return filepath.Join(dir, name[:i+len(bundleExt)]), true
}

// removeQuietly deletes paths, logging anything other than "already gone". Both
// callers are cleanups of a job that is already over, so a file that cannot be
// removed is worth a line in the log and nothing more.
func removeQuietly(paths ...string) {
	for _, p := range paths {
		if err := os.Remove(p); err != nil && !errors.Is(err, os.ErrNotExist) {
			slog.Warn("ota: could not remove file", "file", p, "error", err)
		}
	}
}

// announceInstalled publishes OTABeginCmd for a bundle this process has just
// installed. That message is what closes the Cloud job (RFC-14 §5.1).
//
// It is sent exactly once, here, and nowhere else — in particular not at startup or
// on a reconnect. See the package comment: a Linux board can hold and run several
// Apps, some installed by hand from App Lab, so there is no single "deployed App" to
// announce, and announcing one anyway would be a claim this process cannot make.
//
// A failure to publish is logged and dropped. Nothing retries it, and nothing
// remembers the digest afterwards, so the Cloud job stays open until the operator
// looks. That is the accepted cost of not making a false claim on every boot.
func (f *OTAFSM) announceInstalled(digest [32]byte) {
	if !f.isConnected() {
		slog.Warn("ota: broker not connected, the install confirmation could not be sent",
			"sha256", hex.EncodeToString(digest[:]))
		return
	}
	if err := f.publish(command.OTABeginCmd{SHA256: digest}); err != nil {
		slog.Warn("ota: install confirmation publish failed",
			"sha256", hex.EncodeToString(digest[:]), "error", err)
		return
	}
	slog.Info("ota: install confirmed to the cloud", "sha256", hex.EncodeToString(digest[:]))
}

// runIdle waits for a deploy job.
func (f *OTAFSM) runIdle(ctx context.Context) stateFn {
	f.transition(StateIdle)
	f.job = nil
	f.bundle = ""
	f.publishSnapshot()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-f.connUp:
			// A held failure goes out as soon as there is somewhere to send it, rather
			// than waiting for the next job to carry it out on its coat-tails.
			f.flushOutbox()
		case ev := <-f.events:
			switch e := ev.(type) {
			case evUpdate:
				return f.accept(e.cmd)
			default:
				slog.Debug("ota: ignoring unexpected event while idle", "event", ev.String())
			}
		}
	}
}

// accept takes on an incoming job and moves to OtaAvailable.
//
// The URL is NOT validated here: storage-api owns that check — it must parse and be
// https with a host, and deliberately nothing more, since the cloud chooses the URL
// and the C++ library consumes it verbatim — so duplicating it would put the same
// rule in two places. An unusable URL therefore surfaces from the first Download
// call and is reported from Fetch, which is also what the C++ reference does:
// OTADefaultCloudProcessInterface::startOTA returns UrlParseErrorFail after the job
// has been announced, not before.
func (f *OTAFSM) accept(cmd command.OTAUpdateCmd) stateFn {
	f.job = &Job{ID: cmd.ID, URL: cmd.URL, SHA256: cmd.FinalSHA}
	slog.Info("ota: deploy job received",
		"job_id", f.job.IDHex(), "final_sha256", hex.EncodeToString(cmd.FinalSHA[:]))

	// Remember the job so a crash mid-deploy can pick it back up. Losing the record
	// costs resumability, not the deploy, so a write failure is a warning rather
	// than a reason to reject a job the operator asked for.
	if err := f.saveJob(); err != nil {
		slog.Warn("ota: could not record the deploy job; a restart will not resume it",
			"job_id", f.job.IDHex(), "error", err)
	}

	// cmd.InitialSHA carries what the Cloud believes is installed. It is not checked
	// against anything: this process keeps no record of what the board runs, because
	// a Linux board can hold several Apps and the user can install them from App Lab
	// without the Cloud knowing. The target digest is what matters for the job.

	return f.runOtaAvailable
}

// runOtaAvailable reports that the job was accepted and validated.
func (f *OTAFSM) runOtaAvailable(ctx context.Context) stateFn {
	f.transition(StateOtaAvailable)
	// No approval gate: an App deploy is always operator-initiated from the Cloud
	// UI, so the consent the MCU library waits for has already been given.
	return f.runStartOTA
}

// runStartOTA reports the phase in which the transfer is set up. URL validation,
// the free-space check and the first HTTPS request all happen inside the storage
// client, on entry to Fetch.
func (f *OTAFSM) runStartOTA(ctx context.Context) stateFn {
	f.transition(StateStartOTA)
	return f.runFetch
}

// runFetch downloads the bundle, republishing the byte count as it goes.
func (f *OTAFSM) runFetch(ctx context.Context) stateFn {
	f.transition(StateFetch)
	job := *f.job
	dest := f.bundlePath(job)

	err := f.storage.Download(ctx, downloader.Request{
		URL:            job.URL,
		ExpectedSHA256: job.SHA256,
		DestPath:       dest,
	}, func(written, total int64) {
		f.setProgress(written, total)
		f.report(StateFetch, f.progressData(written))
	})
	if err != nil {
		if ctx.Err() != nil {
			// Shutdown, not a failure. Both the in-flight record and the storage
			// client's own resume state stay on disk, so the next run continues from
			// where this one stopped instead of reporting an error the operator would
			// have to act on.
			slog.Info("ota: download interrupted by shutdown, progress kept for resume",
				"job_id", job.IDHex())
			return nil
		}
		// ErrDownload is the download phase's generic code, and so the honest answer
		// for a failure no layer classified (RFC-14 §5.10).
		return f.fail(decodeError(err, ErrDownload), err)
	}

	f.bundle = dest
	return f.runFlashOTA
}

// runFlashOTA hands the verified bundle to arduino-app-cli and forwards its
// progress. The daemon never opens the archive (RFC-14 §5.9).
func (f *OTAFSM) runFlashOTA(ctx context.Context) stateFn {
	f.transition(StateFlashOTA)
	job := *f.job

	req := installRequestFor(job, f.bundle)
	slog.Info("ota: handing bundle over to arduino-app-cli",
		"job_id", job.IDHex(), "bundle", req.BundlePath)

	err := f.install(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			slog.Info("ota: install interrupted by shutdown", "job_id", job.IDHex())
			return nil
		}
		// ErrInstallFailed doubles as the install phase's generic code, so an install
		// failure arduino-app-cli did not qualify still lands in the right phase.
		return f.fail(decodeError(err, ErrInstallFailed), err)
	}

	// Forget the job BEFORE confirming it, and in this order for a reason. The
	// confirmation is what closes the job cloud-side; if it went out first and the
	// daemon died before the cleanup, the in-flight record would survive and the next
	// start would redo a deploy the Cloud already considers finished. Cleaning up
	// first can only lose the confirmation, which leaves the job open — the failure
	// the user chose to accept.
	//
	// The bundle also does not stay: at multiple GB it is removed on completion
	// (RFC-14 §5.9).
	f.cleanUpJob(job)
	f.announceInstalled(job.SHA256)

	slog.Info("ota: app deploy completed",
		"job_id", job.IDHex(), "installed_sha256", hex.EncodeToString(job.SHA256[:]))

	f.job = nil
	f.bundle = ""
	f.releaseSlot()
	return f.runIdle
}

// install runs the handover under a stall watchdog and forwards each progress
// event to the Cloud.
//
// The watchdog is what bounds a hung install (RFC-14 §5.5): an install has no
// natural duration — unpacking a multi-GB archive and restarting containers can
// legitimately take minutes — so the thing worth timing out on is *silence*, not
// total elapsed time. Every progress event resets the timer; installTimeout of
// complete silence cancels the install and fails the job, instead of leaving the
// Cloud job open forever.
//
// It lives here rather than inside an appinstaller.Installer so every
// implementation (the future arduino-app-cli SSE client, and any test double) is
// bounded the same way. It is also at a different level from the timeouts the
// installer itself needs: this one measures silence on the progress callback and
// knows nothing about the transport, while connect and read deadlines on the stream
// belong to the installer and surface as appinstaller.ErrFailed.
func (f *OTAFSM) install(ctx context.Context, req appinstaller.Request) error {
	ictx, cancel := context.WithCancel(ctx)
	defer cancel()

	beat := make(chan struct{}, 1)
	stalled := make(chan struct{})
	go func() {
		timer := time.NewTimer(f.installTimeout)
		defer timer.Stop()
		for {
			select {
			case <-ictx.Done():
				return
			case <-beat:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(f.installTimeout)
			case <-timer.C:
				slog.Error("ota: install produced no progress within the stall timeout, cancelling",
					"timeout", f.installTimeout)
				close(stalled)
				cancel()
				return
			}
		}
	}()

	// onProgress runs on this goroutine — appinstaller.Installer requires the
	// callback to be pumped from Install's own frame — so the report path keeps its
	// single-goroutine ownership of job and reporting state.
	err := f.inst.Install(ictx, req, func(percent int32) {
		select {
		case beat <- struct{}{}:
		default: // a beat is already pending; one is enough to reset the timer
		}
		f.report(StateFlashOTA, percent)
	})
	if err != nil {
		// Only reinterpret the error as a stall if the watchdog actually fired —
		// otherwise a real install error would be misreported as a timeout.
		select {
		case <-stalled:
			return fmt.Errorf("%w: arduino-app-cli reported none for %s",
				errInstallStalled, f.installTimeout)
		default:
		}
	}
	return err
}

// runFail reports the failure and returns to Idle.
//
// It never blocks on connectivity: with the broker down the report is held (see the
// outbox field) and the process carries on, so the next job is accepted normally
// instead of colliding with a deploy that is over in all but name.
func (f *OTAFSM) runFail(ctx context.Context) stateFn {
	// transition publishes OTAProgressCmd(8, failCode) — do it before clearing
	// the job, which the message is keyed by.
	f.transition(StateFail)
	slog.Error("ota: app deploy failed",
		"job_id", f.jobIDForLog(), "code", f.failCode, "code_value", int32(f.failCode),
		"error", f.failCause)

	// The Cloud has been told; it closes the job. Keeping the partial download would
	// only make the next Resume re-fetch a bundle nobody is waiting for.
	if f.job != nil {
		f.cleanUpJob(*f.job)
	}
	f.job, f.bundle = nil, ""
	f.failCode, f.failCause = ErrNone, nil
	f.releaseSlot()
	return f.runIdle
}

// cleanUpJob drops everything a finished job leaves behind: its record, its bundle,
// and any transfer state the downloader is still holding for it.
func (f *OTAFSM) cleanUpJob(job Job) {
	dest := f.bundlePath(job)
	f.forgetJob()
	removeQuietly(dest)
	f.storage.Discard(dest)
}

// fail records the outcome and routes to runFail.
func (f *OTAFSM) fail(code Error, cause error) stateFn {
	f.failCode, f.failCause = code, cause
	return f.runFail
}

// ── Reporting ────────────────────────────────────────────────────────────────

// transition moves to a new state, publishing an OTAProgressCmd for the states
// the Cloud tracks. Mirrors the C++ dispatcher, which reports once per state
// change from OtaAvailable upwards.
func (f *OTAFSM) transition(s State) {
	if s == f.state {
		return
	}
	slog.Info("ota: state transition", "from", f.state, "to", s, "job_id", f.jobIDForLog())
	f.state = s
	f.publishSnapshot()

	if !s.reportable() {
		return
	}
	data := int32(0)
	if s == StateFail {
		data = int32(f.failCode)
	}
	f.report(s, data)
}

// report publishes one OTAProgressCmd.
//
// With no job in flight there is nothing for the Cloud to correlate the message with,
// so nothing is sent — the C++ reportStatus returns early on a null context for the
// same reason.
//
// With no broker the two kinds of report part ways: a progress report is dropped,
// because the next one carries the same cumulative byte count, while a Fail report
// goes to the outbox, because nothing will ever repeat it.
func (f *OTAFSM) report(state State, stateData int32) {
	if f.job == nil {
		return
	}
	cmd := command.OTAProgressCmd{
		ID:        f.job.ID,
		State:     uint8(state),
		StateData: stateData,
		Timestamp: f.reportTimestamp(),
	}

	if !f.isConnected() {
		if state == StateFail {
			f.hold(cmd)
			return
		}
		slog.Debug("ota: skipping progress report, broker not connected",
			"state", state, "state_data", stateData)
		return
	}

	// Anything held goes first, so a failure can never arrive after a message composed
	// later than it.
	f.flushOutbox()

	if err := f.publish(cmd); err != nil {
		slog.Warn("ota: progress publish failed",
			"job_id", f.job.IDHex(), "state", state, "state_data", stateData, "error", err)
		return
	}
	slog.Debug("ota: progress reported", "job_id", f.job.IDHex(), "state", state, "state_data", stateData)
}

// publish sends an uplink OTA command on the device command channel.
func (f *OTAFSM) publish(cmd any) error {
	switch c := cmd.(type) {
	case command.OTABeginCmd:
		return f.pub.PublishCommand(f.deviceID, command.From(c))
	case command.OTAProgressCmd:
		return f.pub.PublishCommand(f.deviceID, command.From(c))
	default:
		return errors.New("ota: not an uplink OTA command")
	}
}

// reportTimestamp returns a microsecond timestamp that never repeats, so several
// reports inside the same second stay strictly ordered for the Cloud. Same
// scheme as OTACloudProcessInterface::reportStatus.
//
// Locked because Deliver publishes the -50 refusal from the caller's goroutine while
// the run goroutine may be reporting progress: two goroutines, one counter.
func (f *OTAFSM) reportTimestamp() uint64 {
	sec := uint64(f.now().Unix())

	f.mu.Lock()
	defer f.mu.Unlock()
	if sec == f.reportLastSec {
		f.reportCounter++
	} else {
		f.reportLastSec = sec
		f.reportCounter = 0
	}
	return sec*1_000_000 + f.reportCounter
}

// progressData converts a byte count into OTAProgressCmd.StateData.
//
// KNOWN PROTOCOL LIMIT: StateData is int32 on the wire (the C++ struct field is
// int32_t), so it tops out at 2,147,483,647 — under 2 GiB. Bundles are expected
// to exceed that, and at that point the byte count is clamped: the Cloud UI shows
// progress freezing near 2 GiB while the download continues correctly. Clamping
// is the least-bad option available device-side; scaling the value instead would
// silently break the UI's `state_data / bundle_size` arithmetic. Reporting
// progress for bundles above 2 GiB needs a protocol change (a wider field, or a
// percentage) — flagged as an open point on RFC-14.
func (f *OTAFSM) progressData(written int64) int32 {
	if written > math.MaxInt32 {
		if !f.clampWarned {
			f.clampWarned = true
			slog.Warn("ota: bundle exceeds 2 GiB — progress byte count clamped to the int32 wire limit; cloud-side progress will appear stalled",
				"written", written, "wire_limit", int64(math.MaxInt32))
		}
		return math.MaxInt32
	}
	return int32(written)
}

// ── snapshot helpers ─────────────────────────────────────────────────────────

func (f *OTAFSM) publishSnapshot() {
	snap := Snapshot{State: f.state}
	if f.job != nil {
		snap.JobID = f.job.IDHex()
	}
	f.mu.Lock()
	// Preserve byte counters across a state change so a Fetch→FlashOTA
	// transition does not blank the last known progress.
	snap.Downloaded, snap.Total = f.snapshot.Downloaded, f.snapshot.Total
	if f.state == StateIdle {
		snap.Downloaded, snap.Total = 0, 0
	}
	f.snapshot = snap
	f.mu.Unlock()
}

func (f *OTAFSM) setProgress(written, total int64) {
	f.mu.Lock()
	f.snapshot.Downloaded, f.snapshot.Total = written, total
	f.mu.Unlock()
}

func (f *OTAFSM) jobIDForLog() string {
	if f.job == nil {
		return ""
	}
	return f.job.IDHex()
}
