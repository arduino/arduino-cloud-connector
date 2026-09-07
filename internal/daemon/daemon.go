// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package daemon implements the Daemon FSM — three states covering the full
// daemon lifecycle:
//
//	CheckInternet → Provisioning → Run
//	       ▲          (idle wait                ▲
//	       │          for /v1/provisioning      │
//	       │          /start; then runs         │
//	       │          the CSR attempt)          │
//	       └────────────────────────────────────┘
//	                          reprovision request from REST
//
// The thing-handshake / MQTT-steady-state logic lives in the nested Cloud FSM
// (package internal/daemon/cloud) and is spawned by the Run state.
//
// The daemon does NOT persist its own state. On restart the post-internet state
// is derived from the provisioning Service's State (itself derived from the
// on-disk in-flight marker + credentials): Provisioned → Run, an in-flight
// attempt → resume Provisioning, otherwise idle in Provisioning.
//
// Reprovisioning can be requested via Reprovision. From Run, the request stops
// the Cloud FSM gracefully, runs the synchronous provisioning prelude (marker +
// wipe, its result returned to the caller), then transitions to Provisioning to
// run the CSR attempt.
package daemon

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/arduino/arduino-cloud-connector/internal/config"
	"github.com/arduino/arduino-cloud-connector/internal/daemon/cloud"
	"github.com/arduino/arduino-cloud-connector/internal/keystore"
	"github.com/arduino/arduino-cloud-connector/internal/mqtt"
	"github.com/arduino/arduino-cloud-connector/internal/provisioning"
	"github.com/arduino/arduino-cloud-connector/internal/senml"
	"github.com/arduino/arduino-cloud-connector/internal/variables"
)

// ── States ────────────────────────────────────────────────────────────────────

// State is one of the three daemon states.
type State string

const (
	StateCheckInternet State = "CheckInternet"
	StateProvisioning  State = "Provisioning"
	StateRun           State = "Run"
)

const (
	internetCheckBackoff = 5 * time.Second
	internetCheckTimeout = 5 * time.Second
	// outboundQueueSize bounds the channel feeding the outbound worker. A full
	// queue back-pressures the REST handler (preserving order) rather than
	// dropping values.
	outboundQueueSize = 256
	// outboundReorderWindow is how long the outbound worker holds the
	// earliest-timestamped pending value before publishing it, giving any
	// concurrently-submitted value with an earlier timestamp time to arrive and
	// be ordered ahead of it. Two concurrent EnqueueVariable calls stamp their
	// timestamps microseconds apart but may reach the worker out of order; this
	// window lets the priority queue reorder them (and absorb scheduling/GC
	// jitter) before they hit the broker. Sized to the broker's ~33ms
	// inter-message interval (it accepts ~30 msg/s). Best-effort: values stamped
	// more than a window apart are already ordered. This is purely a reorder
	// window, not a rate limit — if an app publishes faster, values still go out
	// as fast as they become ready.
	outboundReorderWindow = 33 * time.Millisecond
	// ntpProbeHost is Arduino's NTP server, used as a neutral connectivity
	// probe. Deliberately decoupled from the MQTT broker so the check works
	// even when the broker is intentionally unreachable (mock builds) or
	// behind region failover.
	ntpProbeHost = "time.arduino.cc:123"
)

// ErrBusy is returned by Reprovision when the daemon FSM is not currently ready
// to accept a request — either a reprovision is already in progress, or the FSM
// is busy in a state that does not handle commands (e.g. CheckInternet).
var ErrBusy = errors.New("daemon: not ready to reprovision (busy or already in progress)")

// ── Snapshot ─────────────────────────────────────────────────────────────────

// Snapshot is an atomic view of the daemon state and (if running) the nested
// Cloud FSM's state.
type Snapshot struct {
	State State           // daemon FSM state
	Cloud *cloud.Snapshot // non-nil only when State == StateRun
}

// ── Commands ─────────────────────────────────────────────────────────────────

type daemonCmd interface{ cmdName() string }

// cmdReprovision asks the FSM to (re)provision. organizationID is the optional
// organization the board should be provisioned into (empty = none). reply
// receives the result of the synchronous prelude (provisioning.BeginProvisioning:
// marker write + wipe + optional organization id); the caller blocks on it so
// the REST response reflects whether the prelude succeeded. The longer CSR
// attempt then runs asynchronously.
type cmdReprovision struct {
	organizationID string
	reply          chan error
}

func (cmdReprovision) cmdName() string { return "reprovision" }

// ── FSM ───────────────────────────────────────────────────────────────────────

// Daemon is the daemon's top-level state machine. It owns the Cloud FSM when
// in StateRun and the provisioning service when in StateProvisioning.
type Daemon struct {
	cfg     config.Config
	ks      *keystore.Keystore
	reg     *variables.Registry
	provSvc *provisioning.Service
	client  mqtt.Client

	// commands from external code (REST handlers).
	cmds chan daemonCmd

	// outbound feeds locally-set variable values to the single worker
	// (runOutbound), which reorders them by timestamp before delivery.
	outbound chan outboundValue
	// outboundSeq assigns a monotonic submission sequence to each outbound
	// value, used as a stable tiebreaker when two values share a timestamp.
	outboundSeq atomic.Uint64

	// run-goroutine-private state
	state State
	cloud *cloud.FSM

	// published atomically on every transition
	mu       sync.RWMutex
	snapshot Snapshot

	done chan struct{}
}

// New creates the daemon FSM bound to the given subsystems.
func New(cfg config.Config, ks *keystore.Keystore, reg *variables.Registry, provSvc *provisioning.Service, client mqtt.Client) *Daemon {
	return &Daemon{
		cfg:      cfg,
		ks:       ks,
		reg:      reg,
		provSvc:  provSvc,
		client:   client,
		cmds:     make(chan daemonCmd), // unbuffered: a non-blocking send succeeds only when the FSM is parked waiting for a command
		outbound: make(chan outboundValue, outboundQueueSize),
		state:    StateCheckInternet,
		snapshot: Snapshot{State: StateCheckInternet},
		done:     make(chan struct{}),
	}
}

// Snapshot returns a copy of the current externally-observable state.
func (d *Daemon) Snapshot() Snapshot {
	d.mu.RLock()
	defer d.mu.RUnlock()
	snap := d.snapshot
	if d.cloud != nil {
		c := d.cloud.Snapshot()
		snap.Cloud = &c
	}
	return snap
}

// Done returns a channel closed when Run has returned.
func (d *Daemon) Done() <-chan struct{} { return d.done }

// Reprovision asks the daemon FSM to (re)provision and blocks until the
// synchronous prelude — write the in-flight marker, wipe the old credentials and
// persist the optional organizationID — has run, returning its result. The
// longer CSR attempt continues asynchronously afterwards. organizationID is
// optional (empty = provision without an associated organization). Returns
// ErrBusy if the FSM is not currently parked waiting for a command (a reprovision
// already in progress, or a non-command state such as CheckInternet); returns
// ctx.Err() if the caller's context is cancelled before the prelude completes.
// Safe to call from any goroutine.
func (d *Daemon) Reprovision(ctx context.Context, organizationID string) error {
	slog.Debug("daemon: reprovision requested via API", "organization_id", organizationID)
	reply := make(chan error, 1)
	select {
	case d.cmds <- cmdReprovision{organizationID: organizationID, reply: reply}:
	default:
		slog.Warn("daemon: reprovision rejected — FSM not parked for commands (busy or mid-provisioning)")
		return ErrBusy
	}
	select {
	case err := <-reply:
		slog.Debug("daemon: reprovision prelude returned to caller", "error", err)
		return err
	case <-ctx.Done():
		slog.Warn("daemon: reprovision caller context cancelled before prelude result", "error", ctx.Err())
		return ctx.Err()
	}
}

// ErrNotSteady is returned by the outbound worker's publish step when the cloud
// FSM is not in the Steady state and a thing_id is not available.
var ErrNotSteady = errors.New("daemon: cloud not in steady state")

// ErrThingUnavailable is returned by EnqueueVariable when the cloud has not
// reached Steady (no thing assigned / initial last-values sync not complete).
// The value is deliberately NOT queued: it could not be delivered and, if
// stored, would become a bogus "last value". The REST layer surfaces this so
// the app can log that the thing is unavailable and keep the value locally
// until the cloud syncs.
var ErrThingUnavailable = errors.New("daemon: no thing assigned (cloud not steady)")

// CloudSteady reports whether the daemon is running and the nested Cloud FSM has
// reached Steady — i.e. a thing is assigned and the initial last-values sync has
// completed (or been exhausted). Only then can locally-set values be delivered
// to the cloud, and only then does the SSE stream expose real last values
// instead of a thing_unavailable frame.
func (d *Daemon) CloudSteady() bool {
	snap := d.Snapshot()
	return snap.State == StateRun && snap.Cloud != nil && snap.Cloud.State == cloud.StateSteady
}

// outboundValue is one locally-set variable value queued for ordered delivery.
// ts is stamped at API receipt to capture the true submission order; seq is a
// stable tiebreaker for values sharing a timestamp.
type outboundValue struct {
	name  string
	value any
	ts    time.Time
	seq   uint64
}

// EnqueueVariable submits a locally-set variable value for ordered delivery to
// the cloud. A single worker (runOutbound) reorders pending values by their
// timestamp and then, one at a time, stores each in the registry and publishes
// it to the broker — so concurrent writers, or one high-throughput writer, are
// delivered in timestamp order. The timestamp (and sequence) are stamped here,
// as early as possible, so a value that loses the race to enter the queue is
// still ordered correctly behind/ahead of its peers.
//
// Blocks (back-pressure) only if the queue is full; returns ctx.Err() if ctx is
// cancelled (e.g. the client disconnected) before the value could be queued.
func (d *Daemon) EnqueueVariable(ctx context.Context, name string, value any) error {
	// Reject (and do not queue) while no thing is assigned yet: the value could
	// not reach the cloud and, if stored via the outbound worker, would become a
	// stale "last value" replayed to later subscribers. The app is told via
	// ErrThingUnavailable so it can log and keep the value locally until sync.
	if !d.CloudSteady() {
		return ErrThingUnavailable
	}
	ov := outboundValue{
		name:  name,
		value: value,
		ts:    time.Now().UTC(),
		seq:   d.outboundSeq.Add(1),
	}
	// Diagnostic: a full queue means the single outbound worker (runOutbound) is
	// not draining — almost always because a publish is wedged on a dead or
	// half-open broker connection. The send below then back-pressures this REST
	// handler until a slot frees or the client's request context is cancelled
	// (the app hits its read timeout first). Surface it at Warn so the stall is
	// visible at info level instead of failing silently.
	if len(d.outbound) == cap(d.outbound) {
		slog.Warn("daemon: outbound queue full — REST PUT is now blocking; the outbound worker is likely stuck on a cloud publish",
			"name", name, "queue_cap", cap(d.outbound))
	}
	select {
	case d.outbound <- ov:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// runOutbound delivers locally-set values to the cloud in timestamp order. It
// keeps pending values in a min-heap keyed by timestamp and publishes the
// earliest only once it has waited outboundReorderWindow — long enough for any
// concurrently-submitted value with an earlier timestamp to have arrived and
// sorted ahead of it. This corrects the case where two concurrent
// EnqueueVariable calls reach the worker out of submission order. Runs for the
// daemon's lifetime. A publish error (e.g. not yet steady) does NOT roll back
// the registry update — the local value is kept and the cloud receives it on
// the next LastValues.begin handshake.
func (d *Daemon) runOutbound(ctx context.Context) {
	pending := &outboundHeap{}
	heap.Init(pending)

	timer := time.NewTimer(time.Hour)
	timer.Stop()

	for {
		var timerC <-chan time.Time
		if pending.Len() > 0 {
			wait := time.Until((*pending)[0].ts.Add(outboundReorderWindow))
			if wait <= 0 {
				ov := heap.Pop(pending).(outboundValue)
				d.reg.SetValue(ov.name, ov.value, ov.ts)
				if err := d.publishVariable(ov.name, ov.value); err != nil {
					// Elevated from Debug: a value the app set is NOT reaching the
					// cloud (kept only in the local registry). At info level this
					// used to be invisible, so the daemon looked healthy while
					// silently dropping updates.
					slog.Warn("daemon: variable not delivered to cloud, kept locally", "name", ov.name, "reason", err)
				}
				continue
			}
			timer.Reset(wait)
			timerC = timer.C
		}

		select {
		case <-ctx.Done():
			return
		case ov := <-d.outbound:
			heap.Push(pending, ov)
		case <-timerC:
		}

		// Stop/drain the timer before the next Reset (standard reuse pattern).
		if timerC != nil && !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}

// outboundHeap is a min-heap of pending outbound values ordered by timestamp,
// then submission sequence — so the earliest-submitted value is always at the
// root regardless of the order values reach the worker.
type outboundHeap []outboundValue

func (h outboundHeap) Len() int { return len(h) }
func (h outboundHeap) Less(i, j int) bool {
	if h[i].ts.Equal(h[j].ts) {
		return h[i].seq < h[j].seq
	}
	return h[i].ts.Before(h[j].ts)
}
func (h outboundHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *outboundHeap) Push(x any) { *h = append(*h, x.(outboundValue)) }

func (h *outboundHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// publishVariable encodes the given locally-set variable as SenML+CBOR and
// publishes it on the outbound property topic for the currently-assigned thing.
// Returns ErrNotSteady if the daemon is not in Run state or no thing_id is known
// yet.
//
// For a multi-value property attribute (name "property:attribute") the WHOLE
// property is published — all sibling attributes at their current registry
// values — in a single message, mirroring the C++ ArduinoIoTCloud library
// (appendAttributesToCloud always encodes every attribute). Arduino Cloud
// rebuilds a structured property (e.g. a ColoredLight) from each inbound
// message and resets any attribute absent from it to its default, so a partial
// update (only "clight:hue") would silently switch "clight:swi" back to false.
// Sending the full packet keeps the unmodified attributes (their last value)
// intact.
//
// Ordering is guaranteed by runOutbound publishing in sequence, not by any wire
// timestamp: the payload carries only name+value.
func (d *Daemon) publishVariable(name string, value any) error {
	snap := d.Snapshot()
	if snap.State != StateRun || snap.Cloud == nil || snap.Cloud.ThingID == "" {
		cloudState := "<no cloud FSM attached>"
		thingID := ""
		if snap.Cloud != nil {
			cloudState = string(snap.Cloud.State)
			thingID = snap.Cloud.ThingID
		}
		// This is the "sending stopped, no error" case: it names exactly why the
		// value isn't going out — wrong daemon state, no cloud FSM attached, or
		// connected-but-no-thing_id (FSM stuck before Steady after a reprovision).
		slog.Debug("daemon: NOT publishing variable — cloud not ready",
			"name", name, "daemon_state", snap.State, "cloud_state", cloudState, "thing_id", thingID)
		return ErrNotSteady
	}
	payload, err := senml.Encode(d.propertyPacket(name, value))
	if err != nil {
		return fmt.Errorf("daemon: encode variable %q: %w", name, err)
	}
	return d.client.PublishProperty(snap.Cloud.ThingID, payload)
}

// propertyPacket returns the SenML variables to publish for the locally-set
// variable (name, value). A plain scalar publishes just itself. A multi-value
// property attribute ("property:attribute") publishes the complete property:
// every sibling attribute currently in the registry, with the just-set value
// substituted for name (the registry is updated right before publish, but this
// guards against a concurrent overwrite). See publishVariable for why the whole
// property must travel in one message.
func (d *Daemon) propertyPacket(name string, value any) []senml.Variable {
	prop, _, isAttribute := strings.Cut(name, ":")
	if !isAttribute {
		return []senml.Variable{{Name: name, Value: value}}
	}
	siblings := d.reg.WithPrefix(prop + ":")
	if len(siblings) == 0 {
		return []senml.Variable{{Name: name, Value: value}}
	}
	vars := make([]senml.Variable, 0, len(siblings))
	for _, v := range siblings {
		val := v.Value
		if v.Name == name {
			val = value
		}
		vars = append(vars, senml.Variable{Name: v.Name, Value: val})
	}
	return vars
}

// Run drives the daemon FSM. Blocks until ctx is cancelled.
func (d *Daemon) Run(ctx context.Context) {
	defer close(d.done)

	// Serialise outbound variable delivery for the whole daemon lifetime.
	go d.runOutbound(ctx)

	slog.Info("daemon: starting", "provisioning_status", d.provSvc.State())

	next := d.runCheckInternet
	for next != nil {
		next = next(ctx)
	}
}

type daemonStateFn func(ctx context.Context) daemonStateFn

// ── State handlers ──────────────────────────────────────────────────────────

func (d *Daemon) runCheckInternet(ctx context.Context) daemonStateFn {
	d.transition(StateCheckInternet)

	delay := time.Duration(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
		if isInternetReachable(ctx) {
			slog.Info("daemon: internet reachable, choosing next state",
				"provisioning_status", d.provSvc.State())
			return d.pickPostInternetState()
		}
		slog.Warn("daemon: internet not reachable, retrying", "delay", internetCheckBackoff)
		delay = internetCheckBackoff
	}
}

// pickPostInternetState returns the next state to enter once internet is up,
// derived entirely from the provisioning state on disk (no persisted daemon
// state). The provisioning Service decides this from the in-flight marker and
// the credential files:
//
//   - StateProvisioning (in-flight marker, within window) → resume the attempt
//     (the prelude already ran before the crash/restart). The provisioning Service
//     decides which half to resume from: with a certificate already on disk it
//     continues from provision/complete instead of spending a second CSR.
//   - StateProvisioned (credentials present, no marker)   → Run. The marker now
//     survives until provision/complete has succeeded, so this state means the
//     certificate is activated and the broker will accept it — not merely that a
//     certificate exists.
//   - StateUnprovisioned / StateError                     → Provisioning, idle,
//     waiting for /v1/provisioning/start.
func (d *Daemon) pickPostInternetState() daemonStateFn {
	switch d.provSvc.State() {
	case provisioning.StateProvisioning:
		return func(c context.Context) daemonStateFn { return d.runProvisioning(c, true) }
	case provisioning.StateProvisioned:
		return d.runRun
	default: // StateUnprovisioned, StateError
		return func(c context.Context) daemonStateFn { return d.runProvisioning(c, false) }
	}
}

// runProvisioning is the Provisioning state.
//
// If runAttemptNow is true the CSR attempt runs immediately — either because the
// prelude (marker write + wipe) just succeeded for a fresh /start, or because an
// in-flight marker survived a crash and the attempt is being resumed. Otherwise
// the FSM idles, waiting for a cmdReprovision from the REST API.
//
// On success: transition to Run. On failure: stay in Provisioning (the
// provisioning Service's State reports provisioning/error), waiting for the next
// cmdReprovision. While the attempt runs the FSM is not reading commands, so a
// concurrent /start gets ErrBusy (409).
func (d *Daemon) runProvisioning(ctx context.Context, runAttemptNow bool) daemonStateFn {
	d.transition(StateProvisioning)

	for {
		if runAttemptNow {
			runAttemptNow = false
			slog.Info("daemon: running provisioning attempt")
			if err := d.provSvc.RunAttempt(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				slog.Error("daemon: provisioning attempt failed; staying in Provisioning",
					"error", err, "status", d.provSvc.State())
			} else {
				slog.Info("daemon: provisioning succeeded, transitioning to Run")
				return d.runRun
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case cmd := <-d.cmds:
			switch c := cmd.(type) {
			case cmdReprovision:
				runAttemptNow = d.beginReprovision(c)
			default:
				slog.Debug("daemon: ignoring unexpected command in Provisioning", "cmd", cmd.cmdName())
			}
		}
	}
}

// beginReprovision runs the synchronous provisioning prelude (write marker, wipe
// old credentials) and replies to the REST caller with its result. Returns true
// iff the prelude succeeded and the CSR attempt should run next.
func (d *Daemon) beginReprovision(c cmdReprovision) bool {
	err := d.provSvc.BeginProvisioning(c.organizationID)
	if err != nil {
		slog.Error("daemon: provisioning prelude failed", "error", err)
	}
	c.reply <- err
	return err == nil
}

// runRun is the Run state.
//
// Spawns the nested Cloud FSM and supervises it. On cmdReprovision, gracefully
// stops the Cloud FSM and transitions back to Provisioning.
func (d *Daemon) runRun(ctx context.Context) daemonStateFn {
	d.transition(StateRun)

	f, err := cloud.New(d.cfg, d.ks, d.reg, d.client)
	if err != nil {
		slog.Error("daemon: failed to init cloud FSM; falling back to CheckInternet", "error", err)
		return d.runCheckInternet
	}

	cloudCtx, cancelCloud := context.WithCancel(ctx)
	d.attachCloud(f)
	go f.Run(cloudCtx)

	for {
		select {
		case <-ctx.Done():
			slog.Debug("daemon: Run context cancelled; stopping cloud FSM")
			cancelCloud()
			<-f.Done()
			d.detachCloud()
			return nil
		case cmd := <-d.cmds:
			switch c := cmd.(type) {
			case cmdReprovision:
				slog.Info("daemon: reprovision requested from Run; stopping cloud FSM")
				cancelCloud()
				slog.Debug("daemon: waiting for cloud FSM to finish shutdown (may block if a broker call is wedged)")
				<-f.Done()
				slog.Debug("daemon: cloud FSM shutdown complete; detaching")
				d.detachCloud()
				// Run the prelude (marker + wipe) and reply to the caller only
				// after the cloud connection is fully torn down, so the wipe
				// never races the cloud FSM reading credentials.
				proceed := d.beginReprovision(c)
				slog.Info("daemon: cloud FSM stopped; transitioning to Provisioning", "run_csr_attempt", proceed)
				return func(ctx context.Context) daemonStateFn { return d.runProvisioning(ctx, proceed) }
			default:
				slog.Debug("daemon: ignoring unexpected command in Run", "cmd", cmd.cmdName())
			}
		}
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

func (d *Daemon) transition(state State) {
	if state != d.state {
		slog.Info("daemon: state transition", "from", d.state, "to", state)
	}
	d.state = state
	d.mu.Lock()
	d.snapshot.State = state
	if state != StateRun {
		d.snapshot.Cloud = nil
	}
	d.mu.Unlock()
}

func (d *Daemon) attachCloud(f *cloud.FSM) {
	d.mu.Lock()
	d.cloud = f
	d.mu.Unlock()
}

func (d *Daemon) detachCloud() {
	d.mu.Lock()
	d.cloud = nil
	d.snapshot.Cloud = nil
	d.mu.Unlock()
}

// ── internet check ───────────────────────────────────────────────────────────

// isInternetReachable does a best-effort NTP round-trip against Arduino's time
// server (time.arduino.cc) as the connectivity probe. It is deliberately
// decoupled from the MQTT broker: the broker may be intentionally unreachable
// (mock builds) or behind region failover, while time.arduino.cc is a stable,
// always-on endpoint that proves DNS resolution plus outbound connectivity.
// NTP over UDP/123 needs no special privileges (unlike a raw-socket ICMP ping).
func isInternetReachable(ctx context.Context) bool {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "udp", ntpProbeHost)
	if err != nil {
		return false
	}
	defer conn.Close() //nolint:errcheck

	deadline := time.Now().Add(internetCheckTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	// Minimal NTPv3 client request: first byte = LI(0) | VN(3) | Mode(3).
	req := make([]byte, 48)
	req[0] = 0x1B
	if _, err := conn.Write(req); err != nil {
		return false
	}
	resp := make([]byte, 48)
	if _, err := conn.Read(resp); err != nil {
		return false
	}
	return true
}

// ── debug stringer ───────────────────────────────────────────────────────────

func (s Snapshot) String() string {
	if s.Cloud == nil {
		return fmt.Sprintf("State=%s", s.State)
	}
	return fmt.Sprintf("State=%s Cloud=%s thing_id=%q", s.State, s.Cloud.State, s.Cloud.ThingID)
}
