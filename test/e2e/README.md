# E2E testing tool

This is a testing tool to perform End-to-End tests on the `arduino-cloud-connector`. 

The tool tests the behaviour of the cloud-connector starting from how it interacts with the external services.

The tool exposes the MQTT Server and an HTTP Rest API Server to simulate the Cloud services the `arduino-cloud-connector` uses.

## Running the tests

Two binaries are involved: the daemon under test, and the tester that drives it.

**1. Build the daemon.** The `mock` build tag is not optional — it makes the
UHWID readable off a dev machine and `DeviceNetConfig` deterministic:

```bash
task build:mock
```

**2. Run the scenarios.** Either driver works; they call the same code, so they
cannot disagree.

Under `go test`, one subtest per file:

```bash
task test:e2e
```

```bash
go test -run 'TestScenarios/full-lifecycle' .
```

Or build the tester and run it as a command, which is what you want against a
daemon on a real board or a staging host:

```bash
task build-e2e
```

```bash
../../build/arduino-cloud-connector-e2e -daemon ../../build/arduino-cloud-connector-mock
```

| Flag | Default | Meaning |
|---|---|---|
| `-daemon` | `$E2E_DAEMON_BIN` | the daemon binary to drive (required) |
| `-run` | all | only scenarios whose name contains this substring |
| `-scenarios` | `scenarios` | directory holding the YAML files |
| `-artifacts` | `_artifacts` | where to write the report and the event dump |
| `-version` | | print the tester version and exit |

**3. Read the report**, not the console: `_artifacts/<name>.txt` places the
failed expectation at the instant it was waiting and prints which fields
differed. `_artifacts/<name>.json` is the same run as data.

The harness's own unit tests spawn no daemon and run separately:

```bash
task test:e2e:unit
```


## Scenario file structure

Reference for the YAML files under `scenarios/`. Each file is one end-to-end
test: an ordered list of steps, run against the real daemon binary.

- [Running the tests](#running-the-tests)
- [File structure](#file-structure) — `name`, `strict_events`, `tolerate`, `fakes`, `steps`
- [Steps](#steps) — every step and every parameter
- [Placeholders](#placeholders)
- [Ordering rules](#ordering-rules)

A scenario has five sections. Only `steps` is required.

```yaml
name: full-lifecycle
strict_events: true
tolerate:
  - { source: mqtt, kind: mqtt_publish, cmd: Thing.begin }
fakes:
  provisioning_api:
    csr: [{ respond: status, status: 503 }, { respond: issue_cert }]
steps:
  - await_daemon_state: { state: Provisioning }
  - expect_publish: { cmd: Device.begin }
```

Unknown keys are an error, both at the top level and inside a step's
parameters — a misspelled name is never silently ignored.

### `name`

The scenario's name, used in the report and as the artifact file name.
Optional: defaults to the file name without its extension.

### `strict_events`

Boolean, default `false`. When `true`, the run also fails if any
**significant** event was neither claimed by a step nor listed in `tolerate`.
This is what catches traffic the daemon should not have sent.

Significant means protocol-level fact. Everything else — the daemon's log
lines, the status poller, MQTT keepalives, the harness narrating its own
injections — is timeline context and never reaches the check. The
[event catalogue](#event-catalogue) below says which is which.

Write a new scenario with `strict_events: false`, then turn it on: the report
lists exactly what you did not claim.

### `tolerate`

A list of predicates describing events that are correct but that no step
claims. Only consulted when `strict_events: true`; entries are ignored
otherwise.

**How an entry matches.** An event is forgiven when **every** key in the entry
matches it. So fewer keys means broader: `{ source: mqtt }` forgives all MQTT
traffic, while adding keys narrows it down. Entries are independent — an event
needs to match only one of them.

Only significant events ever reach the check, so those are the only ones worth
an entry.

#### Event catalogue

Which `source` and `kind` pairs exist, whether they are significant, and where
their attributes are documented.

| `source` | `kind` | Significant | Attributes |
|---|---|---|---|
| `provisioning_api` | `http_request` | **yes** | see [`expect_api_call`](#expect_api_call) |
| `provisioning_api` | `harness_note` | **yes** | `error` — the fake API's own HTTP server failed. A harness bug, not the daemon's |
| `mqtt` | `mqtt_connect` | **yes** | see [`expect_mqtt_connect`](#expect_mqtt_connect) |
| `mqtt` | `mqtt_disconnect` | **yes** | see [`expect_disconnect`](#expect_disconnect) |
| `mqtt` | `mqtt_subscribe` | **yes** | see [`expect_subscribe`](#expect_subscribe) |
| `mqtt` | `mqtt_unsubscribe` | **yes** | see [`expect_unsubscribe`](#expect_unsubscribe) |
| `mqtt` | `mqtt_publish` | **yes** | see [`expect_publish`](#expect_publish) |
| `mqtt` | `mqtt_tls_error` | **yes** | see [`expect_tls_error`](#expect_tls_error) |
| `mqtt` | `mqtt_keepalive` | no | `client_id` |
| `mqtt` | `harness_note` | no | the harness's own downlink injections, from `cloud_publish` and `cloud_publish_prop` |
| `sse` | `sse_frame` | **yes** | see [`expect_sse`](#expect_sse) |
| `sse` | `harness_note` | **yes** | `action: sse_closed` with `variable` and possibly `error`, or `note: unrecognised SSE line` with `line` |
| `daemon_process` | `process_exit` | **yes** | see [`expect_daemon_exit`](#expect_daemon_exit) |
| `daemon_process` | `harness_note` | no | `action`: `started` (with `binary`, `pid`), `sigterm`, `sigterm_unsupported` |
| `daemon_status` | `status_poll` | no | see [`await_daemon_state`](#await_daemon_state) |
| `daemon_log` | `daemon_line` | no | `line`, `stream` (`stdout` or `stderr`) |
| `ntp` | `ntp_probe` | no | `occurrence`, `bytes`, `replied` |
| `app` | `harness_note` | no | `action`: `daemon_ready`, `identity`, `sse_subscribe`, `put`, `post` |

Every source has exactly the kinds listed above and no others. Note two
asymmetries that catch people out:

- **`harness_note` is significant on `provisioning_api` and `sse`, but not on
  `mqtt`, `app` or `daemon_process`.** Significance is decided per source: all
  `sse` and all `provisioning_api` events count, whatever their kind. So
  closing a variable stream mid-scenario produces a significant
  `sse`/`harness_note` that needs claiming or tolerating.
- **`mqtt_ack` is declared in the event log but never emitted.** Using it in an
  entry is not an error and forgives nothing.

#### The other keys

Beyond `source`, `kind` and `seq`, **every key names an attribute of the
event** and constrains it to that value. Those attribute names are exactly the
ones documented for each step: the last column of the catalogue says where to
find them.

So to forgive the retried `Thing.begin`, look up [`expect_publish`](#expect_publish),
find that a command publish carries `cmd`, and write it:

```yaml
tolerate:
  - { source: mqtt, kind: mqtt_publish, cmd: Thing.begin }
```

Add more attributes to narrow it. This forgives only the value the app itself
wrote, echoed back on its own subscription, so any other unexpected frame still
fails the run:

```yaml
tolerate:
  - { source: sse, kind: sse_frame, event: update, variable: temp, value: 42 }
```

A key that the event does not carry simply never matches, so the entry forgives
nothing and the run still fails — a misspelled attribute is silent here, unlike
in a step. When an entry does not seem to work, check the name against the
step's table.

Comparison works as it does in a step: numbers are compared numerically
(`value: 42` matches a decoded `42.0`), everything else as text.

`strict_events: false` is equivalent to an entry with no keys at all, which
matches everything.

### `fakes`

Fault injection, applied before the daemon starts. Today it has one key.

`provisioning_api` holds a queue of responses per endpoint. Each request pops
the next; an exhausted queue falls back to the happy default.

```yaml
fakes:
  provisioning_api:
    csr:      [{ respond: status, status: 503 }, { respond: issue_cert }]
    complete: [{ respond: status, status: 500 }, { respond: ok }]
```

The two endpoint keys are `csr` and `complete`. Each entry takes:

| Attribute | Meaning | Values |
|---|---|---|
| `respond` | what the fake answers | see the table below |
| `status` | the HTTP status, when `respond: status` | any integer status |
| `body` | raw body override | any string |
| `delay` | wait before answering | duration, e.g. `2s` |

| `respond` | Answer |
|---|---|
| `issue_cert` | signs a real certificate for the submitted CSR — the default for `csr` |
| `ok` | 200 with an empty body — the default for `complete` |
| `status` | the `status` given, for retry scenarios |
| `malformed` | 200 with a body the daemon cannot parse: the transport succeeded, the payload did not |
| `bad_signature` | 200, well formed, signed by a key that is **not** the CA. The daemon stores the certificate and only the broker later refuses it |
| `hang` | accepts the request and never answers, so the daemon's own timeout ends it |

Because every request is an event, retries are directly assertable with
`occurrence`. Budget the timeouts: the daemon's provisioning back-off is 2s and
cannot be shortened from here, so two 503s cost about six seconds.

### `steps`

An ordered list. Each entry is a single-key mapping: the step name, and its
parameters.

```yaml
steps:
  - app_get_identity: {}
  - expect_publish: { cmd: Device.begin }
```

Steps come in two kinds:

- **Expectations** (`expect_*`, `await_*`) wait for an event and check it. Their
  parameters are **validators**: the step is satisfied only by an event whose
  every constrained field holds the value written here, so a value that does
  not match makes the test fail. See
  [parameters are validators](#parameters-are-validators).
- **Actions** (`app_*`, `cloud_*`, `stop_daemon`) make something happen: they
  call the daemon's REST API, publish as the cloud, or stop the process.

There is no sleep step and there will not be one: every wait is a condition
with a timeout.

## Steps

| Step | What it does |
|---|---|
| **Expectations** | |
| [`expect_api_call`](#expect_api_call) | waits for a request to the fake Provisioning API |
| [`expect_mqtt_connect`](#expect_mqtt_connect) | waits for the daemon to connect to the broker |
| [`expect_disconnect`](#expect_disconnect) | waits for the daemon to disconnect from the broker |
| [`expect_subscribe`](#expect_subscribe) | waits for a subscription to one topic |
| [`expect_unsubscribe`](#expect_unsubscribe) | waits for an unsubscription from one topic |
| [`expect_publish`](#expect_publish) | waits for a message the daemon publishes |
| [`expect_prop_publish`](#expect_prop_publish) | same as above, phrased for property values |
| [`expect_tls_error`](#expect_tls_error) | waits for the broker to refuse the client certificate |
| [`expect_sse`](#expect_sse) | waits for one frame on a subscribed variable stream |
| [`expect_daemon_exit`](#expect_daemon_exit) | waits for the daemon process to terminate |
| [`await_daemon_state`](#await_daemon_state) | waits for a daemon state |
| [`await_cloud_state`](#await_cloud_state) | waits for a cloud FSM state |
| [`await_provisioning_state`](#await_provisioning_state) | waits for a provisioning state |
| **Actions** | |
| [`app_get_identity`](#app_get_identity) | reads the board identity and learns `uhwid` |
| [`app_get_status`](#app_get_status) | reads the daemon status and re-learns the identifiers |
| [`app_start_provisioning`](#app_start_provisioning) | asks the daemon to provision |
| [`app_put`](#app_put) | writes a variable as an app would |
| [`app_sse_subscribe`](#app_sse_subscribe) | opens a variable's event stream |
| [`app_post`](#app_post) | POSTs to any REST endpoint (escape hatch) |
| [`cloud_publish`](#cloud_publish) | sends a command as the cloud |
| [`cloud_publish_prop`](#cloud_publish_prop) | changes a property value as the cloud |
| [`stop_daemon`](#stop_daemon) | shuts the daemon down and waits for it |

### Parameters are validators

On every `expect_*` and `await_*` step, the parameters are not a description of
what you hope to see — **they are checks, and the test fails when they do not
hold.**

The step is satisfied only by an event for which *all* the constrained fields
carry exactly the values written in the scenario. A field left out is not
checked at all; a field written down must match. So this passes only if the
daemon really announced that library version, on that topic, at that QoS — any
other combination does not satisfy the step:

```yaml
- expect_publish:
    cmd: Device.begin
    lib_version: 0.0.0-e2e-mock
    topic: "/a/d/{device_id}/c/up"
    qos: 1
```

**What happens on a mismatch.** The step keeps waiting, because the event that
arrived did not satisfy it; when `timeout` runs out, the step fails and with it
the scenario. Every step after it is skipped, and the report names the field
that differed, quoting the event that came closest:

```
 9 ✗  expect_publish   mqtt_publish mqtt cmd=DeviceNetConfig network_type=wifi ssid=WRONG timeout 15s

    4/5 constraints satisfied — differs:
        attrs.ssid             want "WRONG"           got "SSIDTEST1"
```

Two consequences worth knowing:

- A wrong value does not fail instantly — it fails after the timeout. Keep
  timeouts only as long as the thing being waited for actually needs.
- The event that did not match stays unclaimed, so with
  [`strict_events: true`](#strict_events) it is reported a second time by the
  final sweep. That is a hint, not a second bug.

**Comparison.** Values are compared for equality, tolerating the type
difference between YAML and the wire: numbers numerically, so `value: 21`
matches a decoded `21.0`; everything else as text, which is why `thing_id: ""`
works. Only equality is available.

**`timeout` is the exception**: it is the one parameter that is not checked
against the event. A duration string (`"30s"`, `"2m"`) or a bare integer
meaning seconds. **Default: 15s.**

---

### `expect_api_call`

Waits for a request to the fake Provisioning API.

| Parameter | Meaning | Values |
|---|---|---|
| `endpoint` | which endpoint was called | `provision/csr`, `provision/complete`, `unknown` |
| `occurrence` | 1-based call count for that endpoint — how you assert a retry | integer |
| `method` | HTTP method | `GET`, `POST`, … |
| `path` | the request path | string |
| `respond` | the answer the fake decided | `issue_cert`, `ok`, `status`, `malformed`, `bad_signature`, `hang` |
| `status` | the HTTP status returned | integer |
| `auth_present` | whether the board token was sent. Its **value** is never recorded | `true`, `false` |
| `csr_subject` | the CSR subject as submitted. `provision/csr` only | e.g. `"CN={uhwid}"` |
| `device_id` | the identity assigned to the certificate. `provision/csr` only | string |
| `cert_subject` | the subject of the issued certificate. `provision/csr` only | string |

```yaml
# the CSR was submitted, with the subject the daemon must send and nothing else
- expect_api_call: { endpoint: provision/csr, csr_subject: "CN={uhwid}", timeout: 30s }

# the certificate was activated
- expect_api_call: { endpoint: provision/complete, auth_present: true }

# with `fakes` queueing two 503s: the third attempt is the one that succeeds
- expect_api_call: { endpoint: provision/csr, occurrence: 3, status: 200, timeout: 30s }
```

### `expect_mqtt_connect`

Waits for the daemon to connect to the broker. The mutual-TLS handshake has
already succeeded by the time this event exists.

| Parameter | Meaning | Values |
|---|---|---|
| `client_id` | the MQTT client id the daemon used | string (the daemon uses its `device_id`) |
| `cert_cn` | common name of the certificate it presented | string |
| `client_id_matches_cert_cn` | whether the two agree. A mismatch is how a stale `device_id` or a certificate from an earlier provisioning shows up | `true`, `false` |
| `clean_session` | | the daemon sends `false` |
| `keepalive` | seconds | the daemon sends `30` |
| `protocol_version` | | the daemon sends `4` (MQTT 3.1.1) |

```yaml
# the connection proves the certificate was issued, activated and accepted
- expect_mqtt_connect: { client_id_matches_cert_cn: true, timeout: 60s }

# the full session contract, when that is what you are testing
- expect_mqtt_connect:
    client_id: "{device_id}"
    clean_session: false
    keepalive: 30
    protocol_version: 4
```

### `expect_disconnect`

Waits for the daemon to disconnect from the broker.

| Parameter | Meaning | Values |
|---|---|---|
| `client_id` | | string |
| `expire` | whether the session was expired | `true`, `false` |
| `error` | the reason, when the disconnect was not clean | string |

```yaml
# after a stop_daemon, or when testing a reconnect
- expect_disconnect: { client_id: "{device_id}" }

# the session must survive the disconnect, so it can resume
- expect_disconnect: { expire: false }
```

### `expect_subscribe`

Waits for a subscription. One event per topic filter, even when a single
SUBSCRIBE packet carries several.

| Parameter | Meaning | Values |
|---|---|---|
| `topic` | the filter subscribed to | `/a/d/{device_id}/c/dw`, `/a/t/{thing_id}/e/i` |
| `client_id` | | string |
| `qos` | requested QoS | the daemon uses `1` |
| `reason_code` | the SUBACK code | `1` on success (QoS 1 granted) |

```yaml
# the command channel, subscribed before the daemon announces itself
- expect_subscribe: { topic: "/a/d/{device_id}/c/dw" }

# the property channel, only possible once a thing_id is known
- expect_subscribe: { topic: "/a/t/{thing_id}/e/i", qos: 1, reason_code: 1 }
```

### `expect_unsubscribe`

Waits for an unsubscription.

| Parameter | Meaning | Values |
|---|---|---|
| `topic` | the filter dropped | string |
| `client_id` | | string |

```yaml
# what the daemon does on Thing.detach
- expect_unsubscribe: { topic: "/a/t/{thing_id}/e/i" }
```

### `expect_publish`

Waits for a message the daemon publishes. Always available:

| Parameter | Meaning | Values |
|---|---|---|
| `topic` | | `/a/d/<device_id>/c/up`, `/a/t/<thing_id>/e/o` |
| `direction` | | always `uplink` — the harness's own injections are not matchable here |
| `client_id` | | string |
| `qos` | | the daemon uses `1` |
| `retain` | | `true`, `false` |
| `size` | payload bytes | integer |
| `decode_error` | set when the payload did not decode. A wire-format regression shows up here instead of as a timeout | string |

On a **command** topic (`/c/up`, `/c/dw`) the payload is decoded too:

| Parameter | Meaning | Values |
|---|---|---|
| `cmd` | the command name | `Device.begin`, `DeviceNetConfig`, `Thing.begin`, `Thing.update`, `Thing.detach`, `LastValues.begin`, `LastValues.update`, `Timezone.request`, `Timezone.update`, `OTA.begin`, `OTA.update`, `OTA.progress` |
| `cmd_tag` | the CBOR tag | e.g. `0x10700` |

…plus the fields of that particular command:

| `cmd` | Fields |
|---|---|
| `Device.begin` | `lib_version` |
| `Thing.begin`, `Thing.update`, `Thing.detach` | `thing_id` (empty string on the first `Thing.begin`) |
| `DeviceNetConfig` | `network_type` (`unknown`, `wifi`, `lora`, `gsm`, `nb`, `catm1`, `ethernet`, `cellular`); `ssid` when wifi; `ip`, `dns`, `gateway`, `netmask` (hex) when ethernet |
| `LastValues.begin`, `Timezone.request` | none |
| `LastValues.update` | `values_len` |
| `Timezone.update` | `offset`, `until` |
| `OTA.begin` | `sha256` (hex) |
| `OTA.update` | `id`, `url`, `initial_sha`, `final_sha` |
| `OTA.progress` | `id`, `state`, `state_data`, `timestamp` |

On a **property** topic (`/e/o`, `/e/i`) the SenML payload is decoded instead:

| Parameter | Meaning | Values |
|---|---|---|
| `values` | how many values the message carried | integer |
| `value.<name>` | the value of that variable, e.g. `value.temp` | number, string or bool |
| `variable` | the variable name — single-value messages only | string |
| `value` | its value — single-value messages only | number, string or bool |

```yaml
# the daemon announcing itself
- expect_publish: { cmd: Device.begin }

# the first Thing.begin asks for a thing: the empty id is the assertion
- expect_publish: { cmd: Thing.begin, thing_id: "" }

# deterministic only under -tags mock
- expect_publish: { cmd: DeviceNetConfig, network_type: wifi, ssid: SSIDTEST1 }

# a specific version, on a specific topic
- expect_publish:
    cmd: Device.begin
    lib_version: 0.0.0-e2e-mock
    topic: "/a/d/{device_id}/c/up"
    qos: 1
```

### `expect_prop_publish`

Identical to `expect_publish` — same event, same parameters. The separate name
exists because it reads better next to a property topic.

```yaml
# a value the app wrote, on its way to the cloud
- expect_prop_publish: { topic: "/a/t/{thing_id}/e/o", variable: temp, value: 42.0 }

# a batch: assert the count and each value by name
- expect_prop_publish:
    topic: "/a/t/{thing_id}/e/o"
    values: 2
    value.temp: 42.0
    value.humidity: 61
```

### `expect_tls_error`

Waits for the broker to refuse the client certificate. A refused handshake is
its own event so that it can never be mistaken for a connect, and so it does
not degrade into a timeout with no explanation.

| Parameter | Meaning | Values |
|---|---|---|
| `reason` | why it was refused | e.g. `no certificate presented`, `certificate does not parse: …` |
| `cert_cn` | common name, when the certificate parsed | string |
| `cert_issuer` | issuer, when the certificate parsed | string |

```yaml
# pair it with `fakes: provisioning_api: csr: [{respond: bad_signature}]`:
# the daemon stores the certificate and only the broker refuses it
- expect_tls_error: { cert_cn: "{device_id}", timeout: 60s }
```

### `expect_sse`

Waits for one frame on a variable stream opened by `app_sse_subscribe`.

| Parameter | Meaning | Values |
|---|---|---|
| `variable` | the stream the frame arrived on | string |
| `event` | the SSE event name | `lastvalue`, `lastvalue_missing`, `thing_unavailable`, `update` |
| `name` | the variable name in the payload | string |
| `value` | the value | number, string or bool |
| `timestamp` | when the daemon stamped it | RFC 3339 string |
| `last_value` | present and `true` on a sync frame | `true` |
| `decode_error` | set when the payload was not JSON | string |

The **first** frame on a stream is always a sync frame, and which one depends
on the daemon's state: `thing_unavailable` while the cloud is not steady,
`lastvalue` when the variable has a cloud value, `lastvalue_missing` when it
does not. Subscribe after `await_cloud_state: { state: Steady }` if you want it
to be deterministic. Every later change is `update`.

```yaml
# the sync frame, when the variable already has a cloud value
- expect_sse: { event: lastvalue, variable: temp, value: 21.5, last_value: true }

# a live change pushed by the cloud
- expect_sse: { event: update, variable: temp, value: 30.0 }

# subscribing before the cloud is steady gets this instead
- expect_sse: { event: thing_unavailable, variable: temp }

# a variable the cloud has never had a value for
- expect_sse: { event: lastvalue_missing, variable: humidity }
```

### `expect_daemon_exit`

Waits for the daemon process to terminate.

| Parameter | Meaning | Values |
|---|---|---|
| `exit_code` | | integer |
| `expected` | whether something announced the exit | `true`, `false` |
| `signal` | the signal that ended it, when there was one | string |
| `panic` | the panic line, when it panicked | string |

Only usable after `stop_daemon`: an exit nobody announced **aborts the whole
scenario** immediately, with the daemon's last stderr lines attached, rather
than letting every following expectation time out in turn.

```yaml
- stop_daemon: {}
- expect_daemon_exit: { exit_code: 0, expected: true }
```

### `await_daemon_state`

Waits for a status poll reporting the given daemon state.

| Parameter | Meaning | Values |
|---|---|---|
| `state` | the daemon's top-level state | `CheckInternet`, `Provisioning`, `Run` |
| `provisioning` | the provisioning state in the same poll | `unprovisioned`, `provisioning`, `provisioned`, `error` |
| `cloud_state` | the cloud FSM state in the same poll | `Disconnected`, `Reconnecting`, `Connecting`, `AnnouncingDevice`, `AwaitingThingID`, `SyncingLastValues`, `Steady` |
| `device_id` | | string, absent while empty |
| `thing_id` | | string, absent while empty |
| `organization_id` | | string, absent while empty |

One poll carries all of these at once, so a single step can assert the state
*and* the identifiers. Polls are recorded only when something changes.

```yaml
# with no credentials on disk the daemon waits here, doing nothing
- await_daemon_state: { state: Provisioning, provisioning: unprovisioned, timeout: 30s }

# provisioning is done and the cloud FSM has taken over
- await_daemon_state: { state: Run, device_id: "{device_id}", timeout: 30s }
```

### `await_cloud_state`

The same poll, with `state` reading the cloud FSM state instead
(`Disconnected`, `Reconnecting`, `Connecting`, `AnnouncingDevice`,
`AwaitingThingID`, `SyncingLastValues`, `Steady`). Every other parameter above
still applies.

```yaml
# the handshake finished, and the daemon agrees with the harness on both ids
- await_cloud_state: { state: Steady, thing_id: "{thing_id}", device_id: "{device_id}" }

# after cutting the broker, the daemon must come back by itself
- await_cloud_state: { state: Reconnecting, timeout: 60s }
```

### `await_provisioning_state`

The same poll, with `state` reading the provisioning state — **lower case**.
Every other parameter above still applies.

```yaml
# the credentials are on disk and the certificate is activated
- await_provisioning_state: { state: provisioned, timeout: 30s }

# it ran out of its retry window: pair this with a `fakes` queue of failures
- await_provisioning_state: { state: error, timeout: 60s }
```

---

### `app_get_identity`

Reads `GET /v1/identity` and puts `uhwid` in the bag — the one value only the
daemon knows, and what lets a later step assert the CSR subject.

**No parameters.** Fails if the daemon returns no uhwid, or no board token: the
token authenticates every provisioning call, and without it the next step fails
with an opaque 401 instead of naming the cause. The token is deliberately kept
out of the bag, because the bag is written to the artifact.

```yaml
- app_get_identity: {}
- expect_api_call: { endpoint: provision/csr, csr_subject: "CN={uhwid}" }
```

### `app_get_status`

Reads `GET /v1/status` and re-learns `device_id`, `thing_id` and
`organization_id` from the daemon, replacing what the harness seeded. Its
reason to exist is re-provisioning, where the daemon takes a **new**
`device_id`. An empty field is never learned, so a seed is never blanked.

| Parameter | Meaning | Values |
|---|---|---|
| `require` | fields that must be present, so the failure lands on this step instead of later on a topic built from a stale value | a list of `device_id`, `thing_id`, `organization_id` |

```yaml
# after a second provisioning: pick up the new identity and use it
- app_get_status: { require: [device_id] }
- expect_subscribe: { topic: "/a/d/{device_id}/c/dw" }

# just record what the daemon reports, requiring nothing
- app_get_status: {}
```

### `app_start_provisioning`

POSTs `/v1/provisioning/start`, which is what App Lab does and the only way out
of the `Provisioning` state. Expects 202; a 409 means one was already running,
which is a different failure from an unreachable daemon.

| Parameter | Meaning | Values |
|---|---|---|
| `organization_id` | optional, exactly as in the API — a board can be provisioned without one. Also stored in the bag | string |

```yaml
# the plain case
- app_start_provisioning: {}

# with an organization, which then becomes available as {organization_id}
- app_start_provisioning: { organization_id: 6f1c2d3e-4567-89ab-cdef-0123456789ab }
```

### `app_put`

`PUT /v1/variables/{name}` — the app writing a variable.

| Parameter | Meaning | Values |
|---|---|---|
| `variable` | the variable name (required) | string |
| `value` | the value to write | number, string or bool |

The daemon answers 409 unless the cloud is `Steady`. Note that the write also
comes back on the app's own stream as an `update` frame.

```yaml
- app_put: { variable: temp, value: 42.0 }
- expect_prop_publish: { topic: "/a/t/{thing_id}/e/o", variable: temp, value: 42.0 }
```

```yaml
# values do not have to be numbers
- app_put: { variable: led, value: true }
- app_put: { variable: label, value: "kitchen" }
```

### `app_sse_subscribe`

Opens `GET /v1/variables/{name}/events`. Returns once the response headers are
in, so a following injection cannot race it; the stream stays open until the
scenario ends.

| Parameter | Meaning | Values |
|---|---|---|
| `variable` | the variable to stream (required) | string |

```yaml
- app_sse_subscribe: { variable: temp }
- expect_sse: { event: lastvalue, variable: temp, value: 21.5 }
```

### `app_post`

Escape hatch for a REST endpoint with no named step yet.

| Parameter | Meaning | Values |
|---|---|---|
| `path` | the path to POST to (required) | e.g. `/v1/provisioning/start` |
| `body` | JSON body | a mapping |
| `status` | the status to require | integer |

```yaml
- app_post: { path: /v1/provisioning/start, status: 202 }
```

```yaml
# with a body
- app_post:
    path: /v1/provisioning/start
    body: { organization_id: 6f1c2d3e-4567-89ab-cdef-0123456789ab }
    status: 202
```

### `cloud_publish`

Publishes a command on the device's downlink topic, as the cloud would.

| Parameter | Meaning | Values |
|---|---|---|
| `cmd` | which command (required) | `Thing.update`, `Thing.detach`, `LastValues.update` — only what the cloud actually sends; anything else is an error |
| `thing_id` | for `Thing.update` and `Thing.detach` | string |
| `values` | for `LastValues.update`: a list of `{name, value, time}` | `time` is optional |

```yaml
# attach a thing, which is what unblocks the retried Thing.begin
- cloud_publish: { cmd: Thing.update, thing_id: "{thing_id}" }

# answer the last-values request and let the daemon reach Steady
- cloud_publish:
    cmd: LastValues.update
    values:
      - { name: temp, value: 21.5 }
      - { name: led, value: true }

# detach it again
- cloud_publish: { cmd: Thing.detach, thing_id: "{thing_id}" }
```

### `cloud_publish_prop`

Publishes property values on the thing's inbound topic — what an operator
changing a value in the Cloud UI produces.

| Parameter | Meaning | Values |
|---|---|---|
| `variable` | one variable's name | string |
| `value` | its value | number, string or bool |
| `values` | several at once, instead of `variable`/`value`: a list of `{name, value, time}` | |
| `thing_id` | defaults to `{thing_id}` from the bag | string |

Give either `variable` + `value` or `values`.

```yaml
# one variable changed in the Cloud UI
- cloud_publish_prop: { variable: temp, value: 30.0 }
- expect_sse: { event: update, variable: temp, value: 30.0 }
```

```yaml
# several at once
- cloud_publish_prop:
    values:
      - { name: temp, value: 30.0 }
      - { name: humidity, value: 61 }
```

### `stop_daemon`

Shuts the daemon down and waits for it to go. This is what announces the exit,
so it must come before `expect_daemon_exit`.

| Parameter | Meaning | Values |
|---|---|---|
| `timeout` | how long to wait for the process to leave | duration, default 15s |

On a host without SIGTERM the process is killed instead, and the report says
the graceful path was not exercised — a shutdown scenario should check that
rather than pass on a kill.

```yaml
- stop_daemon: { timeout: 30s }
- expect_daemon_exit: { exit_code: 0, expected: true }
```

## Placeholders

`{key}` anywhere in a parameter is replaced from the variable bag at the moment
the step runs, so a value the daemon learns later can be written in step 3. An
unknown key is an **error**, never left in place as text.

| Key | Available from |
|---|---|
| `device_id` | the start — the identity the fake Provisioning API will assign |
| `thing_id` | the start |
| `api_url`, `broker_url`, `ntp_addr`, `daemon_url`, `data_dir` | the start |
| `uhwid` | after `app_get_identity` |
| `organization_id` | after `app_start_provisioning` with one, or `app_get_status` |

The bag is written to the artifact, so no credential is ever put in it.

## Ordering rules

Three things to know, because they decide where a step can go:

1. **The order of the steps is the ordering assertion.** Each expectation
   resumes scanning where the previous one stopped, so "Thing.begin after
   Device.begin" needs no syntax. Actions do not move that position.
2. **Do not order two events that race against each other.** A value the app
   writes is stored (reaching the SSE stream) before it is published to the
   broker, but one path is an HTTP stream and the other an MQTT round trip.
   Assert one and put the other in `tolerate`.
3. **Do not wait on a state between two protocol events.** Status polls are
   recorded only on change, so the poll that first reports `provisioned` can
   arrive *after* the MQTT connect; consuming it would skip past the connect
   and the next step would wait forever. Prefer the protocol event as the
   proof, and constrain the identifiers on a poll you are already waiting for.

## Not available yet

- **Silencing the NTP probe** from a scenario: `fakes` only has
  `provisioning_api`, so a connectivity-loss scenario needs a `fakes: ntp:` key
  first.
- **Injecting a malformed MQTT payload**: `cloud_publish` and
  `cloud_publish_prop` only produce well-formed messages.
- **Changing timers**: the status poll interval, the readiness timeout and the
  daemon's own back-offs are fixed. Only the per-step `timeout` is yours.
- **Operators other than equality**: `ne`, `contains`, `prefix`, `gt` and `lt`
  exist in the event log but no step exposes them.
