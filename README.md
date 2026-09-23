# arduino-cloud-connector

Arduino IoT Cloud connector for Linux boards.

`arduino-cloud-connector` is a Go systemd daemon that connects a Linux board (Uno Q and
Ventuno Q) to Arduino IoT Cloud. It runs in
the background and handles the whole board-to-cloud lifecycle.
It takes care of:

- **Provisioning** — It registers the board to Arduino Cloud and establishes a secure communication with it.
- **Persistent mTLS MQTT connection** to the Arduino Cloud broker and autonomous reconnection with back-off.
- **Cloud variable exchange** — keeps the board-side and cloud-side values of each
  variable in sync, applying a conflict-resolution policy (device wins, cloud wins,
  or most-recent wins) and replaying the cloud "last value" on every (re)connection
  so actuators are restored to the state they should be in.
- **A localhost API** consumed by App Lab and Cloud Brick Apps to read the board
  identity, trigger provisioning, inspect the daemon status and exchange cloud
  variables.

## Local API

The daemon exposes a REST API on `127.0.0.1` and, for apps running in containers, an
equivalent UNIX domain socket. The full contract is described in
[`docs/openapi.yaml`](docs/openapi.yaml). In short:

- **App Lab** reads the board identity (`GET /v1/identity`), inspects status
  (`GET /v1/status`) and triggers (re)provisioning (`POST /v1/provisioning/start`).
- **Cloud Brick Apps** exchange cloud variables:
  - `PUT /v1/variables/{name}` publishes a value to the cloud;
  - `GET /v1/variables/{name}/events` streams value updates as Server-Sent Events.

## Requirements

- [Task](https://taskfile.dev) and Docker — to build the `.deb` package.
- `adb` (or another transfer method) — to copy the package onto a board.

## Building the Debian package

The board runs the daemon from a `.deb` package, cross-compiled inside Docker:

```sh
task build-deb                  # arm64 by default → ./build/*.deb
task build-deb ARCH=arm64       # explicit architecture
```

The resulting package bundles the Arduino root CA certificates and installs them
into the system trust store, so the daemon trusts the Arduino Cloud broker out of
the box. On install it registers and starts the `arduino-cloud-connector` systemd
service.

## Installing on a board

Copy the generated `.deb` onto the board (for example over `adb`), install it with
`dpkg -i` and let the affected services restart. Once installed, the daemon runs as
the `arduino-cloud-connector` systemd service.

## Configuration

Configuration is read from the environment with the `ARDUINO_CLOUD_CONNECTOR__`
prefix (see [`internal/config`](internal/config)). The available settings cover the
data directory, the API port, the UNIX socket path, the MQTT broker and Provisioning
API endpoints, an optional MQTT CA file, the host used for the connectivity probe,
and the log level.
