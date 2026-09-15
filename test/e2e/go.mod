module github.com/arduino/arduino-cloud-connector/test/e2e

go 1.26.4

require (
	github.com/arduino/arduino-cloud-connector v0.0.0
	github.com/eclipse/paho.mqtt.golang v1.5.1
	github.com/mochi-mqtt/server/v2 v2.7.9
	golang.org/x/crypto v0.55.0
	gopkg.in/yaml.v3 v3.0.1
)

require (
	github.com/fxamacker/cbor/v2 v2.9.2 // indirect
	github.com/gorilla/websocket v1.5.3 // indirect
	github.com/rs/xid v1.4.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	golang.org/x/net v0.58.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
)

replace github.com/arduino/arduino-cloud-connector => ../..
