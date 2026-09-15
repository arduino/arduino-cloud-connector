module github.com/arduino/arduino-cloud-connector/test/e2e

go 1.26.4

require (
	github.com/arduino/arduino-cloud-connector v0.0.0
	golang.org/x/crypto v0.55.0
)

replace github.com/arduino/arduino-cloud-connector => ../..
