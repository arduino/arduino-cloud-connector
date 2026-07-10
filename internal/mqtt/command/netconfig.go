// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package command

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// tagDeviceNetConfig is the CBOR tag for DeviceNetConfigCmd. Unlike the other
// commands it is emitted by MarshalCBOR, not the codec TagSet (which skips types
// implementing cbor.Marshaler); it stays in registeredTags only so Cmd.String
// can name it.
const tagDeviceNetConfig uint64 = 0x011100

// NetworkType is the adapter typeID the cloud expects as the first array element
// of DeviceNetConfigCmd (C++ getEncodingParams).
type NetworkType uint8

const (
	NetworkTypeUnknown  NetworkType = 0
	NetworkTypeWiFi     NetworkType = 1
	NetworkTypeLoRa     NetworkType = 2
	NetworkTypeGSM      NetworkType = 3
	NetworkTypeNB       NetworkType = 4
	NetworkTypeCatM1    NetworkType = 5
	NetworkTypeEthernet NetworkType = 6
	NetworkTypeCellular NetworkType = 7
)

// DeviceNetConfigCmd (tag 0x011100, Up) reports the active network adapter so
// the connection method is visible in the Cloud UI. Sent once per connection,
// between DeviceBeginCmd and ThingBeginCmd (C++ handleSendCapabilities order).
//
// Wire format is a CBOR array, first element Type, the rest depending on Type:
//
//	WiFi:     [1, ssid]
//	Ethernet: [6, ip, dns, gateway, netmask]   (byte strings; empty = unset/DHCP)
//	other:    [type]
type DeviceNetConfigCmd struct {
	Type NetworkType

	// SSID is encoded only for NetworkTypeWiFi.
	SSID string

	// Ethernet addresses in network byte order (4 bytes IPv4, 16 IPv6), encoded
	// only for NetworkTypeEthernet. Empty/nil becomes a zero-length byte string
	// (the cloud reads it as unset/DHCP).
	IP      []byte
	DNS     []byte
	Gateway []byte
	Netmask []byte
}

// netConfigEM encodes nil slices as zero-length byte strings (0x40), not CBOR
// null, so an unset Ethernet address matches the firmware's empty-IP encoding.
var netConfigEM cbor.EncMode

func init() {
	em, err := cbor.EncOptions{NilContainers: cbor.NilContainerAsEmpty}.EncMode()
	if err != nil {
		panic(fmt.Errorf("command: build DeviceNetConfig EncMode: %w", err))
	}
	netConfigEM = em
}

// MarshalCBOR emits the tag and the type-dependent array directly (see
// tagDeviceNetConfig for why the codec TagSet can't).
func (c DeviceNetConfigCmd) MarshalCBOR() ([]byte, error) {
	var content []interface{}
	switch c.Type {
	case NetworkTypeWiFi:
		content = []interface{}{uint8(c.Type), c.SSID}
	case NetworkTypeEthernet:
		content = []interface{}{uint8(c.Type), c.IP, c.DNS, c.Gateway, c.Netmask}
	default:
		content = []interface{}{uint8(c.Type)}
	}
	return netConfigEM.Marshal(cbor.Tag{Number: tagDeviceNetConfig, Content: content})
}
