// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package command

import (
	"fmt"

	"github.com/fxamacker/cbor/v2"
)

// The App lifecycle messages (RFC-16 §3). Their arrays have a variable length — the
// apps list grows with the Apps, the command's parameter is optional, a successful
// response has no details — which the codec TagSet's fixed `toarray` layout cannot
// express, so every type marshals itself, tag included, like DeviceNetConfigCmd.
const (
	tagAppsList           uint64 = 0x013000
	tagAppCommand         uint64 = 0x013001
	tagAppCommandResponse uint64 = 0x013002
)

// AppStatus is the state of an App in an AppsListCmd.
type AppStatus int64

const (
	AppRunning AppStatus = 1
	AppStopped AppStatus = 0
	AppFailure AppStatus = -1
)

// AppInfo is one App of an AppsListCmd.
type AppInfo struct {
	// Name is "<name>:<version>" (RFC-16), the same string an AppCommandCmd names.
	Name    string
	Status  AppStatus
	Default bool
}

// AppsListCmd (tag 0x013000, Up) reports every App installed on the board (RFC-16 §3)
// as one flat array, three elements per App:
//
//	[name_1, status_1, default_1, name_2, status_2, default_2, …]
//
// An empty array is legal and means the board has no Apps — never "not known yet".
type AppsListCmd struct {
	Apps []AppInfo
}

// MarshalCBOR implements cbor.Marshaler.
func (c AppsListCmd) MarshalCBOR() ([]byte, error) {
	content := make([]any, 0, 3*len(c.Apps))
	for _, a := range c.Apps {
		content = append(content, a.Name, int64(a.Status), a.Default)
	}
	return cbor.Marshal(cbor.Tag{Number: tagAppsList, Content: content})
}

// UnmarshalCBOR implements cbor.Unmarshaler, so the shape round-trips in tests.
func (c *AppsListCmd) UnmarshalCBOR(data []byte) error {
	var items []cbor.RawMessage
	if err := cbor.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("apps list: %w", err)
	}
	if len(items)%3 != 0 {
		return fmt.Errorf("apps list: array of %d elements, want a multiple of 3", len(items))
	}
	out := AppsListCmd{Apps: make([]AppInfo, len(items)/3)}
	for i := range out.Apps {
		a := &out.Apps[i]
		if err := cbor.Unmarshal(items[3*i], &a.Name); err != nil {
			return fmt.Errorf("apps list: app %d name: %w", i, err)
		}
		if err := cbor.Unmarshal(items[3*i+1], (*int64)(&a.Status)); err != nil {
			return fmt.Errorf("apps list: app %d status: %w", i, err)
		}
		if err := cbor.Unmarshal(items[3*i+2], &a.Default); err != nil {
			return fmt.Errorf("apps list: app %d default: %w", i, err)
		}
	}
	*c = out
	return nil
}

// AppCommand is what an AppCommandCmd asks for.
type AppCommand int64

const (
	AppStop       AppCommand = 0
	AppStart      AppCommand = 1
	AppDelete     AppCommand = 2
	AppSetDefault AppCommand = 3
)

func (c AppCommand) String() string {
	switch c {
	case AppStop:
		return "stop"
	case AppStart:
		return "start"
	case AppDelete:
		return "delete"
	case AppSetDefault:
		return "set-default"
	default:
		return fmt.Sprintf("unknown(%d)", int64(c))
	}
}

// AppCommandCmd (tag 0x013001, Down) asks the board to act on one of its Apps
// (RFC-16 §3):
//
//	[app_name, command, operation_id]
//	[app_name, command, operation_id, parameter]
type AppCommandCmd struct {
	// AppName names the App. RFC-16 writes it "<name>:<version>"; the daemon treats
	// it as opaque and hands it to arduino-app-cli verbatim as the App id.
	AppName string
	Command AppCommand
	// OperationID is the Cloud's id of the request, echoed in every response to it.
	OperationID string
	// Parameter refines the command, nil when the Cloud sent none. For a start, 1
	// (or none) means stop every running App first, 0 means fail if another App is
	// running.
	Parameter *int64
}

// MarshalCBOR implements cbor.Marshaler. The daemon never sends this command; it
// exists so tests can build the exact bytes the Cloud sends.
func (c AppCommandCmd) MarshalCBOR() ([]byte, error) {
	content := []any{c.AppName, int64(c.Command), c.OperationID}
	if c.Parameter != nil {
		content = append(content, *c.Parameter)
	}
	return cbor.Marshal(cbor.Tag{Number: tagAppCommand, Content: content})
}

// UnmarshalCBOR implements cbor.Unmarshaler. It receives the tag's content: the codec
// has already matched the tag.
func (c *AppCommandCmd) UnmarshalCBOR(data []byte) error {
	var items []cbor.RawMessage
	if err := cbor.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("app command: %w", err)
	}
	if len(items) != 3 && len(items) != 4 {
		return fmt.Errorf("app command: array of %d elements, want 3 or 4", len(items))
	}
	var out AppCommandCmd
	if err := cbor.Unmarshal(items[0], &out.AppName); err != nil {
		return fmt.Errorf("app command: app name: %w", err)
	}
	if err := cbor.Unmarshal(items[1], (*int64)(&out.Command)); err != nil {
		return fmt.Errorf("app command: command: %w", err)
	}
	if err := cbor.Unmarshal(items[2], &out.OperationID); err != nil {
		return fmt.Errorf("app command: operation id: %w", err)
	}
	if len(items) == 4 {
		var p int64
		if err := cbor.Unmarshal(items[3], &p); err != nil {
			return fmt.Errorf("app command: parameter: %w", err)
		}
		out.Parameter = &p
	}
	*c = out
	return nil
}

// AppCommandStatus is the status of an AppCommandResponseCmd.
type AppCommandStatus int64

const (
	AppCommandSucceeded  AppCommandStatus = 0
	AppCommandFailed     AppCommandStatus = 1
	AppCommandInProgress AppCommandStatus = 2
)

// AppCommandResponseCmd (tag 0x013002, Up) reports on an AppCommandCmd, keyed by its
// operation id (RFC-16 §3):
//
//	[operation_id, 2, percent]   in progress
//	[operation_id, 0]            succeeded: no details
//	[operation_id, 1, code]      failed, with a negative error code
type AppCommandResponseCmd struct {
	OperationID string
	Status      AppCommandStatus
	// Details is the percentage while in progress and the error code on failure. It
	// is nil — and absent from the wire — on success.
	Details *int32
}

// MarshalCBOR implements cbor.Marshaler.
func (c AppCommandResponseCmd) MarshalCBOR() ([]byte, error) {
	content := []any{c.OperationID, int64(c.Status)}
	if c.Details != nil {
		content = append(content, int64(*c.Details))
	}
	return cbor.Marshal(cbor.Tag{Number: tagAppCommandResponse, Content: content})
}

// UnmarshalCBOR implements cbor.Unmarshaler, so the shape round-trips in tests.
func (c *AppCommandResponseCmd) UnmarshalCBOR(data []byte) error {
	var items []cbor.RawMessage
	if err := cbor.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("app command response: %w", err)
	}
	if len(items) != 2 && len(items) != 3 {
		return fmt.Errorf("app command response: array of %d elements, want 2 or 3", len(items))
	}
	var out AppCommandResponseCmd
	if err := cbor.Unmarshal(items[0], &out.OperationID); err != nil {
		return fmt.Errorf("app command response: operation id: %w", err)
	}
	if err := cbor.Unmarshal(items[1], (*int64)(&out.Status)); err != nil {
		return fmt.Errorf("app command response: status: %w", err)
	}
	if len(items) == 3 {
		var d int32
		if err := cbor.Unmarshal(items[2], &d); err != nil {
			return fmt.Errorf("app command response: details: %w", err)
		}
		out.Details = &d
	}
	*c = out
	return nil
}
