// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

// Package cmdslot is the board's single command slot. The board carries out one
// Cloud command at a time, whatever its kind — an App deploy (RFC-14) and an App
// lifecycle command (RFC-16) exclude each other as much as two of either do — and
// the first to arrive wins: the others are refused, not queued, with the "another
// command in progress" code of their own protocol (-49 in both).
//
// Refusing rather than queueing is the point. A queued command would start minutes
// or hours later, for a request the Cloud has long since timed out.
package cmdslot

import "sync"

// Slot admits one holder at a time. The zero value is free and ready to use.
type Slot struct {
	mu     sync.Mutex
	holder string
}

// TryAcquire takes the slot for holder if it is free, and reports whether it did.
// It never blocks.
func (s *Slot) TryAcquire(holder string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holder != "" {
		return false
	}
	s.holder = holder
	return true
}

// Release frees the slot if holder holds it. Releasing a slot someone else holds is
// a no-op, so a late or repeated release can never free another command's slot.
func (s *Slot) Release(holder string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holder == holder {
		s.holder = ""
	}
}

// Holder names who holds the slot, "" when it is free. For logs.
func (s *Slot) Holder() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.holder
}
