// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package cmdslot

import (
	"sync"
	"testing"
)

func TestSlotAdmitsOneHolderAtATime(t *testing.T) {
	var s Slot
	if !s.TryAcquire("ota") {
		t.Fatal("a free slot refused its first holder")
	}
	if s.TryAcquire("app:op-1") {
		t.Fatal("a held slot admitted a second holder")
	}
	if got := s.Holder(); got != "ota" {
		t.Errorf("holder: got %q want ota", got)
	}
	s.Release("ota")
	if !s.TryAcquire("app:op-1") {
		t.Fatal("a released slot refused the next holder")
	}
}

// A release by someone who does not hold the slot must not free it: that would let
// two commands run at once.
func TestSlotReleaseByANonHolderIsANoOp(t *testing.T) {
	var s Slot
	s.TryAcquire("app:op-1")
	s.Release("ota")
	if got := s.Holder(); got != "app:op-1" {
		t.Errorf("a stranger's release freed the slot: holder %q", got)
	}
}

func TestSlotIsSafeForConcurrentAcquires(t *testing.T) {
	var (
		s   Slot
		wg  sync.WaitGroup
		mu  sync.Mutex
		won int
	)
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if s.TryAcquire(string(rune('a' + i%26))) {
				mu.Lock()
				won++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if won != 1 {
		t.Errorf("%d holders acquired the slot at once, want 1", won)
	}
}
