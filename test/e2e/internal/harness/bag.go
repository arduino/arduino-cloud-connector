// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package harness

import (
	"fmt"
	"sort"
	"strings"
	"sync"
)

// Bag is the scenario-scoped variable bag: the only dynamic thing a scenario
// file can refer to.
//
// It is what keeps YAML free of logic. A scenario writes "{thing_id}" and the
// runner substitutes it at the moment the step runs, so the file needs no
// expressions, no assignments and no way to compute anything -- the values
// either come from the harness at setup or are learned from the daemon as the
// scenario progresses.
type Bag struct {
	mu     sync.RWMutex
	values map[string]string
}

// NewBag returns an empty bag.
func NewBag() *Bag {
	return &Bag{values: map[string]string{}}
}

// Set stores a value, replacing any previous one.
func (b *Bag) Set(key, value string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.values[key] = value
}

// Get reads a value.
func (b *Bag) Get(key string) (string, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	v, ok := b.values[key]
	return v, ok
}

// MustGet reads a value the harness itself set, and panics when it is missing.
// Only for keys Setup always provides.
func (b *Bag) MustGet(key string) string {
	v, ok := b.Get(key)
	if !ok {
		panic(fmt.Sprintf("harness: bag has no %q (have %s)", key, strings.Join(b.Keys(), ", ")))
	}
	return v
}

// Keys lists what the bag holds, sorted, for an error message that helps.
func (b *Bag) Keys() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	keys := make([]string, 0, len(b.values))
	for k := range b.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Snapshot copies the bag, for the artifact dump.
func (b *Bag) Snapshot() map[string]string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	out := make(map[string]string, len(b.values))
	for k, v := range b.values {
		out[k] = v
	}
	return out
}

// Interpolate replaces every {placeholder} with its value.
//
// An unknown placeholder is an ERROR, not a literal left in place. A scenario
// that asks for a topic "/a/t/{thing_id}/e/i" before the thing id is known
// must fail saying so; substituting nothing would build a topic that silently
// never matches, and the report would blame the expectation instead of the
// scenario.
//
// The syntax is deliberately this small. Anything more -- defaults, nesting,
// arithmetic -- would be an expression language, and the rule is that a
// scenario needing logic gets a new Go primitive instead.
func (b *Bag) Interpolate(s string) (string, error) {
	if !strings.ContainsRune(s, '{') {
		return s, nil
	}

	var out strings.Builder
	rest := s
	for {
		open := strings.IndexRune(rest, '{')
		if open < 0 {
			out.WriteString(rest)
			return out.String(), nil
		}
		closeAt := strings.IndexRune(rest[open:], '}')
		if closeAt < 0 {
			return "", fmt.Errorf("unclosed placeholder in %q", s)
		}
		closeAt += open

		key := rest[open+1 : closeAt]
		if key == "" {
			return "", fmt.Errorf("empty placeholder in %q", s)
		}
		value, ok := b.Get(key)
		if !ok {
			return "", fmt.Errorf("unknown placeholder {%s} in %q (the bag holds %s)",
				key, s, strings.Join(b.Keys(), ", "))
		}
		out.WriteString(rest[:open])
		out.WriteString(value)
		rest = rest[closeAt+1:]
	}
}
