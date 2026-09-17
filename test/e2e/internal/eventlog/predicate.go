// This file is part of arduino-cloud-connector.
//
// SPDX-FileCopyrightText: Arduino s.r.l. and/or its affiliated companies
// SPDX-License-Identifier: GPL-3.0-or-later

package eventlog

import (
	"fmt"
	"sort"
	"strings"
)

// A Predicate is DATA, not a func(Event) bool, and that is a deliberate
// constraint rather than an accident of style.
//
// A closure can only answer yes/no. When a scenario times out waiting for
// something, "no" is nearly useless: the interesting question is which events
// came CLOSE and on which field they differed. Decomposing the predicate into
// named constraints gives that for free — every match yields a score and a
// per-field want/got — which is what turns a bare "timeout after 15s" into
//
//	3/4 constraints satisfied — differs:
//	    attrs.lib_version    want "0.0.0-e2e"    got "0.0.0-dev"
//
// It also maps one-to-one onto the YAML step parameters, so the scenario file
// stays declarative and never grows an expression language.
type Predicate struct {
	// Label is how the expectation is printed in a failure report, e.g.
	// "mqtt PUBLISH cmd=Device.begin".
	Label       string
	Constraints []Constraint
}

// Constraint is one field comparison. Field is "source", "kind", "seq", or
// "attrs.<name>".
type Constraint struct {
	Field string
	Op    Op
	Want  any
}

// Op is the comparison operator.
type Op string

const (
	OpEq       Op = "eq"
	OpNe       Op = "ne"
	OpContains Op = "contains"
	OpPrefix   Op = "prefix"
	OpGt       Op = "gt"
	OpLt       Op = "lt"
)

// Eq is shorthand for the overwhelmingly common case.
func Eq(field string, want any) Constraint {
	return Constraint{Field: field, Op: OpEq, Want: want}
}

// MatchResult is the outcome of testing one event against one predicate.
// Passed/Total give the near-miss score; Failures says exactly what differed.
type MatchResult struct {
	OK       bool
	Passed   int
	Total    int
	Failures []ConstraintFailure
}

// ConstraintFailure records one unsatisfied constraint. Present distinguishes
// "the field holds a different value" from "the field is not there at all",
// which are very different bugs and read very differently in a report.
type ConstraintFailure struct {
	Constraint Constraint
	Got        any
	Present    bool
}

// Match tests e against p, always evaluating every constraint (never
// short-circuiting) because the score and the failure list are the point.
func (p Predicate) Match(e Event) MatchResult {
	res := MatchResult{Total: len(p.Constraints)}
	for _, c := range p.Constraints {
		got, present := resolveField(e, c.Field)
		if present && satisfies(c.Op, got, c.Want) {
			res.Passed++
			continue
		}
		res.Failures = append(res.Failures, ConstraintFailure{
			Constraint: c,
			Got:        got,
			Present:    present,
		})
	}
	res.OK = res.Passed == res.Total
	return res
}

// String renders the predicate the way a failure report shows it.
func (p Predicate) String() string {
	if p.Label != "" {
		return p.Label
	}
	parts := make([]string, 0, len(p.Constraints))
	for _, c := range p.Constraints {
		parts = append(parts, fmt.Sprintf("%s %s %v", c.Field, c.Op, c.Want))
	}
	return strings.Join(parts, " ")
}

// resolveField extracts a field from an event. The second result reports
// whether the field exists at all.
func resolveField(e Event, field string) (any, bool) {
	switch field {
	case "source":
		return string(e.Source), true
	case "kind":
		return string(e.Kind), true
	case "seq":
		return e.Seq, true
	}
	if name, ok := strings.CutPrefix(field, "attrs."); ok {
		v, present := e.Attrs[name]
		return v, present
	}
	return nil, false
}

func satisfies(op Op, got, want any) bool {
	switch op {
	case OpEq:
		return looseEqual(got, want)
	case OpNe:
		return !looseEqual(got, want)
	case OpContains:
		return strings.Contains(toString(got), toString(want))
	case OpPrefix:
		return strings.HasPrefix(toString(got), toString(want))
	case OpGt, OpLt:
		g, gok := toFloat(got)
		w, wok := toFloat(want)
		if !gok || !wok {
			return false
		}
		if op == OpGt {
			return g > w
		}
		return g < w
	default:
		return false
	}
}

// looseEqual compares across the type boundary between YAML and Go.
//
// Scenario values arrive from YAML as int/float64/string/bool, while Attrs
// hold whatever the decoder produced — a SenML value may be float64 where the
// YAML said 21 (an int), and a port may be int where the YAML said "1883".
// Comparing with == would make a scenario fail for a reason that has nothing
// to do with the daemon, so numbers are compared numerically and everything
// else falls back to its string form.
func looseEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if af, aok := toFloat(a); aok {
		if bf, bok := toFloat(b); bok {
			return af == bf
		}
	}
	if ab, aok := a.(bool); aok {
		if bb, bok := b.(bool); bok {
			return ab == bb
		}
	}
	return toString(a) == toString(b)
}

func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	case float32:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}

func toString(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fmt.Sprintf("%v", v)
}

// NearMiss is a candidate event that failed to match, kept for the report.
type NearMiss struct {
	Event  Event
	Result MatchResult
}

// nearMisses scores every candidate against p and returns the best ones, most
// nearly-matching first.
//
// Events that satisfied no constraint at all are dropped: they are unrelated
// traffic, and listing them would bury the one line the reader needs. Ties are
// broken by Seq so the output is deterministic.
func nearMisses(events []Event, p Predicate, limit int) []NearMiss {
	var out []NearMiss
	for _, e := range events {
		r := p.Match(e)
		if r.OK || r.Passed == 0 {
			continue
		}
		out = append(out, NearMiss{Event: e, Result: r})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Result.Passed != out[j].Result.Passed {
			return out[i].Result.Passed > out[j].Result.Passed
		}
		return out[i].Event.Seq < out[j].Event.Seq
	})
	if len(out) > limit {
		out = out[:limit]
	}
	return out
}
