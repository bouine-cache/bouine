package server

import (
	"fmt"
	"strings"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TrafficClassPatterns are one class's host patterns, pre-compiled at
// boot: exact hosts go into a map keyed on the lowercased pattern,
// suffix ("*.example.com") and prefix ("www.example.*" /
// "www.example*") globs go into ordered lists, all lowercased once.
// Forms and counts were validated by config.Validate.
type trafficClassPatterns struct {
	name   string
	exact  map[string]struct{}
	suffix []string // ".example.com" — matches any host ending in it, strictly longer
	prefix []string // "www.example." (from ".*") or "www.example" (from trailing "*")
}

// TrafficClassSpec is one configured traffic class as seen by the
// classifier: a name plus its host patterns. The engine passes
// config.MetricsConfig.TrafficClasses values here; the server layer
// takes the plain pair (not the config type) so L1 keeps its strict
// dependency diet (depguard: server imports no other internal layer
// besides observability/platform).
type TrafficClassSpec struct {
	Name  string
	Hosts []string
}

// TrafficClassifier maps a request Host to a configured traffic-class
// name (ADR-0047). Declaration order is precedence: the first class
// whose host pattern matches wins. A nil classifier (no classes
// configured) classifies everything as "unclassified" — the same label
// every request carries when the feature is absent, so the metric shape
// never changes between deployments.
//
// Returned names are the classifier's stable config-owned strings:
// callers may retain them beyond the request lifetime (the reactor
// metrics ring's retain-safety contract) and the Host input can only
// select among them, never mint a new value — the same guarantee
// pattern upstream_pool has. Matching is case-insensitive with the
// port stripped, identical to route matching (matchRoute); the
// request Host is never lowercased — strings.ToLower allocates on any
// case change, and the hit path must not.
type TrafficClassifier struct {
	classes []trafficClassPatterns

	// shadows holds the boot-time shadow-detection findings (see
	// detectShadowedPatterns): configured patterns that can never match
	// because an earlier class already claims every host they match.
	shadows []string
}

// NewTrafficClassifier compiles the configured traffic classes. The
// slice must already have passed config.Validate (the engine runs
// validation before any listener accepts). Returns nil for an empty
// configuration — Classify on nil returns "unclassified".
func NewTrafficClassifier(classes []TrafficClassSpec) *TrafficClassifier {
	if len(classes) == 0 {
		return nil
	}
	c := &TrafficClassifier{classes: make([]trafficClassPatterns, len(classes))}
	for i := range classes {
		pc := &c.classes[i]
		pc.name = classes[i].Name
		pc.exact = make(map[string]struct{}, len(classes[i].Hosts))
		for _, h := range classes[i].Hosts {
			p := compileTrafficPattern(h)
			switch p.kind {
			case patternSuffix:
				pc.suffix = append(pc.suffix, p.match)
			case patternPrefix:
				pc.prefix = append(pc.prefix, p.match)
			default:
				pc.exact[p.match] = struct{}{}
			}
		}
	}
	c.shadows = detectShadowedPatterns(classes)
	return c
}

// Classify returns the traffic-class name for host (which may carry a
// port). Zero allocations: one uppercase scan gates the case-folded
// comparisons, map lookups and length-bounded EqualFold over the
// lowercased patterns — the same comparison matchRoute runs for route
// hosts.
func (c *TrafficClassifier) Classify(host string) string {
	if c == nil {
		return api.TrafficClassUnclassified
	}
	host = stripHostPort(host)
	mixedCase := hasUppercase(host)
	for i := range c.classes {
		pc := &c.classes[i]
		if _, ok := pc.exact[host]; ok {
			return pc.name
		}
		// Only a mixed-case Host can fold-match a pattern the direct
		// (lowercased-key) lookup missed; skipping the scan keeps the
		// common all-lowercase miss cheap.
		if mixedCase {
			for k := range pc.exact {
				if api.EqualFold(host, k) {
					return pc.name
				}
			}
		}
		for _, sfx := range pc.suffix {
			// Strictly longer: "*.example.com" matches subdomains,
			// never the bare ".example.com" or "example.com".
			if len(host) > len(sfx) && api.EqualFold(host[len(host)-len(sfx):], sfx) {
				return pc.name
			}
		}
		for _, pfx := range pc.prefix {
			if len(host) < len(pfx) || !api.EqualFold(host[:len(pfx)], pfx) {
				continue
			}
			// "www.example.*" (prefix ends in ".") requires at least one
			// label after the dot; the bare "www.example*" form lets the
			// star match the empty string, so the bare prefix matches too.
			if len(host) > len(pfx) || pfx[len(pfx)-1] != '.' {
				return pc.name
			}
		}
	}
	return api.TrafficClassUnclassified
}

// ClassNames returns the configured class names in declaration order.
// The engine passes the same slice to PreResolveTrafficClasses so the
// classifier's outputs and the metrics slot table cannot diverge.
// Returns nil for a nil classifier.
func (c *TrafficClassifier) ClassNames() []string {
	if c == nil {
		return nil
	}
	names := make([]string, len(c.classes))
	for i := range c.classes {
		names[i] = c.classes[i].name
	}
	return names
}

// ShadowedPatterns returns the boot-time shadow-detection findings:
// one message per configured host pattern that an earlier class (see
// ClassNames for the declaration-order contract) fully shadows, so the
// pattern can never select its class and the operator's later class
// silently receives no traffic for it. Detection runs once at compile
// time; the returned slice is nil or non-empty, never empty-non-nil.
func (c *TrafficClassifier) ShadowedPatterns() []string {
	if c == nil {
		return nil
	}
	return c.shadows
}

// trafficPattern is one compiled host pattern for shadow analysis:
// the same lowercased forms the classifier matches on, kept next to
// the original pattern string for operator-facing messages.
type trafficPattern struct {
	match string
	kind  patternKind
}

type patternKind uint8

const (
	patternExact patternKind = iota
	patternSuffix
	patternPrefix
)

// compileTrafficPattern lowercases and reduces a validated host
// pattern (config.Validate already rejected malformed globs) to the
// exact / suffix / prefix forms Classify matches. It is the single
// source of the pattern-form reduction — NewTrafficClassifier and the
// shadow detector both use it, so detection and matching can never
// disagree about what a pattern means.
func compileTrafficPattern(p string) trafficPattern {
	lh := strings.ToLower(p)
	switch {
	case strings.HasPrefix(lh, "*."):
		return trafficPattern{match: lh[1:], kind: patternSuffix}
	case strings.HasSuffix(lh, ".*"), strings.HasSuffix(lh, "*"):
		return trafficPattern{match: lh[:len(lh)-1], kind: patternPrefix}
	default:
		return trafficPattern{match: lh, kind: patternExact}
	}
}

// patternShadows reports whether earlier pattern a fully shadows later
// pattern b: every host b matches, a matches too, so under
// first-match precedence b can never select its class. Single-pattern
// cross-kind pairs (suffix vs prefix) only ever overlap partially — a
// lone pattern cannot pin both ends of a host — so they never shadow.
func patternShadows(a, b trafficPattern) bool {
	// An exact pattern matches exactly one host, so it is shadowed iff
	// the earlier pattern matches that host.
	if b.kind == patternExact {
		switch a.kind {
		case patternExact:
			return a.match == b.match
		case patternSuffix:
			return len(b.match) > len(a.match) && strings.HasSuffix(b.match, a.match)
		default: // prefix
			// The prefix's own match condition (Classify): the empty
			// continuation is allowed only for the bare-star form, whose
			// reduced prefix does not end in '.'.
			return strings.HasPrefix(b.match, a.match) &&
				(len(b.match) > len(a.match) || a.match[len(a.match)-1] != '.')
		}
	}
	if b.kind == patternSuffix {
		// A suffix matches arbitrarily long hosts with arbitrary
		// beginnings; only a suffix it extends (or equals) shadows it.
		return a.kind == patternSuffix &&
			(b.match == a.match ||
				(len(b.match) > len(a.match) && strings.HasSuffix(b.match, a.match)))
	}
	// b is a prefix: every host matching it starts with b.match and is
	// at least as long, so any prefix a that b.match extends shadow it
	// — the length conditions of a's own match are implied by b's.
	return a.kind == patternPrefix && strings.HasPrefix(b.match, a.match)
}

// detectShadowedPatterns walks the classes in declaration order and
// returns one message per later-class pattern fully shadowed by an
// earlier class. Patterns inside the same class are skipped: they
// resolve to the same label, so shadowing them changes nothing.
func detectShadowedPatterns(classes []TrafficClassSpec) []string {
	type compiledClass struct {
		name     string
		raw      []string
		patterns []trafficPattern
	}
	compiled := make([]compiledClass, len(classes))
	for i, tc := range classes {
		cc := &compiled[i]
		cc.name = tc.Name
		for _, h := range tc.Hosts {
			cc.raw = append(cc.raw, h)
			cc.patterns = append(cc.patterns, compileTrafficPattern(h))
		}
	}
	var msgs []string
	shadowedBy := func(b trafficPattern, before int) (string, string, bool) {
		for i := range before {
			earlier := &compiled[i]
			for m := range earlier.patterns {
				if patternShadows(earlier.patterns[m], b) {
					return earlier.name, earlier.raw[m], true
				}
			}
		}
		return "", "", false
	}
	for j := 1; j < len(compiled); j++ {
		later := &compiled[j]
		for k := range later.patterns {
			if byClass, byPattern, ok := shadowedBy(later.patterns[k], j); ok {
				msgs = append(msgs, fmt.Sprintf(
					"traffic_class: host pattern %q in class %q can never match: fully shadowed by %q in class %q (declaration order is precedence, ADR-0047)",
					later.raw[k], later.name, byPattern, byClass))
			}
		}
	}
	return msgs
}

// hasUppercase reports whether s contains an ASCII uppercase byte.
// Hosts are ASCII (RFC 9110 §5.1); non-ASCII bytes cannot fold-match
// the validated lowercase patterns either way.
func hasUppercase(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= 'A' && s[i] <= 'Z' {
			return true
		}
	}
	return false
}
