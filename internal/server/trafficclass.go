package server

import (
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
			lh := strings.ToLower(h)
			switch {
			case strings.HasPrefix(lh, "*."):
				pc.suffix = append(pc.suffix, lh[1:])
			case strings.HasSuffix(lh, ".*"):
				pc.prefix = append(pc.prefix, lh[:len(lh)-1])
			case strings.HasSuffix(lh, "*"):
				pc.prefix = append(pc.prefix, lh[:len(lh)-1])
			default:
				pc.exact[lh] = struct{}{}
			}
		}
	}
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
