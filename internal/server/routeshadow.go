package server

import (
	"fmt"
	"strings"
)

// ShadowedRoutes returns the boot-time shadow-detection findings: one
// message per registered route that an earlier route fully shadows, so
// it can never be selected (declaration order is precedence, identical
// to the traffic-class detector in trafficclass.go — ADR-0047 pattern,
// applied to routes by issue #772). Detection runs once per AddRoute
// call; the returned slice is nil or non-empty, never empty-non-nil.
// Boot proceeds regardless: the caller surfaces the findings, it does
// not fail on them.
func (rt *Router) ShadowedRoutes() []string {
	return rt.shadows
}

// shadowedByEarlier walks the routes registered before e and reports
// the first one that fully covers e's match predicate. Every dimension
// (host, path, methods) must be covered: a request e matches is then
// always matched by the earlier route too, so under first-match
// precedence e is dead config.
func (rt *Router) shadowedByEarlier(e *routeEntry) (string, bool) {
	for i := range rt.routes {
		earlier := &rt.routes[i]
		if routeShadowsRoute(earlier, e) {
			return fmt.Sprintf(
				"route %q can never match: fully shadowed by earlier route %q (declaration order is precedence)",
				e.label, earlier.label), true
		}
	}
	return "", false
}

// routeShadowsRoute reports whether every request matching later also
// matches earlier, making later unreachable under first-match
// precedence. Conservative by design: any pair whose relation is not
// decidable (a regex against a different predicate) reports false —
// silence is safe, a false finding would send operators chasing a
// ghost.
func routeShadowsRoute(earlier, later *routeEntry) bool {
	return hostCovers(earlier.hostPat, later.hostPat) &&
		pathCovers(earlier, later) &&
		methodsCovers(earlier.methods, later.methods)
}

// hostCovers reports whether every host matching b also matches a.
// An absent host constraint (empty exact pattern) matches every host;
// a constrained a can never cover an unconstrained b.
func hostCovers(a, b trafficPattern) bool {
	if a.match == "" && a.kind == patternExact {
		return true
	}
	if b.match == "" && b.kind == patternExact {
		return false
	}
	// Route hosts only ever compile to exact or suffix kinds, so this
	// reuses the traffic-class shadow logic verbatim (patternShadows):
	// exact covers the identical exact; a suffix covers an exact host
	// extending it and a narrower suffix extending it.
	return patternShadows(a, b)
}

// pathCovers reports whether every path matching later also matches
// earlier. Decidable pairs only: an absent constraint covers anything;
// prefix-extends-prefix is plain HasPrefix; two byte-identical regexes
// match identical path sets. A regex against any other predicate is
// undecidable in general and reports false.
func pathCovers(earlier, later *routeEntry) bool {
	if earlier.pathPrefix == "" && earlier.pathRe == nil {
		return true
	}
	if later.pathPrefix == "" && later.pathRe == nil {
		return false
	}
	if earlier.pathRe == nil && later.pathRe == nil {
		return strings.HasPrefix(later.pathPrefix, earlier.pathPrefix)
	}
	return earlier.pathRe != nil && later.pathRe != nil && earlier.pathRaw == later.pathRaw
}

// methodsCovers reports whether every method matching b also matches
// a. A nil set matches all methods, so nil a covers anything and a
// restricted a cannot cover an unrestricted b.
func methodsCovers(a, b map[string]bool) bool {
	if a == nil {
		return true
	}
	if b == nil {
		return false
	}
	for m := range b {
		if !a[m] {
			return false
		}
	}
	return true
}
