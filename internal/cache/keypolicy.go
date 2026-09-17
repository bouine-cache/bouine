package cache

import (
	"sort"
	"strings"
)

// KeyPolicy encodes cache key construction rules for a route.
// Allocated once at handler construction; read-only on the hot path.
// All lookups are O(1) (map) or O(params x prefixes) on the hot path
// (typically O(8x5) = 40 comparisons for 8 params x 5 prefixes).
//
// KeyPolicy covers query string and Vary header policy. Path
// canonicalization is handled at parse time (L1) and is not part of
// KeyPolicy.
type KeyPolicy struct {
	stripParams    map[string]bool // exact names to strip from query
	keepParams     map[string]bool // when non-nil, allowlist (only these participate)
	excludeHeaders map[string]bool // headers to exclude from Vary variant key
	// includeHeaders is the route's cache.key.include_headers allow-list,
	// trimmed, lowercased, and sorted at construction; read-only
	// afterwards. The fields are unioned into the stored Vary at
	// object-build time (see effectiveVary), so the variant-key
	// hash-input builders in vary.go never need to know about them.
	includeHeaders []string
	stripPrefixes  []string // prefix patterns to strip, capped at 16
	stripEmpty     bool     // strip params with empty values
	dedup          bool     // keep first value (in request order) for duplicate params
}

// shouldStripParam returns true if the query param should be excluded
// from the cache key. Zero-allocation: all checks are map lookups,
// prefix comparisons, or boolean tests.
//
// Evaluation order:
//  1. keepParams (allowlist): if set and param is NOT in it -> strip.
//     If set and param IS in it -> continue to dedup check (dedup still applies).
//  2. stripEmpty: if value is empty -> strip. Does NOT apply to allowlisted
//     params (keepParams == nil guard ensures this).
//  3. stripParams: if in blocklist -> strip.
//  4. stripPrefixes: if any prefix matches -> strip. O(len(stripPrefixes)).
//  5. dedup: if already seen this param name -> strip. First occurrence wins.
func (p *KeyPolicy) shouldStripParam(k, v string, seen *stackSeen) bool {
	if p == nil {
		return false
	}
	// 1. keepParams: if set and param is NOT in it -> strip.
	//    If set and param IS in it -> fall through to dedup.
	if p.keepParams != nil {
		if !p.keepParams[k] {
			return true
		}
	} else {
		// 2. stripEmpty: only applies when no allowlist.
		if p.stripEmpty && v == "" {
			return true
		}
	}
	// 3. Blocklist: exact name match.
	if p.stripParams != nil && p.stripParams[k] {
		return true
	}
	// 4. Prefix matching: O(len(stripPrefixes)), capped at 16.
	for i := range p.stripPrefixes {
		if strings.HasPrefix(k, p.stripPrefixes[i]) {
			return true
		}
	}
	// 5. Dedup: if we've already seen this param name, strip the duplicate.
	if p.dedup && seen != nil && seen.contains(k) {
		return true
	}
	return false
}

// markSeen records a param name as seen for dedup tracking.
func (p *KeyPolicy) markSeen(k string, seen *stackSeen) {
	if p != nil && p.dedup && seen != nil {
		seen.add(k)
	}
}

// stackSeen tracks seen param names on the stack (fast path, <=8 params).
// The fast path bails to slow path when param count exceeds 8, so
// stackSeen never overflows in the fast path.
type stackSeen struct {
	names [8]string
	n     int
}

func (s *stackSeen) contains(k string) bool {
	for i := range s.n {
		if s.names[i] == k {
			return true
		}
	}
	return false
}

func (s *stackSeen) add(k string) {
	if s.n < len(s.names) {
		s.names[s.n] = k
		s.n++
	}
}

// HasQueryPolicy reports whether any query-string policy is active.
func (p *KeyPolicy) HasQueryPolicy() bool {
	if p == nil {
		return false
	}
	return p.stripParams != nil || p.keepParams != nil ||
		len(p.stripPrefixes) > 0 || p.stripEmpty || p.dedup
}

// NewKeyPolicy constructs a KeyPolicy from the given parameters.
// All maps are pre-allocated; the returned policy is read-only.
// includeHeaders is trimmed, lowercased, and sorted here: config
// validation guarantees no duplicates, so the canonical form makes the
// stored union deterministic and lets effectiveVary dedupe a field the
// origin also lists in Vary (its own trims must match).
func NewKeyPolicy(stripParams, keepParams, excludeHeaders map[string]bool, stripPrefixes []string, stripEmpty, dedup bool, includeHeaders []string) *KeyPolicy {
	if len(includeHeaders) > 0 {
		lowered := make([]string, 0, len(includeHeaders))
		for _, h := range includeHeaders {
			if h = strings.TrimSpace(strings.ToLower(h)); h != "" {
				lowered = append(lowered, h)
			}
		}
		if len(lowered) == 0 {
			includeHeaders = nil
		} else {
			sort.Strings(lowered)
			includeHeaders = lowered
		}
	} else {
		includeHeaders = nil
	}
	return &KeyPolicy{
		stripParams:    stripParams,
		keepParams:     keepParams,
		stripPrefixes:  stripPrefixes,
		stripEmpty:     stripEmpty,
		dedup:          dedup,
		excludeHeaders: excludeHeaders,
		includeHeaders: includeHeaders,
	}
}

// ShouldExcludeHeader returns true if the given header name should be
// excluded from the Vary variant key.
func (p *KeyPolicy) ShouldExcludeHeader(h string) bool {
	if p == nil || p.excludeHeaders == nil {
		return false
	}
	return p.excludeHeaders[h]
}
