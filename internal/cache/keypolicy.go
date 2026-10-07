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
	// includeJoined is includeHeaders pre-joined ("a, b, c") so the
	// collapse flight key need not re-join on every cold miss.
	includeJoined string
	stripPrefixes []string // prefix patterns to strip, capped at 16
	// cookiePresence is the route's cache.key.cookie_presence list
	// (issue #768): cookie names whose presence (never their values)
	// participates in the variant key. Normalized (trimmed,
	// lowercased, sorted, deduped) here like includeHeaders.
	// Cluster note: identical across nodes, same hazard class as
	// include_headers.
	cookiePresence []string
	stripEmpty     bool // strip params with empty values
	dedup          bool // keep first value (in request order) for duplicate params
	// excludeHost, when true, omits the host segment from the primary
	// key (cache.key.include_host: false). Read on every key build; the
	// default (false) is today's behaviour. See includeHostKey.
	excludeHost bool
	// verbatimAE selects the Accept-Encoding variant-key policy.
	// false (default): the AE value is reduced to a bucket by
	// encodingBucket (docs/architecture.md §3.3) and the origin-bound
	// request carries the canonical token (rewriteOutboundAE).
	// true (config encoding_policy: verbatim): the raw AE string is
	// sorted lowercased (pre-bucketing behavior) and forwarded to the
	// origin unchanged — for origins that vary response bodies by the
	// full AE string.
	// Cluster note: like include_headers, this field must be identical
	// across nodes serving the route or nodes store/resolve variants
	// under different keys (peer gates fail safe — miss, never a wrong
	// body).
	verbatimAE bool
}

// cookiePresenceField is the synthetic Vary field name carrying the
// cookie-presence bits through the variant-key machinery. It is never
// a legal Vary field an origin can send for: Vary field names are
// header names (RFC 9110 §12.5.5), and this name is not one — it is
// reserved by this package. An origin literally sending it would be
// claiming a "header" that does not exist; its requests never carry
// such a header, so every lookup hashes the same value and no
// functional collision is possible (an attacker sending it in Vary
// merely adds one constant field to the key, the same as any
// never-present header).
const cookiePresenceField = "x-bouine-cookie-presence"

// CookiePresenceVary returns the synthetic Vary field value that
// carries the route's cookie-presence bits through effectiveVary: the
// presence-keyed dimension is unioned into the stored Vary exactly
// like include_headers fields (same mechanism, same store/lookup
// pairing guarantees). Empty string when the route does not key on
// cookie presence.
func (p *KeyPolicy) CookiePresenceVary() string {
	if p == nil || len(p.cookiePresence) == 0 {
		return ""
	}
	return cookiePresenceField
}

// hasCookiePresence reports whether the route keys variants on cookie
// presence. Nil-safe.
func (p *KeyPolicy) hasCookiePresence() bool {
	return p != nil && len(p.cookiePresence) > 0
}

// cookiePresenceValue reduces the Cookie header value(s) for this
// request to the presence-bit string that joins the variant key: one
// character per listed name (sorted order), "1" for present, "0" for
// absent. Cookie values never appear — presence only (issue #768:
// cardinality + PII). The bits are positional over the sorted
// normalized name list, so the value is deterministic across nodes
// and config-order independent.
//
// One pass over the cookie pairs; each pair's name is looked up
// against the scanner. Allocations: the returned string (≤16 bytes)
// is the only heap use — it is produced on the lookup path (hit and
// miss alike) for Vary-carrying objects on presence-keyed routes.
// The zero-alloc hit-path budget applies to flag-off routes, which
// never take this branch (effectiveVary only adds the synthetic field
// when the route lists names).
func (p *KeyPolicy) cookiePresenceValue(cookieValue string) string {
	if !p.hasCookiePresence() || cookieValue == "" {
		return ""
	}
	bits := make([]byte, len(p.cookiePresence))
	for i := range bits {
		bits[i] = '0'
	}
	for pair := range strings.SplitSeq(cookieValue, ";") {
		name, _, _ := strings.Cut(pair, "=")
		n := len(name)
		for n > 0 && (name[n-1] == ' ' || name[n-1] == '\t') {
			n--
		}
		off := 0
		for off < n && (name[off] == ' ' || name[off] == '\t') {
			off++
		}
		token := name[off:n]
		if token == "" {
			continue
		}
		// Linear scan over the sorted-by-length list: presence lists
		// are small (≤16), and the common all-absent / all-present
		// cases short-circuit on the first matching length group.
		for i, listed := range p.cookiePresence {
			if len(listed) == len(token) && asciiEqualFoldStrings(token, listed) {
				bits[i] = '1'
				break
			}
		}
	}
	return string(bits)
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

// includeHostKey reports whether the primary key must carry the host
// segment. nil policy (the common case) keeps host keyed: the admin
// purge/refresh paths build keys with a nil policy, and a nil-safe
// default of "include host" is what every existing route does today.
func includeHostKey(p *KeyPolicy) bool {
	return p == nil || !p.excludeHost
}

// NewKeyPolicy constructs a KeyPolicy from the given parameters.
// All maps are pre-allocated; the returned policy is read-only.
// includeHeaders is trimmed, lowercased, and sorted here: config
// validation guarantees no duplicates, so the canonical form makes the
// stored union deterministic and lets effectiveVary dedupe a field the
// origin also lists in Vary (its own trims must match).
// excludeHost carries cache.key.include_host: false — the one field
// that is true by default, hence an inverted "exclude" parameter.
// Cookie presence (cache.key.cookie_presence, issue #768) is NOT a
// parameter: it is set via WithCookiePresence, mirroring SetVerbatimAE,
// so the 74 existing call sites (tests and the builder) stay stable.
// Normalization is identical to includeHeaders (trim, lowercase, sort,
// dedupe) — the presence bits are positional over the sorted list.
func NewKeyPolicy(stripParams, keepParams, excludeHeaders map[string]bool, stripPrefixes []string, stripEmpty, dedup bool, includeHeaders []string, excludeHost bool) *KeyPolicy {
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
		includeJoined:  strings.Join(includeHeaders, ", "),
		excludeHost:    excludeHost,
	}
}

// WithCookiePresence sets the cookie-presence list after
// construction (mirrors SetVerbatimAE's setter pattern for optional
// keying dimensions). Mutating a policy after the handler is serving
// is forbidden — stored VaryKeys would no longer match freshly
// computed ones.
func (p *KeyPolicy) WithCookiePresence(names []string) *KeyPolicy {
	if p == nil {
		return nil
	}
	p.cookiePresence = normalizeCookiePresence(names)
	return p
}

// normalizeCookiePresence trims, lowercases, sorts, and dedupes the
// cookie-presence list. The presence bits are positional over this
// canonical form (cookiePresenceValue), so every node must reduce the
// config to the same order — which validation's duplicate rejection
// and this normalization together guarantee.
func normalizeCookiePresence(names []string) []string {
	if len(names) == 0 {
		return nil
	}
	lowered := make([]string, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, n := range names {
		n = strings.TrimSpace(strings.ToLower(n))
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		lowered = append(lowered, n)
	}
	if len(lowered) == 0 {
		return nil
	}
	sort.Strings(lowered)
	return lowered
}

// ShouldExcludeHeader returns true if the given header name should be
// excluded from the Vary variant key.
func (p *KeyPolicy) ShouldExcludeHeader(h string) bool {
	if p == nil || p.excludeHeaders == nil {
		return false
	}
	return p.excludeHeaders[h]
}

// hasIncludeHeaders reports whether the route declares an
// include_headers allow-list. Nil-safe; read-only.
func (p *KeyPolicy) hasIncludeHeaders() bool {
	return p != nil && len(p.includeHeaders) > 0
}

// flightVary returns the declared-dimensions field list the collapse
// flight key must hash when the flight key is the primary key:
// include_headers plus the synthetic cookie-presence field when
// declared, pre-joined as a field-list string. Both dimensions union
// into the STORED Vary the same way (effectiveVary), and the flight key
// hashes them through the same varyHeaderValue normalization as the
// storage variant key — any divergence between the two keyings is a
// wrong-body handoff.
func (p *KeyPolicy) flightVary() string {
	if p == nil {
		return ""
	}
	if !p.hasCookiePresence() {
		return p.includeJoined
	}
	if p.includeJoined == "" {
		return cookiePresenceField
	}
	return p.includeJoined + ", " + cookiePresenceField
}

// hasFlightDimensions reports whether the route declares include_headers
// or cookie presence. Nil-safe.
func (p *KeyPolicy) hasFlightDimensions() bool {
	return p.hasIncludeHeaders() || p.hasCookiePresence()
}

// SetVerbatimAE sets the Accept-Encoding key policy. Called once at
// handler construction from the route's encoding_policy config; the
// zero value (bucket) needs no call. Mutating a policy after the
// handler is serving is forbidden — stored VaryKeys would no longer
// match freshly computed ones.
func (p *KeyPolicy) SetVerbatimAE(verbatim bool) {
	if p == nil {
		return
	}
	p.verbatimAE = verbatim
}

// verbatimEncoding reports whether the policy pins Accept-Encoding
// keying to the verbatim (sorted) pre-bucketing behavior. Nil policy
// buckets (the documented default).
func (p *KeyPolicy) verbatimEncoding() bool {
	return p != nil && p.verbatimAE
}
