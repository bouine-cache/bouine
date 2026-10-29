package cache

import (
	"fmt"
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
	// derivedVary is the Vary list this policy alone determines
	// (include_headers unioned with the synthetic cookie-presence
	// field), pre-joined in effectiveVary's stored shape ("a, b",
	// sorted, deduplicated) so the ADR-0071 peer paths read a constant
	// instead of re-deriving it per miss. Empty when the policy keys no
	// variants. Recomputed by computeDerivedVary — called by
	// NewKeyPolicy and WithCookiePresence, the two construction sites.
	derivedVary   string
	stripPrefixes []string // prefix patterns to strip, capped at 16
	// cookiePresence is the route's cache.key.cookie_presence list
	// (issue #768): cookie names whose presence (never their values)
	// participates in the variant key. Normalized (trimmed,
	// lowercased, sorted, deduped) here like includeHeaders.
	// Cluster note: identical across nodes, same hazard class as
	// include_headers.
	cookiePresence []string
	// cookieValues is the route's cache.key.cookie_values list (issue
	// #870): cookie names whose VALUES participate in the variant key
	// (the Cloudflare cookie.include equivalent). Normalized like
	// cookiePresence; positional over the sorted list in
	// cookieValuesValue. Cluster note: identical across nodes — same
	// hazard class as include_headers.
	cookieValues []string
	stripEmpty   bool // strip params with empty values
	dedup        bool // keep first value (in request order) for duplicate params
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

// cookieValuesField is the synthetic Vary field name carrying the
// cookie-VALUE pairs through the variant-key machinery (issue #870),
// reserved exactly like cookiePresenceField: not a header name a
// request can carry, so an origin sending it in Vary adds only a
// never-present field to the key (the same constant-value argument
// cookiePresenceField's comment makes).
const cookieValuesField = "x-bouine-cookie-values"

// maxCookieValueNames mirrors config validation's cap (loader.go):
// cookie_values lists are capped at 4 names there. Kept as a local
// constant so the stackVals array in cookieValuesValue and the cap the
// config layer enforces cannot drift apart silently — the array is
// sized defensively and a longer list (only constructible
// programmatically, validation rejects it) falls back to a heap slice.
const maxCookieValueNames = 4

// QuerySummary returns a short description of the query-parameter
// handling for admin key-derivation reporting (bouine keys find,
// issue #816): "all" when every parameter participates in the key
// (the default), otherwise the active filters joined by commas. A
// nil policy (no route matched — the default policy) reports "all".
// Admin path only; never called on the hot path.
func (p *KeyPolicy) QuerySummary() string {
	if p == nil {
		return "all"
	}
	var parts []string
	if n := len(p.keepParams); n > 0 {
		parts = append(parts, fmt.Sprintf("allow:%d", n))
	}
	if n := len(p.stripParams); n > 0 {
		parts = append(parts, fmt.Sprintf("strip:%d", n))
	}
	if n := len(p.stripPrefixes); n > 0 {
		parts = append(parts, fmt.Sprintf("strip-prefix:%d", n))
	}
	if p.stripEmpty {
		parts = append(parts, "strip-empty")
	}
	if p.dedup {
		parts = append(parts, "dedup")
	}
	if len(parts) == 0 {
		return "all"
	}
	return strings.Join(parts, ",")
}

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

// hasCookieValues reports whether the route keys variants on cookie
// values. Nil-safe.
func (p *KeyPolicy) hasCookieValues() bool {
	return p != nil && len(p.cookieValues) > 0
}

// hasCookieKeying reports whether the route keys variants on cookies
// at all (presence or values). Nil-safe; the refresh registry, the
// collapse flight headers, and effectiveVary all gate on this — the
// Cookie header must be saved / snapshotted / unioned exactly when a
// synthetic cookie field can join the variant key.
func (p *KeyPolicy) hasCookieKeying() bool {
	return p.hasCookiePresence() || p.hasCookieValues()
}

// CookieValuesVary returns the synthetic Vary field value that carries
// the route's cookie-value pairs through effectiveVary: the
// value-keyed dimension is unioned into the stored Vary exactly like
// include_headers fields and the presence field (same mechanism, same
// store/lookup pairing guarantees). Empty string when the route does
// not key on cookie values.
func (p *KeyPolicy) CookieValuesVary() string {
	if p == nil || len(p.cookieValues) == 0 {
		return ""
	}
	return cookieValuesField
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

// cookieValuesValue reduces the Cookie header value(s) for this
// request to the value-pair string that joins the variant key:
// "name=value" pairs positional over the sorted normalized name
// list, joined by "; ". Deterministic and config-order independent
// like the presence bits, so every node reduces to the same bytes.
// Canonicalization (ADR-0072, pinned in cookie_values_test.go):
//
//   - names matched case-insensitively against the trimmed request
//     pair name; values kept case-sensitive and trimmed of
//     leading/trailing OWS (RFC 6265 §5.2), DQUOTEs kept (§5.2 rule 6)
//   - a duplicate listed name: the LAST occurrence wins (RFC 6265
//     §5.4 — browsers send longer-path cookies first)
//   - an absent or empty-valued listed cookie contributes the empty
//     value — exactly one variant, matching include_headers'
//     absent-header semantics (RFC 9111 Vary)
//   - no Cookie header at all reduces to "" (the presence
//     convention: a request that carries no cookies hashes the
//     constant-empty contribution)
//
// The "; " pair separator composes with the ";" pair split without
// folding: an RFC 6265 cookie-value can never contain ";" or ","
// (§4.1 cookie-octet exclusions), so no value can forge a pair
// boundary. One allocation: the joined string — the pair values ride
// a stack array (≤4 names) and the builder slice never escapes
// except through the string conversion. Value-keyed-route costs
// only: routes without cookie_values never take this branch.
func (p *KeyPolicy) cookieValuesValue(cookieValue string) string {
	if !p.hasCookieValues() || cookieValue == "" {
		return ""
	}
	n := len(p.cookieValues)
	var stackVals [maxCookieValueNames]string
	var vals []string
	if n <= maxCookieValueNames {
		vals = stackVals[:n]
	} else {
		vals = make([]string, n)
	}
	for pair := range strings.SplitSeq(cookieValue, ";") {
		name, val, _ := strings.Cut(pair, "=")
		token := trimCookieOWS(name)
		if token == "" {
			continue
		}
		// Linear scan over the sorted list: value lists are small
		// (≤4, config-capped), and each pair matches at most one name
		// (config validation rejects case-insensitive duplicates).
		for i, listed := range p.cookieValues {
			if len(listed) == len(token) && asciiEqualFoldStrings(token, listed) {
				// Last occurrence wins: overwrite without a check.
				vals[i] = trimCookieOWS(val)
				break
			}
		}
	}
	total := 0
	for i, listed := range p.cookieValues {
		total += len(listed) + 1 + len(vals[i]) + 2
	}
	total -= 2 // no separator after the last pair
	b := make([]byte, total)
	off := 0
	for i, listed := range p.cookieValues {
		if i > 0 {
			b[off] = ';'
			b[off+1] = ' '
			off += 2
		}
		off += copy(b[off:], listed)
		b[off] = '='
		off++
		off += copy(b[off:], vals[i])
	}
	return string(b)
}

// trimCookieOWS strips the RFC 6265 §5.2 optional whitespace (SP,
// HTAB) from both ends of a cookie name or value. Same trimming
// cookiePresenceValue applies inline to names.
func trimCookieOWS(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t') {
		end--
	}
	return s[start:end]
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
	p := &KeyPolicy{
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
	p.computeDerivedVary()
	return p
}

// computeDerivedVary derives the derivedVary constant (ADR-0071): the
// Vary list the policy alone determines — include_headers unioned with
// the synthetic cookie-presence field — in exactly effectiveVary's
// stored shape (sorted, deduplicated, ", "-joined), so a correct
// derivation hashes to the stored variant keys byte-for-byte. Called
// only at policy construction; mutating a policy after the handler is
// serving is already forbidden.
func (p *KeyPolicy) computeDerivedVary() {
	fields := make([]string, 0, len(p.includeHeaders)+len(p.cookiePresence)+len(p.cookieValues)+1)
	fields = append(fields, p.includeHeaders...)
	if p.hasCookiePresence() {
		fields = append(fields, cookiePresenceField)
	}
	if p.hasCookieValues() {
		fields = append(fields, cookieValuesField)
	}
	sort.Strings(fields)
	uniq := fields[:0]
	for i, f := range fields {
		if i == 0 || f != fields[i-1] {
			uniq = append(uniq, f)
		}
	}
	p.derivedVary = strings.Join(uniq, ", ")
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
	p.cookiePresence = normalizeCookieNameList(names)
	p.computeDerivedVary()
	return p
}

// WithCookieValues sets the cookie-values list after construction
// (issue #870), mirroring WithCookiePresence. Same mutation rule: a
// policy serving a handler must never change — stored VaryKeys would
// no longer match freshly computed ones.
func (p *KeyPolicy) WithCookieValues(names []string) *KeyPolicy {
	if p == nil {
		return nil
	}
	p.cookieValues = normalizeCookieNameList(names)
	p.computeDerivedVary()
	return p
}

// normalizeCookieNameList trims, lowercases, sorts, and dedupes a
// cookie-name list (cookie_presence, cookie_values). The reduction
// (cookiePresenceValue / cookieValuesValue) is positional over this
// canonical form, so every node must reduce the config to the same
// order — which validation's duplicate rejection and this
// normalization together guarantee.
func normalizeCookieNameList(names []string) []string {
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
// include_headers plus the synthetic cookie fields when declared,
// pre-joined as a field-list string. This is derivedVary — the same
// sorted, deduplicated, ", "-joined shape effectiveVary stores — read
// as a constant instead of re-derived per cold miss. Both dimensions
// union into the STORED Vary the same way (effectiveVary), and the
// flight key hashes them through the same varyHeaderValue
// normalization as the storage variant key — any divergence between
// the two keyings is a wrong-body handoff. variantKeyCore re-sorts
// the split fields, so the joined order is cosmetic; sharing one
// derivation guarantees the field SETS can never diverge.
func (p *KeyPolicy) flightVary() string {
	if p == nil {
		return ""
	}
	return p.derivedVary
}

// hasFlightDimensions reports whether the route declares include_headers
// or cookie keying (presence or values). Nil-safe.
func (p *KeyPolicy) hasFlightDimensions() bool {
	return p.hasIncludeHeaders() || p.hasCookieKeying()
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
