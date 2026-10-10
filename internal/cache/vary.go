package cache

import (
	"sort"
	"strconv"
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/bouine-cache/xxhash/v3"

	"github.com/bouine-cache/bouine/pkg/api"
)

// DefaultMaxVariants is the default cap on stored variants per primary
// key. Enforced by the handler: a Put is skipped when the variant count for
// a primary key exceeds this value, preventing Vary blow-up attacks.
// Overridable per route via cache.max_variants (HandlerConfig.MaxVariants).
const DefaultMaxVariants = 1024

// maxVaryFields caps the number of Vary header fields we process.
// RFC 9110 does not limit Vary fields, but >16 is pathological and
// almost certainly an attack. The stack buffer for field names is sized
// accordingly.
const maxVaryFields = 16

// varyContainsStar reports whether the Vary header value contains "*"
// as one of its field names (RFC 9110 §12.5.5, RFC 9111 §4.1).
func varyContainsStar(vary string) bool {
	for f := range strings.SplitSeq(vary, ",") {
		if strings.TrimSpace(f) == "*" {
			return true
		}
	}
	return false
}

// varySource abstracts how the variant-key computation reads a request
// header value and materializes a full header.Map on the overflow path.
// Concrete adapters: header.Map (slow serving path), *fasthttp.RequestHeader
// (fasthttp Peek path), *api.RawRequest (H1 fast path). A generic type
// constraint, not a runtime interface — variantKeyCore instantiates per
// adapter type, so the hit path carries no interface boxing.
type varySource interface {
	getValue(key string) string
	toMap() header.Map
}

// mapVarySrc adapts header.Map for the generic variant-key core.
type mapVarySrc struct{ m header.Map }

func (s mapVarySrc) getValue(key string) string { return s.m.Get(key) }
func (s mapVarySrc) toMap() header.Map          { return s.m }

// fastVarySrc adapts *fasthttp.RequestHeader. Converts values to string
// only for the specific fields listed in Vary (typically 1-2).
type fastVarySrc struct{ h *fasthttp.RequestHeader }

func (s fastVarySrc) getValue(key string) string { return string(s.h.Peek(key)) }
func (s fastVarySrc) toMap() header.Map          { return headerFromFastHTTPReqHeader(s.h) }

// rawVarySrc adapts *api.RawRequest (the H1 fast path's parsed headers).
type rawVarySrc struct{ r *api.RawRequest }

func (s rawVarySrc) getValue(key string) string { return s.r.Header(key) }
func (s rawVarySrc) toMap() header.Map          { return reqHeaderMapFromRaw(s.r) }

// VariantKey computes a composite storage key from the primary key and
// the Vary header. If the response has no Vary (or Vary contains "*",
// which is unmatchable per RFC 9111 §4.1), the primary key is returned.
// Header names listed in exclude are skipped — the variant key is
// computed as if those headers were absent from the Vary list. When
// exclusion empties the Vary list entirely, the variant key collapses
// to the primary key.
//
// Delegates to the shared variantKeyCore — the single implementation of
// the Vary variant-key computation for every request representation.
func VariantKey(primary api.Key, vary string, reqHeader header.Map, policy *KeyPolicy) api.Key {
	return variantKeyCore(primary, vary, mapVarySrc{reqHeader}, policy)
}

// VariantKeyFast computes the variant key using fasthttp.RequestHeader
// directly, avoiding the headerFromCtx allocation (which builds a full
// header.Map with string() for every request header).
func VariantKeyFast(primary api.Key, vary string, reqHeader *fasthttp.RequestHeader, policy *KeyPolicy) api.Key {
	return variantKeyCore(primary, vary, fastVarySrc{reqHeader}, policy)
}

// VariantKeyFromRaw computes the variant key from a RawRequest (the H1
// fast path). Same contract as VariantKey; overflow falls back to the
// alloc path via rawHeaderMap instead of silently returning the primary
// key (the old mirror's behavior, which could serve the wrong variant).
func VariantKeyFromRaw(primary api.Key, vary string, req *api.RawRequest, policy *KeyPolicy) api.Key {
	return variantKeyCore(primary, vary, rawVarySrc{req}, policy)
}

// variantKeyCore is the single Vary variant-key computation: split and
// sort field names, hash "field=value;" pairs sorted by field name.
// Zero-alloc fast path: when the Vary header has ≤ maxVaryFields fields
// and the total hash input fits in 256 bytes, the function uses a
// stack-allocated buffer and xxhash.Sum64 instead of allocating a
// *xxhash.Digest on the heap. Falls back to the allocation path for
// pathological inputs.
//
//nolint:gocyclo // 17: Vary header parsing is inherently branchy
func variantKeyCore[S varySource](primary api.Key, vary string, src S, policy *KeyPolicy) api.Key {
	if vary == "" {
		return primary
	}
	if varyContainsStar(vary) {
		// Vary:* is unmatchable (RFC 9111 §4.1). isCacheBlocked
		// refuses to store such responses, so this branch is never
		// reached against real stored data. Returning primary makes
		// the branch a no-op if the gate is ever bypassed.
		return primary
	}

	// Parse and sort Vary field names using a stack-allocated array.
	// Avoids strings.Split []string allocation and sort.Strings slice.
	var fields [maxVaryFields]string
	n := 0
	for f := range strings.SplitSeq(vary, ",") {
		if n >= maxVaryFields {
			// Pathological Vary — fall back to alloc path.
			return variantKeySlow(primary, vary, src.toMap(), policy)
		}
		fields[n] = strings.ToLower(strings.TrimSpace(f))
		n++
	}
	if n == 0 {
		return primary
	}
	// Inline insertion sort (n is typically 1-3, max 16).
	for i := 1; i < n; i++ {
		for j := i; j > 0 && fields[j-1] > fields[j]; j-- {
			fields[j-1], fields[j] = fields[j], fields[j-1]
		}
	}

	// Build hash input into a stack buffer and use xxhash.Sum64
	// (no heap allocation) instead of xxhash.New() (allocates *Digest).
	var buf [256]byte
	off := 0
	written := false
	for i := 0; i < n; i++ {
		f := fields[i]
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		var val string
		if f == cookiePresenceField && policy.hasCookiePresence() {
			// Synthetic cookie-presence field (issue #768): the hash
			// input is the presence-bit string over the route's
			// listed cookie names, never the raw Cookie value (values
			// are PII + cardinality). All Cookie lines participate:
			// presenceCookieValue canonicalizes every adapter's Cookie
			// content into one "; "-joined §4.2 value (see its note).
			val = policy.cookiePresenceValue(presenceCookieValue(src))
		} else {
			val = varyHeaderValue(f, src.getValue(f), policy)
		}
		needed := len(f) + 1 + len(val) + 1 // f=val;
		if off+needed > len(buf) {
			// Buffer overflow — fall back to alloc path.
			return variantKeySlow(primary, vary, src.toMap(), policy)
		}
		off += copy(buf[off:], f)
		buf[off] = '='
		off++
		off += copy(buf[off:], val)
		buf[off] = ';'
		off++
		written = true
	}
	if !written {
		return primary
	}
	return primary.WithVary(xxhash.Sum64(buf[:off]))
}

// presenceCookieValue returns the canonical Cookie field value the
// cookie-presence computation must scan, for any varySource — every
// line's pairs joined by "; " per RFC 6265 §4.2, the value
// api.RawRequest.CookieValue and header.Map.CookieAll produce. The
// one-line shapes (header.Map built from fasthttp's All(), fasthttp's
// Peek) already carry the joined value; the multi-entry shapes (a map
// with repeated Cookie entries, the RawRequest the h1parser produces)
// keep lines separate, and a single getValue would see only the
// first — a presence bit must never depend on which line a cookie
// landed on. The "; " separator composes with cookiePresenceValue's
// ";"-split without folding any pair into another line's value
// (GetAll's ", " join would — the bug class CookieAll exists to
// prevent).
func presenceCookieValue[S varySource](src S) string {
	switch s := any(src).(type) {
	case rawVarySrc:
		return s.r.CookieValue()
	case mapVarySrc:
		return s.m.CookieAll()
	case fastVarySrc:
		// fasthttp's Peek joins collected cookies with "; "
		// (appendRequestCookieBytes) but a PRE-collection Peek sees
		// only the first line — wrong for presence keying. PeekAll
		// never collects: pre-collection it returns one element per
		// Cookie line, post-collection the single joined value — the
		// join below canonicalizes the first shape and is a no-op
		// view of the second.
		if lines := s.h.PeekAll(header.Cookie); len(lines) > 1 {
			first := true
			var b []byte
			for _, line := range lines {
				if len(line) == 0 {
					continue
				}
				if !first {
					b = append(b, ';', ' ')
				}
				b = append(b, line...)
				first = false
			}
			return string(b)
		}
		return src.getValue(header.Cookie)
	default:
		return src.getValue(header.Cookie)
	}
}

// headerFromFastHTTPReqHeader builds a header.Map from a fasthttp
// request header. Used as a fallback by VariantKeyFast when the
// stack buffer overflows.
func headerFromFastHTTPReqHeader(h *fasthttp.RequestHeader) header.Map {
	hm := header.NewMap(h.Len())
	for k, v := range h.All() {
		hm.AppendEntryCanonical(header.BytesToString(k), header.BytesToString(v))
	}
	hm.SortEntries()
	return hm
}

// variantKeySlow is the fallback allocation path for Vary headers that
// exceed the stack buffer limits (too many fields or too much data).
func variantKeySlow(primary api.Key, vary string, reqHeader header.Map, policy *KeyPolicy) api.Key {
	fields := strings.Split(strings.ToLower(vary), ",")
	for i, f := range fields {
		fields[i] = strings.TrimSpace(f)
	}
	sort.Strings(fields)
	h := xxhash.New()
	written := false
	for _, f := range fields {
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		_, _ = h.WriteString(f)
		_, _ = h.WriteString("=")
		var val string
		if f == cookiePresenceField && policy.hasCookiePresence() {
			// CookieAll joins every Cookie line with the §4.2
			// separator; presence bits over the joined value are
			// identical to the per-line scan (GetAll's ", " join
			// folds pairs into the previous line's last value —
			// the bug class CookieAll exists to prevent).
			val = policy.cookiePresenceValue(reqHeader.CookieAll())
		} else {
			val = varyHeaderValue(f, reqHeader.Get(f), policy)
		}
		_, _ = h.WriteString(val)
		_, _ = h.WriteString(";")
		written = true
	}
	if !written {
		return primary
	}
	return primary.WithVary(h.Sum64())
}

// normalizeHeaderValue lowercases and sorts comma-separated tokens in
// a header value so "en, FR" and "fr, en" produce the same key.
func normalizeHeaderValue(v string) string {
	v = strings.TrimSpace(v)
	if !strings.Contains(v, ",") {
		return strings.ToLower(v)
	}
	parts := strings.Split(v, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(strings.ToLower(p))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

// varyHeaderValue normalizes one Vary-nominated request header value
// for the variant-key lookup hash (variantKeyCore / variantKeySlow —
// the paths behind VariantKey, VariantKeyFast, VariantKeyFromRaw).
//
// Accept-Encoding is bucketed by encodingBucket unless the policy pins
// verbatim (verbatim_encoding: true) — docs/plans/
// accept-encoding-bucketing.md §3.2, ADR-0051. Accept-Language is
// bucketed by langBucket (plan §10) — no config: an unbucketable value
// (absent, "*", malformed, all q=0) falls back to the legacy
// normalization, so degenerate chains keep today's keying exactly.
// Every other field keeps this path's legacy lowercase+sort
// normalization (RFC 9111 §4.1 permits normalizing "in a way that is
// known to have identical semantics"; the pre-bucketing behavior
// lowercased all values).
//
// NOTE: this is deliberately NOT the same dispatch as
// varyAssertionValue (key.go) — the two paths had different non-AE
// semantics before bucketing (this one normalized every field, the
// assertion path only the four list-valued headers) and changing
// either would silently rekey every stored variant on routes with
// custom Vary headers. Only the bucketing rules are shared, and they
// are pinned identical across both by
// TestVaryKeyEncodingBucket_ParityAcrossPaths.
//
// COLLAPSE-KEY INVARIANT (ADR-0057): the flight key routes its declared
// dimensions through this same function (collapseFlightKey ->
// variantKeyCore). It must stay the SINGLE value-normalization authority
// for both the flight key and the storage variant key: any future
// normalization (e.g. a language-fold rule) must land HERE, never on one
// path only, or the keys' equivalence classes diverge and a follower
// parks on a leader whose variant it does not hold. Pinned by
// TestCollapseFlightKey (flight/variant parity).
func varyHeaderValue(field, value string, policy *KeyPolicy) string {
	switch field {
	case "accept-encoding":
		if !policy.verbatimEncoding() {
			return encodingBucket(value)
		}
	case "accept-language":
		if tag, ok := langBucket(value); ok {
			// The key lowercases (tags are case-insensitive and the
			// key must be canonical); the outbound rewrite keeps the
			// original casing.
			return strings.ToLower(tag)
		}
	}
	return normalizeHeaderValue(value)
}

// encodingBucket reduces an Accept-Encoding field value (RFC 9110
// §12.5.3) to the single content coding bouine negotiates with the
// origin: "zstd", "br", "gzip", or "identity".
//
// This is the bucketing docs/architecture.md §3.3 documents. RFC 9111
// §4.1 permits normalizing a Vary-nominated header "in a way that is
// known to have identical semantics"; the bucket is the negotiation
// outcome, not the token set — "gzip, deflate, br" and "br, gzip" both
// produce "br", so one stored variant serves both request populations.
//
// Selection: the highest-weight coding among {br, zstd, gzip} wins
// (ties broken br > zstd > gzip). A weight
// of 0 excludes (RFC 9110 §12.5.3). deflate, identity, and unknown
// tokens contribute nothing: the bucket set is exactly the codings
// bouine re-serves verbatim, and the origin-bound request carries the
// canonical token (rewriteOutboundAE), so an origin that cannot
// produce the coding falls back to identity for the whole bucket —
// consistent bytes, never a Content-Encoding the bucket cannot honor.
//
// Absent, empty, or all-excluded input yields "identity" rather than a
// 406: bouine is a cache, not a negotiator; operators needing strict
// 406 semantics can exclude the header from the key instead.
//
// Zero-allocation: the value is scanned in place. Malformed weights
// parse as far as the well-formed prefix and a fully malformed value
// yields "identity"; parsing never fails outward.
func encodingBucket(v string) string {
	// bestRank tracks the winning coding's position in the bucket
	// order; bestQ is the best weight seen. -1/-1 means nothing
	// acceptable was found yet, which falls through to "identity".
	bestRank, bestQ := -1, float64(-1)
	for rest := strings.TrimSpace(v); rest != ""; {
		var item string
		item, rest, _ = strings.Cut(rest, ",")
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		// Split "coding[;q=weight]" — the weight parameter is the
		// only parameter Accept-Encoding defines (RFC 9110 §12.5.3).
		coding, params, _ := strings.Cut(item, ";")
		coding = strings.ToLower(strings.TrimSpace(coding))
		rank, ok := aeRank(coding)
		if !ok {
			continue
		}
		q := 1.0 // absent weight = 1 (RFC 9110 §5.2.1 default)
		if params != "" {
			if qv, ok := parseAEWeight(params); ok {
				q = qv
			}
		}
		// RFC 9110 §12.5.3: a weight of 0 means "not acceptable" —
		// the coding is excluded from selection entirely, it must not
		// win on a q=0 > initial -1 comparison.
		if q <= 0 {
			continue
		}
		// Strictly-better wins: higher weight, or equal weight at a
		// better rank (br > zstd > gzip). Rank ties keep the first
		// token; any of them names the same bucket, so order is moot.
		if q > bestQ || (q == bestQ && rank > bestRank) {
			bestRank, bestQ = rank, q
		}
	}
	switch bestRank {
	case 3:
		return "br"
	case 2:
		return "zstd"
	case 1:
		return "gzip"
	default:
		return "identity"
	}
}

// aeRank maps a content-coding token to its position in the bucket
// order (br > zstd > gzip). ok=false for every other coding — deflate,
// identity, and unknown tokens do not participate in bucketing; the
// bucket set is exactly the codings bouine re-serves verbatim.
//
// br outranks zstd despite zstd's better compression ratio: the
// bucket exists to maximize variant sharing, and the two dominant
// browser spellings ("...br, zstd" and "...br" without zstd) share
// the br bucket when ties prefer br. Preferring zstd would split the
// modern-browser population into two variants that fetch identical
// bodies from any origin that serves both — the one case bucketing
// exists to collapse. The Varnish builtin VCL makes the same choice
// for the same reason. An origin that serves only zstd still gets
// asked for br and falls back to identity for the whole bucket —
// consistent bytes at the cost of compression on that route.
func aeRank(coding string) (rank int, ok bool) {
	switch coding {
	case "br":
		return 3, true
	case "zstd":
		return 2, true
	case "gzip":
		return 1, true
	}
	return 0, false
}

// parseAEWeight extracts the q parameter from an Accept-Encoding item's
// parameter list (";q=0.5" or "; q=0.5"). ok=false for a missing or
// malformed weight; the caller keeps the RFC default of 1.
func parseAEWeight(params string) (float64, bool) {
	for rest := params; rest != ""; {
		var p string
		p, rest, _ = strings.Cut(rest, ";")
		p = strings.TrimSpace(p)
		if len(p) < 2 || !strings.EqualFold(p[:2], "q=") {
			continue
		}
		// RFC 9110 §5.2.1: weight is 0-4 digits "." 0-3 digits.
		q, err := strconv.ParseFloat(strings.TrimSpace(p[2:]), 64)
		if err != nil || q < 0 {
			return 0, false
		}
		return q, true
	}
	return 0, false
}

// langBucket reduces an Accept-Language field value (RFC 9110 §12.5.4)
// to the single language tag a conformant origin would select: the
// highest-weight tag, in the client's original casing (tags are
// case-insensitive per RFC 9110 §12.5.4; the KEY lowercases it, but
// the outbound rewrite preserves the conventional casing — origins
// that string-compare locale lists expect "fr-FR", not "fr-fr").
// Same negotiation-outcome model as
// encodingBucket (docs/plans/accept-encoding-bucketing.md §10):
// "fr-FR,fr;q=0.9,en;q=0.8" and "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7"
// both produce "fr-fr", so one stored variant serves both chains.
//
// Ties resolve lexicographically (smallest tag wins). Unlike AE — a
// closed four-coding set where ties could be policy-ranked — AL's tag
// set is open, so the tie rule must be a pure function of the
// candidate set: otherwise fill and lookup disagree across chain
// spellings and the cross-variant body-swap bug class returns. The
// deterministic rule also keeps the suite's order- and
// case-normalisation tests passing.
//
// Wrong-body safety on ties: serving the "de" variant to a client that
// tied "en, de" serves a language the client declared acceptable at
// top weight — the behavior the upstream cache-tests
// vary-normalise-lang-select test (kind: optimal) specifies. Real
// browsers send distinct q-cascades and never tie.
//
// ok=false for absent, empty, or unparseable input and for inputs
// with no acceptable tag (only "*", or every tag at q=0): the caller
// falls back to the legacy normalization, which preserves today's
// keying exactly for those degenerate values. Layer 1 deliberately
// does NOT collapse subtags or scripts (en-US vs en-GB, zh-CN vs
// zh-TW) — merging them would serve region-variant bodies from
// origins that key on subtags; that collapse is the
// Content-Language-anchored layer 2 (plan §10.5).
//
// Zero allocations in langBucket itself (comparisons are allocation-
// free ASCII folds); the dispatch lowercases the winner once for the
// key. Versus the legacy sort path's 3-4 allocs on multi-token values.
func langBucket(v string) (bucket string, ok bool) {
	// The winner's tag is kept in its original casing and lowercased
	// once at the end — per-item ToLower allocates per tag, which the
	// gate benchmark caught at 4 allocs/op. All comparisons below are
	// case-insensitive (language tags are ASCII; strings.EqualFold
	// and langTagLess do not allocate).
	best, bestQ := "", float64(-1)
	rest := strings.TrimSpace(v)
	for rest != "" {
		var item string
		item, rest, _ = strings.Cut(rest, ",")
		tag, params, _ := strings.Cut(strings.TrimSpace(item), ";")
		tag = strings.TrimSpace(tag)
		if !isLanguageTag(tag) {
			// Empty, "*", or malformed tags never participate in
			// selection. A wildcard matches any language and cannot
			// name a bucket: a chain with real tags always has a tag
			// to serve, and a chain without one falls back to the
			// legacy key (ok=false).
			continue
		}
		q := 1.0 // absent weight = 1 (RFC 9110 §5.2.1 default)
		if params != "" {
			if qv, parsed := parseAEWeight(params); parsed {
				q = qv
			}
		}
		// RFC 9110 §12.5.4: a weight of 0 means "not acceptable".
		if q <= 0 {
			continue
		}
		switch {
		case q > bestQ:
			best, bestQ = tag, q
		case q == bestQ && strings.EqualFold(tag, best):
			// The same tag spelled again.
		case q == bestQ && langTagLess(tag, best):
			// Order independence: bestQ only ever increases, so once
			// the maximum weight is seen, every equal-weight tag
			// competes by lexicographic minimum and lower weights
			// never displace it. The winner is therefore a pure
			// function of the candidate set, not chain order.
			best = tag
		}
	}
	if best == "" {
		return "", false
	}
	return best, true
}

// langTagLess reports whether a < b case-insensitively for ASCII
// language tags (shorter-prefix rule included), without allocating.
func langTagLess(a, b string) bool {
	n := min(len(a), len(b))
	for i := 0; i < n; i++ {
		ca, cb := foldASCII(a[i]), foldASCII(b[i])
		if ca != cb {
			return ca < cb
		}
	}
	return len(a) < len(b)
}

// foldASCII lowercases one ASCII byte.
func foldASCII(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + ('a' - 'A')
	}
	return c
}

// isLanguageTag reports whether the token is a well-formed BCP 47
// language tag shape, case-insensitively: an alphanumeric primary
// subtag (2-8 chars), optionally followed by hyphen-separated
// alphanumeric subtags. "*" and malformed tokens ("!!!") are rejected;
// the full BCP 47 grammar (script/region/variant positions, singleton
// rules) is deliberately not enforced — the check exists to keep
// garbage out of bucket selection, not to validate i18n data.
func isLanguageTag(tag string) bool {
	if tag == "" || tag == "*" {
		return false
	}
	segStart, segLen := 0, 0
	segs := 0
	for i := 0; i <= len(tag); i++ {
		if i == len(tag) || tag[i] == '-' {
			if segLen == 0 || segLen > 8 {
				return false
			}
			for j := segStart; j < segStart+segLen; j++ {
				if !isTagChar(tag[j]) {
					return false
				}
			}
			segs++
			segStart, segLen = i+1, 0
			continue
		}
		segLen++
	}
	// 1-8 subtags total; more is not a tag anyone negotiates with.
	return segs >= 1 && segs <= 8
}

// isTagChar reports whether c is an alphanumeric tag character
// (case-insensitive; BCP 47 primaries and subtags).
func isTagChar(c byte) bool {
	switch {
	case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		return true
	}
	return false
}
