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

// MaxVariants is the default cap on stored variants per primary key.
// Enforced by the handler: a Put is skipped when the variant count for
// a primary key exceeds this value, preventing Vary blow-up attacks.
const MaxVariants = 64

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
		val := varyHeaderValue(f, src.getValue(f), policy)
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
		val := varyHeaderValue(f, reqHeader.Get(f), policy)
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
// accept-encoding-bucketing.md §3.2, ADR-0051. Every other field keeps
// this path's legacy lowercase+sort normalization (RFC 9111 §4.1
// permits normalizing "in a way that is known to have identical
// semantics"; the pre-bucketing behavior lowercased all values).
//
// NOTE: this is deliberately NOT the same dispatch as
// varyAssertionValue (key.go) — the two paths had different non-AE
// semantics before bucketing (this one normalized every field, the
// assertion path only the four list-valued headers) and changing
// either would silently rekey every stored variant on routes with
// custom Vary headers. Only the AE rule is shared, and it is pinned
// identical across both by TestVaryKeyEncodingBucket_ParityAcrossPaths.
func varyHeaderValue(field, value string, policy *KeyPolicy) string {
	if field == "accept-encoding" && !policy.verbatimEncoding() {
		return encodingBucket(value)
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
// Selection: the highest-weight coding among {zstd, br, gzip} wins
// (ties broken zstd > br > gzip, the coding-quality order). A weight
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
		// better rank (zstd > br > gzip). Rank ties keep the first
		// token; any of them names the same bucket, so order is moot.
		if q > bestQ || (q == bestQ && rank > bestRank) {
			bestRank, bestQ = rank, q
		}
	}
	switch bestRank {
	case 3:
		return "zstd"
	case 2:
		return "br"
	case 1:
		return "gzip"
	default:
		return "identity"
	}
}

// aeRank maps a content-coding token to its position in the bucket
// order (zstd > br > gzip). ok=false for every other coding — deflate,
// identity, and unknown tokens do not participate in bucketing; the
// bucket set is exactly the codings bouine re-serves verbatim.
func aeRank(coding string) (rank int, ok bool) {
	switch coding {
	case "zstd":
		return 3, true
	case "br":
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
