package cache

import (
	"net/url"
	"strings"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// NominalAcceptEncoding is the request context `bouine explain`
// simulates (issue #814): the browser-dialect Accept-Encoding every
// realistic br-capable client sends, so the explained variant is the
// one the dominant request population would select. It is a display
// constant for the admin plane only — the data plane never sees it.
const NominalAcceptEncoding = "gzip, deflate, br, zstd"

// EncodingBucket reports the content-coding bucket the data plane
// negotiates for an Accept-Encoding value (br > zstd > gzip, ties
// break br; see encodingBucket). Exported for the admin explain
// plane (issue #814) so the bucket label it shows is produced by the
// same picker the data plane keys with — a copied approximation
// would drift. Cold-path use; the hit path keeps calling
// encodingBucket.
func EncodingBucket(v string) string {
	return encodingBucket(v)
}

// CacheableWithRouteDefaults reports whether a response with the
// given status and headers would be stored by this route: the same
// IsCacheableWithDefault call the fill path makes, with the route's
// negative-TTL table and operator default TTL. The reqHeader is the
// nominal request the admin explain plane pins (issue #814) — explain
// re-derives the verdict instead of recording it. Read-only; the
// data plane keeps its own call.
func (h *Handler) CacheableWithRouteDefaults(status int, reqHeader, respHeader header.Map) bool {
	return IsCacheableWithDefault(status, reqHeader, respHeader, h.fp.neg, h.fp.defaultTTL)
}

// PoolName returns the route's upstream pool name, or "" for
// pool-less routes. Read-only accessor for the admin explain plane
// (issue #814); logUpstreamPool keeps its own "_default" metrics
// fallback.
func (h *Handler) PoolName() string {
	return h.poolName
}

// VariantsOf returns the live variant store keys tracked for
// primaryKey, in no particular order, without touching the store:
// the variantSets index that purge and MaxVariants enforcement
// already maintain (populated on the fill path, never consulted on
// hits). Read-only introspection for the admin explain plane
// (issue #814), which resolves each key with the side-effect-free
// store Peek — enumerating this way is O(variants of this key),
// never a store scan.
//
// Unstable.
func (h *Handler) VariantsOf(primaryKey api.Key) []api.Key {
	h.variantMu.Lock()
	set := h.variantSets[primaryKey]
	var out []api.Key
	if len(set) > 0 {
		out = make([]api.Key, 0, len(set))
		for k := range set {
			out = append(out, k)
		}
	}
	h.variantMu.Unlock()
	return out
}

// StrippedParamNames returns the names of the query parameters the
// policy strips from rawQuery, in first-appearance order,
// deduplicated. It mirrors appendCanonicalQuerySlowString's strip
// decision on the decoded view — allowlist, blocklist, prefixes, then
// stripEmpty-when-all-values-empty — so the derivation explain
// reports cannot drift from the key the data plane computes. Dedup
// never appears here: it trims duplicate values, not names.
// Exported for the admin explain plane (issue #814); cold-path only.
func (p *KeyPolicy) StrippedParamNames(rawQuery string) []string {
	if p == nil || !p.HasQueryPolicy() || rawQuery == "" {
		return nil
	}
	params, _ := url.ParseQuery(rawQuery)
	order := make([]string, 0, len(params))
	for s := rawQuery; s != ""; {
		var seg string
		if i := strings.IndexByte(s, '&'); i >= 0 {
			seg, s = s[:i], s[i+1:]
		} else {
			seg, s = s, ""
		}
		k, _, _ := strings.Cut(seg, "=")
		if decoded, err := url.QueryUnescape(k); err == nil {
			k = decoded
		}
		if _, dup := containsParam(order, k); !dup {
			order = append(order, k)
		}
	}
	var out []string
	for _, k := range order {
		if p.shouldStripParamSlow(k, params[k]) {
			out = append(out, k)
		}
	}
	return out
}

func containsParam(names []string, k string) (int, bool) {
	for i, n := range names {
		if n == k {
			return i, true
		}
	}
	return 0, false
}

// shouldStripParamSlow applies the decoded-view strip decision the
// slow canonicalizer uses (appendCanonicalQuerySlowString): allowlist,
// blocklist, prefixes, then stripEmpty when every occurrence of the
// name carries an empty value. Must stay in sync with it — drift here
// means the explain derivation lies about the stored key.
func (p *KeyPolicy) shouldStripParamSlow(k string, values []string) bool {
	if p.keepParams != nil && !p.keepParams[k] {
		return true
	}
	if p.stripParams != nil && p.stripParams[k] {
		return true
	}
	for i := range p.stripPrefixes {
		if strings.HasPrefix(k, p.stripPrefixes[i]) {
			return true
		}
	}
	if p.stripEmpty && p.keepParams == nil {
		for _, v := range values {
			if v != "" {
				return false
			}
		}
		return len(values) > 0
	}
	return false
}
