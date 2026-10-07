package cache

import (
	"context"
	"sort"
	"strings"

	"github.com/bouine-cache/bouine/pkg/api"
)

// varyDriftInc is incremented once per detected Vary drift; nil-safe.
// Wired by the engine to bouine_vary_drift_total.
type varyDriftInc = interface{ Inc() }

// varyFieldsEqual compares two Vary union strings as FIELD SETS, not
// byte strings. The stored VaryValue and the fresh effectiveVary can
// differ cosmetically on an include-free route (effectiveVary's union
// normalization runs only when the route declares includes or cookie
// presence; the passthrough stores the origin's raw joined lines, so
// "X-A, X-B" and "x-b, x-a" are the same surface spelled differently).
// A set comparison eliminates every cosmetic difference and leaves
// only real surface changes — a field appearing or disappearing.
func varyFieldsEqual(a, b string) bool {
	if a == b {
		return true
	}
	fa := varyFieldSet(a)
	fb := varyFieldSet(b)
	if len(fa) != len(fb) {
		return false
	}
	sort.Strings(fa)
	sort.Strings(fb)
	for i := range fa {
		if fa[i] != fb[i] {
			return false
		}
	}
	return true
}

// varyFieldSet splits a Vary union into trimmed, lowercased fields.
func varyFieldSet(v string) []string {
	fields := make([]string, 0, strings.Count(v, ",")+1)
	for f := range strings.SplitSeq(v, ",") {
		fields = append(fields, strings.ToLower(strings.TrimSpace(f)))
	}
	return fields
}

// detectVaryDrift compares the variation surface a stored object was
// declared with against the fresh origin response that is about to
// replace or revalidate it (ADR-0058). The stored VaryValue is the
// resolver's memory of what the origin varies on — every variant-key
// computation (lookup, revalidate, and the warm flight key via
// collapseFlightKey) consults it — so a fresh response declaring a
// different surface means the stored declaration is stale and every
// key still derived from it is suspect.
//
// The comparison is on the effectiveVary union (origin Vary plus the
// route's include_headers/cookie_presence), computed exactly as the
// store paths compute it: equal unions select variants by the same
// dimensions, regardless of field order or case (effectiveVary sorts
// and lowercases). The include side is route-fixed (h.policy), so a
// detection is always an origin-side declaration change — the
// operator's config did not move between the two responses.
//
// What counts as drift: the origin adds a Vary field it previously did
// not send, or stops sending one it did. Adding a field is the unsafe
// direction (the stored surface selects too coarsely — the ADR-0057
// shape in storage); dropping one selects too finely (correct bodies,
// lost sharing), but both sides are purged because the whole stored
// variant set was selected under the abandoned surface.
//
// What deliberately does NOT count:
//   - A stored VaryValue of "" — either the origin genuinely declared
//     one body per URL (no drift possible until it starts declaring),
//     or a pre-v4 warm-tier blob whose empty value re-derives on its
//     next store. Treating empty-vs-declared as drift would fire once
//     per legacy blob; the fresh fill re-keys the resolver anyway.
//   - Mere reordering/re-casing of fields: compared as field sets
//     (varyFieldsEqual), so a route whose passthrough VaryValue kept
//     the origin's raw spelling does not false-positive on a
//     cosmetically reordered revalidation. A "*" field stays a real
//     change: it can only appear in the fresh union when the 304-merge
//     path let it through (effectiveVary's store-path contract), and
//     the stored surface was selecting real variant keys.
//   - A 304 whose merged Vary matches the stored declaration:
//     refreshFrom304 recomputes the pair from the merged headers, so
//     the comparison sees the merged declaration, not the raw 304.
//
// On drift the handler Purges the primary key — resolver and every
// tracked variant (RFC 9111 §4.4) — then counts and logs. Purging is
// the fail-safe: the caller's normal store path re-stores the fresh
// response under keys derived from the FRESH declaration, so what the
// purge removes is the window in which the stale resolver kept
// computing variant keys under the abandoned surface (serving
// wrong-variant bodies from surviving variants, and keying warm
// collapsing flights on the old variant key) until TTL. Purge is
// idempotent against a concurrent eviction and tolerates a store
// error (logged by Purge's callers, non-fatal here).
//
// The primary key is recomputed from the triggering request — the
// revalidate paths hold the lookup (variant) key, and the object
// being revalidated may itself be a variant entry whose Key is the
// variant key; the resolver entry lives under the primary key.
// BuildKey is zero-alloc and deterministic, identical to the one the
// request that produced the stale object was keyed with (the primary
// key is a pure function of method/host/path/query and the route
// policy, which is immutable on the Handler).
//
// Called on the revalidate paths only — foreground revalidate,
// background SWR revalidate, and background refresh — after the fresh
// response is known good (304 or cacheable 2xx). A 5xx or uncacheable
// response never declares a trustworthy surface and never triggers the
// signal; the stale object stays served per the stale-fallback gates.
func (h *Handler) detectVaryDrift(ctx context.Context, stale *api.Object, ri RequestInfo, freshVary string) {
	if stale == nil || stale.VaryValue == "" || varyFieldsEqual(stale.VaryValue, freshVary) {
		return
	}
	primaryKey := BuildKey(ri, h.policy)
	_, _ = h.Purge(ctx, primaryKey)
	if h.VaryDriftInc != nil {
		h.VaryDriftInc.Inc()
	}
	if h.logger != nil {
		h.logger.Warn("vary drift: origin declaration changed since store, purging resolver and variants",
			"key", primaryKey.Hex(),
			"stored_vary", stale.VaryValue,
			"fresh_vary", freshVary)
	}
}
