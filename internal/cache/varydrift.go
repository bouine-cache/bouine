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
// byte strings: the passthrough stores the origin's raw Vary spelling,
// so reordered or re-cased fields are the same surface. Only a field
// appearing or disappearing is a change.
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

// detectVaryDrift compares the stored object's declared variation
// surface against the fresh origin response about to replace or
// revalidate it (ADR-0058). The stored VaryValue feeds every
// variant-key computation (lookup, revalidate, warm flight key), so a
// changed surface means every key still derived from it is suspect.
//
// The comparison is on the effectiveVary union (origin Vary plus the
// route's includes), computed as the store paths compute it. The
// include side is route-fixed (h.policy), so a detection is always an
// origin-side change. NOT counted: an empty stored VaryValue (a
// genuine Vary-less declaration, or a pre-v4 warm blob whose value
// re-derives on next store) and cosmetic field reorder/re-case
// (compared as field sets). Adding a field is the unsafe direction
// (stored surface selects too coarsely); dropping one loses sharing —
// but both sides are purged, because the whole stored variant set was
// selected under the abandoned surface.
//
// On drift: Purge the primary key (resolver + tracked variants,
// RFC 9111 §4.4), then count and log. The caller's normal store path
// re-stores the fresh response under keys derived from the FRESH
// declaration, so the purge removes the window in which the stale
// resolver kept serving wrong-variant bodies and keying warm flights
// on the old variant keys. The primary key is recomputed from the
// triggering request (the revalidate paths hold the variant key).
//
// Called on the revalidate paths only, after the fresh response is
// known good (304 or cacheable 2xx) — an origin under duress is not
// a declaration source; the stale object stays served per the
// stale-fallback gates.
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
