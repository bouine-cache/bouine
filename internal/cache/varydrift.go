package cache

import (
	"context"
	"sort"
	"strings"

	"github.com/valyala/fasthttp"

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

// isDriftDeclarationSource reports whether a fresh response's status
// may be treated as a Vary-declaration source (ADR-0058): 304 and the
// cacheable 2xx statuses. Everything else — 5xx/4xx origin duress, and
// the 3xx/4xx statuses a proxy can revalidate into (301, 302, 404 with
// negative caching, …) — is refused so the drift purge can never be
// triggered by a response that is not, or must not be, stored under the
// new declaration. Cacheability itself (no-store, private, …) is the
// caller's store gate: this function only rules on the status, the one
// dimension it owns.
func isDriftDeclarationSource(status int) bool {
	return status == fasthttp.StatusNotModified || (status >= 200 && status < 300)
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
// The good-response trigger table (ADR-0058) is ENFORCED HERE, not left
// to the call sites: only 304 and cacheable 2xx responses may signal.
// An origin under duress (5xx, 4xx) is not a declaration source — a
// bare error page carries no Vary and would purge live variants on
// every revalidation of a no-cache route during a single outage; an
// uncacheable response (no-store, private, …) will not be stored, so
// purging the old surface would leave the route resolver-less with
// nothing fresh landing under the new declaration. The stale object
// stays served per the stale-fallback gates in both cases.
//
// Callers must still invoke this only on the revalidate paths and only
// with the declaration the store path would actually store.
func (h *Handler) detectVaryDrift(ctx context.Context, stale *api.Object, ri RequestInfo, statusCode int, freshVary string) {
	if stale == nil || stale.VaryValue == "" || !isDriftDeclarationSource(statusCode) || varyFieldsEqual(stale.VaryValue, freshVary) {
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
