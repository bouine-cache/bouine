package cache

import (
	"strconv"
	"strings"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// downstreamcc.go implements ADR-0063 downstream freshness sync: the
// synthesized CDN-Cache-Control (RFC 9213) line emitted on client-facing
// serves of routes configured with downstream_freshness: sync, so that no
// downstream CDN trusts content beyond bouine's own clock.
//
// Why synthesis instead of mutating Cache-Control: ADR-0011 guarantees
// byte-for-byte Cache-Control passthrough to browsers and downstream
// caches; RFC 9213 exists precisely so an intermediary can speak to the
// CDN tier without touching the general-purpose header. Cache-Control is
// therefore never mutated (invariant I1); only the targeted field is
// synthesized, and the origin's own RFC 9213-family and Surrogate-Control
// lines are removed on sync routes so the synthesis is authoritative
// (invariant I3).
//
// All builders here are allocation-free: they append into a
// caller-provided buffer with strconv.AppendInt, because the fast-path
// hit shares them (fastpath.go appendDynamicHeaders) under a zero-alloc
// budget (AGENTS.md §7). SetCanonical copies the bytes, so stack buffers
// are safe on the slow path.

// downstreamSync is the wire value of routes.*.cache.downstream_freshness
// that enables synthesis (config.DownstreamFreshnessSync, ADR-0063 D3).
const downstreamSync = "sync"

// cdcStrict is the strict cap directive list: no numeric clock, no stale
// window. Emitted for serves outside every sanctioned staleness window
// (stayin-alive grace, max-stale authorization beyond the object's own
// window, heuristic staleness past the SWR window) and for relayed 304s
// whose stored entry keeps a stale clock (ADR-0063 §Decision point 2) —
// a downstream cache then revalidates per request instead of adopting
// bouine's exceptional serving posture as entry policy.
const cdcStrict = "public, max-age=0"

// appendServeCDCLine appends the full wire line
// "Cdn-Cache-Control: <directives>\r\n" for a serve of a stored object to
// hbuf and returns it. Zero allocations; used by the fast-path hit
// (appendDynamicHeaders) and the composed-head builder.
func appendServeCDCLine(hbuf []byte, obj *api.Object, now time.Time, result cacheResult) []byte {
	hbuf = append(hbuf, header.CDNCacheControl...)
	hbuf = append(hbuf, ": "...)
	hbuf = appendServeCDCDirectives(hbuf, obj, now, result)
	return append(hbuf, '\r', '\n')
}

// appendServeCDCDirectives appends only the directive list for a serve of
// a stored object (no header name, no CRLF). result selects the serve
// class:
//
//   - cacheHit: public, max-age=<seconds until bouine's wall> (floor 0),
//     plus must-revalidate when the origin sent it (invariant I2).
//   - cacheRevalidated: same as cacheHit, except stored no-cache objects
//     (stored and served only after per-request validation,
//     cacheable.go, engine.go's Revalidate decision) synthesize
//     "public, no-cache" — a numeric clock there would license
//     unvalidated downstream serving, which RFC 9111 §5.2.2.4 forbids.
//   - cacheStale: the mirror — "public, max-age=0,
//     stale-while-revalidate=<window remaining>" when the serve is
//     sanctioned by the object's own SWR window (natural aging, soft
//     purge, which rides the same window, or max-stale within it) or by
//     its SIE window (origin-error fallback serves); strict
//     "public, max-age=0" otherwise.
//
// The remaining-time math is (StoredAt + TTL [+ window]) − now, not the
// ADR's shorthand "TTL − Age": ComputeAge folds OriginAge into Age while
// the stored TTL already excludes it (computeTTL), so TTL − Age would
// undershoot the wall by OriginAge on responses that arrived pre-aged
// behind an upstream CDN — the exact deployment this feature targets.
// Truncation to whole seconds floors the value (never overshoots).
func appendServeCDCDirectives(hbuf []byte, obj *api.Object, now time.Time, result cacheResult) []byte {
	if result == cacheStale {
		return appendStaleMirrorDirectives(hbuf, obj, now)
	}
	if result == cacheRevalidated && obj.RespNoCache {
		return append(hbuf, "public, no-cache"...)
	}
	hbuf = append(hbuf, "public, max-age="...)
	hbuf = strconv.AppendInt(hbuf, cdcWallRemaining(obj, now), 10)
	if obj.RespMustRevalidate {
		hbuf = append(hbuf, ", must-revalidate"...)
	}
	return hbuf
}

// appendStaleMirrorDirectives appends the directive list for a stale
// serve: the SWR mirror when the serve falls inside the object's own SWR
// window, the SIE mirror inside the SIE window, and the strict cap
// ("public, max-age=0", no stale window) outside both. The window checks
// mirror api.Object.StaleForSWR / StaleForSIE exactly (window = StoredAt
// + TTL + window), so the mirror is only emitted when the serving engine
// itself sanctioned the serve by that same window; per-request client
// authorization (max-stale) and outage posture (stayin-alive grace) beyond
// the windows clamp strict instead of becoming downstream entry policy
// (ADR-0063 §Decision point 2). Heuristic staleness rides the mirror
// while inside the SWR window and clamps strict past it, exactly as the
// engine's evalStale serves it.
func appendStaleMirrorDirectives(hbuf []byte, obj *api.Object, now time.Time) []byte {
	if rem := cdcWindowRemaining(obj, obj.StaleWhileRevalidate, now); rem > 0 {
		hbuf = append(hbuf, "public, max-age=0, stale-while-revalidate="...)
		return strconv.AppendInt(hbuf, rem, 10)
	}
	if rem := cdcWindowRemaining(obj, obj.StaleIfError, now); rem > 0 {
		hbuf = append(hbuf, "public, max-age=0, stale-while-revalidate="...)
		return strconv.AppendInt(hbuf, rem, 10)
	}
	return append(hbuf, "public, max-age=0"...)
}

// cdcWallRemaining returns the whole seconds until bouine's freshness
// wall for obj (StoredAt + TTL), floored at zero.
func cdcWallRemaining(obj *api.Object, now time.Time) int64 {
	rem := obj.StoredAt.Add(obj.TTL).Sub(now)
	if rem <= 0 {
		return 0
	}
	return int64(rem / time.Second)
}

// cdcWindowRemaining returns the whole seconds remaining in the
// stale-serve window (StoredAt + TTL + window) − now; negative when the
// window is absent or has elapsed. A soft-purged object (TTL=0, StoredAt
// reset at purge time) gets its window measured from the purge, matching
// the serving engine's StaleForSWR semantics.
func cdcWindowRemaining(obj *api.Object, window time.Duration, now time.Time) int64 {
	if window <= 0 {
		return -1
	}
	rem := obj.StoredAt.Add(obj.TTL + window).Sub(now)
	if rem <= 0 {
		return -1
	}
	return int64(rem / time.Second)
}

// appendFillCDCDirectives appends the directive list for a fill (MISS)
// serve per ADR-0063 D4: the min() rule — max-age=min(origin effective
// max-age, bouine final TTL) — plus the origin's qualitative
// revalidation directives, with the I2 no-cache carve-out: storable
// no-cache responses synthesize "public, no-cache", never a numeric
// clock, because the min() rule would otherwise admit a
// no-revalidation-required downstream entry for a full bouine TTL on a
// route whose origin demanded per-reuse validation.
//
// respCC is the parsed origin directives (CDC-preferred when the origin
// sent a targeted field, per cdnCacheControl); objTTL is bouine's final
// TTL for the fill (override, negative ladder and jitter applied — the
// same value used for storage); originEffective is the origin's freshness
// lifetime from the same headers, zero when the origin sent no explicit
// freshness (heuristic / ttl_default / negative fills), in which case
// the min() reduces to bouine's TTL.
func appendFillCDCDirectives(hbuf []byte, respCC Directives, originEffective, objTTL time.Duration) []byte {
	if respCC.NoCache {
		return append(hbuf, "public, no-cache"...)
	}
	cdc := objTTL
	if originEffective > 0 && originEffective < cdc {
		cdc = originEffective
	}
	if cdc < 0 {
		cdc = 0
	}
	hbuf = append(hbuf, "public, max-age="...)
	hbuf = strconv.AppendInt(hbuf, int64(cdc/time.Second), 10)
	if respCC.MustRevalidate {
		hbuf = append(hbuf, ", must-revalidate"...)
	}
	if respCC.ProxyRevalidate {
		hbuf = append(hbuf, ", proxy-revalidate"...)
	}
	return hbuf
}

// originEffectiveMaxAge returns the origin's effective freshness
// lifetime for a fill: s-maxage when set, else max-age when set, else a
// valid Expires − Date, else zero (no explicit freshness). This is the
// same precedence FreshnessLifetimeH applies; it exists only so the
// fill cap can min() against the origin's own clock (ADR-0063 D4).
func originEffectiveMaxAge(respCC Directives, resMap header.Map) time.Duration {
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge
	}
	expiresAt := parseHTTPDate(resMap.Get(header.Expires))
	if expiresAt.IsZero() {
		return 0
	}
	date := parseHTTPDate(resMap.Get(header.Date))
	if date.IsZero() {
		return 0
	}
	if rem := expiresAt.Sub(date); rem > 0 {
		return rem
	}
	return 0
}

// removeCDCFamily removes the origin's RFC 9213-family and
// Surrogate-Control lines from dst on sync routes (invariant I3: the
// synthesis is authoritative, so a downstream cache must never see both
// the origin's line and bouine's). Passthrough routes never call this —
// origin lines forward byte-for-byte there.
func removeCDCFamily(dst *fasthttp.ResponseHeader) {
	for _, name := range header.CDCFamilyHeaders {
		dst.Del(name)
	}
}

// fillFinalTTLFor computes bouine's final TTL for a fill response at
// header-processing time: fillFinalTTL (the single definition the object
// is stored with) over the parsed origin directives and the corrected
// initial age. Called once by the sync-mode fill cap; the returned value
// is threaded into buildObject as a preset so the cap and the stored
// clock are the same number (ADR-0063 P2).
func (h *Handler) fillFinalTTLFor(resMap header.Map, delay time.Duration, parsed parsedResponse, now time.Time) time.Duration {
	originAge := correctedInitialAge(parseOriginAge(resMap), delay, resMap.Get(header.Date), now)
	return h.fp.fillFinalTTL(resMap, parsed.status, parsed.respCC, originAge, now)
}

// isCDCFamilyHeader reports whether k is one of the RFC 9213-family or
// Surrogate-Control header names (case-insensitive; stored keys are
// canonical, but this also guards header.Map iteration over raw lines).
func isCDCFamilyHeader(k string) bool {
	for _, name := range header.CDCFamilyHeaders {
		if strings.EqualFold(k, name) {
			return true
		}
	}
	return false
}
