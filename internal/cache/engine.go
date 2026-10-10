package cache

import (
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// engine.go implements the RFC 9111 cache state machine.
// Decision and Disposition types, the shared evaluate core, and the private
// helper functions (evalMiss, freshWithRequestCC, evalStale,
// revalidateOrMiss) live here. All other logic is in the sibling files.

// headerGetter is the minimal read interface for HTTP headers. Both
// header.Map and header.Map satisfy it, so parseOriginAge can accept
// either type without conversion.
// Used as a generic type constraint, not a runtime interface, to avoid
// boxing allocations when header.Map (48 bytes) is passed by value.
type headerGetter interface {
	Get(key string) string
}

// Decision is the outcome of the cache state machine.
type Decision int

const (
	// Hit means serve from cache, no origin contact.
	Hit Decision = iota
	// Miss means not in cache; fetch from origin.
	Miss
	// Revalidate means stale; conditional fetch possible.
	Revalidate
	// StaleHit means serve stale (SWR/SIE window).
	StaleHit
	// Bypass means directives forbid caching.
	Bypass
)

// Disposition describes what the caller should do after the Decision.
type Disposition struct {
	Object   *api.Object
	Decision Decision
}

// Evaluate runs the RFC 9111 state machine on a RequestInfo (the
// header.Map serving path). Request directives are parsed here; the
// decision itself is delegated to the shared evaluate below so every
// serving path — header.Map, fasthttp Peek, RawRequest fast path — makes
// identical decisions.
func Evaluate(ri RequestInfo, obj *api.Object, now time.Time) Disposition {
	if ri.GetMethod() != "GET" && ri.GetMethod() != "HEAD" {
		return Disposition{Decision: Bypass}
	}

	// Lead 2 fast-path: most API / browser requests carry no Cache-Control
	// header. A direct map lookup avoids ParseCacheControl + string parsing
	// entirely. The empty Directives{} is safe — all fields default to
	// "absent" (false / zero), which is the correct interpretation of a
	// missing Cache-Control header per RFC 9111 §5.2.
	var reqCC Directives
	if rawCC := ri.Header.Get(header.CacheControl); rawCC != "" {
		reqCC = ParseCacheControl(rawCC)
	}

	// Pragma: no-cache is equivalent to Cache-Control: no-cache
	// for HTTP/1.0 compatibility (RFC 9111 §5.4).
	if !reqCC.NoCache && ri.Header.Get(header.Pragma) == "no-cache" {
		reqCC.NoCache = true
	}

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	// Response-side gate from the parsed response Cache-Control.
	// ponytail: the ccStr fetch is inlined rather than calling objDirectives
	// — objDirectives does not inline (cost 135 > budget 80) and the extra
	// call frame is a measured regression on the zero-alloc Evaluate_Hit
	// hot path. The cold paths use the helper; this one copy is the
	// deliberate exception.
	ccStr := obj.CacheControl
	if ccStr == "" {
		ccStr = obj.Header.Get(header.CacheControl)
	}
	respCC := ParseCacheControl(ccStr)
	return evaluate(obj, reqCC, respGateFromDirectives(respCC), now)
}

// respGate is the response-side Cache-Control subset the RFC 9111 state
// machine branches on: whether the stored response forbids serving without
// revalidation (no-cache) and whether it forbids serving stale
// (must-revalidate / proxy-revalidate). It decouples the decision core from
// where the directives come from — a parsed Cache-Control string (Evaluate)
// or the pre-parsed api.Object flags set at cache-fill time (fast paths) —
// so one implementation covers every serving path.
type respGate struct {
	noCache        bool
	mustRevalidate bool
}

// respGateFromDirectives derives the gate from a parsed response
// Cache-Control.
func respGateFromDirectives(respCC Directives) respGate {
	return respGate{
		noCache:        respCC.NoCache,
		mustRevalidate: respCC.MustRevalidate || respCC.ProxyRevalidate,
	}
}

// respGateFromObject derives the gate from the pre-parsed flags set by
// buildObject at cache-fill time (RespMustRevalidate already ORs
// proxy-revalidate there). Nil-safe: a nil object yields the zero gate;
// evaluate never reads the gate for a nil object anyway.
func respGateFromObject(obj *api.Object) respGate {
	if obj == nil {
		return respGate{}
	}
	return respGate{noCache: obj.RespNoCache, mustRevalidate: obj.RespMustRevalidate}
}

// evaluate is the single RFC 9111 decision state machine. Inputs are the
// pre-parsed request directives, the response-side gate, and the object.
// All parameters are values or pointers already on hand — zero allocs.
// The method check is the caller's: Evaluate rejects non-GET/HEAD,
// evaluateFast checks Peek'd bytes, and the RawRequest fast path is
// qualified GET/HEAD by construction.
func evaluate(obj *api.Object, reqCC Directives, rg respGate, now time.Time) Disposition {
	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	// RFC 9111 §5.2.1.4 / §5.2.2: no-cache on either side → revalidate
	// conditionally when validators exist, else refetch in full.
	if rg.noCache || reqCC.NoCache {
		if obj.ETag != "" || !obj.LastModified.IsZero() {
			return Disposition{Decision: Revalidate, Object: obj}
		}
		return Disposition{Decision: Miss}
	}

	if freshWithRequestCC(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}

	// must-revalidate / proxy-revalidate forbid serving stale
	// (RFC 9111 §5.2.2.3).
	if rg.mustRevalidate {
		return revalidateOrMiss(obj)
	}
	return evalStale(reqCC, obj, now)
}

// objDirectives returns the parsed Cache-Control directives for a stored
// object. It reads the pre-merged CacheControl string set by buildObject at
// cache-fill time and falls back to the header map only for warm-tier or
// legacy objects whose field is empty. Single definition — every path that
// needs a stored object's directives goes through here, never re-implementing
// the merge-then-parse dance.
func objDirectives(obj *api.Object) Directives {
	cc := obj.CacheControl
	if cc == "" {
		cc = obj.Header.Get(header.CacheControl)
	}
	return ParseCacheControl(cc)
}

// effectiveOriginAge returns the Age the object had at the origin. It prefers
// the pre-parsed field and falls back to re-parsing the header for warm-tier
// objects or legacy builds where the transient field is zero.
func effectiveOriginAge(obj *api.Object) time.Duration {
	if obj.OriginAge != 0 {
		return obj.OriginAge
	}
	return parseOriginAge(obj.Header)
}

func evalMiss(reqCC Directives) Disposition {
	if reqCC.OnlyIfCached {
		// RFC 9111 §5.2.1.7: return 504 Gateway Timeout.
		return Disposition{Decision: Bypass}
	}
	return Disposition{Decision: Miss}
}

// freshWithRequestCC reports whether obj is fresh enough to serve given the
// request's Cache-Control directives. Base freshness is delegated to
// api.Object.Fresh — the single source of truth — and request directives can
// only narrow it, never extend it (RFC 9111 §5.2.1).
//
// OriginAge is NOT re-applied to base freshness: computeTTL already subtracted
// it from TTL at store time, so api.Object.Fresh accounts for it via the
// StoredAt+TTL expiry. Re-adding it here is the double-count bug that declared
// objects stale OriginAge seconds early behind a CDN.
func freshWithRequestCC(obj *api.Object, reqCC Directives, now time.Time) bool {
	if !obj.Fresh(now) {
		return false
	}
	if reqCC.MaxAgeSet {
		// current_age = elapsed since store + age already accrued at origin.
		age := now.Sub(obj.StoredAt) + effectiveOriginAge(obj)
		if age > reqCC.MaxAge {
			return false
		}
	}
	if reqCC.MinFreshSet {
		// Remaining freshness lifetime = freshness_lifetime - current_age,
		// which reduces to (StoredAt+TTL) - now (OriginAge cancels because it
		// is in both terms).
		if obj.StoredAt.Add(obj.TTL).Sub(now) < reqCC.MinFresh {
			return false
		}
	}
	return true
}

// evalStale evaluates the stale-path directives (RFC 9111 §4.2) once
// freshness has failed and must-revalidate was ruled out by the gate:
// max-stale, stale-while-revalidate, stale-if-error, and heuristic
// freshness. Shared by every serving path — the response Cache-Control
// is parsed lazily here because this branch only runs for stale objects,
// well off the hit path.
func evalStale(reqCC Directives, obj *api.Object, now time.Time) Disposition {
	originAge := effectiveOriginAge(obj)
	if reqCC.MaxStaleSet {
		age := now.Sub(obj.StoredAt) + originAge
		// RFC 9111 §5.2.1.2: stale age = current_age - freshness_lifetime.
		// freshness_lifetime = TTL + originAge (TTL = freshness_lifetime - originAge).
		staleAge := age - (obj.TTL + originAge)
		if staleAge <= reqCC.MaxStale {
			return Disposition{Decision: StaleHit, Object: obj}
		}
	}
	// stale-while-revalidate (RFC 5861 §3): serve stale immediately,
	// background revalidation triggered by the Hit/StaleHit handler.
	if obj.StaleForSWR(now) {
		return Disposition{Decision: StaleHit, Object: obj}
	}
	// stale-if-error (RFC 5861 §4): object is within SIE window, but we
	// MUST attempt revalidation first; the handler serves stale only if
	// origin returns an error. Return Revalidate so the request goes to
	// origin; the revalidate path checks for 5xx and falls back to stale.
	if obj.StaleForSIE(now) {
		return revalidateOrMiss(obj)
	}
	// Heuristic freshness (RFC 9111 §4.2.2): when the response has no explicit
	// freshness directives at all (no max-age, s-maxage, or Expires header),
	// the object was cached purely heuristically via Last-Modified/10%.
	// Without must-revalidate, a cache MAY serve it stale rather than always
	// revalidating on every miss.
	respCC := objDirectives(obj)
	if !respCC.MaxAgeSet && !respCC.SMaxAgeSet && obj.Header.Get(header.Expires) == "" {
		return Disposition{Decision: StaleHit, Object: obj}
	}
	return revalidateOrMiss(obj)
}

func revalidateOrMiss(obj *api.Object) Disposition {
	if obj.ETag != "" || !obj.LastModified.IsZero() {
		return Disposition{Decision: Revalidate, Object: obj}
	}
	return Disposition{Decision: Miss}
}
