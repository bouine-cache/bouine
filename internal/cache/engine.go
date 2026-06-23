package cache

import (
	"net/http"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// engine.go implements the RFC 9111 cache state machine.
// Decision and Disposition types, Evaluate, and the private helper
// functions (evalMiss, evalNoCache, freshWithRequestCC, evalStale,
// revalidateOrMiss) live here. All other logic is in the sibling files.

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
	Decision Decision
	Object   *api.Object
}

// Evaluate runs the RFC 9111 state machine.
func Evaluate(r *http.Request, obj *api.Object, now time.Time) Disposition {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return Disposition{Decision: Bypass}
	}

	// Lead 2 fast-path: most API / browser requests carry no Cache-Control
	// header. A direct map lookup avoids ParseCacheControl + string parsing
	// entirely. The empty Directives{} is safe — all fields default to
	// "absent" (false / zero), which is the correct interpretation of a
	// missing Cache-Control header per RFC 9111 §5.2.
	var reqCC Directives
	if rawCC, hasCCReq := r.Header["Cache-Control"]; hasCCReq {
		reqCC = ParseCacheControl(rawCC[0])
		if len(rawCC) > 1 {
			// Rare: multiple Cache-Control headers. Re-parse merged value.
			reqCC = ParseCacheControl(mergeHeaderValues(r.Header, "Cache-Control"))
		}
	}

	// Pragma: no-cache is equivalent to Cache-Control: no-cache
	// for HTTP/1.0 compatibility (RFC 9111 §5.4).
	if !reqCC.NoCache && r.Header.Get("Pragma") == "no-cache" {
		reqCC.NoCache = true
	}

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	// Lead 1: use pre-parsed CacheControl field instead of re-reading the
	// header map on every hit. CacheControl is set once by buildObject at
	// cache-fill time. Fall back to header lookup only when the field is
	// empty (warm-tier load, test fixtures, or objects built before the
	// field was introduced).
	ccStr := obj.CacheControl
	if ccStr == "" {
		ccStr = mergeHeaderValues(obj.Header, "Cache-Control")
	}
	respCC := ParseCacheControl(ccStr)

	if d, ok := evalNoCache(reqCC, respCC, obj); ok {
		return d
	}
	if freshWithRequestCC(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}
	return evalStale(reqCC, respCC, obj, now)
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

func evalNoCache(reqCC, respCC Directives, obj *api.Object) (Disposition, bool) {
	if respCC.NoCache || reqCC.NoCache {
		if obj.ETag != "" || !obj.LastModified.IsZero() {
			return Disposition{Decision: Revalidate, Object: obj}, true
		}
		return Disposition{Decision: Miss}, true
	}
	return Disposition{}, false
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

func evalStale(reqCC, respCC Directives, obj *api.Object, now time.Time) Disposition {
	if respCC.MustRevalidate || respCC.ProxyRevalidate {
		return revalidateOrMiss(obj)
	}
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
	if !respCC.MaxAgeSet && !respCC.SMaxAgeSet && obj.Header.Get("Expires") == "" {
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
