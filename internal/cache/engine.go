package cache

import (
	"net/http"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// Decision is the outcome of the cache state machine.
type Decision int

const (
	// Hit — serve from cache, no origin contact.
	Hit Decision = iota
	// Miss — not in cache; fetch from origin.
	Miss
	// Revalidate — stale but revalidation possible (conditional req).
	Revalidate
	// StaleHit — serve stale (SWR/SIE window).
	StaleHit
	// Bypass — request or response directives forbid caching.
	Bypass
)

// Disposition describes what the caller should do after the Decision.
type Disposition struct {
	Decision Decision
	Object   *api.Object // non-nil on Hit, StaleHit, Revalidate.
}

// Evaluate runs the RFC 9111 state machine. It is pure — no I/O, no
// side effects.
func Evaluate(r *http.Request, obj *api.Object, now time.Time) Disposition {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return Disposition{Decision: Bypass}
	}

	reqCC := ParseCacheControl(r.Header.Get("Cache-Control"))

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	respCC := ParseCacheControl(obj.Header.Get("Cache-Control"))

	if d, ok := evalNoCache(reqCC, respCC, obj); ok {
		return d
	}
	if isFresh(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}
	return evalStale(reqCC, respCC, obj, now)
}

func evalMiss(reqCC Directives) Disposition {
	if reqCC.OnlyIfCached {
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

func isFresh(obj *api.Object, reqCC Directives, now time.Time) bool {
	age := now.Sub(obj.StoredAt)
	fresh := obj.Fresh(now)

	if reqCC.MaxAgeSet && age > reqCC.MaxAge {
		fresh = false
	}
	if reqCC.MinFreshSet && (obj.TTL-age) < reqCC.MinFresh {
		fresh = false
	}
	return fresh
}

func evalStale(reqCC, respCC Directives, obj *api.Object, now time.Time) Disposition {
	if respCC.MustRevalidate || respCC.ProxyRevalidate {
		return revalidateOrMiss(obj)
	}
	if reqCC.MaxStaleSet {
		staleAge := now.Sub(obj.StoredAt) - obj.TTL
		if staleAge <= reqCC.MaxStale {
			return Disposition{Decision: StaleHit, Object: obj}
		}
	}
	if obj.StaleButServable(now) {
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

// IsCacheable determines whether an origin response should be stored.
// Per RFC 9111 §3 and PLAN.md §3.4.
func IsCacheable(status int, reqHeader, respHeader http.Header) bool {
	respCC := ParseCacheControl(respHeader.Get("Cache-Control"))

	// no-store → never cache.
	if respCC.NoStore {
		return false
	}
	// private → not cacheable by shared cache.
	if respCC.Private {
		return false
	}
	// Set-Cookie → not cacheable by default (PLAN.md §3.4).
	if respHeader.Get("Set-Cookie") != "" {
		return false
	}
	// Authorization + no public/must-revalidate/s-maxage → not
	// cacheable (RFC 9111 §3.5).
	if reqHeader.Get("Authorization") != "" {
		if !respCC.Public && !respCC.MustRevalidate && !respCC.SMaxAgeSet {
			return false
		}
	}

	// Must have explicit freshness or heuristic freshness.
	if respCC.MaxAgeSet || respCC.SMaxAgeSet {
		return true
	}
	if respHeader.Get("Expires") != "" {
		return true
	}

	// Heuristic freshness for selected status codes (RFC 9111 §4.2.2).
	switch status {
	case 200, 203, 204, 300, 301, 308, 404, 405, 410, 414, 501:
		return true
	}

	return false
}

// ComputeAge calculates the Age header value per RFC 9111 §4.2.3.
func ComputeAge(obj *api.Object, now time.Time) time.Duration {
	return now.Sub(obj.StoredAt)
}

// ConditionalHeaders sets If-None-Match and If-Modified-Since on a
// revalidation request from the stored object's validators.
func ConditionalHeaders(req *http.Request, obj *api.Object) {
	if obj.ETag != "" {
		req.Header.Set("If-None-Match", obj.ETag)
	}
	if !obj.LastModified.IsZero() {
		req.Header.Set("If-Modified-Since",
			obj.LastModified.UTC().Format(http.TimeFormat))
	}
}
