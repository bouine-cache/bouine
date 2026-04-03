package cache

import (
	"net/http"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// engine.go implements the RFC 9111 cache state machine.
// Decision and Disposition types, Evaluate, and the private helper
// functions (evalMiss, evalNoCache, isFresh, evalStale, revalidateOrMiss)
// live here. All other logic is in the sibling files.

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

	reqCC := ParseCacheControl(mergeHeaderValues(r.Header, "Cache-Control"))

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

	respCC := ParseCacheControl(mergeHeaderValues(obj.Header, "Cache-Control"))

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

func isFresh(obj *api.Object, reqCC Directives, now time.Time) bool {
	// Compute the response age including any origin Age header.
	age := now.Sub(obj.StoredAt) + parseOriginAge(obj.Header)
	fresh := age < obj.TTL

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
		age := now.Sub(obj.StoredAt) + parseOriginAge(obj.Header)
		staleAge := age - obj.TTL
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
