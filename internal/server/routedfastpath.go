package server

import (
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// RoutedFastPath adapts a Router into an api.FastPathHandler for the
// H1 listeners. It exists because the router owns route semantics
// (first-match-wins over host, path prefix, and methods): a fast path
// keyed on anything less serves hits for requests the slow path would
// never route the same way — wrong content, not just wrong metrics
// (issue #696).
//
// TryHit resolves the request's route with the router's own table and
// delegates to that route's registered fast-path handler. A request
// matching no route, or a route registered without a fast path,
// declines (nil, false) and falls through to the slow path — the same
// authority the router's ServeRequest has.
//
// Release is route-agnostic by design: the h1parser and reactor call it
// after the response is written, once the request buffer (and with it
// the route inputs) has been reused, so the route cannot be
// re-resolved. The release handler injected at construction must
// therefore accept responses from any route's handler — the production
// cache.FastPathHandler.Release returns responses to global sync.Pools
// and ignores its receiver, so any instance is a valid target. The
// wrapper keeps one injected instance for the process lifetime and
// holds no per-request state, which keeps TryHit/Release safe across
// the many parser goroutines sharing this single handler.
//
// Unstable.
type RoutedFastPath struct {
	router  *Router
	release api.FastPathHandler // Release target; TryHit never called on it
}

// NewRoutedFastPath wraps a Router. release must implement the
// receiver-agnostic Release contract described on RoutedFastPath; it
// may be one of the per-route handlers. The router must be fully
// built (AddRoute calls done) before requests are served; routes are
// added during startup, before any listener accepts.
func NewRoutedFastPath(rt *Router, release api.FastPathHandler) *RoutedFastPath {
	return &RoutedFastPath{
		router:  rt,
		release: release,
	}
}

// TryHit implements api.FastPathHandler. It resolves the route with
// the router's shared matchRoute (first-match-wins over host, path
// prefix, and methods) and delegates to the matched route's handler —
// the same authority the router's ServeRequest has. Returns (nil,
// false) when no route matches or the matched route has no fast path.
// The traffic class is stamped on the response in the same pass: a
// config-owned string, empty when no classifier is configured (read
// as "unclassified" by the metrics consumer).
func (r *RoutedFastPath) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	re := r.router.matchRoute(req.Host, req.Path, req.Method)
	if re == nil || re.fastPath == nil {
		return nil, false
	}
	resp, ok := re.fastPath.TryHit(req, now)
	if !ok || resp == nil {
		return resp, ok
	}
	if r.router.trafficClassify != nil {
		resp.TrafficClass = r.router.trafficClassify.Classify(req.Host)
	}
	return resp, true
}

// Release implements api.FastPathHandler. The response may have been
// produced by any of the router's per-route handlers — see the type
// doc for why routing is not re-attempted.
func (r *RoutedFastPath) Release(resp *api.FastPathResponse) {
	if resp == nil {
		return
	}
	r.release.Release(resp)
}
