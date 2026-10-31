package server

import (
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// RoutedFastPath adapts a Router into an api.FastPathHandler for the
// H1 listeners. It exists because the router owns route semantics
// (first-match-wins over host — exact or "*.suffix" wildcard —, path
// prefix or pre-compiled regex, and methods): a fast path keyed on
// anything less serves hits for requests the slow path would never
// route the same way — wrong content, not just wrong metrics
// (issue #696).
//
// TryHit resolves the request's route with the router's own table and
// delegates to that route's registered fast-path handler. A request
// matching no route, or a route registered without a fast path,
// declines (nil, false) and falls through to the slow path — the same
// authority the router's ServeRequest has.
//
// Release is route-agnostic by design: the h1parser calls it
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

// routeMemo is the per-connection route-resolution memo stored on
// RawRequest.MemoRoute. host is the exact request Host string the route
// was resolved for (byte equality — no folding, no allocation) and path
// the exact Path that matched; re is the matched routeEntry. A memo hit
// requires byte-equal Host AND Path, so the memo re-validates nothing:
// on a keep-alive connection the host is constant and single-URL load
// (the hot-only benchmark shape, and any CDN-fronted fleet pinned to one
// hostname per backend) hits the memo every request. Allocated once per
// (connection, host, path) change, not per request.
type routeMemo struct {
	host   string
	path   string
	method string
	re     *routeEntry
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
// the router's shared matchRoute (first-match-wins over host — exact
// or "*.suffix" wildcard —, path prefix or pre-compiled regex, and
// methods) and delegates to the matched route's handler — the same
// authority the router's ServeRequest has. Returns (nil,
// false) when no route matches or the matched route has no fast path.
// The traffic class is stamped on the response in the same pass: a
// config-owned string, empty when no classifier is configured (read
// as "unclassified" by the metrics consumer).
func (r *RoutedFastPath) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	// Per-connection route memo: the h1parser reuses one RawRequest scratch
	// per connection, so a memo hit on the previous request skips
	// matchRoute's table walk entirely. The memo key is the exact
	// (Host, Path, Method) triple — byte equality, no allocation — valid
	// because the router is immutable after boot: routes are registered in
	// cmd/builder.go before any listener accepts, and config reload reports
	// route changes under needs_restart instead of mutating the table. On
	// a keep-alive connection pinned to one URL
	// (the hot-only shape) every request after the first hits the memo.
	if memo, _ := req.MemoRoute.(*routeMemo); memo != nil && memo.host == req.Host &&
		memo.path == req.Path && memo.method == req.Method {
		re := memo.re
		if re.fastPath == nil {
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
	re := r.router.matchRoute(req.Host, req.Path, req.Method)
	if re != nil {
		memo, _ := req.MemoRoute.(*routeMemo)
		if memo == nil {
			memo = &routeMemo{}
			req.MemoRoute = memo
		}
		memo.host = req.Host
		memo.path = req.Path
		memo.method = req.Method
		memo.re = re
	} else {
		req.MemoRoute = nil
	}
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
