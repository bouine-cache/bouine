package server

import (
	"strings"
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

// TryHit implements api.FastPathHandler. It mirrors the router's
// matching loop (host case-insensitive + optional port strip, path
// prefix, method set, first match wins) and delegates to the matched
// route's handler. Returns (nil, false) when no route matches or the
// matched route has no fast path.
func (r *RoutedFastPath) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	host := stripHostPort(req.Host)
	for i := range r.router.routes {
		re := &r.router.routes[i]
		if re.host != "" && !strings.EqualFold(re.host, host) {
			continue
		}
		if re.pathPrefix != "" && !strings.HasPrefix(req.Path, re.pathPrefix) {
			continue
		}
		if re.methods != nil && !re.methods[req.Method] {
			continue
		}
		if re.fastPath == nil {
			return nil, false
		}
		return re.fastPath.TryHit(req, now)
	}
	return nil, false
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

// stripHostPort removes the port from a Host header value, mirroring
// Router.ServeRequest's authority normalization (an IPv6 literal keeps
// its brackets; a bare ":port" is preserved, matching lastIndex semantics).
func stripHostPort(host string) string {
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		return host[:idx]
	}
	return host
}
