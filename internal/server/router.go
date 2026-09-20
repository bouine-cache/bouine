package server

import (
	"strings"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// Router is the data-plane HTTP handler. It matches an incoming request
// to a route and dispatches it to the corresponding cache handler.
//
// Stable.
type Router struct {
	logger          observability.Logger
	metrics         *RouterMetrics
	trafficClassify *TrafficClassifier
	routes          []routeEntry
}

type routeEntry struct {
	methods    map[string]bool // nil = match all methods
	handler    fasthttp.RequestHandler
	fastPath   api.FastPathHandler // per-route H1 fast path; nil = none
	host       string
	pathPrefix string
	label      string
	labelVal   string
	pool       string
}

// RouterMetrics are the data-plane counters exposed by the router.
type RouterMetrics struct {
	RequestsTotal interface{ Inc() }
	NoRouteTotal  interface{ Inc() }
}

// RouterConfig configures a Router.
type RouterConfig struct {
	Logger          observability.Logger
	Metrics         *RouterMetrics
	TrafficClassify *TrafficClassifier
}

// NewRouter builds a Router. Routes are matched in the order they are
// added; the first match wins.
func NewRouter(cfg RouterConfig) *Router {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.Metrics == nil {
		cfg.Metrics = &RouterMetrics{}
	}
	return &Router{
		logger:          cfg.Logger,
		metrics:         cfg.Metrics,
		trafficClassify: cfg.TrafficClassify,
	}
}

// AddRoute registers a route entry. When methods is non-empty, only
// requests whose HTTP method is in the set match this route. pool is
// the route's upstream pool ("" for pool-less routes, e.g. static
// files): the metrics middleware uses it as the upstream_pool label and
// falls back to "_default" when empty, so that label set stays bounded
// by the pool configuration. The label still feeds the dashboard rings.
// fastPath, when non-nil, is the route's H1 fast-path handler (see
// NewRoutedFastPath); routes without one never receive TryHit.
func (rt *Router) AddRoute(host, pathPrefix, label, pool string, methods []string, handler fasthttp.RequestHandler, fastPath api.FastPathHandler) {
	if label == "" {
		switch {
		case host != "":
			label = host + ":" + pathPrefix
		case pathPrefix != "":
			label = pathPrefix
		default:
			label = "_catch-all"
		}
	}
	var mset map[string]bool
	if len(methods) > 0 {
		mset = make(map[string]bool, len(methods))
		for _, m := range methods {
			mset[m] = true
		}
	}
	rt.routes = append(rt.routes, routeEntry{
		host:       strings.ToLower(host),
		pathPrefix: pathPrefix,
		methods:    mset,
		label:      label,
		labelVal:   label,
		pool:       pool,
		handler:    handler,
		fastPath:   fastPath,
	})
}

// stripHostPort removes the port from a Host header value (an IPv6
// literal keeps its brackets; a bare ":port" is preserved, matching
// lastIndex semantics). Shared authority normalization for ServeRequest,
// MatchByHostPath, and RoutedFastPath.TryHit.
func stripHostPort(host string) string {
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		return host[:idx]
	}
	return host
}

// matchRoute returns the first route matching host (with port —
// stripped here), path prefix, and method, using the router's single
// first-match-wins table; nil when none matches. An empty method skips
// method matching (used by the method-agnostic MatchByHostPath). A
// matched route may carry a nil fastPath — the caller decides.
func (rt *Router) matchRoute(host, path, method string) *routeEntry {
	host = stripHostPort(host)
	for i := range rt.routes {
		re := &rt.routes[i]
		if re.host != "" && !strings.EqualFold(re.host, host) {
			continue
		}
		if re.pathPrefix != "" && !strings.HasPrefix(path, re.pathPrefix) {
			continue
		}
		if method != "" && re.methods != nil && !re.methods[method] {
			continue
		}
		return re
	}
	return nil
}

// MatchByHostPath returns the label of the first route matching
// host:path, or "" if none. Uses the same matching logic as ServeRequest
// (host case-insensitive + pathPrefix HasPrefix) but skips method
// matching. Used by admin BuildKeyForURL to find the route's policy.
func (rt *Router) MatchByHostPath(host, path string) string {
	if re := rt.matchRoute(host, path, ""); re != nil {
		return re.label
	}
	return ""
}

// ServeRequest implements fasthttp.RequestHandler. It matches an
// incoming request to a route and dispatches to the route's handler.
func (rt *Router) ServeRequest(ctx *fasthttp.RequestCtx) {
	if rt.metrics.RequestsTotal != nil {
		rt.metrics.RequestsTotal.Inc()
	}

	// Classified from the raw Host before route resolution so no-route
	// 404s carry the label too. With no classifier the UserValue stays
	// absent and the middleware falls back to "unclassified".
	if rt.trafficClassify != nil {
		ctx.SetUserValue(header.XBouineTrafficClass, rt.trafficClassify.Classify(string(ctx.Host())))
	}

	re := rt.matchRoute(string(ctx.Host()), string(ctx.Path()), string(ctx.Method()))
	if re == nil {
		if rt.metrics.NoRouteTotal != nil {
			rt.metrics.NoRouteTotal.Inc()
		}
		ctx.Error("no matching route", fasthttp.StatusNotFound)
		return
	}
	// Attribution travels as UserValues, not request headers: the
	// old header form was forwarded verbatim upstream, leaking
	// internal route names. The pool UserValue is set only for
	// pool-bearing routes, so the middleware's _default fallback
	// applies to static routes.
	ctx.SetUserValue(header.XBouineRoute, re.labelVal)
	if re.pool != "" {
		ctx.SetUserValue(header.XBouinePool, re.pool)
	}
	re.handler(ctx)
}
