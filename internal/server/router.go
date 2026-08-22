package server

import (
	"strings"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// Router is the data-plane HTTP handler. It matches an incoming request
// to a route and dispatches it to the corresponding cache handler.
//
// Stable.
type Router struct {
	routes  []routeEntry
	logger  observability.Logger
	metrics *RouterMetrics
}

type routeEntry struct {
	host       string
	pathPrefix string
	methods    map[string]bool // nil = match all methods
	label      string
	labelVal   string
	handler    fasthttp.RequestHandler
}

// RouterMetrics are the data-plane counters exposed by the router.
type RouterMetrics struct {
	RequestsTotal interface{ Inc() }
	NoRouteTotal  interface{ Inc() }
}

// RouterConfig configures a Router.
type RouterConfig struct {
	Logger  observability.Logger
	Metrics *RouterMetrics
}

// NewRouter builds a Router. Routes are matched in the order they are
// added; the first match wins.
func NewRouter(cfg RouterConfig) *Router {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.Metrics == nil {
		cfg.Metrics = &RouterMetrics{}
	}
	return &Router{
		logger:  cfg.Logger,
		metrics: cfg.Metrics,
	}
}

// AddRoute registers a route entry. When methods is non-empty, only
// requests whose HTTP method is in the set match this route.
func (rt *Router) AddRoute(host, pathPrefix, label string, methods []string, handler fasthttp.RequestHandler) {
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
		handler:    handler,
	})
}

// MatchByHostPath returns the label of the first route matching
// host:path, or "" if none. Uses the same matching logic as ServeRequest
// (host case-insensitive + pathPrefix HasPrefix) but skips method
// matching. Used by admin BuildKeyForURL to find the route's policy.
func (rt *Router) MatchByHostPath(host, path string) string {
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	for i := range rt.routes {
		re := &rt.routes[i]
		if re.host != "" && !strings.EqualFold(re.host, host) {
			continue
		}
		if re.pathPrefix != "" && !strings.HasPrefix(path, re.pathPrefix) {
			continue
		}
		return re.label
	}
	return ""
}

// ServeRequest implements fasthttp.RequestHandler. It matches the
// incoming request to a route and dispatches to the route's handler.
func (rt *Router) ServeRequest(ctx *fasthttp.RequestCtx) {
	if rt.metrics.RequestsTotal != nil {
		rt.metrics.RequestsTotal.Inc()
	}

	host := string(ctx.Host())
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}
	path := string(ctx.Path())

	for i := range rt.routes {
		re := &rt.routes[i]
		if re.host != "" && !strings.EqualFold(re.host, host) {
			continue
		}
		if re.pathPrefix != "" && !strings.HasPrefix(path, re.pathPrefix) {
			continue
		}
		if re.methods != nil && !re.methods[string(ctx.Method())] { //nolint:staticcheck // SA6001: method is used once per iteration, not worth inlining
			continue
		}
		ctx.Request.Header.Set(header.XBouineRoute, re.labelVal)
		re.handler(ctx)
		return
	}

	if rt.metrics.NoRouteTotal != nil {
		rt.metrics.NoRouteTotal.Inc()
	}
	ctx.Error("no matching route", fasthttp.StatusNotFound)
}
