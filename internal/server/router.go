package server

import (
	"net/http"
	"strings"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/header"
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
	labelVal   []string
	handler    http.Handler
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
func (rt *Router) AddRoute(host, pathPrefix, label string, methods []string, handler http.Handler) {
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
		labelVal:   []string{label},
		handler:    handler,
	})
}

// ServeHTTP implements http.Handler.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if rt.metrics.RequestsTotal != nil {
		rt.metrics.RequestsTotal.Inc()
	}

	host := strings.ToLower(r.Host)
	if idx := strings.LastIndex(host, ":"); idx > 0 {
		host = host[:idx]
	}

	for i := range rt.routes {
		re := &rt.routes[i]
		if re.host != "" && re.host != host {
			continue
		}
		if re.pathPrefix != "" && !strings.HasPrefix(r.URL.Path, re.pathPrefix) {
			continue
		}
		if re.methods != nil && !re.methods[r.Method] {
			continue
		}
		r.Header[header.XBouineRoute] = re.labelVal
		re.handler.ServeHTTP(w, r)
		return
	}

	if rt.metrics.NoRouteTotal != nil {
		rt.metrics.NoRouteTotal.Inc()
	}
	http.Error(w, "no matching route", http.StatusNotFound)
}
