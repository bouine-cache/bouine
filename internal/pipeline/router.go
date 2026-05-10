// Package pipeline is the L2 request pipeline. It sits between the
// listener (L1) and the cache engine (L4). In phase 1, there is no
// cache — the pipeline is a thin route matcher that delegates directly
// to the upstream pool handler (L5).
//
// Pipeline stages (configurable, ordered):
//  1. URL & host normalization.
//  2. Route matching (first-match wins).
//  3. Delegate to the matched pool's reverse-proxy handler.
//
// ACL, request collapsing, and cache key construction land in later
// phases.
package pipeline

import (
	"log/slog"
	"net/http"
	"strings"
)

// Router is the data-plane HTTP handler. It matches an incoming request
// to a route and dispatches it to the corresponding pool handler.
//
// Stable.
type Router struct {
	routes  []routeEntry
	logger  *slog.Logger
	metrics *Metrics
}

type routeEntry struct {
	host       string
	pathPrefix string
	handler    http.Handler
}

// Metrics are the data-plane counters exposed by the pipeline. Phase 1
// ships a minimal set; the full RED counters land in phase 3.
type Metrics struct {
	// RequestsTotal is incremented for every request entering the
	// pipeline. nil-safe (no-op if nil).
	RequestsTotal interface{ Inc() }
	// NoRouteTotal counts requests that did not match any route.
	NoRouteTotal interface{ Inc() }
}

// RouterConfig configures a Router.
type RouterConfig struct {
	Logger  *slog.Logger
	Metrics *Metrics
}

// NewRouter builds a Router from a set of routes. Routes are matched in
// the order they are added; the first match wins.
func NewRouter(cfg RouterConfig) *Router {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.Metrics == nil {
		cfg.Metrics = &Metrics{}
	}
	return &Router{
		logger:  cfg.Logger,
		metrics: cfg.Metrics,
	}
}

// AddRoute registers a route entry. host and pathPrefix may be empty
// (match-all).
func (rt *Router) AddRoute(host, pathPrefix string, handler http.Handler) {
	rt.routes = append(rt.routes, routeEntry{
		host:       strings.ToLower(host),
		pathPrefix: pathPrefix,
		handler:    handler,
	})
}

// ServeHTTP implements http.Handler. It is the entry point of the data
// plane — mounted on every listener.
func (rt *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if rt.metrics.RequestsTotal != nil {
		rt.metrics.RequestsTotal.Inc()
	}

	host := strings.ToLower(r.Host)
	// Strip port from host for matching.
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
		re.handler.ServeHTTP(w, r)
		return
	}

	if rt.metrics.NoRouteTotal != nil {
		rt.metrics.NoRouteTotal.Inc()
	}
	http.Error(w, "no matching route", http.StatusNotFound)
}
