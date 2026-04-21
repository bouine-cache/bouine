// Package pipeline is the L2 request pipeline. It sits between the
// listener (L1) and the cache engine (L4). The pipeline matches routes
// and delegates to the cache handler (L4), which falls through to the
// upstream pool handler (L5) on miss.
//
// Pipeline stages (configurable, ordered):
//  1. URL & host normalization.
//  2. Route matching (first-match wins).
//  3. Delegate to the matched cache handler.
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
	label      string   // value written to X-Bouine-Route; set by AddRoute
	labelVal   []string // pre-allocated []string{label}: avoids alloc on hot path
	handler    http.Handler
}

// Metrics are the data-plane counters exposed by the pipeline.
// Includes RED (rate, errors, duration) counters.
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
// (match-all). label is written to the X-Bouine-Route request header
// on every matched request so downstream metrics and ring buffers receive
// a stable, operator-visible key. An empty label defaults to
// host:pathPrefix (or just pathPrefix when host is empty).
func (rt *Router) AddRoute(host, pathPrefix, label string, handler http.Handler) {
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
	rt.routes = append(rt.routes, routeEntry{
		host:       strings.ToLower(host),
		pathPrefix: pathPrefix,
		label:      label,
		labelVal:   []string{label}, // pre-allocated once; reused on every request
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
		// Direct map write: avoids CanonicalMIMEHeaderKey call and
		// []string{label} allocation. "X-Bouine-Route" is already canonical.
		r.Header["X-Bouine-Route"] = re.labelVal
		re.handler.ServeHTTP(w, r)
		return
	}

	if rt.metrics.NoRouteTotal != nil {
		rt.metrics.NoRouteTotal.Inc()
	}
	http.Error(w, "no matching route", http.StatusNotFound)
}
