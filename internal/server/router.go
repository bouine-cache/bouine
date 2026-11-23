package server

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// Router is the data-plane HTTP handler. It matches an incoming request
// to a route and dispatches it to the corresponding cache handler.
//
// The route table lives in an immutable snapshot behind an
// atomic.Pointer (issue #951, config reload phase 2): matchRoute does
// one atomic load per request, zero allocations, and returns a
// *routeEntry into that snapshot, so in-flight requests during a swap
// are safe. Routes are registered with AddRoute/AddRouteSpec into a
// build buffer that NewRouter pre-publishes, so an unfrozen router
// already serves; Freeze/SetTable swap in a new immutable table.
//
// Stable.
type Router struct {
	logger          observability.Logger
	metrics         *RouterMetrics
	trafficClassify *TrafficClassifier
	// table is the live immutable route snapshot. matchRoute loads it
	// once per request.
	table atomic.Pointer[routeTable]
	// build accumulates routes between AddRoute calls; Freeze moves
	// it into table. Guarded by buildMu so a reload swap never races
	// an in-flight registration.
	build *routeTable

	buildMu sync.Mutex
}

// routeTable is the immutable route snapshot installed by Freeze and
// SetTable. Never mutated after publication; a config reload builds a
// fresh one with TableBuilder and swaps it with one atomic store.
type routeTable struct {
	// byIdentity indexes routes by their config-identity string
	// (decision 12) so a reload diff can reuse unchanged entries.
	byIdentity map[string]int
	routes     []routeEntry
	shadows    []string
}

// RouteTable is the public view of the immutable route table: the
// engine hands it to Router.SetTable and reads identity entries back
// during a reload diff.
type RouteTable = routeTable

// EntryByIdentity returns the route entry registered under identity,
// for reload reuse of unchanged routes. The identity key is computed
// by the caller (cmd layer) from the config route.
func (t *RouteTable) EntryByIdentity(identity string) (RouteEntryView, bool) {
	if t == nil {
		return RouteEntryView{}, false
	}
	i, ok := t.byIdentity[identity]
	if !ok {
		return RouteEntryView{}, false
	}
	re := t.routes[i]
	return RouteEntryView{
		Host: re.hostPat.Host(), PathPrefix: re.pathPrefix, Path: re.pathRaw,
		Label: re.label, Pool: re.pool, Methods: re.methods,
		Handler: re.handler, FastPath: re.fastPath,
	}, true
}

// RouteEntryView is the public, read-only projection of a routeEntry
// used by the reload diff to reuse unchanged routes.
type RouteEntryView struct {
	Handler    fasthttp.RequestHandler
	FastPath   api.FastPathHandler
	Methods    map[string]bool
	Host       string
	PathPrefix string
	Path       string
	Label      string
	Pool       string
}

type routeEntry struct {
	// Pointer-bearing fields first and the trafficPattern (whose
	// pattern-kind byte drags padding) last, so the struct's GC scan
	// prefix is contiguous and minimal (govet fieldalignment).
	methods    map[string]bool // nil = match all methods
	handler    fasthttp.RequestHandler
	fastPath   api.FastPathHandler // per-route H1 fast path; nil = none
	pathRe     *regexp.Regexp      // pre-compiled match.path; nil = none
	pathPrefix string
	pathRaw    string // raw match.path pattern, kept for shadow messages
	label      string
	labelVal   string
	pool       string
	hostPat    trafficPattern // compiled host predicate (exact or leading "*." suffix)
}

// RouteSpec is the declarative form of a route registration: the full
// match predicate plus the wiring AddRoute takes positionally. Host is
// an exact host or a leading "*." wildcard (suffix match anchored on
// the label boundary). Path is an anchored RE2 pattern matched against
// the request path, mutually exclusive with PathPrefix; it is compiled
// once here, never per request. The spec must have passed
// config.Validate (AddRouteSpec still reports regex compile failures
// so unvalidated callers cannot register a never-matching route
// silently).
type RouteSpec struct {
	// Pointer-bearing fields grouped, the Methods slice last, so the
	// struct's GC scan prefix is minimal (govet fieldalignment).
	Handler    fasthttp.RequestHandler
	FastPath   api.FastPathHandler
	Host       string
	PathPrefix string
	Path       string
	Label      string
	Pool       string
	Methods    []string
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
	rt := &Router{
		logger:          cfg.Logger,
		metrics:         cfg.Metrics,
		trafficClassify: cfg.TrafficClassify,
		build:           &routeTable{},
	}
	// Publish the build buffer as the initial table so routes added
	// before the first Freeze/SetTable are visible; Freeze swaps in a
	// fresh buffer and republishes.
	rt.table.Store(rt.build)
	return rt
}

// AddRoute registers a route entry with an exact host and a path
// prefix — the pre-#772 predicate surface, kept for the many existing
// call sites. See AddRouteSpec for the wildcard-host and regex-path
// forms. When methods is non-empty, only requests whose HTTP method is
// in the set match this route. pool is the route's upstream pool (""
// for pool-less routes, e.g. static files): the metrics middleware
// uses it as the upstream_pool label and falls back to "_default" when
// empty, so that label set stays bounded by the pool configuration.
// The label still feeds the dashboard rings. fastPath, when non-nil,
// is the route's H1 fast-path handler (see NewRoutedFastPath); routes
// without one never receive TryHit.
func (rt *Router) AddRoute(host, pathPrefix, label, pool string, methods []string, handler fasthttp.RequestHandler, fastPath api.FastPathHandler) {
	rt.addRoute(RouteSpec{
		Host:       host,
		PathPrefix: pathPrefix,
		Label:      label,
		Pool:       pool,
		Methods:    methods,
		Handler:    handler,
		FastPath:   fastPath,
	}, nil)
}

// AddRouteSpec registers a route with the full match predicate
// (issue #772): wildcard host and/or anchored RE2 path. The regex is
// compiled once, here; a compile failure is returned so the caller can
// log-and-skip instead of serving a route that never matches. Routes
// are matched in the order they are added; the first match wins.
func (rt *Router) AddRouteSpec(spec RouteSpec) error {
	var re *regexp.Regexp
	if spec.Path != "" {
		var err error
		re, err = regexp.Compile(spec.Path)
		if err != nil {
			return fmt.Errorf("route %q match.path %q: %w", spec.Label, spec.Path, err)
		}
	}
	rt.addRoute(spec, re)
	return nil
}

// TableBuilder accumulates route entries into a fresh routeTable,
// used by the engine to build the swapped-in table during a config
// reload (issue #951) without touching the live router. The finished
// table is installed with Router.SetTable.
type TableBuilder struct {
	t        *routeTable
	identity string
}

// NewTableBuilder returns an empty TableBuilder.
func NewTableBuilder() *TableBuilder { return &TableBuilder{t: &routeTable{}} }

// Add registers one spec, compiling match.path and running shadow
// detection. A regex compile failure is returned, not stored.
func (b *TableBuilder) Add(spec RouteSpec) error {
	var re *regexp.Regexp
	if spec.Path != "" {
		var err error
		re, err = regexp.Compile(spec.Path)
		if err != nil {
			return fmt.Errorf("route %q match.path %q: %w", spec.Label, spec.Path, err)
		}
	}
	addRouteTo(b.t, spec, re, b.identity)
	return nil
}

// WithIdentity sets the config identity (decision 12) recorded for
// the next Add call, so the built table can be diffed by identity
// during a reload.
func (b *TableBuilder) WithIdentity(identity string) *TableBuilder {
	b.identity = identity
	return b
}

// Table returns the built immutable table. The builder must not be
// reused afterwards: the returned table is shared with the router.
func (b *TableBuilder) Table() *RouteTable { return b.t }

// Freeze publishes the routes registered so far as the router's live
// immutable table: it atomically swaps the build buffer into table and
// resets the buffer. Boot calls it once after buildRouter; reload
// swaps use SetTable with a TableBuilder-built table instead.
func (rt *Router) Freeze() {
	rt.buildMu.Lock()
	t := rt.build
	rt.build = &routeTable{}
	rt.buildMu.Unlock()
	rt.table.Store(t)
}

// SetTable atomically swaps the live route table (config reload phase
// 2, issue #951). The handed-over table must be treated as immutable
// by the caller afterwards: the router only reads it. Handlers of
// routes replaced by the swap are not closed here — the engine owns
// handler lifecycle (Close-at-swap, decision 10).
func (rt *Router) SetTable(t *RouteTable) {
	rt.table.Store(t)
}

// LiveTable returns the current immutable route table snapshot (nil
// before the first publish). Used by the engine to fold the live
// routes into the boot snapshot.
func (rt *Router) LiveTable() *RouteTable { return rt.table.Load() }

// RouteCount reports how many routes the live table holds (status and
// tests).
func (rt *Router) RouteCount() int {
	if t := rt.table.Load(); t != nil {
		return len(t.routes)
	}
	return 0
}

// routesForTest returns the live table's route slice for label
// assertions in tests. Production code must use matchRoute.
func (rt *Router) routesForTest() []routeEntry {
	if t := rt.table.Load(); t != nil {
		return t.routes
	}
	return nil
}

// addRoute is the single place a validated RouteSpec becomes a stored
// routeEntry, so the label derivation, host compilation, method-set
// build, and shadow detection can never diverge between AddRoute and
// AddRouteSpec. pathRe is the pre-compiled spec.Path (nil when unset).
func (rt *Router) addRoute(spec RouteSpec, pathRe *regexp.Regexp) {
	rt.buildMu.Lock()
	defer rt.buildMu.Unlock()
	addRouteTo(rt.build, spec, pathRe, "")
}

// addRouteTo appends one compiled routeEntry to table, running shadow
// detection against the routes already in it. identity is the
// caller-computed config identity (decision 12); empty skips the
// byIdentity index.
func addRouteTo(table *routeTable, spec RouteSpec, pathRe *regexp.Regexp, identity string) {
	label := spec.Label
	if label == "" {
		switch {
		case spec.Host != "":
			label = spec.Host + ":" + spec.PathPrefix
		case spec.PathPrefix != "":
			label = spec.PathPrefix
		case spec.Path != "":
			label = spec.Path
		default:
			label = "_catch-all"
		}
	}
	var mset map[string]bool
	if len(spec.Methods) > 0 {
		mset = make(map[string]bool, len(spec.Methods))
		for _, m := range spec.Methods {
			mset[m] = true
		}
	}
	e := &routeEntry{
		hostPat:    compileRouteHost(spec.Host),
		pathPrefix: spec.PathPrefix,
		pathRaw:    spec.Path,
		pathRe:     pathRe,
		methods:    mset,
		label:      label,
		labelVal:   label,
		pool:       spec.Pool,
		handler:    spec.Handler,
		fastPath:   spec.FastPath,
	}
	if msg, ok := shadowedByEarlier(table, e); ok {
		table.shadows = append(table.shadows, msg)
	}
	if identity != "" {
		if table.byIdentity == nil {
			table.byIdentity = map[string]int{}
		}
		table.byIdentity[identity] = len(table.routes)
	}
	table.routes = append(table.routes, *e)
}

// Host returns the raw host pattern behind a compiled route host
// predicate (used by the reload diff to rebuild RouteSpecs for
// unchanged routes).
func (p trafficPattern) Host() string {
	if p.kind == patternSuffix {
		return "*" + p.match
	}
	return p.match
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

// compileRouteHost reduces a validated host pattern to the forms
// matchRoute compares against: a leading "*." wildcard becomes the
// suffix ".rest" (matches any host ending in it, strictly longer — the
// same label-boundary semantics as the traffic-class classifier),
// anything else stays exact. Lowercased once, here.
func compileRouteHost(host string) trafficPattern {
	lh := strings.ToLower(host)
	if strings.HasPrefix(lh, "*.") {
		return trafficPattern{match: lh[1:], kind: patternSuffix}
	}
	return trafficPattern{match: lh, kind: patternExact}
}

// matchRoute returns the first route matching host (with port —
// stripped here), path predicate (prefix or pre-compiled regex), and
// method, using the router's single first-match-wins table; nil when
// none matches. An empty method skips method matching (used by the
// method-agnostic MatchByHostPath). A matched route may carry a nil
// fastPath — the caller decides. Host comparisons stay zero-allocation:
// exact matches use api.EqualFold, wildcard matches a length-bounded
// EqualFold on the suffix.
func (rt *Router) matchRoute(host, path, method string) *routeEntry {
	host = stripHostPort(host)
	t := rt.table.Load()
	if t == nil {
		return nil
	}
	for i := range t.routes {
		re := &t.routes[i]
		if !hostMatches(re.hostPat, host) {
			continue
		}
		if !pathMatches(re, path) {
			continue
		}
		if method != "" && re.methods != nil && !re.methods[method] {
			continue
		}
		return re
	}
	return nil
}

// hostMatches reports whether the request host (port already
// stripped) satisfies the compiled route host predicate. An empty
// pattern matches every host (the catch-all form).
func hostMatches(pat trafficPattern, host string) bool {
	if pat.kind == patternSuffix {
		// Strictly longer: "*.example.com" matches subdomains,
		// never the bare ".example.com" or "example.com".
		return len(host) > len(pat.match) &&
			api.EqualFold(host[len(host)-len(pat.match):], pat.match)
	}
	return pat.match == "" || api.EqualFold(pat.match, host)
}

// pathMatches reports whether the request path satisfies the route's
// path predicate: the pre-compiled regex when one is registered, the
// raw prefix otherwise. Both absent matches every path. The regex is
// compiled once at registration (AddRouteSpec), so this is evaluation
// cost only — never fmt, never compilation, per request.
func pathMatches(re *routeEntry, path string) bool {
	if re.pathRe != nil {
		return re.pathRe.MatchString(path)
	}
	return re.pathPrefix == "" || strings.HasPrefix(path, re.pathPrefix)
}

// MatchByHostPath returns the label of the first route matching
// host:path, or "" if none. Uses the same matching logic as ServeRequest
// (host case-insensitive exact or "*.suffix" wildcard + path prefix or
// pre-compiled regex) but skips method matching. Used by admin
// BuildKeyForURL to find the route's policy.
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
