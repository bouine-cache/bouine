// Package admin is the L7 control plane. It serves the admin API,
// health/readiness probes, metrics, and the dashboard SPA. The admin
// surface MUST stay on its own listener; it is never bound on the
// data-plane port (see AGENTS.md §2).
//
// The admin server uses fasthttp.Server. pprof handlers are wrapped via
// the pprofwrapper package, which isolates the net/http dependency.
// ADR-0034 documents the decision.
package admin

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"net"
	"regexp"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/admin/pprofwrapper"
	"github.com/bouine-cache/bouine/internal/buildinfo"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// Config controls the admin server.
//
// Stable.
type Config struct {
	Logger             observability.Logger
	PeerPutHandler     fasthttp.RequestHandler
	PeerPurgeHandler   fasthttp.RequestHandler
	Metrics            *observability.Metrics
	PeersFn            func() []api.PeerInfo
	PurgeFn            func(key api.Key) error
	BanFn              func(expr api.BanExpr) (int, error)
	CacheCheckFn       func(ctx context.Context, rawURL string) CacheCheckResult
	ConfigFn           func() any
	StatsFn            func() api.Stats
	DrainFn            func()
	RefreshFn          func(key api.Key) error
	ConditionsFn       func() []Condition
	PeerBanHandler     fasthttp.RequestHandler
	PeerRefreshHandler fasthttp.RequestHandler
	ReadyFn            func() bool
	CFStatusFn         func() CloudflareStatus
	DashboardHandler   fasthttp.RequestHandler
	OnRefreshed        func(ctx context.Context, url string)
	OnBanned           func(ctx context.Context, expr api.BanExpr)
	PeerFetchHandler   fasthttp.RequestHandler
	CFPropagateFn      func(ctx context.Context, req CFPropagateRequest) error
	PeerMetricsHandler fasthttp.RequestHandler
	OnPurged           func(ctx context.Context, url string)
	FaviconHandler     fasthttp.RequestHandler
	Addr               string
	Token              string
	RateLimitPerSecond int
	MaxBodyBytes       int
	MaxBatchSize       int
	PprofEnabled       bool
}

// Condition is a readiness condition status entry for the
// /readyz?detail=1 JSON response.
type Condition struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
}

// swapHandler is an atomic wrapper around fasthttp.RequestHandler.
type swapHandler struct {
	h atomic.Value
}

func (s *swapHandler) ServeRequest(ctx *fasthttp.RequestCtx) {
	s.h.Load().(fasthttp.RequestHandler)(ctx)
}

func (s *swapHandler) Store(h fasthttp.RequestHandler) {
	s.h.Store(h)
}

// Server is the admin HTTP server with lifecycle methods matching the
// supervised-group contract.
//
// Stable.
type Server struct {
	resolved atomic.Value
	inner    *fasthttp.Server
	swap     *swapHandler
	addr     string
	cfg      Config
}

// NewMinimal creates an admin server with only healthz, readyz, version,
// and drain routes.
func NewMinimal(addr string, readyFn func() bool, conditionsFn func() []Condition, drainFn func(), logger observability.Logger) *Server {
	if addr == "" {
		addr = ":9000"
	}
	logger = observability.ResolveLogger(logger)
	s := &Server{
		cfg:  Config{Addr: addr, ReadyFn: readyFn, ConditionsFn: conditionsFn, DrainFn: drainFn, Logger: logger},
		addr: addr,
	}
	handler := s.minimalHandler()
	sh := &swapHandler{}
	sh.Store(handler)
	s.swap = sh
	s.inner = &fasthttp.Server{
		Handler:               sh.ServeRequest,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          5 * time.Second,
		IdleTimeout:           30 * time.Second,
		NoDefaultServerHeader: true,
	}
	return s
}

// SwapHandler atomically replaces the server's handler.
func (s *Server) SwapHandler(h fasthttp.RequestHandler) {
	if s.swap != nil {
		s.swap.Store(h)
	}
}

// New constructs the admin server.
func New(cfg Config) *Server {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.Addr == "" {
		cfg.Addr = ":9000"
	}
	s := &Server{cfg: cfg, addr: cfg.Addr}
	handler := s.fullHandler()
	s.inner = &fasthttp.Server{
		Handler:               handler,
		ReadTimeout:           5 * time.Second,
		WriteTimeout:          5 * time.Second,
		IdleTimeout:           30 * time.Second,
		NoDefaultServerHeader: true,
	}
	if cfg.PprofEnabled {
		s.inner.WriteTimeout = 0
	}
	return s
}

func (s *Server) minimalHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		switch string(ctx.Path()) {
		case "/healthz":
			s.healthz(ctx)
		case "/readyz":
			s.readyz(ctx)
		case "/version":
			s.version(ctx)
		case "/drain":
			s.drain(ctx)
		default:
			ctx.Error("not found", fasthttp.StatusNotFound)
		}
	}
}

func (s *Server) fullHandler() fasthttp.RequestHandler {
	core := s.routeHandler()
	authed := s.authMiddleware(core)
	maxBody := s.cfg.MaxBodyBytes
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	limited := s.bodyLimitMiddleware(authed, maxBody)
	var top = limited
	if s.cfg.RateLimitPerSecond > 0 {
		top = s.rateLimitMiddleware(limited, s.cfg.RateLimitPerSecond)
	}
	if s.cfg.DashboardHandler != nil {
		dashHandler := s.cfg.DashboardHandler
		faviconHandler := s.cfg.FaviconHandler
		return func(ctx *fasthttp.RequestCtx) {
			p := string(ctx.Path())
			if p == "/" {
				ctx.Redirect("/dashboard/", fasthttp.StatusFound)
				return
			}
			if strings.HasPrefix(p, "/dashboard/") {
				dashHandler(ctx)
				return
			}
			if faviconHandler != nil {
				switch p {
				case "/favicon.ico":
					ctx.Redirect("/favicon/favicon.ico", fasthttp.StatusMovedPermanently)
					return
				case "/apple-touch-icon.png":
					ctx.Redirect("/favicon/apple-touch-icon.png", fasthttp.StatusMovedPermanently)
					return
				case "/site.webmanifest":
					ctx.Redirect("/favicon/site.webmanifest", fasthttp.StatusMovedPermanently)
					return
				}
				if strings.HasPrefix(p, "/favicon/") || p == "/logo.png" || p == "/logo-white.png" {
					faviconHandler(ctx)
					return
				}
			}
			top(ctx)
		}
	}
	return top
}

func (s *Server) routeHandler() fasthttp.RequestHandler {
	peerPurge, peerBan, peerRefresh, peerFetch, peerPut, peerMetrics := s.buildPeerHandlers()
	pprofIndexHandler, pprofMap := s.buildPprofHandlers()

	return func(ctx *fasthttp.RequestCtx) {
		p := string(ctx.Path())
		m := string(ctx.Method())

		if s.handleCoreRoutes(ctx, p) {
			return
		}
		if s.handleWriteRoutes(ctx, p, m) {
			return
		}
		if s.handlePeerRoutes(ctx, p, peerPurge, peerBan, peerRefresh, peerFetch, peerPut, peerMetrics) {
			return
		}
		if s.handleDataRoutes(ctx, p) {
			return
		}
		if pprofMap != nil && strings.HasPrefix(p, "/debug/pprof/") {
			if h, ok := pprofMap[p]; ok {
				h(ctx)
				return
			}
			if pprofIndexHandler != nil {
				pprofIndexHandler(ctx)
				return
			}
		}
		ctx.Error("not found", fasthttp.StatusNotFound)
	}
}

func (s *Server) buildPeerHandlers() (peerPurge, peerBan, peerRefresh, peerFetch, peerPut, peerMetrics fasthttp.RequestHandler) {
	if s.cfg.PeerPurgeHandler != nil {
		peerPurge = s.cfg.PeerPurgeHandler
	}
	if s.cfg.PeerBanHandler != nil {
		peerBan = s.cfg.PeerBanHandler
	}
	if s.cfg.PeerRefreshHandler != nil {
		peerRefresh = s.cfg.PeerRefreshHandler
	}
	if s.cfg.PeerFetchHandler != nil {
		peerFetch = s.cfg.PeerFetchHandler
	}
	if s.cfg.PeerPutHandler != nil {
		peerPut = s.cfg.PeerPutHandler
	}
	if s.cfg.PeerMetricsHandler != nil {
		peerMetrics = s.cfg.PeerMetricsHandler
	}
	return
}

func (s *Server) buildPprofHandlers() (fasthttp.RequestHandler, map[string]fasthttp.RequestHandler) {
	if !s.cfg.PprofEnabled {
		return nil, nil
	}
	pprofIndexHandler := pprofwrapper.Index()
	pprofMap := pprofwrapper.Map()
	return pprofIndexHandler, pprofMap
}

func (s *Server) handleCoreRoutes(ctx *fasthttp.RequestCtx, p string) bool {
	switch p {
	case "/healthz":
		s.healthz(ctx)
		return true
	case "/readyz":
		s.readyz(ctx)
		return true
	case "/version":
		s.version(ctx)
		return true
	case "/drain":
		s.drain(ctx)
		return true
	case "/metrics":
		if s.cfg.Metrics != nil {
			s.cfg.Metrics.Handler()(ctx)
			return true
		}
	case "/v1/cluster/peers":
		if s.cfg.PeersFn != nil {
			s.clusterPeers(ctx)
			return true
		}
	case "/v1/auth/check":
		if s.cfg.Token != "" {
			s.authCheck(ctx)
			return true
		}
	}
	return false
}

func (s *Server) handleWriteRoutes(ctx *fasthttp.RequestCtx, p, m string) bool {
	requirePost := func(fn func(*fasthttp.RequestCtx), available bool) bool {
		if !available {
			return false
		}
		if m != "POST" {
			ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
			return true
		}
		fn(ctx)
		return true
	}
	switch p {
	case "/v1/purge":
		return requirePost(s.purge, s.cfg.PurgeFn != nil)
	case "/v1/purge/batch":
		return requirePost(s.purgeBatch, s.cfg.PurgeFn != nil)
	case "/v1/ban":
		return requirePost(s.ban, s.cfg.BanFn != nil)
	case "/v1/refresh":
		return requirePost(s.refresh, s.cfg.RefreshFn != nil)
	case "/v1/cloudflare/propagate":
		return requirePost(s.cloudflarePropagate, s.cfg.CFPropagateFn != nil)
	}
	return false
}

func (s *Server) handlePeerRoutes(ctx *fasthttp.RequestCtx, p string, peerPurge, peerBan, peerRefresh, peerFetch, peerPut, peerMetrics fasthttp.RequestHandler) bool {
	switch p {
	case "/v1/peer/purge":
		if peerPurge != nil {
			peerPurge(ctx)
			return true
		}
	case "/v1/peer/ban":
		if peerBan != nil {
			peerBan(ctx)
			return true
		}
	case "/v1/peer/refresh":
		if peerRefresh != nil {
			peerRefresh(ctx)
			return true
		}
	case "/v1/peer/fetch":
		if peerFetch != nil {
			peerFetch(ctx)
			return true
		}
	case "/v1/peer/put":
		if peerPut != nil {
			peerPut(ctx)
			return true
		}
	case "/v1/peer/metrics":
		if peerMetrics != nil {
			peerMetrics(ctx)
			return true
		}
	}
	return false
}

func (s *Server) handleDataRoutes(ctx *fasthttp.RequestCtx, p string) bool {
	switch p {
	case "/v1/cloudflare/status":
		if s.cfg.CFStatusFn != nil {
			s.cloudflareStatus(ctx)
			return true
		}
	case "/v1/stats":
		if s.cfg.StatsFn != nil {
			s.stats(ctx)
			return true
		}
	case "/v1/config":
		if s.cfg.ConfigFn != nil {
			s.configHandler(ctx)
			return true
		}
	case "/v1/debug/cachecheck":
		if s.cfg.CacheCheckFn != nil {
			s.cacheCheck(ctx)
			return true
		}
	}
	return false
}

func (s *Server) healthz(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(ctx *fasthttp.RequestCtx) {
	ready := s.cfg.ReadyFn == nil || s.cfg.ReadyFn()
	if string(ctx.QueryArgs().Peek("detail")) != "" {
		conditions := []Condition{}
		if s.cfg.ConditionsFn != nil {
			conditions = s.cfg.ConditionsFn()
		}
		status := "ready"
		code := fasthttp.StatusOK
		if !ready {
			status = "not-ready"
			code = fasthttp.StatusServiceUnavailable
		}
		writeJSON(ctx, code, map[string]any{"status": status, "conditions": conditions})
		return
	}
	if !ready {
		writeJSON(ctx, fasthttp.StatusServiceUnavailable, map[string]string{"status": "not-ready"})
		return
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) version(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{
		"version": buildinfo.Version,
		"commit":  buildinfo.Commit,
		"date":    buildinfo.Date,
	})
}

func (s *Server) drain(ctx *fasthttp.RequestCtx) {
	if s.cfg.DrainFn != nil {
		s.cfg.DrainFn()
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "drained"})
}

// Handler returns the server's request handler, useful for testing.
func (s *Server) Handler() fasthttp.RequestHandler {
	return s.inner.Handler
}

func (s *Server) clusterPeers(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, s.cfg.PeersFn())
}

func (s *Server) purge(ctx *fasthttp.RequestCtx) {
	var req struct {
		URL string `json:"url"`
	}
	if !decodeJSON(ctx, &req) {
		return
	}
	if req.URL == "" {
		ctx.Error("bad request: url field is required", fasthttp.StatusBadRequest)
		return
	}
	key := cache.BuildKeyFromURL(req.URL, nil)
	if err := s.cfg.PurgeFn(key); err != nil {
		writeJSON(ctx, fasthttp.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cfg.OnPurged != nil {
		s.cfg.OnPurged(context.Background(), req.URL)
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "purged"})
}

func (s *Server) purgeBatch(ctx *fasthttp.RequestCtx) {
	maxBatch := s.cfg.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = 1000
	}
	var req struct {
		URLs []string `json:"urls"`
	}
	if !decodeJSON(ctx, &req) {
		return
	}
	if len(req.URLs) == 0 {
		ctx.Error("bad request: urls field is required", fasthttp.StatusBadRequest)
		return
	}
	if len(req.URLs) > maxBatch {
		writeJSON(ctx, fasthttp.StatusRequestEntityTooLarge, map[string]any{
			"error": "batch size exceeds maximum", "max_batch_size": maxBatch, "provided": len(req.URLs),
		})
		return
	}
	for _, u := range req.URLs {
		if u == "" {
			ctx.Error("bad request: urls must not contain empty entries", fasthttp.StatusBadRequest)
			return
		}
	}
	purged, failed := 0, 0
	for _, u := range req.URLs {
		key := cache.BuildKeyFromURL(u, nil)
		if err := s.cfg.PurgeFn(key); err != nil {
			failed++
			continue
		}
		purged++
		if s.cfg.OnPurged != nil {
			s.cfg.OnPurged(context.Background(), u)
		}
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{"status": "purged", "count": purged, "failed": failed})
}

func (s *Server) authCheck(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ban(ctx *fasthttp.RequestCtx) {
	var expr api.BanExpr
	if !decodeJSON(ctx, &expr) {
		return
	}
	if expr.HostRegex == "" && expr.PathRegex == "" && expr.SurrogateKey == "" {
		ctx.Error("bad request: at least one of host_regex, path_regex, or surrogate_key is required", fasthttp.StatusBadRequest)
		return
	}
	if expr.HostRegex != "" {
		if _, err := regexp.Compile(expr.HostRegex); err != nil {
			ctx.Error("bad request: invalid host_regex: "+err.Error(), fasthttp.StatusBadRequest)
			return
		}
	}
	if expr.PathRegex != "" {
		if _, err := regexp.Compile(expr.PathRegex); err != nil {
			ctx.Error("bad request: invalid path_regex: "+err.Error(), fasthttp.StatusBadRequest)
			return
		}
	}
	count, err := s.cfg.BanFn(expr)
	if err != nil {
		writeJSON(ctx, fasthttp.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cfg.OnBanned != nil {
		s.cfg.OnBanned(context.Background(), expr)
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]any{"status": "banned", "count": count})
}

func (s *Server) refresh(ctx *fasthttp.RequestCtx) {
	var req struct {
		URL string `json:"url"`
	}
	if !decodeJSON(ctx, &req) {
		return
	}
	if req.URL == "" {
		ctx.Error("bad request: url field is required", fasthttp.StatusBadRequest)
		return
	}
	key := cache.BuildKeyFromURL(req.URL, nil)
	if err := s.cfg.RefreshFn(key); err != nil {
		writeJSON(ctx, fasthttp.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cfg.OnRefreshed != nil {
		s.cfg.OnRefreshed(context.Background(), req.URL)
	}
	writeJSON(ctx, fasthttp.StatusOK, map[string]string{"status": "refreshed"})
}

// Serve starts the admin server on the configured address.
func (s *Server) Serve(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return err
	}
	s.resolved.Store(ln.Addr().String())
	s.cfg.Logger.Info("admin server listening", "addr", s.resolved.Load().(string))
	errCh := make(chan error, 1)
	go func() { errCh <- s.inner.Serve(ln) }()
	select {
	case <-ctx.Done():
		_ = ln.Close()
		_ = s.inner.Shutdown()
		return nil
	case err := <-errCh:
		if err != nil && err != fasthttp.ErrConnectionClosed {
			return err
		}
		return nil
	}
}

// Addr returns the resolved address the server is listening on.
func (s *Server) Addr() string {
	if v := s.resolved.Load(); v != nil {
		return v.(string)
	}
	return s.addr
}

func (s *Server) rateLimitMiddleware(next fasthttp.RequestHandler, perSecond int) fasthttp.RequestHandler {
	capacity := perSecond
	if capacity > 10000 {
		capacity = 10000
	}
	tokens := make(chan struct{}, capacity)
	for i := 0; i < capacity; i++ {
		tokens <- struct{}{}
	}
	refillInterval := time.Second / time.Duration(capacity)
	if refillInterval < time.Millisecond {
		refillInterval = time.Millisecond
	}
	go func() {
		ticker := time.NewTicker(refillInterval)
		defer ticker.Stop()
		for range ticker.C {
			select {
			case tokens <- struct{}{}:
			default:
			}
		}
	}()
	return func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) != "POST" {
			next(ctx)
			return
		}
		select {
		case <-tokens:
			next(ctx)
		default:
			writeJSON(ctx, fasthttp.StatusTooManyRequests, map[string]string{"error": "rate limit exceeded"})
		}
	}
}

func (s *Server) bodyLimitMiddleware(next fasthttp.RequestHandler, maxBytes int) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		if string(ctx.Method()) == "POST" && len(ctx.PostBody()) > maxBytes {
			writeJSON(ctx, fasthttp.StatusRequestEntityTooLarge, map[string]string{"error": "request body too large"})
			return
		}
		next(ctx)
	}
}

func (s *Server) authMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	exempt := map[string]bool{
		"/healthz": true, "/readyz": true, "/drain": true,
		"/metrics": true, "/version": true, "/v1/cluster/peers": true,
		"/v1/peer/fetch": true, "/v1/peer/put": true, "/v1/peer/purge": true,
		"/v1/peer/ban": true, "/v1/peer/refresh": true, "/v1/peer/metrics": true,
	}
	return func(ctx *fasthttp.RequestCtx) {
		defer func() {
			if v := recover(); v != nil {
				s.cfg.Logger.Error("admin handler panic", "path", string(ctx.Path()), "panic", v)
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				s.cfg.Logger.Error("admin handler stack", "stack", string(buf[:n]))
				ctx.Error("internal server error", fasthttp.StatusInternalServerError)
			}
		}()
		p := string(ctx.Path())
		if exempt[p] {
			next(ctx)
			return
		}
		if s.cfg.PprofEnabled && strings.HasPrefix(p, "/debug/pprof/") {
			next(ctx)
			return
		}
		if s.cfg.Token == "" {
			next(ctx)
			return
		}
		want := "Bearer " + s.cfg.Token
		got := string(ctx.Request.Header.Peek(header.Authorization))
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			ctx.Response.Header.Set(header.WWWAuthenticate, `Bearer realm="bouine-admin"`)
			ctx.Error("unauthorized", fasthttp.StatusUnauthorized)
			return
		}
		next(ctx)
	}
}

func writeJSON(ctx *fasthttp.RequestCtx, code int, v any) {
	ctx.Response.Header.Set(header.ContentType, "application/json")
	ctx.SetStatusCode(code)
	_ = json.NewEncoder(ctx).Encode(v)
}

func decodeJSON(ctx *fasthttp.RequestCtx, v any) bool {
	dec := json.NewDecoder(bytes.NewReader(ctx.PostBody()))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		ctx.Error("bad request: invalid or malformed JSON", fasthttp.StatusBadRequest)
		return false
	}
	return true
}

// CacheCheckResult is the JSON response for the /debug/cachecheck endpoint.
type CacheCheckResult struct {
	URL         string `json:"url"`
	KeyHex      string `json:"key_hex"`
	Source      string `json:"source"`
	CacheResult string `json:"cache_result"`
	Route       string `json:"route,omitempty"`
	Pool        string `json:"pool,omitempty"`
	TTL         string `json:"ttl,omitempty"`
	Age         string `json:"age,omitempty"`
}

// CloudflareStatus is the JSON response for the Cloudflare integration status endpoint.
type CloudflareStatus struct {
	LastError     *string `json:"last_error"`
	LastSuccessAt *string `json:"last_success_at"`
	ZoneID        string  `json:"zone_id,omitempty"`
	CircuitState  string  `json:"circuit_state,omitempty"`
	LastLagMs     int64   `json:"last_lag_ms,omitempty"`
	DLQDepth      int     `json:"dlq_depth,omitempty"`
	TokenCount    int     `json:"token_count,omitempty"`
	Enabled       bool    `json:"enabled"`
	Async         bool    `json:"async"`
	BatchEnabled  bool    `json:"batch_enabled,omitempty"`
}

// CFPropagateRequest is the JSON body for Cloudflare purge/propagation requests.
type CFPropagateRequest struct {
	Kind  string   `json:"kind"`
	Items []string `json:"items"`
}

func (s *Server) cloudflareStatus(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, s.cfg.CFStatusFn())
}

func (s *Server) cloudflarePropagate(ctx *fasthttp.RequestCtx) {
	var req CFPropagateRequest
	if !decodeJSON(ctx, &req) {
		return
	}
	if req.Kind == "" || len(req.Items) == 0 {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "kind and items are required"})
		return
	}
	if err := s.cfg.CFPropagateFn(context.Background(), req); err != nil {
		writeJSON(ctx, fasthttp.StatusBadGateway, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(ctx, fasthttp.StatusAccepted, map[string]string{"status": "accepted"})
}

func (s *Server) stats(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, s.cfg.StatsFn())
}

func (s *Server) configHandler(ctx *fasthttp.RequestCtx) {
	writeJSON(ctx, fasthttp.StatusOK, s.cfg.ConfigFn())
}

func (s *Server) cacheCheck(ctx *fasthttp.RequestCtx) {
	rawURL := string(ctx.QueryArgs().Peek("url"))
	if rawURL == "" {
		writeJSON(ctx, fasthttp.StatusBadRequest, map[string]string{"error": "missing url query parameter"})
		return
	}
	result := s.cfg.CacheCheckFn(context.Background(), rawURL)
	writeJSON(ctx, fasthttp.StatusOK, result)
}
