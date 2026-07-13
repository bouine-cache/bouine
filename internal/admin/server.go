// Package admin is the L7 control plane. It serves the admin API,
// health/readiness probes, metrics, and (in later phases) the
// dashboard SPA. The admin surface MUST stay on its own listener; it
// is never bound on the data-plane port (see AGENTS.md §2).
//
// The admin server uses net/http.ServeMux — the same HTTP stack as the
// data plane. ADR-0006 documents the decision.
package admin

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/buildinfo"
	"github.com/bouine-cache/bouine/internal/cache"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Config controls the admin server.
//
// Stable.
type Config struct {
	// Addr is the listen address (e.g. ":9000"). Empty defaults to
	// ":9000".
	Addr string
	// Logger is the structured logger. Required.
	Logger observability.Logger
	// ReadyFn reports whether the server is ready to serve traffic.
	// nil is treated as "always ready".
	ReadyFn func() bool
	// Metrics is the Prometheus registry. If non-nil, /metrics is
	// mounted.
	Metrics *observability.Metrics
	// PeersFn, if non-nil, returns the list of live cluster peers for
	// GET /v1/cluster/peers.
	PeersFn func() []api.PeerInfo
	// PurgeFn, if non-nil, handles purge requests.
	PurgeFn func(key api.Key) error
	// BanFn, if non-nil, handles ban requests.
	BanFn func(expr api.BanExpr) (int, error)
	// Token is the bearer token required on all write endpoints.
	// If empty, the server refuses to start (engine generates one).
	Token string
	// MaxBatchSize caps URLs per POST /v1/purge/batch. Zero = 1000.
	MaxBatchSize int
	// RateLimitPerSecond caps write requests per second. Zero = unlimited.
	RateLimitPerSecond int
	// RefreshFn, if non-nil, handles soft-purge (refresh) requests.
	RefreshFn func(key api.Key) error
	// PeerPurgeHandler, if non-nil, handles incoming purge broadcasts
	// from peer nodes. Mounted at POST /v1/peer/purge (auth-exempt;
	// callers are trusted cluster peers on the internal network).
	PeerPurgeHandler http.Handler
	// PeerBanHandler, if non-nil, handles incoming ban broadcasts from
	// peers. Mounted at POST /v1/peer/ban (auth-exempt; same rationale).
	PeerBanHandler http.Handler
	// CFStatusFn, if non-nil, returns a snapshot of the Cloudflare
	// integration state for GET /v1/cloudflare/status.
	CFStatusFn func() CloudflareStatus
	// OnPurged, if non-nil, is called after a successful purge with the raw
	// URL. Used for downstream CDN propagation (e.g. Cloudflare).
	OnPurged func(ctx context.Context, url string)
	// OnRefreshed, if non-nil, is called after a successful soft-purge with
	// the raw URL.
	OnRefreshed func(ctx context.Context, url string)
	// OnBanned, if non-nil, is called after a successful ban.
	OnBanned func(ctx context.Context, expr api.BanExpr)
	// PeerFetchHandler, if non-nil, serves peer cache-lookup requests
	// PeerFetchHandler, if non-nil, handles peer-fetch RPCs
	// from other cluster nodes. Mounted at POST /v1/peer/fetch (no auth
	// required — callers are trusted cluster peers on the internal
	// network; protected by network policy / mTLS in production).
	PeerFetchHandler http.Handler
	// PeerMetricsHandler, if non-nil, is mounted at GET /v1/peer/metrics
	// (behind bearer-token auth) so peers can fetch this node's ring summary.
	PeerMetricsHandler http.Handler
	// DashboardHandler, if non-nil, is mounted at /dashboard/ outside the
	// bearer-token middleware; the dashboard manages its own session-cookie auth.
	DashboardHandler http.Handler
	// FaviconHandler, if non-nil, serves /favicon/* assets (no auth required).
	FaviconHandler http.Handler
	// PprofEnabled mounts net/http/pprof under /debug/pprof/* on the
	// admin port. Routes are auth-exempt; admin port is network-isolated.
	// Default false.
	PprofEnabled bool
	// ConditionsFn, if non-nil, returns the status of individual readiness
	// conditions. Used by /readyz?detail=1 to expose per-condition state
	// for operator diagnostics during slow startup.
	ConditionsFn func() []Condition
}

// Condition is a readiness condition status entry for the
// /readyz?detail=1 JSON response.
type Condition struct {
	Name  string `json:"name"`
	Ready bool   `json:"ready"`
}

// swapHandler is an atomic wrapper around http.Handler that allows the
// handler to be replaced at runtime without restarting the server. This
// is used during startup: a minimal handler (healthz/readyz only) is
// installed first so K8s probes can reach the admin port before
// subsystems finish loading. The full admin handler is swapped in once
// initSubsystems completes.
type swapHandler struct {
	h atomic.Value // http.Handler
}

func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.h.Load().(http.Handler).ServeHTTP(w, r)
}

func (s *swapHandler) Store(h http.Handler) {
	s.h.Store(h)
}

// Server is the admin HTTP server with lifecycle methods matching the
// supervised-group contract.
//
// Stable.
type Server struct {
	inner    *http.Server
	cfg      Config
	swap     *swapHandler // non-nil when created via NewMinimal
	resolved atomic.Value // stores string
}

// NewMinimal creates an admin server with only healthz, readyz, and
// version routes. The handler can be replaced at runtime via SwapHandler,
// allowing the full admin routes to be installed after subsystem
// initialization without rebinding the listener.
//
// Unstable.
func NewMinimal(addr string, readyFn func() bool, conditionsFn func() []Condition, logger observability.Logger) *Server {
	if addr == "" {
		addr = ":9000"
	}
	logger = observability.ResolveLogger(logger)

	mux := http.NewServeMux()
	s := &Server{
		cfg: Config{Addr: addr, ReadyFn: readyFn, ConditionsFn: conditionsFn, Logger: logger},
		inner: &http.Server{
			Addr:              addr,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
	}
	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /version", s.version)

	sh := &swapHandler{}
	sh.Store(mux)
	s.swap = sh
	s.inner.Handler = sh

	return s
}

// SwapHandler atomically replaces the server's handler. Only valid for
// servers created via NewMinimal. Calling SwapHandler on a server created
// via New is a no-op.
//
// Unstable.
func (s *Server) SwapHandler(h http.Handler) {
	if s.swap != nil {
		s.swap.Store(h)
	}
}

// New constructs the admin server. It does not start listening; call
// Serve.
//
// Stable.
func New(cfg Config) *Server {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.Addr == "" {
		cfg.Addr = ":9000"
	}

	mux := http.NewServeMux()
	s := &Server{
		cfg: cfg,
		inner: &http.Server{
			Addr:              cfg.Addr,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
	}
	// pprof profile captures (e.g. /debug/pprof/profile?seconds=30) can
	// run longer than the default 5s WriteTimeout. When pprof is enabled,
	// the write deadline is disabled for ALL admin endpoints, not just
	// pprof — this is the standard tradeoff for pprof-on-admin. Mitigated
	// by ReadHeaderTimeout=5s, IdleTimeout=30s, rate limiting, and K8s
	// NetworkPolicy isolation of the admin port.
	if cfg.PprofEnabled {
		s.inner.WriteTimeout = 0
	}

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /version", s.version)
	s.mountOptionalRoutes(mux, cfg)
	mux.HandleFunc("POST /v1/config/reload", s.configReload)

	topHandler := s.authMiddleware(mux)
	if cfg.RateLimitPerSecond > 0 {
		topHandler = s.rateLimitMiddleware(topHandler, cfg.RateLimitPerSecond)
	}
	if cfg.DashboardHandler != nil {
		outerMux := http.NewServeMux()
		outerMux.Handle("/dashboard/", cfg.DashboardHandler)
		// Favicon assets and webmanifest — no auth required so browsers can fetch them.
		if cfg.FaviconHandler != nil {
			outerMux.Handle("/favicon/", cfg.FaviconHandler)
			outerMux.HandleFunc("/favicon.ico", faviconRedirect)
			outerMux.HandleFunc("/apple-touch-icon.png", appleTouchRedirect)
			outerMux.HandleFunc("/site.webmanifest", manifestRedirect)
			outerMux.Handle("/logo.png", cfg.FaviconHandler)
			outerMux.Handle("/logo-white.png", cfg.FaviconHandler)
		}
		// Capture authHandler before topHandler is reassigned; the closure
		// below must not call outerMux or it recurses infinitely.
		authHandler := topHandler
		// Redirect bare root to the dashboard so operators who type the
		// admin address in a browser land somewhere useful.
		outerMux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/" {
				http.Redirect(w, r, "/dashboard/", http.StatusFound)
				return
			}
			authHandler.ServeHTTP(w, r)
		})
		topHandler = outerMux
	}
	s.inner.Handler = topHandler

	return s
}

// mountOptionalRoutes registers routes that are only enabled when the
// corresponding config field is non-nil.
func (s *Server) mountOptionalRoutes(mux *http.ServeMux, cfg Config) {
	if cfg.Metrics != nil {
		mux.Handle("GET /metrics", cfg.Metrics.Handler())
	}
	if cfg.PeersFn != nil {
		mux.HandleFunc("GET /v1/cluster/peers", s.clusterPeers)
	}
	if cfg.PurgeFn != nil {
		mux.HandleFunc("POST /v1/purge", s.purge)
		mux.HandleFunc("POST /v1/purge/batch", s.purgeBatch)
	}
	if cfg.BanFn != nil {
		mux.HandleFunc("POST /v1/ban", s.ban)
	}
	if cfg.RefreshFn != nil {
		mux.HandleFunc("POST /v1/refresh", s.refresh)
	}
	if cfg.Token != "" {
		mux.HandleFunc("GET /v1/auth/check", s.authCheck)
	}
	if cfg.PeerPurgeHandler != nil {
		mux.Handle("POST /v1/peer/purge", cfg.PeerPurgeHandler)
	}
	if cfg.PeerBanHandler != nil {
		mux.Handle("POST /v1/peer/ban", cfg.PeerBanHandler)
	}
	if cfg.CFStatusFn != nil {
		mux.HandleFunc("GET /v1/cloudflare/status", s.cloudflareStatus)
	}
	if cfg.PeerFetchHandler != nil {
		mux.Handle("POST /v1/peer/fetch", cfg.PeerFetchHandler)
	}
	if cfg.PeerMetricsHandler != nil {
		mux.Handle("GET /v1/peer/metrics", cfg.PeerMetricsHandler)
	}
	if cfg.PprofEnabled {
		// Register pprof handlers explicitly on our own mux. Importing
		// net/http/pprof also registers on http.DefaultServeMux via init(),
		// but bouine never serves DefaultServeMux, so that registration is
		// dead code in this process.
		mux.HandleFunc("GET /debug/pprof/", pprof.Index)
		mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
		mux.Handle("GET /debug/pprof/heap", pprof.Handler("heap"))
		mux.Handle("GET /debug/pprof/goroutine", pprof.Handler("goroutine"))
		mux.Handle("GET /debug/pprof/block", pprof.Handler("block"))
		mux.Handle("GET /debug/pprof/mutex", pprof.Handler("mutex"))
		mux.Handle("GET /debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	}
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ready := s.cfg.ReadyFn == nil || s.cfg.ReadyFn()

	if r.URL.Query().Has("detail") {
		conditions := []Condition{}
		if s.cfg.ConditionsFn != nil {
			conditions = s.cfg.ConditionsFn()
		}
		status := "ready"
		code := http.StatusOK
		if !ready {
			status = "not-ready"
			code = http.StatusServiceUnavailable
		}
		writeJSON(w, code, map[string]any{
			"status":     status,
			"conditions": conditions,
		})
		return
	}

	if !ready {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not-ready"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) version(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": buildinfo.Version,
		"commit":  buildinfo.Commit,
		"date":    buildinfo.Date,
	})
}

// Handler returns the admin mux for testing with httptest.
//
// Unstable.
func (s *Server) Handler() http.Handler {
	return s.inner.Handler
}

func (s *Server) clusterPeers(w http.ResponseWriter, _ *http.Request) {
	peers := s.cfg.PeersFn()
	writeJSON(w, http.StatusOK, peers)
}

func (s *Server) purge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request: invalid or malformed JSON", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, "bad request: url field is required", http.StatusBadRequest)
		return
	}
	key := cache.BuildKeyFromURL(req.URL)
	if err := s.cfg.PurgeFn(key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cfg.OnPurged != nil {
		s.cfg.OnPurged(r.Context(), req.URL)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "purged"})
}

func (s *Server) purgeBatch(w http.ResponseWriter, r *http.Request) {
	maxBatch := s.cfg.MaxBatchSize
	if maxBatch <= 0 {
		maxBatch = 1000
	}
	var req struct {
		URLs []string `json:"urls"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request: invalid or malformed JSON", http.StatusBadRequest)
		return
	}
	if len(req.URLs) == 0 {
		http.Error(w, "bad request: urls field is required and must not be empty", http.StatusBadRequest)
		return
	}
	if len(req.URLs) > maxBatch {
		writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{
			"error":          "batch size exceeds maximum",
			"max_batch_size": maxBatch,
			"provided":       len(req.URLs),
		})
		return
	}
	purged := 0
	failed := 0
	for _, urlStr := range req.URLs {
		key := cache.BuildKeyFromURL(urlStr)
		if err := s.cfg.PurgeFn(key); err != nil {
			failed++
			continue
		}
		purged++
		if s.cfg.OnPurged != nil {
			s.cfg.OnPurged(r.Context(), urlStr)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "purged",
		"count":  purged,
		"failed": failed,
	})
}

func (s *Server) authCheck(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) ban(w http.ResponseWriter, r *http.Request) {
	var expr api.BanExpr
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&expr); err != nil {
		http.Error(w, "bad request: invalid or malformed JSON", http.StatusBadRequest)
		return
	}
	if expr.HostRegex == "" && expr.PathRegex == "" && expr.SurrogateKey == "" {
		http.Error(w, "bad request: at least one of host_regex, path_regex, or surrogate_key is required", http.StatusBadRequest)
		return
	}
	count, err := s.cfg.BanFn(expr)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cfg.OnBanned != nil {
		s.cfg.OnBanned(r.Context(), expr)
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "banned", "count": count})
}

func (s *Server) configReload(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "reload-requested"})
}

func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	var req struct {
		URL string `json:"url"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		http.Error(w, "bad request: invalid or malformed JSON", http.StatusBadRequest)
		return
	}
	if req.URL == "" {
		http.Error(w, "bad request: url field is required", http.StatusBadRequest)
		return
	}
	key := cache.BuildKeyFromURL(req.URL)
	if err := s.cfg.RefreshFn(key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if s.cfg.OnRefreshed != nil {
		s.cfg.OnRefreshed(r.Context(), req.URL)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "refreshed"})
}

// Serve blocks until the server returns or ctx is cancelled. On
// context cancellation a graceful shutdown is initiated.
//
// Stable.
func (s *Server) Serve(ctx context.Context) error {
	lc := net.ListenConfig{}
	ln, err := lc.Listen(ctx, "tcp", s.inner.Addr)
	if err != nil {
		return err
	}
	s.resolved.Store(ln.Addr().String())

	s.cfg.Logger.Info("admin server listening",
		"addr", s.resolved.Load().(string))

	errCh := make(chan error, 1)
	go func() { errCh <- s.inner.Serve(ln) }()

	select {
	case <-ctx.Done():
		shutCtx, cancel := context.WithTimeout(
			context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		return s.inner.Shutdown(shutCtx)
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// Addr returns the resolved listen address after Serve has been
// called. Before Serve, returns the configured address.
func (s *Server) Addr() string {
	if v := s.resolved.Load(); v != nil {
		return v.(string)
	}
	return s.inner.Addr
}

// rateLimitMiddleware enforces a per-second token bucket on write
// (POST) requests. GET requests (healthz, readyz, metrics, dashboard)
// are always allowed — rate limiting probes would break K8s.
func (s *Server) rateLimitMiddleware(next http.Handler, perSecond int) http.Handler {
	capacity := perSecond
	if capacity > 10000 {
		capacity = 10000
	}
	tokens := make(chan struct{}, capacity)
	// Pre-fill the bucket.
	for i := 0; i < capacity; i++ {
		tokens <- struct{}{}
	}
	// Refill at 1 token per interval, capped at capacity.
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
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}
		select {
		case <-tokens:
			next.ServeHTTP(w, r)
		default:
			writeJSON(w, http.StatusTooManyRequests, map[string]string{
				"error": "rate limit exceeded",
			})
		}
	})
}

// authMiddleware enforces bearer token authentication on all write
// (non-GET) requests. Safe read-only endpoints used by K8s probes
// and monitoring are always allowed without a token.
func (s *Server) authMiddleware(next http.Handler) http.Handler {
	// Paths exempt from auth (K8s probes, Prometheus scrape, cluster RPCs).
	exempt := map[string]bool{
		"/healthz":          true,
		"/readyz":           true,
		"/metrics":          true,
		"/version":          true,
		"/v1/cluster/peers": true,
		// Peer-to-peer fetch RPC: callers are cluster peers on the internal
		// network. Token-auth is not used here; network policy / mTLS guards
		// access in production. Must remain auth-exempt so peers without a
		// shared token can still perform lookups.
		"/v1/peer/fetch": true,
		// Peer-to-peer invalidation RPCs: same rationale as peer fetch.
		// Peers forward purge/ban events via HTTP fan-out in strong mode.
		"/v1/peer/purge": true,
		"/v1/peer/ban":   true,
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Panic recovery: log and return 500 instead of crashing the connection.
		defer func() {
			if v := recover(); v != nil {
				s.cfg.Logger.Error("admin handler panic",
					"path", r.URL.Path,
					"method", r.Method,
					"panic", v)
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				s.cfg.Logger.Error("admin handler stack",
					"path", r.URL.Path,
					"stack", string(buf[:n]))
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
		}()
		if exempt[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		// pprof endpoints are auth-exempt so operators and bench harnesses
		// can capture profiles without a bearer token. The admin port is
		// network-isolated in production.
		if s.cfg.PprofEnabled && strings.HasPrefix(r.URL.Path, "/debug/pprof/") {
			next.ServeHTTP(w, r)
			return
		}
		want := "Bearer " + s.cfg.Token
		got := r.Header.Get(header.Authorization)
		if subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			w.Header().Set(header.WWWAuthenticate, `Bearer realm="bouine-admin"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// faviconRedirect, appleTouchRedirect, manifestRedirect are convenience
// handlers that redirect root-level browser requests to /favicon/*.
func faviconRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/favicon/favicon.ico", http.StatusMovedPermanently)
}

func appleTouchRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/favicon/apple-touch-icon.png", http.StatusMovedPermanently)
}

func manifestRedirect(w http.ResponseWriter, r *http.Request) {
	http.Redirect(w, r, "/favicon/site.webmanifest", http.StatusMovedPermanently)
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set(header.ContentType, "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// CloudflareStatus is returned by GET /v1/cloudflare/status.
type CloudflareStatus struct {
	// Enabled reports whether CF propagation is configured.
	Enabled bool `json:"enabled"`
	// ZoneID is the configured zone (non-secret).
	ZoneID string `json:"zone_id,omitempty"`
	// Async is the configured propagation mode.
	Async bool `json:"async"`
	// LastError is the most recent propagation error, or null if none.
	LastError *string `json:"last_error"`
	// LastSuccessAt is the most recent successful propagation timestamp (RFC 3339).
	LastSuccessAt *string `json:"last_success_at"`
	// LastLagMs is the duration between the last invalidation request
	// and the CF API completion. 0 when no propagation has occurred.
	LastLagMs int64 `json:"last_lag_ms,omitempty"`
}

func (s *Server) cloudflareStatus(w http.ResponseWriter, _ *http.Request) {
	status := s.cfg.CFStatusFn()
	writeJSON(w, http.StatusOK, status)
}
