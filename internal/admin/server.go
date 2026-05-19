// Package admin is the L7 control plane. It serves the admin API,
// health/readiness probes, metrics, and (in later phases) the
// dashboard SPA. The admin surface MUST stay on its own listener; it
// is never bound on the data-plane port (see AGENTS.md §2).
//
// The admin server uses net/http.ServeMux — the same HTTP stack as the
// data plane — so the entire daemon runs on two implementations:
// net/http (H1+H2) and quic-go (H3). ADR-0006 documents the decision.
package admin

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/thylong/bouine/internal/buildinfo"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// Config controls the admin server.
//
// Stable.
type Config struct {
	// Addr is the listen address (e.g. ":9000"). Empty defaults to
	// ":9000".
	Addr string
	// Logger is the structured logger. Required.
	Logger *slog.Logger
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
	// RefreshFn, if non-nil, handles soft-purge (refresh) requests.
	RefreshFn func(key api.Key) error
}

// Server is the admin HTTP server with lifecycle methods matching the
// supervised-group contract.
//
// Stable.
type Server struct {
	inner    *http.Server
	cfg      Config
	resolved atomic.Value // stores string
}

// New constructs the admin server. It does not start listening; call
// Serve.
//
// Stable.
func New(cfg Config) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":9000"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}

	mux := http.NewServeMux()
	s := &Server{
		cfg: cfg,
		inner: &http.Server{
			Addr:              cfg.Addr,
			Handler:           mux,
			ReadHeaderTimeout: 5 * time.Second,
			ReadTimeout:       5 * time.Second,
			WriteTimeout:      5 * time.Second,
			IdleTimeout:       30 * time.Second,
		},
	}

	mux.HandleFunc("GET /healthz", s.healthz)
	mux.HandleFunc("GET /readyz", s.readyz)
	mux.HandleFunc("GET /version", s.version)
	if cfg.Metrics != nil {
		mux.Handle("GET /metrics", cfg.Metrics.Handler())
	}
	if cfg.PeersFn != nil {
		mux.HandleFunc("GET /v1/cluster/peers", s.clusterPeers)
	}
	if cfg.PurgeFn != nil {
		mux.HandleFunc("POST /v1/purge", s.purge)
	}
	if cfg.BanFn != nil {
		mux.HandleFunc("POST /v1/ban", s.ban)
	}
	if cfg.RefreshFn != nil {
		mux.HandleFunc("POST /v1/refresh", s.refresh)
	}
	mux.HandleFunc("POST /v1/config/reload", s.configReload)

	return s
}

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) readyz(w http.ResponseWriter, _ *http.Request) {
	if s.cfg.ReadyFn != nil && !s.cfg.ReadyFn() {
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	key := api.Key(0)
	// Compute key from URL using xxhash — simplified; full implementation
	// would reuse cache.BuildKey with a synthetic request.
	if req.URL != "" {
		h := uint64(0)
		for _, c := range req.URL {
			h = h*31 + uint64(c) //nolint:gosec // hash, truncation intentional
		}
		key = api.Key(h)
	}
	if err := s.cfg.PurgeFn(key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "purged"})
}

func (s *Server) ban(w http.ResponseWriter, r *http.Request) {
	var expr api.BanExpr
	if err := json.NewDecoder(r.Body).Decode(&expr); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	count, err := s.cfg.BanFn(expr)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
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
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	key := api.Key(0)
	if req.URL != "" {
		h := uint64(0)
		for _, c := range req.URL {
			h = h*31 + uint64(c) //nolint:gosec // hash, truncation intentional
		}
		key = api.Key(h)
	}
	if err := s.cfg.RefreshFn(key); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
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

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
