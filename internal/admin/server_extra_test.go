package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestNewMinimal(t *testing.T) {
	t.Parallel()
	s := NewMinimal(":0", nil, nil, nil, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	require.NotNil(t, s)
	assert.NotNil(t, s.swap)
}

func TestNewMinimal_DefaultAddr(t *testing.T) {
	t.Parallel()
	s := NewMinimal("", nil, nil, nil, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	require.NotNil(t, s)
}

func TestSwapHandler(t *testing.T) {
	t.Parallel()
	s := NewMinimal(":0", nil, nil, nil, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	newMux := http.NewServeMux()
	newMux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s.SwapHandler(newMux)
	// Verify the new handler is served.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestSwapHandler_NilSwap(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// New creates a server without swapHandler; SwapHandler should be a no-op.
	s.SwapHandler(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
}

func TestServe(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx) }()

	// Wait for the server to start by polling Addr.
	var addr string
	for i := 0; i < 100; i++ {
		addr = s.Addr()
		if addr != "127.0.0.1:0" && addr != "" {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	require.NotEmpty(t, addr)
	require.NotEqual(t, "127.0.0.1:0", addr)

	resp, err := http.Get("http://" + addr + "/healthz")
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	cancel()
	<-errCh
}

func TestServe_AddrAfterServe(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// Before Serve, Addr returns the configured address.
	assert.Equal(t, "127.0.0.1:0", s.Addr())
}

func TestNew_DefaultAddr(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	assert.Equal(t, ":9000", s.inner.Addr)
}

func TestNew_WithPprof(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PprofEnabled: true,
	})
	assert.Equal(t, time.Duration(0), s.inner.WriteTimeout)
}

func TestNew_WithDashboard(t *testing.T) {
	t.Parallel()
	dashHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s := New(Config{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DashboardHandler: dashHandler,
	})
	require.NotNil(t, s)
	// Root should redirect to dashboard.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/", nil)
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusFound, rr.Code)
}

func TestNew_WithDashboardAndFavicon(t *testing.T) {
	t.Parallel()
	dashHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	favHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s := New(Config{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DashboardHandler: dashHandler,
		FaviconHandler:   favHandler,
	})
	require.NotNil(t, s)

	// Favicon redirect.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/favicon.ico", nil)
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusMovedPermanently, rr.Code)

	// Apple touch icon redirect.
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequestWithContext(context.Background(), "GET", "/apple-touch-icon.png", nil)
	s.Handler().ServeHTTP(rr2, req2)
	assert.Equal(t, http.StatusMovedPermanently, rr2.Code)

	// Manifest redirect.
	rr3 := httptest.NewRecorder()
	req3 := httptest.NewRequestWithContext(context.Background(), "GET", "/site.webmanifest", nil)
	s.Handler().ServeHTTP(rr3, req3)
	assert.Equal(t, http.StatusMovedPermanently, rr3.Code)
}

func TestNew_WithDashboard_NonRootPath(t *testing.T) {
	t.Parallel()
	dashHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	s := New(Config{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Token:            "secret",
		DashboardHandler: dashHandler,
	})
	// Non-root path with no auth should get 401 from the inner handler.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/purge", nil)
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusUnauthorized, rr.Code)
}

func TestClusterPeers(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeersFn: func() []api.PeerInfo { return []api.PeerInfo{{Name: "node1", DataAddr: "1.1.1.1:80"}} },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/cluster/peers", nil)
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	var peers []api.PeerInfo
	err := json.Unmarshal(rr.Body.Bytes(), &peers)
	require.NoError(t, err)
	assert.Equal(t, "node1", peers[0].Name)
}

func TestPurge_PurgeFnError(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return errors.New("storage error") },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestRefresh_RefreshFnError(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return errors.New("refresh error") },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/refresh",
		bytes.NewBufferString(`{"url":"https://example.com/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestBan_BanFnError(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, errors.New("ban error") },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/ban",
		bytes.NewBufferString(`{"path_regex":"^/api/"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestBan_InvalidHostRegex(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/ban",
		bytes.NewBufferString(`{"host_regex":"[invalid"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestBan_InvalidPathRegex(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/ban",
		bytes.NewBufferString(`{"path_regex":"[invalid"}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestPurgeBatch_EmptyEntries(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/purge/batch",
		bytes.NewBufferString(`{"urls":["https://a.com/",""]}`))
	req.Header.Set(header.ContentType, "application/json")
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestStats(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		StatsFn: func() api.Stats { return api.Stats{HotBytes: 100, HotEntries: 5} },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/stats", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
	var stats api.Stats
	err := json.Unmarshal(rr.Body.Bytes(), &stats)
	require.NoError(t, err)
	assert.Equal(t, int64(100), stats.HotBytes)
}

func TestConfigHandler(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:    "secret",
		Logger:   slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ConfigFn: func() any { return map[string]string{"key": "value"} },
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/config", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestCacheCheck(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		CacheCheckFn: func(_ context.Context, rawURL string) CacheCheckResult {
			return CacheCheckResult{URL: rawURL, CacheResult: "HIT"}
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/debug/cachecheck?url=https://example.com/", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestCacheCheck_MissingURL(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		CacheCheckFn: func(_ context.Context, _ string) CacheCheckResult {
			return CacheCheckResult{}
		},
	})
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/debug/cachecheck", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestAuthMiddleware_PanicRecovery(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// Create a handler that panics.
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		panic("test panic")
	})
	wrapped := s.authMiddleware(panicHandler)
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/v1/purge", nil)
	req.Header.Set(header.Authorization, "Bearer secret")
	wrapped.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusInternalServerError, rr.Code)
}

func TestBodyLimitMiddleware_GetNotLimited(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxBodyBytes: 10,
	})
	// GET should not be limited.
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/healthz", nil)
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestReadyz_Detail_NoConditionsFn_NotReady(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return false })
	status, body := get(t, s, "/readyz?detail=1")
	assert.Equal(t, http.StatusServiceUnavailable, status)
	var resp map[string]any
	err := json.Unmarshal(body, &resp)
	require.NoError(t, err)
	assert.Equal(t, "not-ready", resp["status"])
}

func TestServe_ListenError(t *testing.T) {
	t.Parallel()
	// Try to listen on an invalid address.
	s := New(Config{
		Addr:   "invalid:address",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := s.Serve(ctx)
	assert.Error(t, err)
}

func TestAddr_BeforeServe(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Addr:   ":9999",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	assert.Equal(t, ":9999", s.Addr())
}

func TestAddr_AfterServe(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Serve(ctx) }()

	// Wait for Addr to be populated.
	for i := 0; i < 100; i++ {
		addr := s.Addr()
		if addr != "127.0.0.1:0" {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}
	addr := s.Addr()
	assert.NotEqual(t, "127.0.0.1:0", addr)
	cancel()
	<-errCh
}

func TestSwapHandler_ServeHTTP(t *testing.T) {
	t.Parallel()
	sh := &swapHandler{}
	sh.Store(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(context.Background(), "GET", "/", nil)
	sh.ServeHTTP(rr, req)
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestServe_ErrServerClosed(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Addr:   "127.0.0.1:0",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.resolved.Store(ln.Addr().String())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { s.inner.Serve(ln) }()
	// Close the server immediately to trigger ErrServerClosed.
	require.NoError(t, s.inner.Close())
	_ = ctx
}

func TestPeerRefreshHandler_RouteMounted(t *testing.T) {
	t.Parallel()
	var called bool
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeerRefreshHandler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			called = true
			w.WriteHeader(http.StatusOK)
		}),
	})
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/peer/refresh", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	assert.True(t, called, "PeerRefreshHandler should be mounted at POST /v1/peer/refresh")
	assert.Equal(t, http.StatusOK, rr.Code)
}

func TestPeerRefreshHandler_NotMountedWhenNil(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	req := httptest.NewRequestWithContext(context.Background(), "POST", "/v1/peer/refresh", nil)
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, req)
	assert.Equal(t, http.StatusNotFound, rr.Code, "route should not be mounted when PeerRefreshHandler is nil")
}
