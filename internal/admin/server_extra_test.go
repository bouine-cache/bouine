package admin

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
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
	s.SwapHandler(func(ctx *fasthttp.RequestCtx) { ctx.Error("ok", fasthttp.StatusOK) })
	// Verify the new handler is served.
	ctx := testCtx("GET", "/")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestSwapHandler_NilSwap(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// New creates a server without swapHandler; SwapHandler should be a no-op.
	s.SwapHandler(func(ctx *fasthttp.RequestCtx) {})
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

	fc := &fasthttp.Client{}
	req := fasthttp.AcquireRequest()
	fresp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(fresp)
	req.SetRequestURI("http://" + addr + "/healthz")
	require.NoError(t, fc.Do(req, fresp))
	assert.Equal(t, fasthttp.StatusOK, fresp.StatusCode())

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
	assert.Equal(t, ":9000", s.addr)
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
	dashHandler := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	s := New(Config{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DashboardHandler: dashHandler,
	})
	require.NotNil(t, s)
	// Root should redirect to dashboard.
	ctx := testCtx("GET", "/")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusFound, ctx.Response.StatusCode())
}

func TestNew_WithDashboardAndFavicon(t *testing.T) {
	t.Parallel()
	dashHandler := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	favHandler := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	s := New(Config{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		DashboardHandler: dashHandler,
		FaviconHandler:   favHandler,
	})
	require.NotNil(t, s)

	// Root redirects to /dashboard/ (302 Found).
	ctx := testCtx("GET", "/")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusFound, ctx.Response.StatusCode())

	// Favicon redirect (301 Moved Permanently).
	ctx2 := testCtx("GET", "/favicon.ico")
	s.Handler()(ctx2)
	assert.Equal(t, fasthttp.StatusMovedPermanently, ctx2.Response.StatusCode())

	// Apple touch icon redirect.
	ctx3 := testCtx("GET", "/apple-touch-icon.png")
	s.Handler()(ctx3)
	assert.Equal(t, fasthttp.StatusMovedPermanently, ctx3.Response.StatusCode())

	// Manifest redirect.
	ctx4 := testCtx("GET", "/site.webmanifest")
	s.Handler()(ctx4)
	assert.Equal(t, fasthttp.StatusMovedPermanently, ctx4.Response.StatusCode())
}

func TestNew_WithDashboard_NonRootPath(t *testing.T) {
	t.Parallel()
	dashHandler := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	s := New(Config{
		Logger:           slog.New(slog.NewJSONHandler(io.Discard, nil)),
		Token:            "secret",
		DashboardHandler: dashHandler,
	})
	// Non-dashboard path with no auth should get 401 from the inner handler.
	ctx := testCtx("GET", "/v1/purge")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusUnauthorized, ctx.Response.StatusCode())
}

func TestClusterPeers(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeersFn: func() []api.PeerInfo { return []api.PeerInfo{{Name: "node1", DataAddr: "1.1.1.1:80"}} },
	})
	ctx := testCtx("GET", "/v1/cluster/peers")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	var peers []api.PeerInfo
	err := json.Unmarshal(ctx.Response.Body(), &peers)
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
	ctx := testCtxWithBodyAuth("POST", "/v1/purge", []byte(`{"url":"https://example.com/"}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestRefresh_RefreshFnError(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:     "secret",
		Logger:    slog.New(slog.NewJSONHandler(io.Discard, nil)),
		RefreshFn: func(_ api.Key) error { return errors.New("refresh error") },
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/refresh", []byte(`{"url":"https://example.com/"}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestBan_BanFnError(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, errors.New("ban error") },
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/ban", []byte(`{"path_regex":"^/reviews/"}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestBan_InvalidHostRegex(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/ban", []byte(`{"host_regex":"["}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestBan_InvalidPathRegex(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(_ api.BanExpr) (int, error) { return 0, nil },
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/ban", []byte(`{"path_regex":"(?P<name"}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestPurgeBatch_EmptyEntries(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PurgeFn: func(_ api.Key) error { return nil },
	})
	ctx := testCtxWithBodyAuth("POST", "/v1/purge/batch", []byte(`{"urls":[""]}`), "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestStats(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:   "secret",
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		StatsFn: func() api.Stats { return api.Stats{HotBytes: 100, HotEntries: 5} },
	})
	ctx := testCtxWithAuth("GET", "/v1/stats", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
	var stats api.Stats
	err := json.Unmarshal(ctx.Response.Body(), &stats)
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
	ctx := testCtxWithAuth("GET", "/v1/config", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
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
	ctx := testCtxWithAuth("GET", "/v1/debug/cachecheck?url=https://example.com/", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
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
	ctx := testCtxWithAuth("GET", "/v1/debug/cachecheck", "secret")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusBadRequest, ctx.Response.StatusCode())
}

func TestAuthMiddleware_PanicRecovery(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:  "secret",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	// Create a handler that panics. The path must be auth-exempt so
	// the panic happens before auth checks.
	panicHandler := func(ctx *fasthttp.RequestCtx) {
		panic("test panic")
	}
	wrapped := s.authMiddleware(panicHandler)
	ctx := testCtx("GET", "/healthz")
	wrapped(ctx)
	assert.Equal(t, fasthttp.StatusInternalServerError, ctx.Response.StatusCode())
}

func TestBodyLimitMiddleware_GetNotLimited(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Token:        "secret",
		Logger:       slog.New(slog.NewJSONHandler(io.Discard, nil)),
		MaxBodyBytes: 10,
	})
	// GET should not be limited.
	ctx := testCtx("GET", "/healthz")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestReadyz_Detail_NoConditionsFn_NotReady(t *testing.T) {
	t.Parallel()
	s := newTestServer(t, func() bool { return false })
	status, body := get(t, s, "/readyz?detail=1")
	assert.Equal(t, fasthttp.StatusServiceUnavailable, status)
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
	sh.Store(func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	})
	ctx := testCtx("GET", "/")
	sh.ServeRequest(ctx)
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
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
	require.NoError(t, s.inner.Shutdown())
	_ = ctx
}

func TestPeerRefreshHandler_RouteMounted(t *testing.T) {
	t.Parallel()
	var called bool
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		PeerRefreshHandler: func(ctx *fasthttp.RequestCtx) {
			called = true
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
	})
	ctx := testCtx("POST", "/v1/peer/refresh")
	s.Handler()(ctx)
	assert.True(t, called, "PeerRefreshHandler should be mounted at POST /v1/peer/refresh")
	assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())
}

func TestPeerRefreshHandler_NotMountedWhenNil(t *testing.T) {
	t.Parallel()
	s := New(Config{
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
	})
	ctx := testCtx("POST", "/v1/peer/refresh")
	s.Handler()(ctx)
	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode(), "route should not be mounted when PeerRefreshHandler is nil")
}
