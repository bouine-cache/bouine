package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// --- peekConn tests --- (removed: peekConn deleted with h2c detection)

func TestPeekConn_Removed(t *testing.T) {
	t.Skip("peekConn removed — h2c detection dropped with HTTP/2")
}

// --- closeNotifyConn tests --- (removed: closeNotifyConn deleted with net/http fallback)

func TestCloseNotifyConn_Removed(t *testing.T) {
	t.Skip("closeNotifyConn removed — net/http fallback dropped")
}

// --- singleConnListener tests --- (removed: singleConnListener deleted with net/http fallback)

func TestSingleConnListener_Removed(t *testing.T) {
	t.Skip("singleConnListener removed — net/http fallback dropped")
}

// --- Listener Name/Addr/Shutdown tests ---

func TestListener_Name(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{Addr: ":0", Handler: echo200(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	assert.Equal(t, "http", srv.Name())
}

func TestListener_NameHTTPS(t *testing.T) {
	t.Parallel()
	srv := NewHTTPS(ListenerConfig{Addr: ":0", Handler: echo200(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	assert.Equal(t, "https", srv.Name())
}

func TestListener_AddrBeforeServe(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{Addr: "127.0.0.1:8080", Handler: echo200(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	assert.Equal(t, "127.0.0.1:8080", srv.Addr())
}

func TestListener_Shutdown(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{Addr: "127.0.0.1:0", Handler: echo200(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)
	// Shutdown should work.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	err := srv.Shutdown(shutCtx)
	require.NoError(t, err)
	cancel()
	<-errCh
}

// --- Router additional tests ---

func TestRouter_MatchByHostPath(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "/v1", "api-v1", nil, ok200("api"))
	rt.AddRoute("", "/", "root", nil, ok200("root"))

	assert.Equal(t, "api-v1", rt.MatchByHostPath("api.example.com", "/v1/users"))
	assert.Equal(t, "root", rt.MatchByHostPath("other.com", "/anything"))
	// No route matches a non-existent host with no catch-all.
	rt2 := NewRouter(RouterConfig{})
	rt2.AddRoute("only.example.com", "/", "only", nil, ok200("only"))
	assert.Equal(t, "", rt2.MatchByHostPath("nomatch.com", "/nomatch"))
}

func TestRouter_MatchByHostPath_StripsPort(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "", "api", nil, ok200("api"))
	assert.Equal(t, "api", rt.MatchByHostPath("api.example.com:443", "/"))
}

func TestRouter_RouteLabelAuto(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "/v1", "", nil, ok200("api"))
	rt.AddRoute("", "/static", "", nil, ok200("static"))
	rt.AddRoute("", "", "", nil, ok200("catchall"))

	// Labels should be auto-generated.
	assert.Equal(t, "api.example.com:/v1", rt.routes[0].label)
	assert.Equal(t, "/static", rt.routes[1].label)
	assert.Equal(t, "_catch-all", rt.routes[2].label)
}

func TestRouter_MetricsIncrement(t *testing.T) {
	t.Parallel()
	var reqCount, noRoute int
	rt := NewRouter(RouterConfig{
		Metrics: &RouterMetrics{
			RequestsTotal: &counterFunc{f: func() { reqCount++ }},
			NoRouteTotal:  &counterFunc{f: func() { noRoute++ }},
		},
	})
	rt.AddRoute("", "/api", "api", nil, ok200("api"))

	// Matching route should increment RequestsTotal.
	ctx1 := serveRoute(t, rt, "GET", "example.com", "/api/v1")
	assert.Equal(t, 1, reqCount)
	assert.Equal(t, 0, noRoute)
	_ = ctx1

	// Non-matching route should increment both.
	ctx2 := serveRoute(t, rt, "GET", "example.com", "/nope")
	assert.Equal(t, 2, reqCount)
	assert.Equal(t, 1, noRoute)
	_ = ctx2
}

// --- maxConnsError test ---

func TestMaxConnsError(t *testing.T) {
	t.Parallel()
	e := maxConnsError{}
	assert.Equal(t, "max_connections reached", e.Error())
	assert.False(t, e.Timeout())
	assert.True(t, e.Temporary())
}

// --- fastPathEnabled test ---

func TestFastPathEnabled(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{Addr: ":0", Handler: echo200(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	assert.False(t, srv.fastPathEnabled())
}

// --- logReusePortStart test ---

func TestLogReusePortStart(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{Addr: ":0", Handler: echo200(), Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	// Should not panic.
	srv.logReusePortStart("127.0.0.1:0", 4)
}

// --- connLimitConn concurrent test ---

func TestConnLimitConn_ConcurrentClose(t *testing.T) {
	t.Parallel()
	sem := make(chan struct{}, 4)
	var open int32
	_, server := net.Pipe()
	defer func() { _ = server.Close() }()
	c := &connLimitConn{Conn: server, sem: sem, open: &open}
	sem <- struct{}{}
	atomic.AddInt32(&open, 1)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Close()
		}()
	}
	wg.Wait()
	// Should have released exactly one slot.
	assert.Equal(t, int32(0), atomic.LoadInt32(&open))
}

// --- serveConnWithHTTP test (via integration) ---
// serveConnWithHTTP is tested indirectly via TestHTTP_ListenAndServe.

// --- handleCleartextFastPath test (h2c detection) ---

func TestHandleCleartextFastPath_NilParser(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	_ = server.Close()
}

// --- Helper: counterFunc (from handler_test.go pattern) ---

type counterFunc struct {
	f func()
}

func (c *counterFunc) Inc() { c.f() }

// Ensure bytes import is used.
var _ = bytes.MinRead

// --- Fast path integration tests ---

// TestServeFastPath_H1Request verifies that serveFastPath correctly
// routes an HTTP/1.1 cleartext request through the h1parser, which
// falls through to net/http when the fast path handler misses.
func TestServeFastPath_H1Request(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	// Send an HTTP/1.1 GET request.
	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Contains(t, string(buf[:n]), "200")
	assert.Contains(t, string(buf[:n]), "hello")
	cancel()
	<-errCh
}

// TestServeFastPath_H2CPreface — removed (h2c detection dropped with HTTP/2).
func TestServeFastPath_H2CPreface(t *testing.T) {
	t.Skip("h2c preface detection removed — HTTP/2 support dropped")
}

// TestServeFastPath_EmptyConnection verifies that serveFastPath handles
// a connection that sends no data (triggers peek error path).
func TestServeFastPath_EmptyConnection(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	// Connect and immediately close — triggers the peek error path
	// in handleCleartextFastPath.
	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	_ = conn.Close()
	// Give the server a moment to process.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-errCh
}

// TestServeFastPath_HEADRequest verifies that HEAD requests work through
// the fast path (h1parser handles HEAD by falling through to net/http).
func TestServeFastPath_HEADRequest(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:        "127.0.0.1:0",
		Handler:     echo200(),
		Logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		FastPath:    &mockFastPathHandler{},
		FastMetrics: &mockFastPathMetrics{},
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv)

	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Contains(t, string(buf[:n]), "200")
	cancel()
	<-errCh
}

// TestSetSocketOptions_AllEnabled verifies setSocketOptions doesn't panic
// when all options are enabled (fastOpen, deferAccept, reusePort).
func TestSetSocketOptions_AllEnabled(t *testing.T) {
	t.Parallel()
	control := setSocketOptions(slog.New(slog.NewTextHandler(io.Discard, nil)), true, true, true)
	require.NotNil(t, control)
}

// TestSetSocketOptions_AllDisabled verifies setSocketOptions returns a
// non-nil control function even when all options are disabled.
func TestSetSocketOptions_AllDisabled(t *testing.T) {
	t.Parallel()
	control := setSocketOptions(slog.New(slog.NewTextHandler(io.Discard, nil)), false, false, false)
	require.NotNil(t, control)
}

// --- Mock types ---

type mockFastPathHandler struct{}

func (m *mockFastPathHandler) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	return nil, false // always miss — fall through to net/http
}

func (m *mockFastPathHandler) Release(resp *api.FastPathResponse) {}

type mockFastPathMetrics struct{}

func (m *mockFastPathMetrics) RecordHit(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration) {
}
func (m *mockFastPathMetrics) IncrementSmugglingRejected() {}
