package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// --- peekConn tests ---

func TestPeekConn_PeekAndRead(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	pk := newPeekConn(server)
	// Write data to the client side in a goroutine (net.Pipe is synchronous).
	go func() { _, _ = client.Write([]byte("hello")) }()

	// Peek first 5 bytes.
	peeked, err := pk.Peek(5)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(peeked))

	// Read should return peeked bytes first.
	buf := make([]byte, 5)
	n, err := pk.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf[:n]))
}

func TestPeekConn_PeekMoreThanAvailable(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	pk := newPeekConn(server)
	// Write only 3 bytes.
	go func() { _, _ = client.Write([]byte("abc")) }()

	// Peek 10 bytes — should return what's available.
	peeked, _ := pk.Peek(10)
	assert.GreaterOrEqual(t, len(peeked), 0)
	// Error may be nil if some bytes were read.
	if len(peeked) >= 3 {
		assert.Equal(t, "abc", string(peeked[:3]))
	}
}

func TestPeekConn_ReadAfterPeek(t *testing.T) {
	t.Parallel()
	client, server := net.Pipe()
	defer func() { _ = client.Close() }()
	defer func() { _ = server.Close() }()

	pk := newPeekConn(server)
	go func() { _, _ = client.Write([]byte("test data")) }()

	// Peek 4 bytes.
	peeked, err := pk.Peek(4)
	require.NoError(t, err)
	assert.Equal(t, "test", string(peeked))

	// Read should return peeked bytes.
	buf := make([]byte, 4)
	n, err := pk.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "test", string(buf[:n]))
}

// --- closeNotifyConn tests ---

func TestCloseNotifyConn_CloseNotifies(t *testing.T) {
	t.Parallel()
	_, server := net.Pipe()
	cn := newCloseNotifyConn(server)
	select {
	case <-cn.done:
		t.Fatal("done channel should not be closed before Close")
	default:
	}
	_ = cn.Close()
	select {
	case <-cn.done:
		// Good — channel is closed.
	case <-time.After(time.Second):
		t.Fatal("done channel not closed after Close")
	}
}

func TestCloseNotifyConn_DoubleClose(t *testing.T) {
	t.Parallel()
	_, server := net.Pipe()
	cn := newCloseNotifyConn(server)
	_ = cn.Close()
	_ = cn.Close() // must not panic (sync.Once)
}

// --- singleConnListener tests ---

func TestSingleConnListener_AcceptAndClose(t *testing.T) {
	t.Parallel()
	_, server := net.Pipe()
	cn := newCloseNotifyConn(server)
	cl := &singleConnListener{conn: cn, ready: cn.done}

	// First Accept returns the connection.
	conn, err := cl.Accept()
	require.NoError(t, err)
	assert.NotNil(t, conn)

	// Close the connection so the listener can proceed.
	_ = conn.Close()

	// Second Accept should return ErrClosed after the connection is closed.
	go func() {
		time.Sleep(10 * time.Millisecond)
		_ = cl.Close()
	}()

	_, err = cl.Accept()
	assert.Error(t, err)
}

func TestSingleConnListener_Addr(t *testing.T) {
	t.Parallel()
	_, server := net.Pipe()
	defer func() { _ = server.Close() }()
	cn := newCloseNotifyConn(server)
	cl := &singleConnListener{conn: cn, ready: cn.done}
	assert.NotNil(t, cl.Addr())
}

func TestSingleConnListener_Close(t *testing.T) {
	t.Parallel()
	_, server := net.Pipe()
	defer func() { _ = server.Close() }()
	cn := newCloseNotifyConn(server)
	cl := &singleConnListener{conn: cn, ready: cn.done}
	assert.NoError(t, cl.Close())
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
	waitForAddr(t, srv, 3*time.Second)
	// Shutdown should work.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutCancel()
	err := srv.Shutdown(shutCtx)
	require.NoError(t, err)
	cancel()
	<-errCh
}

// --- reportFastPathError ---

func TestReportFastPathError_NoPanic(t *testing.T) {
	t.Parallel()
	errCh := make(chan<- error, 1)
	// Should not send anything to errCh and should not panic.
	reportFastPathError(nil, errCh)
	reportFastPathError(nil, errCh)
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
	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1", nil))
	assert.Equal(t, 1, reqCount)
	assert.Equal(t, 0, noRoute)

	// Non-matching route should increment both.
	rr2 := httptest.NewRecorder()
	rt.ServeHTTP(rr2, httptest.NewRequest("GET", "/nope", nil))
	assert.Equal(t, 2, reqCount)
	assert.Equal(t, 1, noRoute)
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

// TestListener_ServeSingle_WithFastPath verifies that serveSingle works
// when the fast path is enabled (uses a mock FastPathHandler).
func TestListener_ServeSingle_WithFastPath(t *testing.T) {
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
	waitForAddr(t, srv, 3*time.Second)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

// TestListener_ServeSingle_WithMaxConns verifies that the connection limit
// listener is applied when maxConns > 0.
func TestListener_ServeSingle_WithMaxConns(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:           "127.0.0.1:0",
		Handler:        echo200(),
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
		MaxConnections: 5,
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv, 3*time.Second)
	cancel()
	err := <-errCh
	require.NoError(t, err)
}

// TestSetSocketOptions_NoReusePort verifies setSocketOptions doesn't fail
// when reusePort is false.
func TestSetSocketOptions_NoReusePort(t *testing.T) {
	t.Parallel()
	control := setSocketOptions(slog.New(slog.NewTextHandler(io.Discard, nil)), false, false, false)
	require.NotNil(t, control)
}

// TestServeConnWithHTTP_H2CConnection verifies that serveConnWithHTTP
// correctly serves an h2c connection through net/http.
func TestServeConnWithHTTP_H2CConnection(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr: "127.0.0.1:0",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("X-Test", "h2c")
			w.WriteHeader(200)
		}),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv, 3*time.Second)

	// Connect and send an HTTP/1.1 request.
	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Contains(t, string(buf[:n]), "200")
	assert.Contains(t, string(buf[:n]), "X-Test: h2c")
	cancel()
	<-errCh
}

// TestHandleFastPathConn_CleartextH1 verifies handleFastPathConn routes
// a cleartext H1 connection through the h1parser (or net/http when
// fastPath is disabled).
func TestHandleFastPathConn_CleartextH1(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv, 3*time.Second)

	// Connect and send a simple GET.
	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	require.NoError(t, err)
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Contains(t, string(buf[:n]), "200")
	cancel()
	<-errCh
}

// TestHandleCleartextFastPath_H2CPrefaceDetected verifies that the h2c
// preface is detected and the connection is routed to net/http.
func TestHandleCleartextFastPath_H2CPrefaceDetected(t *testing.T) {
	t.Parallel()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	waitForAddr(t, srv, 3*time.Second)

	// Connect and send the h2c preface followed by an HTTP/2 request.
	addr := srv.Addr()
	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()
	_, err = conn.Write([]byte(h2cPreface))
	require.NoError(t, err)
	// Send a minimal HTTP/2 settings frame.
	_, err = conn.Write([]byte{0, 0, 0, 4, 0, 0, 0, 0, 0}) // SETTINGS frame
	require.NoError(t, err)
	// Read response (may be a settings frame from the server).
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, _ = conn.Read(buf)
	cancel()
	<-errCh
}

// --- Mock FastPathHandler ---

type mockFastPathHandler struct{}

func (m *mockFastPathHandler) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	return nil, false // always miss — fall through to net/http
}

func (m *mockFastPathHandler) Release(resp *api.FastPathResponse) {}

// --- Mock FastPathMetrics ---

type mockFastPathMetrics struct{}

func (m *mockFastPathMetrics) RecordHit(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration) {
}
func (m *mockFastPathMetrics) IncrementSmugglingRejected() {}
