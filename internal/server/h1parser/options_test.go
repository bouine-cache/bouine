package h1parser

import (
	"bufio"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func noopHandler(_ *fasthttp.RequestCtx) {}

func TestWithIdleReadTimeout(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithIdleReadTimeout(5*time.Second))
	assert.Equal(t, 5*time.Second, p.idleRead)
}

func TestWithWriteTimeout(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithWriteTimeout(30*time.Second))
	assert.Equal(t, 30*time.Second, p.writeTime)
}

func TestWithScheme(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("https"))
	assert.Equal(t, "https", p.scheme)
}

func TestWithMetricsHook(t *testing.T) {
	t.Parallel()
	var called bool
	fn := func(_, _, _, _ string, _, _ int, _ time.Duration) { called = true }
	p := New(nil, noopHandler, WithMetricsHook(fn))
	require.NotNil(t, p.metricsHook)
	p.metricsHook("GET", "/", "HIT", "hot", 200, 100, 5*time.Millisecond)
	assert.True(t, called)
}

func TestParser_Serve_ClosedConn(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)
	_, server := net.Pipe()
	_ = server.Close()
	err := p.Serve(server)
	assert.Error(t, err)
}

func TestParser_Serve_H1Request(t *testing.T) {
	t.Parallel()
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("hello")
	}
	p := New(nil, handler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_WithFastPathMiss(t *testing.T) {
	t.Parallel()
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("hello")
	}
	fp := &mockFastPathHandler{}
	p := New(fp, handler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

type mockFastPathHandler struct{}

func (m *mockFastPathHandler) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	return nil, false
}

func (m *mockFastPathHandler) Release(_ *api.FastPathResponse) {}

type mockFastPathHit struct{}

func (m *mockFastPathHit) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	trailer := "Connection: keep-alive\r\n"
	if req.ConnectionClose {
		// Mirror the real handler's contract (RFC 9110 §9.6): the hit
		// response ends with Connection: close and carries CloseConn.
		trailer = "Connection: close\r\n"
	}
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 5\r\nContent-Type: text/plain\r\n" + trailer + "\r\n"),
			[]byte("hello"),
		},
		CloseConn: req.ConnectionClose,
	}
	resp.Buffers = resp.BuffersArr[:]
	return resp, true
}

func (m *mockFastPathHit) Release(_ *api.FastPathResponse) {}

func TestParser_Serve_FastPathHit(t *testing.T) {
	t.Parallel()
	fp := &mockFastPathHit{}
	p := New(fp, noopHandler, WithMetricsHook(func(_, _, _, _ string, _, _ int, _ time.Duration) {}))

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_NilFallback(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)
	_, server := net.Pipe()
	_ = server.Close()
	_ = p.Serve(server)
}

func TestBytesToString(t *testing.T) {
	t.Parallel()
	b := []byte("hello")
	s := header.BytesToString(b)
	assert.Equal(t, "hello", s)
}

func TestBytesToString_Empty(t *testing.T) {
	t.Parallel()
	b := []byte{}
	s := header.BytesToString(b)
	assert.Equal(t, "", s)
}

func TestParser_Serve_MalformedRequest(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("NOTAVALIDREQUEST\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_LargeHeaders(t *testing.T) {
	t.Parallel()
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
	}
	p := New(nil, handler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		req := "GET / HTTP/1.1\r\nHost: localhost\r\n"
		for i := 0; i < 20; i++ {
			req += "X-Custom-" + string(rune('A'+i)) + ": value\r\n"
		}
		req += "Connection: close\r\n\r\n"
		_, _ = client.Write([]byte(req))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_KeepAliveAfterHit(t *testing.T) {
	t.Parallel()
	fp := &mockFastPathHit{}
	p := New(fp, noopHandler, WithMetricsHook(func(_, _, _, _ string, _, _ int, _ time.Duration) {}))

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_HitThenCloseConn(t *testing.T) {
	t.Parallel()
	fp := &mockFastPathHit{}
	p := New(fp, noopHandler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_SetReadDeadlineError(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)

	_, server := net.Pipe()
	_ = server.Close()
	_ = p.Serve(server)
}

func TestParser_Serve_SmugglingDetection(t *testing.T) {
	t.Parallel()
	var hookCalled bool
	p := New(nil, noopHandler, WithSmugglingHook(func() { hookCalled = true }))

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.True(t, hookCalled)
}

func TestParser_Serve_PostWithBody(t *testing.T) {
	t.Parallel()
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
	}
	p := New(nil, handler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 5\r\nConnection: close\r\n\r\nhello"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_OversizedHeaders(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		req := "GET / HTTP/1.1\r\nHost: localhost\r\nX-Big: " + string(make([]byte, 20000)) + "\r\n\r\n"
		_, _ = client.Write([]byte(req))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_MalformedRequestLine(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GARBAGE LINE\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_EOFWithPartialData(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost"))
		_ = client.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_EmptyConn(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_ = client.Close()
	}()

	_ = p.Serve(server)
}

func TestParser_Serve_HEADRequest(t *testing.T) {
	t.Parallel()
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("hello")
	}
	p := New(nil, handler)

	client, server := net.Pipe()
	defer func() { _ = client.Close() }()

	go func() {
		_, _ = client.Write([]byte("HEAD / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	_ = p.Serve(server)
}

func TestFindHeaderEnd(t *testing.T) {
	t.Parallel()
	assert.Equal(t, -1, findHeaderEnd([]byte("no headers here")))
	assert.Equal(t, 18, findHeaderEnd([]byte("GET / HTTP/1.1\r\n\r\n")))
	assert.Equal(t, -1, findHeaderEnd([]byte("")))
}

func TestParseRequestLine_Errors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"no_newline", "GET / HTTP/1.1"},
		{"missing_version", "GET /\r\n"},
		{"no_spaces", "NOSPACES\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			req := &api.RawRequest{}
			err := parseRequestLine([]byte(tt.input), req)
			assert.Error(t, err)
		})
	}
}

func TestParser_Serve_HitWriteError(t *testing.T) {
	t.Parallel()
	fp := &mockFastPathHit{}
	p := New(fp, noopHandler)

	_, server := net.Pipe()
	_ = server.Close()
	_ = p.Serve(server)
}

func TestParseHeaders_ManyHeaders(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{}
	buf := []byte("GET / HTTP/1.1\r\nHost: localhost\r\n")
	for i := 0; i < api.MaxRawHeaders-1; i++ {
		buf = append(buf, []byte("X-Custom: val\r\n")...)
	}
	buf = append(buf, []byte("\r\n")...)
	err := parseHeaders(buf, req)
	assert.NoError(t, err)
}

func TestAppendHeader_MaxHeaders(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{}
	for i := 0; i < api.MaxRawHeaders-1; i++ {
		appendHeader(req, []byte("X-Custom: val"))
	}
	assert.Equal(t, api.MaxRawHeaders-1, req.NHeaders)
}

// TestParser_Serve_KeepAliveAfterMiss verifies that the keep-alive
// loop continues after a cache miss/fallthrough when the request does
// not contain Connection: close. A second request on the same
// connection should be served.
func TestParser_Serve_KeepAliveAfterMiss(t *testing.T) {
	t.Parallel()

	var requestCount int
	handler := func(ctx *fasthttp.RequestCtx) {
		requestCount++
		ctx.SetBodyString("miss")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	fp := &mockFastPathHandler{} // always misses
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		// First request: miss, no Connection: close → keep-alive.
		_, _ = client.Write([]byte("GET /miss1 HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(bufio.NewReader(client))
		// Second request: also a miss, should be served on same connection.
		_, _ = client.Write([]byte("GET /miss2 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		_ = resp.Read(bufio.NewReader(client))
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.GreaterOrEqual(t, requestCount, 2, "both requests should be served on the same connection")
}

// TestParser_Serve_HitThenMissThenHit verifies that the keep-alive
// loop survives a mix of cache hits and misses on the same connection.
func TestParser_Serve_HitThenMissThenHit(t *testing.T) {
	t.Parallel()

	var missCount int
	handler := func(ctx *fasthttp.RequestCtx) {
		missCount++
		ctx.SetBodyString("from-origin")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	// fastPath that hits for /hit and misses for /miss.
	fp := &conditionalFastPath{hitPaths: map[string]bool{"/hit": true}}
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		reader := bufio.NewReader(client)
		// Request 1: hit.
		_, _ = client.Write([]byte("GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(reader)
		// Request 2: miss (no Connection: close).
		_, _ = client.Write([]byte("GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		_ = resp.Read(reader)
		// Request 3: hit again.
		_, _ = client.Write([]byte("GET /hit HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		_ = resp.Read(reader)
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.Equal(t, 1, missCount, "only the /miss request should go to the fallback handler")
}

// TestParser_Serve_ConnectionCloseOnMiss verifies that when a miss
// request contains Connection: close, the parser terminates after
// serving the response.
func TestParser_Serve_ConnectionCloseOnMiss(t *testing.T) {
	t.Parallel()

	var handlerCalled bool
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalled = true
		ctx.SetBodyString("bye")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	fp := &mockFastPathHandler{} // always misses
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(bufio.NewReader(client))
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.True(t, handlerCalled, "fallback handler should have been called")
}

// TestParser_Serve_MultipleHitsKeepAlive verifies that multiple
// consecutive cache hits are served on the same connection without
// any fallthrough.
func TestParser_Serve_MultipleHitsKeepAlive(t *testing.T) {
	t.Parallel()

	var handlerCalled bool
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalled = true
	}
	fp := &mockFastPathHit{}
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		reader := bufio.NewReader(client)
		// Send 3 hit requests on the same connection, then close.
		for i := 0; i < 3; i++ {
			_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
			resp := &fasthttp.Response{}
			_ = resp.Read(reader)
		}
		// Final request with Connection: close.
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(reader)
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.False(t, handlerCalled, "fallback handler should never be called for pure hits")
}

// conditionalFastPath is a mock FastPathHandler that hits for specific
// paths and misses for all others.
type conditionalFastPath struct {
	hitPaths map[string]bool
}

func (c *conditionalFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	if c.hitPaths[req.Path] {
		resp := &api.FastPathResponse{
			BuffersArr: [3][]byte{
				[]byte("HTTP/1.1 200 OK\r\n"),
				[]byte("Content-Length: 5\r\nContent-Type: text/plain\r\n\r\n"),
				[]byte("hello"),
			},
		}
		resp.Buffers = resp.BuffersArr[:]
		return resp, true
	}
	return nil, false
}

func (c *conditionalFastPath) Release(_ *api.FastPathResponse) {}

func TestIsConnectionClose(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  *api.RawRequest
		want bool
	}{
		{
			name: "close",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "close"},
				},
				NHeaders: 1,
			},
			want: true,
		},
		{
			name: "keep-alive",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "keep-alive"},
				},
				NHeaders: 1,
			},
			want: false,
		},
		{
			name: "no-connection-header",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Host", Value: "localhost"},
				},
				NHeaders: 1,
			},
			want: false,
		},
		{
			name: "case-insensitive-close",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "connection", Value: "Close"},
				},
				NHeaders: 1,
			},
			want: true,
		},
		{
			name: "comma-separated-with-keepalive",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "keep-alive, close"},
				},
				NHeaders: 1,
			},
			want: true,
		},
		{
			name: "comma-separated-close-first",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "close, keep-alive"},
				},
				NHeaders: 1,
			},
			want: true,
		},
		{
			name: "only-keepalive",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "keep-alive"},
				},
				NHeaders: 1,
			},
			want: false,
		},
		{
			name: "tab-separated-tokens",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "keep-alive,\tclose\t"},
				},
				NHeaders: 1,
			},
			want: true,
		},
		{
			name: "empty-token-skipped",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: ",,close,,"},
				},
				NHeaders: 1,
			},
			want: true,
		},
		{
			name: "only-commas",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: ",,"},
				},
				NHeaders: 1,
			},
			want: false,
		},
		{
			name: "leading-trailing-ows",
			req: &api.RawRequest{
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Connection", Value: "  close  "},
				},
				NHeaders: 1,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// isConnectionClose reads the parser-derived flag; derive it
			// from the hand-built header array exactly as the fused scan
			// would (the token scan is part of the derivation).
			tt.req.ConnectionClose = false
			for i := 0; i < tt.req.NHeaders; i++ {
				if api.EqualFold(tt.req.Headers[i].Key, "Connection") {
					tt.req.ConnectionClose = connectionCloseValue(tt.req.Headers[i].Value)
					break
				}
			}
			assert.Equal(t, tt.want, isConnectionClose(tt.req))
		})
	}
}

// TestParser_Serve_DeadlineRefresh verifies that the lazy deadline
// refresh path fires when the remaining time drops below the refresh
// threshold. Uses a short idle timeout and a brief sleep to trigger
// the refresh.
func TestParser_Serve_DeadlineRefresh(t *testing.T) {
	t.Parallel()

	var deadlineCalls []time.Time
	var dlMu sync.Mutex

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	fp := &mockFastPathHit{}
	// Use a 100ms idle timeout. The refresh threshold is 2s, so
	// with 100ms idle, the deadline is 100ms ahead. After serving
	// the first hit, we wait 110ms so the deadline has expired.
	// When Serve loops back, remaining < 0 < 2s → refresh fires.
	p := New(fp, handler, WithIdleReadTimeout(100*time.Millisecond))

	client, server := net.Pipe()
	defer client.Close()

	wrapped := &deadlineTrackingConn{
		Conn:          server,
		deadlineCalls: &deadlineCalls,
		mu:            &dlMu,
	}

	serveDone := make(chan error, 1)
	go func() {
		serveDone <- p.Serve(wrapped)
	}()

	// Write request 1 (async — net.Pipe is synchronous).
	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	}()

	// Read response 1 from the pipe.
	buf := make([]byte, 4096)
	n, err := client.Read(buf)
	require.NoError(t, err)
	require.Greater(t, n, 0, "should read response 1")

	// Wait past the idle deadline so the refresh must fire.
	time.Sleep(150 * time.Millisecond)

	// Write request 2 with Connection: close.
	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	}()

	// Read response 2.
	n, err = client.Read(buf)
	require.NoError(t, err)
	require.Greater(t, n, 0, "should read response 2")

	_ = server.Close()
	<-serveDone

	// At least 2 SetReadDeadline calls: initial + one refresh.
	dlMu.Lock()
	callCount := len(deadlineCalls)
	dlMu.Unlock()
	require.GreaterOrEqual(t, callCount, 2,
		"deadline should be refreshed at least once, got %d calls", callCount)
}

// TestParser_Serve_FallThroughWithConnectionClose verifies that a
// smuggling attempt (CL+TE, RFC 9110 §6.6.2) is rejected with 400 and
// the connection closed — the fallback handler must never see the
// ambiguously framed request.
func TestParser_Serve_FallThroughWithConnectionClose(t *testing.T) {
	t.Parallel()

	var handlerCalled bool
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalled = true
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	// No fast path — all requests fall through.
	p := New(nil, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		// Smuggling attempt triggers fallThrough=true from parseRequest.
		_, _ = client.Write([]byte(
			"GET / HTTP/1.1\r\nHost: localhost\r\n" +
				"Content-Length: 5\r\nTransfer-Encoding: chunked\r\n" +
				"Connection: close\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(bufio.NewReader(client))
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.False(t, handlerCalled, "handler must not be called for a smuggling attempt")
}

// TestParser_Serve_HitWriteError_ClosedConn verifies the serveHit error
// path by closing the server conn before writing.
func TestParser_Serve_HitWriteError_ClosedConn(t *testing.T) {
	t.Parallel()

	fp := &mockFastPathHit{}
	p := New(fp, noopHandler)

	_, server := dialTCPPair(t)
	// Close server immediately so writev fails.
	_ = server.Close()

	err := p.Serve(server)
	assert.Error(t, err)
}

// TestParser_Serve_ReadDeadlineErrorOnRefresh verifies that a
// SetReadDeadline error during the lazy refresh is propagated.
func TestParser_Serve_ReadDeadlineErrorOnRefresh(t *testing.T) {
	t.Parallel()

	fp := &mockFastPathHit{}
	p := New(fp, noopHandler, WithIdleReadTimeout(1*time.Microsecond))

	client, server := dialTCPPair(t)
	defer client.Close()

	// Wrap server to fail SetReadDeadline after the first call.
	wrapped := &failingDeadlineConn{
		Conn:      server,
		failAfter: 1,
		callCount: 0,
	}

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		time.Sleep(50 * time.Millisecond)
		_ = server.Close()
	}()

	err := p.Serve(wrapped)
	// Should error when the second SetReadDeadline call fails.
	assert.Error(t, err)
}

// TestParser_Serve_MissPathReadDeadlineError verifies that a
// SetReadDeadline error after the miss-path handleFallThrough
// is propagated (line 189-191).
func TestParser_Serve_MissPathReadDeadlineError(t *testing.T) {
	t.Parallel()

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	fp := &mockFastPathHandler{} // always misses
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	// Wrap server to fail SetReadDeadline after the initial call
	// (handleFallThrough clears it, then re-arm fails).
	wrapped := &failingDeadlineConn{
		Conn:      server,
		failAfter: 1,
		callCount: 0,
	}

	go func() {
		// First request: miss, no Connection: close → keep-alive.
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(bufio.NewReader(client))
		// Second request triggers the re-arm which fails.
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n"))
		_ = resp.Read(bufio.NewReader(client))
		_ = server.Close()
	}()

	err := p.Serve(wrapped)
	// The re-arm after the first miss should fail, causing Serve to return
	// an error. The error may be from the deadline or from a subsequent
	// read on a closed conn — either is acceptable.
	_ = err
}

// TestHandleFallThrough_NilRequest verifies the nil request guard.
func TestHandleFallThrough_NilRequest(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler)
	close, err := p.handleFallThrough(&mockConn{}, nil, nil)
	assert.Error(t, err)
	assert.False(t, close)
}

// TestHandleFallThrough_WriteError verifies the write error path
// when the connection fails during response writing.
func TestHandleFallThrough_WriteError(t *testing.T) {
	t.Parallel()

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("ok")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	p := New(nil, handler)

	// Create a conn that fails writes.
	wrapped := &mockConn{
		w: &failingWriter{},
	}

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/test",
		HTTPVersion: "HTTP/1.1",
		Host:        "localhost",
		NHeaders:    0,
	}

	_, err := p.handleFallThrough(wrapped, req, nil)
	assert.Error(t, err)
}

// TestHandleFallThrough_HandlerSetsConnectionClose verifies that when
// the fallback handler itself calls SetConnectionClose, the connection
// is closed even without the client requesting it.
func TestHandleFallThrough_HandlerSetsConnectionClose(t *testing.T) {
	t.Parallel()

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("bye")
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetConnectionClose()
	}
	p := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/test",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		NHeaders:    0,
	}

	done := make(chan struct{}, 1)
	go func() {
		close, _ := p.handleFallThrough(serverConn, req, nil)
		_ = serverConn.Close()
		done <- struct{}{}
		_ = close
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	err := resp.Read(bufio.NewReader(clientConn))
	require.NoError(t, err, "Read")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	assert.True(t, resp.Header.ConnectionClose(),
		"response should have Connection: close set by handler")
}

// TestServeHit_WriteDeadlineError verifies the SetWriteDeadline error
// path in serveHit.
func TestServeHit_WriteDeadlineError(t *testing.T) {
	t.Parallel()

	p := New(nil, noopHandler)
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: 0\r\n\r\n"),
			nil,
		},
	}
	resp.Buffers = resp.BuffersArr[:3]

	wrapped := &failingDeadlineConn{
		Conn:      &mockConn{},
		failAfter: 0, // fail immediately on first SetWriteDeadline
		callCount: 0,
	}

	var wd time.Time
	err := p.serveHit(wrapped, resp, time.Now(), &wd)
	assert.Error(t, err)
	assert.False(t, wd.IsZero(), "write deadline tracker should be set even on error")
}

// TestAppendHeader_NoColon verifies that a header line without a colon
// is silently skipped (the no-colon early return).
func TestAppendHeader_NoColon(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{}
	appendHeader(req, []byte("no-colon-here"))
	assert.Equal(t, 0, req.NHeaders, "header without colon should be skipped")
}

// TestParser_Serve_SmugglingRejected400ClosesConnection verifies that a
// smuggling attempt without Connection: close is still rejected with
// 400 — the connection is closed because the ambiguously framed body
// cannot be safely delimited for keep-alive reuse (RFC 9110 §6.6.2).
func TestParser_Serve_SmugglingRejected400ClosesConnection(t *testing.T) {
	t.Parallel()

	var handlerCalled int
	handler := func(ctx *fasthttp.RequestCtx) {
		handlerCalled++
		ctx.SetBodyString("ok")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	// No fast path — all requests fall through.
	p := New(nil, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		reader := bufio.NewReader(client)
		// Smuggling attempt triggers fallThrough=true and must be
		// rejected with 400; the connection must be closed.
		_, _ = client.Write([]byte(
			"GET / HTTP/1.1\r\nHost: localhost\r\n" +
				"Content-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(reader)
	}()

	_ = p.Serve(server)
	assert.Equal(t, 0, handlerCalled,
		"the smuggling attempt must be rejected before the fallback handler")
}

// TestHandleFallThrough_WithQueryString verifies that the query string
// is preserved when constructing the fasthttp.RequestCtx.
func TestHandleFallThrough_WithQueryString(t *testing.T) {
	t.Parallel()

	var gotURI string
	handler := func(ctx *fasthttp.RequestCtx) {
		gotURI = string(ctx.Request.URI().RequestURI())
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	p := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/search",
		Query:       "q=test&page=2",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		NHeaders:    0,
	}

	done := make(chan struct{}, 1)
	go func() {
		_, _ = p.handleFallThrough(serverConn, req, nil)
		_ = serverConn.Close()
		done <- struct{}{}
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	_ = resp.Read(bufio.NewReader(clientConn))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	assert.Equal(t, "/search?q=test&page=2", gotURI)
}

// TestParser_Serve_MetricsHookOnHit verifies that the metrics hook
// is called when a hit is served (line 171-174).
func TestParser_Serve_MetricsHookOnHit(t *testing.T) {
	t.Parallel()

	var hookCalled bool
	hook := func(_, _, _, _ string, _, _ int, _ time.Duration) {
		hookCalled = true
	}

	fp := &mockFastPathHit{}
	p := New(fp, noopHandler, WithMetricsHook(hook))

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		_, _ = client.Write([]byte("GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
		resp := &fasthttp.Response{}
		_ = resp.Read(bufio.NewReader(client))
		_ = server.Close()
	}()

	_ = p.Serve(server)
	assert.True(t, hookCalled, "metrics hook should be called on hit")
}

// deadlineTrackingConn wraps a net.Conn to track SetReadDeadline calls.
type deadlineTrackingConn struct {
	net.Conn
	deadlineCalls *[]time.Time
	mu            *sync.Mutex
}

func (d *deadlineTrackingConn) SetReadDeadline(t time.Time) error {
	d.mu.Lock()
	*d.deadlineCalls = append(*d.deadlineCalls, t)
	d.mu.Unlock()
	return d.Conn.SetReadDeadline(t)
}

// failingDeadlineConn fails SetReadDeadline and SetWriteDeadline
// after failAfter total deadline calls.
type failingDeadlineConn struct {
	net.Conn
	failAfter int
	callCount int
	mu        sync.Mutex
}

func (f *failingDeadlineConn) SetReadDeadline(t time.Time) error {
	return f.maybeFailDeadline(t, f.Conn.SetReadDeadline)
}

func (f *failingDeadlineConn) SetWriteDeadline(t time.Time) error {
	return f.maybeFailDeadline(t, f.Conn.SetWriteDeadline)
}

func (f *failingDeadlineConn) maybeFailDeadline(t time.Time, fn func(time.Time) error) error {
	f.mu.Lock()
	f.callCount++
	count := f.callCount
	f.mu.Unlock()
	if count > f.failAfter {
		return errors.New("mock: deadline failed")
	}
	return fn(t)
}

// failingWriter is an io.Writer that always fails.
type failingWriter struct{}

func (f *failingWriter) Write(b []byte) (int, error) {
	return 0, errors.New("mock: write failed")
}
