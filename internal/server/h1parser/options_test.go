package h1parser

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

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

func (m *mockFastPathHit) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
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
	s := bytesToString(b)
	assert.Equal(t, "hello", s)
}

func TestBytesToString_Empty(t *testing.T) {
	t.Parallel()
	b := []byte{}
	s := bytesToString(b)
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

var _ = io.ReadAll
