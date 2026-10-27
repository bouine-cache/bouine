package h1parser

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

func TestHandleFallThrough_ServesResponse(t *testing.T) {
	responseBody := []byte("Hello from origin")

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetBody(responseBody)
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/miss",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 2,
	}

	done := make(chan struct{}, 1)
	go func() {
		_, _ = parser.handleFallThrough(serverConn, req, nil)
		_ = serverConn.Close()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	err := resp.Read(bufio.NewReader(clientConn))
	require.NoError(t, err, "Read")
	body := resp.Body()

	assert.Equal(t, 200, resp.StatusCode())
	assert.True(t, bytes.Equal(body, responseBody))
}

// TestHandleFallThrough_TransfersOwnerMissHint pins the fast-path
// owner-miss plumbing: the fallback handler must see
// api.OwnerMissContextKey in its RequestCtx user values exactly when
// RawRequest.OwnerMiss was set, so handleCacheMiss can skip the
// duplicate peer RPC.
func TestHandleFallThrough_TransfersOwnerMissHint(t *testing.T) {
	tests := []struct {
		name       string
		ownerMiss  bool
		wantHinted bool
	}{
		{"flagged", true, true},
		{"unflagged", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			handler := func(ctx *fasthttp.RequestCtx) {
				got = ctx.UserValue(api.OwnerMissContextKey)
				ctx.SetStatusCode(fasthttp.StatusOK)
			}
			parser := New(nil, handler, WithNowFunc(time.Now))
			clientConn, serverConn := dialTCPPair(t)
			defer clientConn.Close()

			req := &api.RawRequest{
				Method:      "GET",
				Path:        "/miss",
				HTTPVersion: "HTTP/1.1",
				Host:        "example.com",
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Host", Value: "example.com"},
					{Key: "Connection", Value: "close"},
				},
				NHeaders:  2,
				OwnerMiss: tt.ownerMiss,
			}

			done := make(chan struct{}, 1)
			go func() {
				_, _ = parser.handleFallThrough(serverConn, req, nil)
				_ = serverConn.Close()
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handleFallThrough did not return within 5s")
			}

			if tt.wantHinted {
				assert.Equal(t, true, got, "hint must be transferred to the fallback ctx")
			} else {
				assert.Nil(t, got, "unflagged request must not carry the hint")
			}
		})
	}
}

// TestHandleFallThrough_TransfersOwnerResolver pins the ADR-0068
// resolver transfer: the metadata-only Vary resolver the fast path
// fetched must reach the fallback handler under
// api.OwnerResolverContextKey exactly when RawRequest.OwnerResolver
// was set, so handleCacheMiss runs the two-step variant resolution
// without repeating the identical bare ask. The pointer must arrive
// unmodified — the object is the identity proof (its Key) the slow
// path verifies.
func TestHandleFallThrough_TransfersOwnerResolver(t *testing.T) {
	tests := []struct {
		name     string
		resolver *api.Object
		wantXfer bool
	}{
		{"flagged", &api.Object{Key: api.Key{0x2a}, VaryValue: "X-Region"}, true},
		{"unset", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			handler := func(ctx *fasthttp.RequestCtx) {
				got = ctx.UserValue(api.OwnerResolverContextKey)
				ctx.SetStatusCode(fasthttp.StatusOK)
			}
			parser := New(nil, handler, WithNowFunc(time.Now))
			clientConn, serverConn := dialTCPPair(t)
			defer clientConn.Close()

			req := &api.RawRequest{
				Method:      "GET",
				Path:        "/miss",
				HTTPVersion: "HTTP/1.1",
				Host:        "example.com",
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Host", Value: "example.com"},
					{Key: "Connection", Value: "close"},
				},
				NHeaders:      2,
				OwnerResolver: tt.resolver,
			}

			done := make(chan struct{}, 1)
			go func() {
				_, _ = parser.handleFallThrough(serverConn, req, nil)
				_ = serverConn.Close()
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handleFallThrough did not return within 5s")
			}

			if tt.wantXfer {
				gotObj, ok := got.(*api.Object)
				require.True(t, ok, "transferred value must be the resolver object")
				assert.Same(t, tt.resolver, gotObj,
					"the fetched resolver must be transferred unmodified — its Key is the identity proof")
			} else {
				assert.Nil(t, got, "request without a fetched resolver must not carry the user value")
			}
		})
	}
}

// TestHandleFallThrough_TransfersOwnerShedHint pins the
// owner-shed plumbing: the fallback handler must see
// api.OwnerShedContextKey in its RequestCtx user values exactly
// when RawRequest.OwnerShed was set, so handleCacheMiss can skip
// the peer retry after a shed.
func TestHandleFallThrough_TransfersOwnerShedHint(t *testing.T) {
	tests := []struct {
		name       string
		ownerShed  bool
		wantHinted bool
	}{
		{"flagged", true, true},
		{"unflagged", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			handler := func(ctx *fasthttp.RequestCtx) {
				got = ctx.UserValue(api.OwnerShedContextKey)
				ctx.SetStatusCode(fasthttp.StatusOK)
			}
			parser := New(nil, handler, WithNowFunc(time.Now))
			clientConn, serverConn := dialTCPPair(t)
			defer clientConn.Close()

			req := &api.RawRequest{
				Method:      "GET",
				Path:        "/miss",
				HTTPVersion: "HTTP/1.1",
				Host:        "example.com",
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Host", Value: "example.com"},
					{Key: "Connection", Value: "close"},
				},
				NHeaders:  2,
				OwnerShed: tt.ownerShed,
			}

			done := make(chan struct{}, 1)
			go func() {
				_, _ = parser.handleFallThrough(serverConn, req, nil)
				_ = serverConn.Close()
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handleFallThrough did not return within 5s")
			}

			if tt.wantHinted {
				assert.Equal(t, true, got, "hint must be transferred to the fallback ctx")
			} else {
				assert.Nil(t, got, "unflagged request must not carry the hint")
			}
		})
	}
}

// TestHandleFallThrough_TransfersOwnerGateRejectHint pins the
// gate-rejection plumbing: the fallback handler must see
// api.OwnerGateRejectContextKey in its RequestCtx user values exactly
// when RawRequest.OwnerGateReject was set, so handleCacheMiss can skip
// the deterministically-rejected peer retry (nil-policy routes).
func TestHandleFallThrough_TransfersOwnerGateRejectHint(t *testing.T) {
	tests := []struct {
		name       string
		gateReject bool
		wantHinted bool
	}{
		{"flagged", true, true},
		{"unflagged", false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got any
			handler := func(ctx *fasthttp.RequestCtx) {
				got = ctx.UserValue(api.OwnerGateRejectContextKey)
				ctx.SetStatusCode(fasthttp.StatusOK)
			}
			parser := New(nil, handler, WithNowFunc(time.Now))
			clientConn, serverConn := dialTCPPair(t)
			defer clientConn.Close()

			req := &api.RawRequest{
				Method:      "GET",
				Path:        "/miss",
				HTTPVersion: "HTTP/1.1",
				Host:        "example.com",
				Headers: [api.MaxRawHeaders]api.RawHeader{
					{Key: "Host", Value: "example.com"},
					{Key: "Connection", Value: "close"},
				},
				NHeaders:        2,
				OwnerGateReject: tt.gateReject,
			}

			done := make(chan struct{}, 1)
			go func() {
				_, _ = parser.handleFallThrough(serverConn, req, nil)
				_ = serverConn.Close()
				done <- struct{}{}
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("handleFallThrough did not return within 5s")
			}

			if tt.wantHinted {
				assert.Equal(t, true, got, "hint must be transferred to the fallback ctx")
			} else {
				assert.Nil(t, got, "unflagged request must not carry the hint")
			}
		})
	}
}

func TestHandleFallThrough_PreservesHeaders(t *testing.T) {
	var gotAccept, gotCustom string
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx *fasthttp.RequestCtx) {
		gotAccept = string(ctx.Request.Header.Peek("Accept"))
		gotCustom = string(ctx.Request.Header.Peek("X-Custom"))
		wg.Done()
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/api",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/html"},
			{Key: "X-Custom", Value: "custom-value"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 4,
	}

	done := make(chan struct{}, 1)
	go func() {
		_, _ = parser.handleFallThrough(serverConn, req, nil)
		_ = serverConn.Close()
		done <- struct{}{}
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

	wg.Wait()
	assert.Equal(t, "text/html", gotAccept)
	assert.Equal(t, "custom-value", gotCustom)
}

func TestHandleFallThrough_PassesExcessBody(t *testing.T) {
	var gotBody []byte
	var wg sync.WaitGroup
	wg.Add(1)

	handler := func(ctx *fasthttp.RequestCtx) {
		gotBody = append(gotBody[:0], ctx.Request.Body()...)
		wg.Done()
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	parser := New(nil, handler, WithNowFunc(time.Now))

	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	excess := []byte(`{"key":"value!!"}`)

	req := &api.RawRequest{
		Method:      "POST",
		Path:        "/submit",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Type", Value: "application/json"},
			{Key: "Content-Length", Value: "17"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 4,
	}

	done := make(chan struct{}, 1)
	go func() {
		_, _ = parser.handleFallThrough(serverConn, req, excess)
		_ = serverConn.Close()
		done <- struct{}{}
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

	wg.Wait()
	assert.True(t, bytes.Equal(gotBody, excess))
}

func dialTCPPair(t *testing.T) (client, server net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err, "Listen")
	t.Cleanup(func() { ln.Close() })

	type acceptResult struct {
		conn net.Conn
		err  error
	}
	ch := make(chan acceptResult, 1)
	go func() {
		c, err := ln.Accept()
		ch <- acceptResult{c, err}
	}()

	c, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err, "Dial")

	res := <-ch
	require.Nil(t, res.err)

	return c, res.conn
}

// TestHandleFallThrough_PoolReaderReset pins the pooled-reader reset
// contract: a checked-out *bufio.Reader must never carry buffered bytes
// from a previous fall-through into the next request's parse. A pipelined
// or aborted request can leave bytes unread in the reader's buffer; the
// pool hands the same entry to the next fall-through (possibly on another
// connection — the Parser is shared listener-wide), so both release and
// checkout must Reset. Without it the stale prefix would corrupt the
// next request's head and the fallback handler would never run.
func TestHandleFallThrough_PoolReaderReset(t *testing.T) {
	var gotPaths []string
	handler := func(ctx *fasthttp.RequestCtx) {
		gotPaths = append(gotPaths, string(ctx.Path()))
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	parser := New(nil, handler)
	req := func(path string) *api.RawRequest {
		return &api.RawRequest{
			Method:      "GET",
			Path:        path,
			HTTPVersion: "HTTP/1.1",
			Host:        "example.com",
			Headers: [api.MaxRawHeaders]api.RawHeader{
				{Key: "Host", Value: "example.com"},
				{Key: "Connection", Value: "close"},
			},
			NHeaders: 2,
		}
	}

	// First fall-through serves and releases its reader to the pool.
	_, err := parser.handleFallThrough(&mockConn{}, req("/first"), nil)
	require.NoError(t, err, "first handleFallThrough")
	require.Equal(t, []string{"/first"}, gotPaths)

	// Dirty the pooled entry the way an aborted or pipelined
	// fall-through could: bytes read into the reader but never consumed
	// by a request must be dropped, not replayed into the next parse.
	dirty := acquireReader(&parser.fallReaderPool, &repeatReader{buf: []byte("STALE-UNREAD-BYTES")})
	_, _ = dirty.Discard(4)
	releaseReader(&parser.fallReaderPool, dirty)

	// The second fall-through checks out that same entry on another
	// connection: the stale buffered bytes must be gone.
	_, err = parser.handleFallThrough(&mockConn{}, req("/second"), nil)
	require.NoError(t, err, "second handleFallThrough")
	require.Equal(t, []string{"/first", "/second"}, gotPaths,
		"stale reader state must not corrupt the next fall-through")
}

// TestHandleFallThroughRaw_ServesOversizeHead pins the oversize path:
// the buffered prefix plus nothing more on the wire parses fully, the
// fallback handler runs, and the response always carries
// Connection: close (the fallback owns the read state afterwards, so the
// connection never continues).
func TestHandleFallThroughRaw_ServesOversizeHead(t *testing.T) {
	buffered := []byte("GET /big HTTP/1.1\r\nHost: example.com\r\nX-Oversize: " +
		strings.Repeat("x", 2048) + "\r\n\r\n")
	var called bool
	w := &bytes.Buffer{}
	handler := func(ctx *fasthttp.RequestCtx) {
		called = true
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	parser := New(nil, handler)

	parser.handleFallThroughRaw(&mockConn{r: bytes.NewReader(nil), w: w}, buffered)

	require.True(t, called, "fallback handler must run for the oversize request")
	assert.Contains(t, w.String(), "200 OK")
	assert.Contains(t, w.String(), "Connection: close")
}

// TestHandleFallThroughRaw_PoolReaderReset mirrors
// TestHandleFallThrough_PoolReaderReset for the 64 KiB oversize pool:
// stale buffered bytes must not leak into the next oversize parse.
func TestHandleFallThroughRaw_PoolReaderReset(t *testing.T) {
	var gotPaths []string
	handler := func(ctx *fasthttp.RequestCtx) {
		gotPaths = append(gotPaths, string(ctx.Path()))
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	parser := New(nil, handler)
	head := func(path string) []byte {
		return []byte("GET " + path + " HTTP/1.1\r\nHost: example.com\r\n\r\n")
	}

	parser.handleFallThroughRaw(&mockConn{}, head("/first"))

	dirty := acquireReader(&parser.oversizeReaderPool, &repeatReader{buf: []byte("STALE-UNREAD-BYTES")})
	_, _ = dirty.Discard(4)
	releaseReader(&parser.oversizeReaderPool, dirty)

	parser.handleFallThroughRaw(&mockConn{}, head("/second"))

	require.Equal(t, []string{"/first", "/second"}, gotPaths,
		"stale reader state must not corrupt the next oversize fall-through")
}

// TestAcquireReleaseReader pins the acquire/release helpers themselves:
// released readers are re-pointed at a non-blocking placeholder (reading
// returns EOF, never stale bytes), keep their buffer size, and a checked
// out reader starts empty regardless of what the previous holder left.
func TestAcquireReleaseReader(t *testing.T) {
	pool := sync.Pool{New: func() any { return bufio.NewReaderSize(eofReader{}, readBufferSize) }}

	br := acquireReader(&pool, &repeatReader{buf: []byte("SOMETHING")})
	_, _ = br.Discard(4)
	require.Equal(t, readBufferSize, br.Size(), "pooled reader buffer size")
	require.NotZero(t, br.Buffered(), "precondition: reader holds unread bytes")
	releaseReader(&pool, br)

	br = acquireReader(&pool, &repeatReader{buf: []byte("SOMETHING")})
	require.Zero(t, br.Buffered(), "checkout must reset buffered state")
	releaseReader(&pool, br)

	var eof eofReader
	n, err := eof.Read(make([]byte, 8))
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
}

type mockConn struct {
	r      io.Reader
	w      io.Writer
	mu     sync.Mutex
	closed bool
}

func (m *mockConn) Read(b []byte) (int, error) {
	if m.r != nil {
		return m.r.Read(b)
	}
	return 0, io.EOF
}

func (m *mockConn) Write(b []byte) (int, error) {
	if m.w != nil {
		return m.w.Write(b)
	}
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return mockAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return mockAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

type mockAddr struct{}

func (mockAddr) Network() string { return "mock" }
func (mockAddr) String() string  { return "mock-addr" }
