package h1parser

import (
	"bufio"
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// multiPathFastPath is a mock FastPathHandler that returns different
// responses depending on the request path. This lets us test that the
// pooled RawRequest and pooled FastPathResponse never mix content
// across keep-alive requests on the same connection.
type multiPathFastPath struct {
	mu     sync.Mutex
	bodies map[string]string
}

func newMultiPathFastPath() *multiPathFastPath {
	return &multiPathFastPath{bodies: make(map[string]string)}
}

func (m *multiPathFastPath) set(path, body string) {
	m.mu.Lock()
	m.bodies[path] = body
	m.mu.Unlock()
}

func (m *multiPathFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	m.mu.Lock()
	body := m.bodies[req.Path]
	m.mu.Unlock()
	if body == "" {
		return nil, false
	}
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte("HTTP/1.1 200 OK\r\n"),
			[]byte("Content-Length: " + itoa(len(body)) + "\r\nContent-Type: text/plain\r\n\r\n"),
			[]byte(body),
		},
	}
	resp.Buffers = resp.BuffersArr[:]
	resp.StatusCode = 200
	resp.BytesOut = len(body)
	return resp, true
}

func (m *multiPathFastPath) Release(_ *api.FastPathResponse) {}

// itoa is a minimal int-to-string to avoid strconv import in test.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for n > 0 {
		pos--
		buf[pos] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[pos:])
}

// readResponse reads one HTTP response from reader and returns the
// status code and body.
func readResponse(t *testing.T, reader *bufio.Reader) (int, []byte) {
	t.Helper()
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(reader))
	return resp.StatusCode(), resp.Body()
}

// TestKeepAlive_TwoHits_DifferentContent verifies that two consecutive
// fast-path hits on the same connection return the correct body for
// each request — not a stale body from the previous request.
func TestKeepAlive_TwoHits_DifferentContent(t *testing.T) {
	t.Parallel()
	fp := newMultiPathFastPath()
	fp.set("/first", "body-first-AAAA")
	fp.set("/second", "body-second-BBBB")
	p := New(fp, noopHandler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		defer server.Close()
		_ = p.Serve(server)
	}()

	reader := bufio.NewReader(client)

	// Request 1.
	_, _ = client.Write([]byte("GET /first HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	status1, body1 := readResponse(t, reader)
	assert.Equal(t, 200, status1)
	assert.Equal(t, "body-first-AAAA", string(body1), "first response must contain first body")

	// Request 2 on the same connection.
	_, _ = client.Write([]byte("GET /second HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	status2, body2 := readResponse(t, reader)
	assert.Equal(t, 200, status2)
	assert.Equal(t, "body-second-BBBB", string(body2), "second response must contain second body")
	assert.NotContains(t, string(body2), "first", "second response must not leak first body")
}

// TestKeepAlive_HitThenMissThenHit verifies content isolation across
// a hit → miss → hit sequence on the same connection. The miss path
// uses handleFallThrough which resets deadlines and uses a different
// code path; the third request must still get the right content.
func TestKeepAlive_HitThenMissThenHit_ContentIsolation(t *testing.T) {
	t.Parallel()

	var missBodies sync.Map
	handler := func(ctx *fasthttp.RequestCtx) {
		body := "origin-" + string(ctx.Request.URI().Path())
		missBodies.Store(string(ctx.Request.URI().Path()), body)
		ctx.SetBodyString(body)
		ctx.SetStatusCode(fasthttp.StatusOK)
	}

	fp := newMultiPathFastPath()
	fp.set("/hit1", "cached-hit-1")
	fp.set("/hit2", "cached-hit-2")
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		defer server.Close()
		_ = p.Serve(server)
	}()

	reader := bufio.NewReader(client)

	// Request 1: hit → "cached-hit-1".
	_, _ = client.Write([]byte("GET /hit1 HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	status1, body1 := readResponse(t, reader)
	assert.Equal(t, 200, status1)
	assert.Equal(t, "cached-hit-1", string(body1))

	// Request 2: miss → fallthrough to origin.
	_, _ = client.Write([]byte("GET /miss1 HTTP/1.1\r\nHost: localhost\r\n\r\n"))
	status2, body2 := readResponse(t, reader)
	assert.Equal(t, 200, status2)
	assert.Equal(t, "origin-/miss1", string(body2))

	// Request 3: hit → "cached-hit-2" (not "cached-hit-1" or "origin-/miss1").
	_, _ = client.Write([]byte("GET /hit2 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	status3, body3 := readResponse(t, reader)
	assert.Equal(t, 200, status3)
	assert.Equal(t, "cached-hit-2", string(body3), "third response must contain hit2 body")
	assert.NotContains(t, string(body3), "hit-1", "third response must not leak first hit body")
	assert.NotContains(t, string(body3), "origin", "third response must not leak miss body")
}

// TestKeepAlive_StaleHeadersNotReused verifies that headers from
// request #1 do not bleed into request #2 when the pooled RawRequest
// is reused. The parser resets scalar fields in getRawRequest but
// does not zero the [100]RawHeader array — we test that NHeaders=0
// prevents stale headers from being read.
func TestKeepAlive_StaleHeadersNotReused(t *testing.T) {
	t.Parallel()

	var requestCount int
	var mu sync.Mutex
	detectedPaths := make(map[int][]string)

	handler := func(ctx *fasthttp.RequestCtx) {
		mu.Lock()
		var hdrs []string
		for key, value := range ctx.Request.Header.All() {
			hdrs = append(hdrs, string(key)+": "+string(value))
		}
		detectedPaths[requestCount] = hdrs
		requestCount++
		mu.Unlock()
		ctx.SetBodyString("ok")
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	fp := &mockFastPathHandler{} // always misses → goes to fallthrough
	p := New(fp, handler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		defer server.Close()
		_ = p.Serve(server)
	}()

	reader := bufio.NewReader(client)

	// Request 1: has a custom header that request 2 does not.
	_, _ = client.Write([]byte("GET /req1 HTTP/1.1\r\nHost: localhost\r\nX-Trace-Id: abc123\r\n\r\n"))
	_, _ = readResponse(t, reader)

	// Request 2: no X-Trace-Id header.
	_, _ = client.Write([]byte("GET /req2 HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"))
	_, _ = readResponse(t, reader)

	// Verify that request 2 did not see X-Trace-Id from request 1.
	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(detectedPaths), 2, "both requests should have been served")

	req2Headers := detectedPaths[1]
	for _, h := range req2Headers {
		assert.NotContains(t, h, "X-Trace-Id", "stale header from request 1 leaked into request 2")
		assert.NotContains(t, h, "abc123", "stale header value from request 1 leaked into request 2")
	}
}

// TestKeepAlive_HitThenHit_SamePath verifies that two consecutive
// requests for the same cached path get the same body, not a corrupted
// or empty one from pool reuse.
func TestKeepAlive_HitThenHit_SamePath(t *testing.T) {
	t.Parallel()
	fp := newMultiPathFastPath()
	fp.set("/cached", "same-body-every-time")
	p := New(fp, noopHandler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		defer server.Close()
		_ = p.Serve(server)
	}()

	reader := bufio.NewReader(client)

	for i := range 3 {
		connHdr := ""
		if i == 2 {
			connHdr = "Connection: close\r\n"
		}
		_, _ = client.Write([]byte("GET /cached HTTP/1.1\r\nHost: localhost\r\n" + connHdr + "\r\n"))
		status, body := readResponse(t, reader)
		assert.Equal(t, 200, status)
		assert.Equal(t, "same-body-every-time", string(body), "iteration %d: body must be correct", i)
	}
}

// TestKeepAlive_ManyRequests_NoCorruption verifies content integrity
// across many keep-alive requests with alternating paths.
func TestKeepAlive_ManyRequests_NoCorruption(t *testing.T) {
	t.Parallel()
	fp := newMultiPathFastPath()
	paths := []string{"/a", "/b", "/c", "/d", "/e"}
	bodies := []string{"AAA", "BBB", "CCC", "DDD", "EEE"}
	for i, path := range paths {
		fp.set(path, bodies[i])
	}
	p := New(fp, noopHandler)

	client, server := dialTCPPair(t)
	defer client.Close()

	go func() {
		defer server.Close()
		_ = p.Serve(server)
	}()

	reader := bufio.NewReader(client)

	// Two full cycles, alternating paths.
	for cycle := range 2 {
		for i, path := range paths {
			connHdr := ""
			if cycle == 1 && i == len(paths)-1 {
				connHdr = "Connection: close\r\n"
			}
			_, _ = client.Write([]byte("GET " + path + " HTTP/1.1\r\nHost: localhost\r\n" + connHdr + "\r\n"))
			status, body := readResponse(t, reader)
			assert.Equal(t, 200, status)
			assert.Equal(t, bodies[i], string(body), "cycle %d path %s: wrong body", cycle, path)
		}
	}
}

// Ensure bytes is used.
var _ = bytes.Contains
