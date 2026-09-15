package h1parser

import (
	"bufio"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/valyala/fasthttp"
)

// pathEchoFastPath hits every path, echoing the path in the body, and
// misses paths starting with /miss (served by the fallback). The echo
// makes follower identity observable in the response body — a re-served
// earlier request answers with the wrong path.
type pathEchoFastPath struct{}

func (m *pathEchoFastPath) TryHit(req *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	if strings.HasPrefix(req.Path, "/miss") {
		return nil, false
	}
	body := "hit:" + req.Path
	head := "HTTP/1.1 200 OK\r\nContent-Length: " + strconv.Itoa(len(body)) +
		"\r\nContent-Type: text/plain\r\n"
	if req.ConnectionClose {
		head += "Connection: close\r\n"
	}
	resp := &api.FastPathResponse{
		BuffersArr: [3][]byte{
			[]byte(head),
			[]byte("\r\n"),
			[]byte(body),
		},
		CloseConn: req.ConnectionClose,
	}
	resp.Buffers = resp.BuffersArr[:]
	return resp, true
}

func (m *pathEchoFastPath) Release(_ *api.FastPathResponse) {}

// missBody echoes the fallback handler's answer for a miss.
func missBody(path string) string { return "miss:" + path }

// readPipelinedResponse reads one HTTP/1.1 response from reader through
// the end of its body (Content-Length). Pipelined responses may land in
// one TCP segment, so the caller must share one bufio.Reader across the
// batch.
func readPipelinedResponse(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	var resp []byte
	contentLength := -1
	for {
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		resp = append(resp, line...)
		if contentLength < 0 && strings.HasPrefix(line, "Content-Length: ") {
			n, cerr := strconv.Atoi(strings.TrimRight(line[len("Content-Length: "):], "\r\n"))
			require.NoError(t, cerr)
			contentLength = n
		}
		if line == "\r\n" {
			if contentLength < 0 {
				t.Fatalf("response had no parseable Content-Length: %q", string(resp))
			}
			body := make([]byte, contentLength)
			_, err = reader.Read(body)
			require.NoError(t, err)
			return string(append(resp, body...))
		}
	}
}

// TestServe_PipelinedBatchFollowerIdentity is the regression test for
// the pipelining identity bug: a hit followed by pipelined requests must
// serve every follower as itself. Previously the fallback re-served the
// already-served hit (its rebuilt head was re-fed with the follower's
// bytes as pipeline) and swallowed every request past the first into
// its read buffer, stalling the follower until the idle deadline killed
// the connection.
func TestServe_PipelinedBatchFollowerIdentity(t *testing.T) {
	t.Parallel()
	p := New(&pathEchoFastPath{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(missBody(string(ctx.Path())))
		ctx.SetStatusCode(200)
	})

	client, server := dialTCPPair(t)
	defer client.Close()

	pipelined := "GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /third HTTP/1.1\r\nHost: localhost\r\n\r\n"
	go func() { _, _ = client.Write([]byte(pipelined)) }()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp1 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp1, "hit:/hit", "first response must answer /hit, got: %q", resp1)
	resp2 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp2, missBody("/miss"),
		"second response must answer /miss — not the hit re-served, got: %q", resp2)
	resp3 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp3, "hit:/third",
		"third response must answer /third — previously stranded until idle close, got: %q", resp3)

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after client close")
	}
}

// TestServe_PipelinedMissFollowerIdentity pins the miss-first ordering:
// a pipelined miss followed by a hit must serve both, in order, on the
// same connection.
func TestServe_PipelinedMissFollowerIdentity(t *testing.T) {
	t.Parallel()
	p := New(&pathEchoFastPath{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString(missBody(string(ctx.Path())))
		ctx.SetStatusCode(200)
	})

	client, server := dialTCPPair(t)
	defer client.Close()

	pipelined := "GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n"
	go func() { _, _ = client.Write([]byte(pipelined)) }()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp1 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp1, missBody("/miss"), "first response must answer /miss, got: %q", resp1)
	resp2 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp2, "hit:/hit", "second response must answer /hit, got: %q", resp2)

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after client close")
	}
}

// TestServe_PipelinedFollowerBodyServed asserts a POST follower with a
// body is served with its body intact across the boundary — the
// leftover contract must not mangle framing on body-carrying requests.
func TestServe_PipelinedFollowerBodyServed(t *testing.T) {
	t.Parallel()
	var bodies []string
	var mu sync.Mutex
	p := New(nil, func(ctx *fasthttp.RequestCtx) {
		mu.Lock()
		bodies = append(bodies, string(ctx.Path())+"="+string(ctx.PostBody()))
		mu.Unlock()
		ctx.SetStatusCode(200)
	})

	client, server := dialTCPPair(t)
	defer client.Close()

	pipelined := "POST /first HTTP/1.1\r\nHost: localhost\r\nContent-Length: 3\r\n\r\nabc" +
		"POST /second HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\n\r\ndefg"
	go func() { _, _ = client.Write([]byte(pipelined)) }()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp1 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp1, "200", "first POST must be served, got: %q", resp1)
	resp2 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp2, "200", "second POST must be served, got: %q", resp2)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(bodies) == 2
	}, 5*time.Second, 5*time.Millisecond, "both requests must reach the handler")
	mu.Lock()
	assert.Equal(t, "/first=abc", bodies[0])
	assert.Equal(t, "/second=defg", bodies[1])
	mu.Unlock()

	_ = client.Close()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after client close")
	}
}

// TestHandleFallThrough_ReturnsPipelinedLeftover is the deterministic
// core of the reactor-return gate: after serving a request whose
// follower bytes were already consumed into the read buffer,
// handleFallThrough must hand those bytes back. Serve defers the
// reactor return while such a leftover exists (returning mid-batch
// would orphan bytes this goroutine holds); with a nil leftover the
// socket is the only unread source and returning is safe.
func TestHandleFallThrough_ReturnsPipelinedLeftover(t *testing.T) {
	t.Parallel()
	p := New(nil, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss:" + string(ctx.Path()))
		ctx.SetStatusCode(200)
	})

	client, server := dialTCPPair(t)
	defer client.Close()

	follower := "GET /follower HTTP/1.1\r\nHost: localhost\r\n\r\n"
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/first",
		HTTPVersion: "HTTP/1.1",
		Host:        "localhost",
		NHeaders:    0,
	}

	served := make(chan struct{})
	go func() {
		closeConn, leftover, err := p.handleFallThrough(server, req, []byte(follower))
		require.NoError(t, err)
		require.False(t, closeConn)
		// The leftover must be a copy the caller can keep: it may not
		// alias the read buffer's internals.
		require.Equal(t, follower, string(leftover),
			"the follower's bytes must be handed back verbatim")
		close(served)
	}()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp := readPipelinedResponse(t, reader)
	assert.Contains(t, resp, "miss:/first")

	select {
	case <-served:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return")
	}
}

// TestServe_PipelinedConnectionCloseStopsBatch asserts Connection:
// close semantics still terminate the batch: the closed request's
// response is served with the close header and the follower bytes are
// discarded with the connection (RFC 9110 §9.6).
func TestServe_PipelinedConnectionCloseStopsBatch(t *testing.T) {
	t.Parallel()
	var handlerCalls int32
	p := New(&pathEchoFastPath{}, func(ctx *fasthttp.RequestCtx) {
		atomic.AddInt32(&handlerCalls, 1)
		ctx.SetStatusCode(200)
	})

	client, server := dialTCPPair(t)
	defer client.Close()

	pipelined := "GET /hit HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n" +
		"GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n"
	go func() { _, _ = client.Write([]byte(pipelined)) }()

	done := make(chan error, 1)
	go func() { done <- p.Serve(server) }()

	client.SetReadDeadline(time.Now().Add(5 * time.Second))
	reader := bufio.NewReader(client)
	resp1 := readPipelinedResponse(t, reader)
	assert.Contains(t, resp1, "hit:/hit", "the close hit must be served")
	assert.Contains(t, resp1, "Connection: close", "the hit response must carry the close header")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not exit after Connection: close response")
	}
	assert.Equal(t, int32(0), atomic.LoadInt32(&handlerCalls),
		"the follower behind a Connection: close request must be discarded, not served")
}
