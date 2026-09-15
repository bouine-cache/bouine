//go:build linux

package h1parser

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

// startPathEchoReactorListener boots the reactor with a fast path that
// hits everything except paths starting with /miss, echoing each
// request's path in the response body. The echo makes follower identity
// observable end-to-end through the reactor's handoff, blocking parser,
// and return path.
func startPathEchoReactorListener(t *testing.T) (addr string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	p := New(&pathEchoFastPath{}, func(ctx *fasthttp.RequestCtx) {
		ctx.SetBodyString("miss:" + string(ctx.Path()))
		ctx.SetStatusCode(200)
	})

	loop, ok := NewReactorLoop(p, ln)
	require.True(t, ok, "epoll reactor must be available on Linux")
	go loop.Run()
	t.Cleanup(loop.Close)
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String()
}

// TestEpollReactor_PipelinedMixedBatchEndToEnd is the full mixed-traffic
// pipeline scenario over real TCP and the real reactor: a batch of
// hit + miss + hit is written in one call. Every response must answer
// its own request — the miss hands off to the blocking parser (with the
// follower's bytes replayed), the blocking parser must not strand the
// follower inside its read buffer, and after the return the reactor
// serves the rest inline. This pins the interaction of W1's leftover
// contract with PR 605's return-to-reactor path: a mid-batch reactor
// return would orphan the follower's bytes on the blocking goroutine.
func TestEpollReactor_PipelinedMixedBatchEndToEnd(t *testing.T) {
	addr := startPathEchoReactorListener(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	pipelined := "GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /third HTTP/1.1\r\nHost: localhost\r\n\r\n"
	_, err = conn.Write([]byte(pipelined))
	require.NoError(t, err)

	reader := bufio.NewReader(conn)
	resp1 := readOneResponseReader(t, reader)
	assert.Contains(t, resp1, "hit:/hit", "first response must answer /hit, got: %q", resp1)
	resp2 := readOneResponseReader(t, reader)
	assert.Contains(t, resp2, "miss:/miss", "second response must answer /miss, got: %q", resp2)
	resp3 := readOneResponseReader(t, reader)
	assert.Contains(t, resp3, "hit:/third",
		"third response must answer /third — the follower behind the miss must not be stranded, got: %q", resp3)
}

// TestEpollReactor_ReturnDeferredUntilBatchDrained asserts the
// return-gate on the real transport: while the blocking parser holds
// buffered follower bytes it must NOT return the connection to the
// reactor (the reactor would read the empty socket while the follower
// lives in blocking-goroutine memory). The return fires once the batch
// is drained — here after the third response.
func TestEpollReactor_ReturnDeferredUntilBatchDrained(t *testing.T) {
	addr := startPathEchoReactorListener(t)

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))

	pipelined := "GET /miss HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /hit HTTP/1.1\r\nHost: localhost\r\n\r\n" +
		"GET /miss2 HTTP/1.1\r\nHost: localhost\r\n\r\n"
	_, err = conn.Write([]byte(pipelined))
	require.NoError(t, err)

	reader := bufio.NewReader(conn)
	resp1 := readOneResponseReader(t, reader)
	assert.Contains(t, resp1, "miss:/miss", "first response must answer /miss, got: %q", resp1)
	// The follower hit is buffered in the blocking parser when the
	// miss response completes. The reactor may serve it only via a
	// mid-batch return — which is exactly what must NOT happen while
	// bytes are buffered. Either the blocking parser serves it inline
	// (no return) or the reactor serves it after a return that carried
	// the follower bytes; both are correct, both answer /hit.
	resp2 := readOneResponseReader(t, reader)
	assert.Contains(t, resp2, "hit:/hit", "second response must answer /hit, got: %q", resp2)
	resp3 := readOneResponseReader(t, reader)
	assert.Contains(t, resp3, "miss:/miss2", "third response must answer /miss2, got: %q", resp3)
}
