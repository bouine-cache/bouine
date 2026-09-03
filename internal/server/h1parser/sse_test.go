package h1parser

import (
	"bufio"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// writeDeadlineCountingConn counts real (non-zero) write-deadline arms on
// the underlying connection — including the re-arms performed by
// idleWriteConn. Zero-time clears (fall-through entry) do not count.
type writeDeadlineCountingConn struct {
	net.Conn
	arms atomic.Int64
}

func (c *writeDeadlineCountingConn) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() {
		c.arms.Add(1)
	}
	return c.Conn.SetWriteDeadline(t)
}

// sseRawReq builds a minimal GET request that announces SSE intent.
func sseRawReq() *api.RawRequest {
	return &api.RawRequest{
		Method:      "GET",
		Path:        "/feed",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/event-stream"},
			{Key: "Connection", Value: "close"},
		},
		NHeaders: 3,
	}
}

// TestFallThrough_StreamedResponseRearmsWriteDeadline pins the streamed
// write-deadline contract on the H1 fall-through path: a response with a
// body stream (SetBodyStreamWriter — SSE and unbuffered passthrough) has
// its write deadline re-armed per Write instead of one absolute arm, so a
// long-lived stream is not cut mid-flight while a client that stops
// reading is still dropped after one idle budget.
func TestFallThrough_StreamedResponseRearmsWriteDeadline(t *testing.T) {
	t.Parallel()

	const events = 3
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/event-stream")
		ctx.SetStatusCode(200)
		ctx.Response.SetBodyStreamWriter(func(w *bufio.Writer) {
			for range events {
				_, _ = w.Write([]byte("data: x\n\n"))
				_ = w.Flush()
			}
		})
	}

	parser := New(nil, handler, WithNowFunc(time.Now))
	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	counting := &writeDeadlineCountingConn{Conn: serverConn}
	done := make(chan struct{}, 1)
	go func() {
		_, _ = parser.handleFallThrough(counting, sseRawReq(), nil)
		_ = serverConn.Close()
		done <- struct{}{}
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(bufio.NewReader(clientConn)))
	assert.Equal(t, 200, resp.StatusCode())
	assert.Equal(t, "data: x\n\ndata: x\n\ndata: x\n\n", string(resp.Body()))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	// One absolute arm (before WriteTo) plus one re-arm per body write:
	// the terminal chunk also writes, so strictly more arms than events.
	assert.GreaterOrEqual(t, counting.arms.Load(), int64(events+1),
		"a streamed response must re-arm the write deadline per Write")
}

// TestFallThrough_BufferedResponseKeepsSingleArm pins that ordinary
// (fully-buffered) fall-through responses keep the one-shot absolute
// write deadline — the safety net is unchanged for non-streaming traffic.
func TestFallThrough_BufferedResponseKeepsSingleArm(t *testing.T) {
	t.Parallel()

	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("hello")
	}

	parser := New(nil, handler, WithNowFunc(time.Now))
	clientConn, serverConn := dialTCPPair(t)
	defer clientConn.Close()

	counting := &writeDeadlineCountingConn{Conn: serverConn}
	done := make(chan struct{}, 1)
	go func() {
		_, _ = parser.handleFallThrough(counting, sseRawReq(), nil)
		_ = serverConn.Close()
		done <- struct{}{}
	}()

	clientConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := &fasthttp.Response{}
	require.NoError(t, resp.Read(bufio.NewReader(clientConn)))
	assert.Equal(t, "hello", string(resp.Body()))

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleFallThrough did not return within 5s")
	}

	assert.Equal(t, int64(1), counting.arms.Load(),
		"a buffered response must arm the write deadline exactly once")
}
