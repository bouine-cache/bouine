package transport_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttputil"

	"github.com/bouine-cache/bouine/internal/transport"
)

func TestClient_Do_WithDeadline(t *testing.T) {
	ln := fasthttputil.NewInmemoryListener()
	defer func() { _ = ln.Close() }()

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			ctx.SetBodyString("hello")
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
	}
	tc := transport.NewClient(client)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()
	req.SetRequestURI("http://example.com/test")
	req.Header.SetMethod("GET")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := tc.Do(ctx, req, resp)
	require.NoError(t, err)
	require.Equal(t, fasthttp.StatusOK, resp.StatusCode())
	require.Equal(t, "hello", string(resp.Body()))
}

func TestClient_Do_CancelledContext(t *testing.T) {
	client := &fasthttp.Client{}
	tc := transport.NewClient(client)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := tc.Do(ctx, req, resp)
	require.Error(t, err)
	require.ErrorIs(t, err, context.Canceled)
}

func TestClient_Do_NoDeadline_NoCancel(t *testing.T) {
	ln := fasthttputil.NewInmemoryListener()
	defer func() { _ = ln.Close() }()

	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			ctx.SetBodyString("nodeadline")
		},
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	client := &fasthttp.Client{
		Dial: func(addr string) (net.Conn, error) {
			return ln.Dial()
		},
	}
	tc := transport.NewClient(client)

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()
	req.SetRequestURI("http://example.com/test")

	err := tc.Do(context.Background(), req, resp)
	require.NoError(t, err)
	require.Equal(t, "nodeadline", string(resp.Body()))
}

func TestNewServer(t *testing.T) {
	fs := &fasthttp.Server{}
	ts := transport.NewServer(fs)
	require.NotNil(t, ts)
	require.NotNil(t, ts.Server)
}

// TestPipelineDo_NoRetryOnTimeout pins that timeouts are never retried:
// a timed-out request may have been processed by the peer, and a retry
// could duplicate a non-idempotent side effect.
func TestPipelineDo_NoRetryOnTimeout(t *testing.T) {
	var hits atomic.Int64
	ln := fasthttputil.NewInmemoryListener()
	srv := &fasthttp.Server{
		Handler: func(ctx *fasthttp.RequestCtx) {
			hits.Add(1)
			time.Sleep(50 * time.Millisecond)
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
	}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Shutdown() }()

	pc := &fasthttp.PipelineClient{
		Addr: "example.com:80",
		Dial: func(addr string) (net.Conn, error) { return ln.Dial() },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	req := fasthttp.AcquireRequest()
	req.SetRequestURI("http://example.com/slow")
	resp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}()
	err := transport.PipelineDo(ctx, pc, req, resp)
	require.Error(t, err)
	require.Equal(t, int64(1), hits.Load(), "timeout must not be retried")
}
