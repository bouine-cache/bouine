package transport_test

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"syscall"
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

// TestPipelineDo_RetriesStaleConnection proves the retry in PipelineDo:
// the server closes the first connection (idle reap) so the next
// request fails with EOF, and the retried request on the fresh
// connection succeeds. Mirrors the preprod
// "error in PipelineClient: EOF" scenario: the peer pod never dies,
// only the idle connection is reaped. The retry gets its own deadline
// so a peer that stays dead cannot stretch the call past the caller's
// budget.
func TestPipelineDo_RetriesStaleConnection(t *testing.T) {
	var hits atomic.Int64
	handler := func(body string) fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			hits.Add(1)
			ctx.SetStatusCode(fasthttp.StatusOK)
			ctx.SetBodyString(body)
		}
	}
	// srv.Serve closes the listener on Shutdown, so each restarted
	// server gets a fresh one. The dial closure reads the pointer, so
	// re-dials land on the current listener and emulate a peer that
	// keeps accepting connections across idle-connection reaps.
	var lnPtr atomic.Pointer[fasthttputil.InmemoryListener]
	lnPtr.Store(fasthttputil.NewInmemoryListener())
	pc := &fasthttp.PipelineClient{
		Addr: "example.com:80",
		Dial: func(addr string) (net.Conn, error) { return lnPtr.Load().Dial() },
	}

	var srv *fasthttp.Server
	start := func(body string) {
		srv = &fasthttp.Server{Handler: handler(body)}
		lnPtr.Store(fasthttputil.NewInmemoryListener())
		go func() { _ = srv.Serve(lnPtr.Load()) }()
	}
	shutdown := func() {
		if srv != nil {
			_ = srv.Shutdown()
		}
	}
	defer shutdown()

	start("ok")
	first := fasthttp.AcquireRequest()
	first.SetRequestURI("http://example.com/warm")
	first.Header.SetMethod(fasthttp.MethodGet)
	firstResp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(first)
		fasthttp.ReleaseResponse(firstResp)
	}()
	require.NoError(t, transport.PipelineDo(context.Background(), pc, first, firstResp))

	// Stop the server so the client's pooled pipelined connection goes
	// stale, like an admin IdleTimeout reap. The in-memory pipe then
	// delivers EOF on the client's next read/write. A fresh server on
	// a fresh listener emulates the peer accepting new connections
	// again; the dial closure always dials the current listener.
	require.NoError(t, srv.Shutdown())
	start("ok2")

	stale := fasthttp.AcquireRequest()
	stale.SetRequestURI("http://example.com/stale")
	stale.Header.SetMethod(fasthttp.MethodPost)
	stale.SetBodyString("payload")
	staleResp := fasthttp.AcquireResponse()
	defer func() {
		fasthttp.ReleaseRequest(stale)
		fasthttp.ReleaseResponse(staleResp)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, transport.PipelineDo(ctx, pc, stale, staleResp))
	require.Equal(t, fasthttp.StatusOK, staleResp.StatusCode())
	require.Equal(t, "ok2", string(staleResp.Body()))
	require.Equal(t, int64(2), hits.Load(), "retry should have served the stale request exactly once more")
}

// TestPipelineDo_RestoresBodyOnRetry pins the body-restore behavior:
// fasthttp consumes the request body on every attempt, so the retry
// must re-deliver it. A POST whose body is dropped would corrupt
// peer-fetch/peer-put RPCs silently.
func TestPipelineDo_RestoresBodyOnRetry(t *testing.T) {
	var bodies atomic.Int64
	handler := func() fasthttp.RequestHandler {
		return func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Request.Body()) == "0123456789ABCDEF" {
				bodies.Add(1)
			}
			ctx.SetStatusCode(fasthttp.StatusOK)
		}
	}
	// srv.Serve closes the listener on Shutdown, so each restarted
	// server gets a fresh one. The dial closure reads the pointer, so
	// re-dials land on the current listener.
	var lnPtr atomic.Pointer[fasthttputil.InmemoryListener]
	lnPtr.Store(fasthttputil.NewInmemoryListener())
	pc := &fasthttp.PipelineClient{
		Addr: "example.com:80",
		Dial: func(addr string) (net.Conn, error) { return lnPtr.Load().Dial() },
	}

	var srv *fasthttp.Server
	start := func() {
		srv = &fasthttp.Server{Handler: handler()}
		lnPtr.Store(fasthttputil.NewInmemoryListener())
		go func() { _ = srv.Serve(lnPtr.Load()) }()
	}
	shutdown := func() {
		if srv != nil {
			_ = srv.Shutdown()
		}
	}
	defer shutdown()

	start()
	warm := fasthttp.AcquireRequest()
	warm.SetRequestURI("http://example.com/warm")
	warm.Header.SetMethod(fasthttp.MethodPost)
	warm.SetBodyString("warm")
	require.NoError(t, transport.PipelineDo(context.Background(), pc, warm, fasthttp.AcquireResponse()))

	require.NoError(t, srv.Shutdown())
	start()

	stale := fasthttp.AcquireRequest()
	stale.SetRequestURI("http://example.com/stale")
	stale.Header.SetMethod(fasthttp.MethodPost)
	stale.SetBodyString("0123456789ABCDEF")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	require.NoError(t, transport.PipelineDo(ctx, pc, stale, fasthttp.AcquireResponse()))
	require.Equal(t, int64(1), bodies.Load(), "server must see the POST body after the retry")
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

// TestIsStalePipelineConnErr_Classification pins the error classes the
// retry treats as stale-connection: EOF, write-EPIPE, and the unexported
// fasthttp "pipeline connection has been stopped" sentinel are
// retryable; timeouts, read resets, and plain errors are not.
func TestIsStalePipelineConnErr_Classification(t *testing.T) {
	t.Parallel()

	require.True(t, transport.IsStalePipelineConnErrForTest(io.EOF))
	require.True(t, transport.IsStalePipelineConnErrForTest(io.ErrUnexpectedEOF))
	require.True(t, transport.IsStalePipelineConnErrForTest(&os.SyscallError{
		Syscall: "write",
		Err:     syscall.EPIPE,
	}))
	require.True(t, transport.IsStalePipelineConnErrForTest(errors.New("pipeline connection has been stopped")))
	require.False(t, transport.IsStalePipelineConnErrForTest(nil))
	require.False(t, transport.IsStalePipelineConnErrForTest(os.ErrDeadlineExceeded))
	require.False(t, transport.IsStalePipelineConnErrForTest(&os.SyscallError{
		Syscall: "read",
		Err:     syscall.ECONNRESET,
	}))
	require.False(t, transport.IsStalePipelineConnErrForTest(errors.New("boom")))
}
