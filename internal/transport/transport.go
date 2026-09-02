package transport

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/valyala/fasthttp"
)

// Client wraps fasthttp.Client for outbound HTTP (origin, peer, health,
// broadcast). It adds context-aware cancellation via [Client.Do].
//
// Unstable.
type Client struct {
	*fasthttp.Client
}

// NewClient creates a transport Client from a raw fasthttp.Client.
// The caller is responsible for configuring TLS, connection pool
// limits, and timeouts on the fasthttp.Client before wrapping it.
func NewClient(fc *fasthttp.Client) *Client {
	if fc == nil {
		fc = &fasthttp.Client{}
	}
	return &Client{Client: fc}
}

// defaultDoTimeout is used when the context has no deadline. It
// matches the cache handler's defaultFetchTimeout (60s). Callers
// that need a different timeout should pass a context with a
// deadline.
const defaultDoTimeout = 60 * time.Second

// Do performs an HTTP request with context-aware cancellation.
// req and resp are fasthttp pooled objects — the caller is responsible
// for acquiring and releasing them.
//
// If ctx has a deadline, Do uses fasthttp.Client.DoDeadline to avoid
// goroutine overhead. If ctx has no deadline, Do uses DoTimeout with
// a 60s default — this avoids spawning a goroutine that would race
// with the caller's pool release of req/resp.
//
// If ctx is already done, Do returns ctx.Err() immediately without
// sending the request.
//
// Callers that need cancellation without a deadline should pass a
// context with an appropriate deadline. AGENTS.md §11 requires
// cancellation within 10ms, which implies a deadline exists.
func (c *Client) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if deadline, ok := ctx.Deadline(); ok {
		return c.DoDeadline(req, resp, deadline)
	}

	return c.DoTimeout(req, resp, defaultDoTimeout)
}

// DoTimeout performs an HTTP request with a fixed timeout. This is a
// convenience wrapper around fasthttp.Client.DoTimeout for callers
// that need a per-call timeout independent of context.
func (c *Client) DoTimeout(req *fasthttp.Request, resp *fasthttp.Response, timeout time.Duration) error {
	return c.Client.DoTimeout(req, resp, timeout)
}

// pipelineStoppedMsg is the error string of fasthttp's unexported
// errPipelineConnStopped ("pipeline connection has been stopped").
// The worker assigns it to pending work when the connection dies
// between requests; fasthttp does not export the sentinel, so the
// classification matches on the message.
const pipelineStoppedMsg = "pipeline connection has been stopped"

// pipelineMaxAttempts is the total number of tries (first attempt +
// retries) PipelineDo makes on a stale pipelined connection. Two
// attempts absorb the EOF-on-reap case; the third absorbs the
// "pipeline connection has been stopped" case where the worker is
// still re-dialing. Each retry re-dials synchronously inside the
// worker, so the extra attempts cost microseconds, not round trips.
const pipelineMaxAttempts = 3

// pipelineRetryDelay separates retries so the pipeline worker can
// observe the dead connection and re-dial between attempts. Without a
// delay, back-to-back calls can land on the same half-dead worker and
// return "pipeline connection has been stopped" (measured: 3 attempts
// with a 5ms gap survived 100/100 idle-reap POSTs with a p-max of
// ~1.5ms; without the gap, ~40% of calls failed).
const pipelineRetryDelay = 5 * time.Millisecond

// PipelineDo performs an HTTP request via a PipelineClient with
// context-aware cancellation. It mirrors Client.Do but for
// fasthttp.PipelineClient, which pipelines requests over a limited
// set of connections per host.
//
// If ctx has a deadline, PipelineDo uses DoDeadline. If ctx has no
// deadline, PipelineDo uses DoTimeout with a 60s default. If ctx is
// already done, PipelineDo returns ctx.Err() immediately.
//
// Unlike fasthttp.Client, PipelineClient does not retry requests that
// fail on a pooled connection the server has just closed: the first
// call after an idle gap surfaces io.EOF (server closed before the
// response), EPIPE (write into a closed socket), or
// "pipeline connection has been stopped" (the worker tore down the
// connection while the request was queued). fasthttp also consumes
// the request body on every attempt, so PipelineDo snapshots the body
// before the first attempt and restores it before each retry.
// Requests sent through PipelineDo must therefore be idempotent
// (peer-fetch and peer-put RPCs are). Retryable errors are exactly
// the stale-connection classes above; timeouts are never retried
// because the request may have been processed.
func PipelineDo(ctx context.Context, c *fasthttp.PipelineClient, req *fasthttp.Request, resp *fasthttp.Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	snapshot := append([]byte(nil), req.Body()...)
	var err error
	for range pipelineMaxAttempts {
		err = pipelineDoRaw(ctx, c, req, resp)
		if err == nil || !isStalePipelineConnErr(err) {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// fasthttp consumed the request body on the failed attempt
		// (swapRequestBody moves it into internal work and
		// releasePipelineWork resets it). Restore before retrying.
		req.ResetBody()
		req.SetBodyRaw(snapshot)
		time.Sleep(pipelineRetryDelay)
	}
	return err
}

func pipelineDoRaw(ctx context.Context, c *fasthttp.PipelineClient, req *fasthttp.Request, resp *fasthttp.Response) error {
	if deadline, ok := ctx.Deadline(); ok {
		return c.DoDeadline(req, resp, deadline)
	}
	return c.DoTimeout(req, resp, defaultDoTimeout)
}

// isStalePipelineConnErr reports whether err indicates the pipelined
// connection was closed by the peer while idle. Three classes:
// io.EOF (server closed before the response arrived), write-EPIPE
// (request written into a socket the peer had already closed), and
// fasthttp's "pipeline connection has been stopped" (the worker tore
// down the connection while the request was queued). All three mean
// the request was never processed, so a retry is safe for idempotent
// RPCs. Timeout errors are NOT retryable here: the request may have
// been processed and the response lost.
func isStalePipelineConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	var se *os.SyscallError
	if errors.As(err, &se) && se.Syscall == "write" {
		return errors.Is(se.Err, syscall.EPIPE)
	}
	return strings.Contains(err.Error(), pipelineStoppedMsg)
}

// IsStalePipelineConnErrForTest exposes the stale-connection error
// classification to tests in the transport_test package.
func IsStalePipelineConnErrForTest(err error) bool {
	return isStalePipelineConnErr(err)
}

// Server wraps fasthttp.Server for inbound HTTP (data plane, admin,
// cluster). It is a thin wrapper that exists to provide a consistent
// type across the codebase and to add logging integration in future
// phases.
//
// Unstable.
type Server struct {
	*fasthttp.Server
}

// NewServer creates a transport Server from a raw fasthttp.Server.
func NewServer(fs *fasthttp.Server) *Server {
	return &Server{Server: fs}
}
