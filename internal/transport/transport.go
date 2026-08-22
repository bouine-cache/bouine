package transport

import (
	"context"
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
