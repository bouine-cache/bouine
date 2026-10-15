package cache

import (
	"context"
	"errors"
	"os"
	"strings"

	"github.com/valyala/fasthttp"
)

// isUpstreamTimeout reports whether an origin fetch error is a timeout.
// Covered, in order: fasthttp's deadline sentinel (DoTimeout / DoDeadline
// return it when the per-request fetch deadline fires), the kernel/context
// deadline sentinels (os.ErrDeadlineExceeded covers dial and read timeouts,
// context.DeadlineExceeded the ctx-bounded background paths), and — last,
// for wrapped transport errors that carry neither sentinel — a message
// naming the timeout, the same strings origin.classifyConnError keys the
// bouine_origin_connection_errors_total "timeout" reason label on. The wire
// status and the metric label must agree on what a "timeout" is.
//
// ErrFetchShed is explicitly not a timeout: the fetch was never dispatched,
// so its message ("origin fetch queue wait timeout") must not leak into the
// classification, whatever order the caller checks it in.
func isUpstreamTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, fasthttp.ErrTimeout) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	if errors.Is(err, ErrFetchShed) {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "timeout") || strings.Contains(s, "deadline")
}

// writeUpstreamError writes the client-facing response for a failed origin
// fetch: 504 Gateway Timeout (RFC 9110 §15.6.5) when the fetch timed out —
// bouine, acting as a gateway, did not receive a timely response from the
// upstream — and 502 Bad Gateway for every other upstream failure (refused,
// reset, DNS, oversized). The shed 503 and the no-client-no-upstream 502
// branches elsewhere never route through here: no fetch was dispatched.
// ctx.Error resets the response (fasthttp RequestCtx.Error), so callers
// write their attribution headers after this returns.
func writeUpstreamError(ctx *fasthttp.RequestCtx, err error) {
	if isUpstreamTimeout(err) {
		ctx.Error("upstream timeout", fasthttp.StatusGatewayTimeout)
		return
	}
	ctx.Error("upstream error", fasthttp.StatusBadGateway)
}
