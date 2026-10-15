package cache

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// slowUpstream returns an origin handler that never answers within the
// 100ms fetch timeout tests arm: the client-visible path must classify the
// aborted fetch as a timeout, not a generic upstream failure.
func slowUpstream(d time.Duration) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		time.Sleep(d)
		_, _ = ctx.WriteString("late")
	}
}

// slowTimeoutHandler builds a Handler wired with a slow origin and a 100ms
// fetch timeout: every fetch aborts as a timeout.
func slowTimeoutHandler(t *testing.T, upstream fasthttp.RequestHandler) *Handler {
	t.Helper()
	return NewHandler(HandlerConfig{
		Upstream:     upstream,
		FastClient:   &testFastClient{handler: upstream},
		Store:        newTestStore(),
		FetchTimeout: 100 * time.Millisecond,
	})
}

func TestIsUpstreamTimeout(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"fasthttp deadline sentinel", fasthttp.ErrTimeout, true},
		{"wrapped fasthttp sentinel", fmt.Errorf("origin fetch: %w", fasthttp.ErrTimeout), true},
		{"context deadline", context.DeadlineExceeded, true},
		{"wrapped context deadline", fmt.Errorf("origin fetch: %w", context.DeadlineExceeded), true},
		{"dial io timeout", errors.New("dial tcp 10.0.0.1:80: i/o timeout"), true},
		{"fetchViaUpstream timeout text", fmt.Errorf("origin fetch: timeout after 100ms"), true},
		{"refused is not a timeout", errors.New("dial tcp 127.0.0.1:1: connect: connection refused"), false},
		{"reset is not a timeout", errors.New("read tcp 10.0.0.2:80->10.0.0.1:1: connection reset by peer"), false},
		{"generic origin down", errors.New("origin down"), false},
		{"shed is not a timeout despite its message", ErrFetchShed, false},
		{"wrapped shed", fmt.Errorf("origin fetch queue wait exceeded 1s: %w", ErrFetchShed), false},
		{"unshareable", ErrStreamUnshareable, false},
		{"abort", ErrAbortHandler, false},
		{"response too large", fmt.Errorf("upstream response exceeds 123 bytes"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, isUpstreamTimeout(tc.err))
		})
	}
}

// TestUpstreamTimeout_Bypass_504 pins the BYPASS-path contract: a timed-out
// upstream fetch surfaces as 504 Gateway Timeout (RFC 9110 §15.6.5) with
// the body "upstream timeout" and the X-Cache/X-Cache-Source attribution,
// while a non-timeout failure keeps 502 Bad Gateway on the same path.
func TestUpstreamTimeout_Bypass_504(t *testing.T) {
	t.Parallel()
	h := slowTimeoutHandler(t, slowUpstream(2*time.Second))

	ctx := testCtx("GET", "http://example.com/slow")
	ctx.Request.Header.Set(header.CacheControl, "no-store")
	serveRequest(h, ctx)

	require.Equal(t, 504, respCode(ctx))
	require.Equal(t, "upstream timeout", respBody(ctx))
	require.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	require.Equal(t, string(api.SourceOrigin), respHeader(ctx, header.XCacheSource))

	// Every other upstream failure keeps 502 on the same path.
	h.fastClient = &errorFastClient{}
	ctx2 := testCtx("GET", "http://example.com/refused")
	ctx2.Request.Header.Set(header.CacheControl, "no-store")
	serveRequest(h, ctx2)

	require.Equal(t, 502, respCode(ctx2))
	require.Equal(t, "BYPASS", respHeader(ctx2, header.XCache))
	require.Equal(t, string(api.SourceOrigin), respHeader(ctx2, header.XCacheSource))
}

// TestUpstreamTimeout_Revalidate_MustRevalidate_504 pins the revalidation
// path: with must-revalidate (RFC 9111 §5.2.2.2 forbids serving stale), a
// timed-out conditional fetch surfaces as 504, and the ctx.Error reset
// must not wipe the attribution headers.
func TestUpstreamTimeout_Revalidate_MustRevalidate_504(t *testing.T) {
	t.Parallel()
	var calls int
	upstream := func(ctx *fasthttp.RequestCtx) {
		calls++
		if calls == 1 {
			ctx.Response.Header.Set(header.CacheControl, "max-age=1, must-revalidate")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("fresh-body"))
			return
		}
		time.Sleep(2 * time.Second)
	}
	h := slowTimeoutHandler(t, upstream)

	seed := testCtx("GET", "http://example.com/must-revalidate-timeout")
	serveRequest(h, seed)
	require.Equal(t, 200, respCode(seed))

	time.Sleep(1500 * time.Millisecond)

	rr := testCtx("GET", "http://example.com/must-revalidate-timeout")
	serveRequest(h, rr)

	require.Equal(t, 504, respCode(rr))
	require.Equal(t, "MISS", respHeader(rr, header.XCache))
	require.Equal(t, string(api.SourceOrigin), respHeader(rr, header.XCacheSource))
}

// TestUpstreamTimeout_InvalidateAndProxy_504 pins the invalidating-method
// path (POST): a timed-out upstream fetch surfaces as 504 with the
// attribution headers written after ctx.Error, so they survive the
// fasthttp response reset.
func TestUpstreamTimeout_InvalidateAndProxy_504(t *testing.T) {
	t.Parallel()
	h := slowTimeoutHandler(t, slowUpstream(2*time.Second))

	ctx := testCtxWithBody("POST", "http://example.com/resource", []byte("write me"))
	serveRequest(h, ctx)

	require.Equal(t, 504, respCode(ctx))
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))
	require.Equal(t, string(api.SourceOrigin), respHeader(ctx, header.XCacheSource))
}
