package cache

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability/tracing"
	tracingtest "github.com/bouine-cache/bouine/internal/observability/tracing/tracingtest"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/valyala/fasthttp"
)

// CCC-32: the bouine.origin span of a MISS served through the tracing
// middleware must be a child of bouine.pipeline, with
// method/path/pool/route attributes. No t.Parallel(): global OTel provider.
func TestMissOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: origin200("miss-body"),
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("miss-body")
		}},
		Store:     storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName:  "product-page",
		RouteName: "products",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	rctx := testCtx("GET", "http://example.com/product-page/products/42/pickers")
	pipeline(rctx)
	require.Equal(t, "MISS", respHeader(rctx, header.XCache))

	origin := sink.Named("bouine.origin")
	require.NotNil(t, origin, "miss must start a bouine.origin span")
	pipe := sink.Named("bouine.pipeline")
	require.NotNil(t, pipe, "middleware must start a bouine.pipeline span")

	assert.Equal(t, pipe.SpanContext().TraceID(), origin.SpanContext().TraceID(),
		"origin span must share the client trace (no orphan root)")
	assert.Equal(t, pipe.SpanContext().SpanID(), origin.Parent().SpanID(),
		"origin span must be a child of bouine.pipeline")
	assert.Equal(t, "GET", sink.Attr(origin, "http.method"))
	assert.Equal(t, "/product-page/products/42/pickers", sink.Attr(origin, "http.path"),
		"slow fetches must be filterable by route path")
	assert.Equal(t, "product-page", sink.Attr(origin, "upstream_pool"))
	assert.Equal(t, "products", sink.Attr(origin, "http.route"),
		"slow fetches must be filterable by the bounded route label")
}

// CCC-32 on the conditional-fetch path: the revalidate fetch's span must
// be a child of bouine.pipeline AND carry the CCC-32 attributes — the
// acceptance criteria apply to every linked path, not just the miss.
// No t.Parallel(): global OTel provider.
func TestRevalidateOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: origin200("reval-body"),
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			// 304 only for the conditional request revalidate sends.
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) == `"v1"` {
				ctx.SetStatusCode(304)
				return
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("reval-body")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "app",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	// Warm, then expire the stored object.
	warm := testCtx("GET", "http://example.com/widget")
	h.ServeRequest(warm)
	require.Equal(t, "MISS", respHeader(warm, header.XCache))
	key := BuildKey(requestInfoFromCtx(warm), nil)
	obj, _, err := h.store.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	obj.StoredAt = obj.StoredAt.Add(-2 * obj.TTL)
	require.NoError(t, h.store.Put(t.Context(), key, obj))

	rctx := testCtx("GET", "http://example.com/widget")
	pipeline(rctx)
	require.Equal(t, "REVALIDATED", respHeader(rctx, header.XCache))

	// The warm-up MISS also produced a detached root origin span; assert
	// on the one in the revalidate request's trace.
	pipe := sink.Named("bouine.pipeline")
	require.NotNil(t, pipe)
	for _, sp := range sink.Ended() {
		if sp.Name() != "bouine.origin" ||
			sp.SpanContext().TraceID() != pipe.SpanContext().TraceID() {
			continue
		}
		assert.Equal(t, pipe.SpanContext().SpanID(), sp.Parent().SpanID(),
			"revalidate origin span must be a child of bouine.pipeline")
		assert.Equal(t, "GET", sink.Attr(sp, "http.method"),
			"revalidate origin span must carry method (CCC-32)")
		assert.Equal(t, "/widget", sink.Attr(sp, "http.path"),
			"revalidate origin span must carry path (CCC-32)")
		assert.Equal(t, "app", sink.Attr(sp, "upstream_pool"),
			"revalidate origin span must carry pool (CCC-32)")
		return
	}
	t.Fatal("no bouine.origin span found in the revalidate request's trace")
}

// CCC-32 on the invalidating-proxy path. No t.Parallel(): global
// OTel provider.
func TestInvalidatingProxyOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("posted")
		},
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("posted")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "app",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	rctx := testCtxWithBody("POST", "http://example.com/widget", []byte("data"))
	pipeline(rctx)
	require.Equal(t, 200, respCode(rctx))

	origin := sink.Named("bouine.origin")
	require.NotNil(t, origin, "invalidating proxy must start a bouine.origin span")
	pipe := sink.Named("bouine.pipeline")
	require.NotNil(t, pipe)
	assert.Equal(t, pipe.SpanContext().TraceID(), origin.SpanContext().TraceID(),
		"invalidating-proxy origin span must share the client trace")
	assert.Equal(t, pipe.SpanContext().SpanID(), origin.Parent().SpanID())
	assert.Equal(t, "POST", sink.Attr(origin, "http.method"))
}

// CCC-32 on the BYPASS path (streamBypass → doFetchStream): the span is
// a child of the pipeline and is actually ended (exported) when the
// fetch is released, instead of leaking. No t.Parallel(): global
// OTel provider.
func TestBypassStreamOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {},
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "no-store")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("bypassed")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "app",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	rctx := testCtxWithHeader("GET", "http://example.com/private", header.CacheControl, "no-store")
	pipeline(rctx)
	require.Equal(t, "BYPASS", respHeader(rctx, header.XCache))

	origin := sink.Named("bouine.origin")
	require.NotNil(t, origin, "bypass must start a bouine.origin span")
	pipe := sink.Named("bouine.pipeline")
	require.NotNil(t, pipe)
	assert.Equal(t, pipe.SpanContext().TraceID(), origin.SpanContext().TraceID(),
		"bypass origin span must share the client trace")
	assert.Equal(t, pipe.SpanContext().SpanID(), origin.Parent().SpanID())
	assert.Equal(t, "/private", sink.Attr(origin, "http.path"))
}

// CCC-32 regression: an unbuffered (streamed) bypass must end its
// bouine.origin span. releaseStreamFetch, the funnel every streaming
// exit takes, owns span.End; before the fix the unbuffered path never
// ended the span, so the slowest fetches (SSE) were invisible in
// Tempo. No t.Parallel(): global OTel provider.
func TestStreamedOriginSpanIsEnded(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {},
		FastClient: &streamFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "no-store")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("streamed")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "app",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	rctx := testCtxWithHeader("GET", "http://example.com/live", header.CacheControl, "no-store")
	pipeline(rctx)
	require.Equal(t, "BYPASS", respHeader(rctx, header.XCache))
	require.True(t, rctx.Response.IsBodyStream(),
		"streamFastClient must produce an unbuffered body stream for this test to cover the streaming path")
	// Drain the streamed body: the stream writer is what calls
	// releaseStreamFetch (and thus ends the span) on the real path;
	// BodyWriteTo runs it in-process here.
	require.NoError(t, rctx.Response.BodyWriteTo(discardWriter{}))

	origin := sink.Named("bouine.origin")
	require.NotNil(t, origin, "streamed bypass must start a bouine.origin span")
	require.False(t, origin.EndTime().IsZero(),
		"the streamed fetch's origin span must be ended and exported, not leaked")
}

// discardWriter consumes a streamed body without inspecting it.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// failingFastClient is a cache.FastClient whose transport always fails,
// exercising the res.Err path of a fetch (Do's error, not an origin 5xx).
type failingFastClient struct{}

func (failingFastClient) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	return errors.New("connection refused")
}

func (failingFastClient) DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error {
	return errors.New("connection refused")
}

// A failed revalidate fetch must record the error on its origin span
// (Status Error): span ownership moved from doFetchBg to revalidate in
// the CCC-32 restructure, and the recording was initially dropped —
// failed fetches exported clean, green spans. No t.Parallel(): global
// OTel provider.
func TestRevalidateOriginSpanRecordsError(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: origin200("reval-body"),
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			// 304 only for the conditional request revalidate sends.
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) == `"v1"` {
				ctx.SetStatusCode(304)
				return
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("reval-body")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "app",
	})

	// Warm, then expire the stored object.
	warm := testCtx("GET", "http://example.com/widget")
	h.ServeRequest(warm)
	require.Equal(t, "MISS", respHeader(warm, header.XCache))
	key := BuildKey(requestInfoFromCtx(warm), nil)
	obj, _, err := h.store.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	obj.StoredAt = obj.StoredAt.Add(-2 * obj.TTL)
	require.NoError(t, h.store.Put(t.Context(), key, obj))

	// Swap in a client whose transport always fails: Do's error is what
	// sets res.Err on the fetch result — the path RecordError keys on
	// (an origin 5xx is a *successful* transport and goes through the
	// stale-on-error gate, not the span error).
	h.fastClient = failingFastClient{}

	// Revalidate without the middleware: the assertion is about error
	// recording, not linkage.
	rctx := testCtx("GET", "http://example.com/widget")
	h.ServeRequest(rctx)

	// The revalidate fetch's span is the last bouine.origin span
	// recorded (the warm-up MISS's span precedes it).
	spans := sink.Ended()
	var revalSpan sdktrace.ReadOnlySpan
	for i := len(spans) - 1; i >= 0; i-- {
		if spans[i].Name() == "bouine.origin" {
			revalSpan = spans[i]
			break
		}
	}
	require.NotNil(t, revalSpan, "revalidate must start a bouine.origin span")
	require.Equal(t, codes.Error, revalSpan.Status().Code,
		"a failed revalidate fetch must record Status Error on its origin span")
	// The error is also an event on the span (RecordError).
	found := false
	for _, ev := range revalSpan.Events() {
		if ev.Name == "exception" {
			found = true
		}
	}
	assert.True(t, found, "RecordError must add an exception event to the span")
}

// The conditional revalidate request carries the W3C traceparent of the
// revalidate caller's origin span — parity with doFetchFast,
// invalidateAndProxy, and doFetchStream (it was the only foreground
// fetch that left bouine without one). No t.Parallel(): global OTel
// provider.
func TestRevalidateInjectsTraceparent(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	var gotTraceparent atomic.Value
	h := NewHandler(HandlerConfig{
		Upstream: origin200("reval-body"),
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) == `"v1"` {
				gotTraceparent.Store(string(ctx.Request.Header.Peek("traceparent")))
				ctx.SetStatusCode(304)
				return
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("reval-body")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "app",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	// Warm, then expire the stored object.
	warm := testCtx("GET", "http://example.com/widget")
	h.ServeRequest(warm)
	require.Equal(t, "MISS", respHeader(warm, header.XCache))
	key := BuildKey(requestInfoFromCtx(warm), nil)
	obj, _, err := h.store.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)
	obj.StoredAt = obj.StoredAt.Add(-2 * obj.TTL)
	require.NoError(t, h.store.Put(t.Context(), key, obj))

	rctx := testCtx("GET", "http://example.com/widget")
	pipeline(rctx)
	require.Equal(t, "REVALIDATED", respHeader(rctx, header.XCache))

	got, _ := gotTraceparent.Load().(string)
	require.NotEmpty(t, got,
		"the conditional revalidate request must carry a traceparent header")
	pipe := sink.Named("bouine.pipeline")
	require.NotNil(t, pipe)
	// traceparent format: 00-<traceid>-<spanid>-01. The trace ID must be
	// the client's.
	parts := strings.Split(got, "-")
	require.Len(t, parts, 4, "traceparent must be W3C format")
	assert.Equal(t, pipe.SpanContext().TraceID().String(), parts[1],
		"the injected traceparent must carry the client trace ID")
}

// The popularity-gated background refresh is the fourth caller of
// collapsedFetchBg. When span ownership moved out of doFetchBg, it was
// the one caller left without a bouine.origin span — its origin
// fetches were invisible in Tempo. The fetch must produce a detached,
// attribute-bearing root span (detached by design: the triggering
// request's pipeline span is already ended when the refresh runs).
// No t.Parallel(): global OTel provider.
func TestBackgroundRefreshOriginSpanIsStarted(t *testing.T) {
	sink := &tracingtest.SpanRecorder{}
	tracingtest.Setup(t, sink)

	h := testRefreshHandler(t, 0)

	// Store via the foreground path so the refresh registry has an
	// entry for the key.
	req := testCtx("GET", "http://example.com/page")
	h.ServeRequest(req)
	require.Equal(t, "MISS", respHeader(req, header.XCache))

	key := h.buildKey(testCtx("GET", "http://example.com/page"))
	obj, _, err := h.store.Get(t.Context(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)

	// The warm-up MISS produced its own origin span; scope the search to
	// spans exported after this point so the refresh's span is
	// unambiguously identified (the recorder accumulates, Ended() does
	// not clear it).
	pre := len(sink.Ended())

	h.doBackgroundRefresh(context.Background(), key, obj, 0)

	var origin sdktrace.ReadOnlySpan
	for _, sp := range sink.Ended()[pre:] {
		if sp.Name() == "bouine.origin" {
			origin = sp
		}
	}
	require.NotNil(t, origin, "background refresh must start a bouine.origin span")
	require.False(t, origin.Parent().IsValid(),
		"background refresh span is detached by design (root span)")
	assert.Equal(t, "GET", sink.Attr(origin, "http.method"))
	assert.Equal(t, "/page", sink.Attr(origin, "http.path"),
		"slow refresh fetches must be filterable by path")
	require.False(t, origin.EndTime().IsZero(),
		"the background refresh span must be ended and exported")
}
