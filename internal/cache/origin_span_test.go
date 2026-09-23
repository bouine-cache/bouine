package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability/tracing"
	tracingtest "github.com/bouine-cache/bouine/internal/observability/tracing/tracingtest"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

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
