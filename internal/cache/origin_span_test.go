package cache

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/bouine-cache/bouine/internal/observability/tracing"
	tracingtest "github.com/bouine-cache/bouine/internal/observability/tracing/tracingtest"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// spanSink is an in-memory sdktrace.SpanExporter (same shape as the
// admin package's spanRecorder).
type spanSink struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (s *spanSink) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spans = append(s.spans, spans...)
	return nil
}

func (s *spanSink) Shutdown(_ context.Context) error { return nil }

func (s *spanSink) named(name string) sdktrace.ReadOnlySpan {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, sp := range s.spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

// CCC-32: the bouine.origin span of a MISS served through the tracing
// middleware must be a child of bouine.pipeline, with method/path/pool
// attributes. No t.Parallel(): global OTel provider.
func TestMissOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &spanSink{}
	tracingtest.Setup(t, sink)

	h := NewHandler(HandlerConfig{
		Upstream: origin200("miss-body"),
		FastClient: &testFastClient{handler: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("miss-body")
		}},
		Store:    storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		PoolName: "product-page",
	})
	pipeline := tracing.FastHTTPMiddleware("bouine.pipeline", h.ServeRequest)

	rctx := testCtx("GET", "http://example.com/product-page/products/42/pickers")
	pipeline(rctx)
	require.Equal(t, "MISS", respHeader(rctx, header.XCache))

	origin := sink.named("bouine.origin")
	require.NotNil(t, origin, "miss must start a bouine.origin span")
	pipe := sink.named("bouine.pipeline")
	require.NotNil(t, pipe, "middleware must start a bouine.pipeline span")

	assert.Equal(t, pipe.SpanContext().TraceID(), origin.SpanContext().TraceID(),
		"origin span must share the client trace (no orphan root)")
	assert.Equal(t, pipe.SpanContext().SpanID(), origin.Parent().SpanID(),
		"origin span must be a child of bouine.pipeline")
	assert.Equal(t, "GET", spanAttr(origin, "http.method"))
	assert.Equal(t, "/product-page/products/42/pickers", spanAttr(origin, "http.path"),
		"slow fetches must be filterable by route path")
	assert.Equal(t, "product-page", spanAttr(origin, "upstream_pool"))
}

// TestRevalidateOriginSpanIsChildOfPipeline covers the conditional-fetch
// path. No t.Parallel(): global OTel provider.
func TestRevalidateOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &spanSink{}
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
	pipe := sink.named("bouine.pipeline")
	require.NotNil(t, pipe)
	var linked sdktrace.ReadOnlySpan
	sink.mu.Lock()
	for _, sp := range sink.spans {
		if sp.Name() == "bouine.origin" && sp.SpanContext().TraceID() == pipe.SpanContext().TraceID() {
			linked = sp
		}
	}
	sink.mu.Unlock()
	require.NotNil(t, linked, "a bouine.origin span must exist inside the revalidate request's trace")
	assert.Equal(t, pipe.SpanContext().SpanID(), linked.Parent().SpanID(),
		"revalidate origin span must be a child of bouine.pipeline")
}

// CCC-32 on the invalidating-proxy path. No t.Parallel(): global
// OTel provider.
func TestInvalidatingProxyOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &spanSink{}
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

	origin := sink.named("bouine.origin")
	require.NotNil(t, origin, "invalidating proxy must start a bouine.origin span")
	pipe := sink.named("bouine.pipeline")
	require.NotNil(t, pipe)
	assert.Equal(t, pipe.SpanContext().TraceID(), origin.SpanContext().TraceID(),
		"invalidating-proxy origin span must share the client trace")
	assert.Equal(t, pipe.SpanContext().SpanID(), origin.Parent().SpanID())
	assert.Equal(t, "POST", spanAttr(origin, "http.method"))
}

// CCC-32 on the BYPASS path (streamBypass → doFetchStream).
// No t.Parallel(): global OTel provider.
func TestBypassStreamOriginSpanIsChildOfPipeline(t *testing.T) {
	sink := &spanSink{}
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

	origin := sink.named("bouine.origin")
	require.NotNil(t, origin, "bypass must start a bouine.origin span")
	pipe := sink.named("bouine.pipeline")
	require.NotNil(t, pipe)
	assert.Equal(t, pipe.SpanContext().TraceID(), origin.SpanContext().TraceID(),
		"bypass origin span must share the client trace")
	assert.Equal(t, pipe.SpanContext().SpanID(), origin.Parent().SpanID())
	assert.Equal(t, "/private", spanAttr(origin, "http.path"))
}

func spanAttr(span sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	return ""
}
