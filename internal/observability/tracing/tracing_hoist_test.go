package tracing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// The tests in this file pin the sampler-drop hoist (finding F4): the
// middleware and StartOriginSpan must skip the attribute building and the
// SDK Start call for requests the configured sampler provably drops, while
// recording exactly what the sampler would record. A recording provider
// (AlwaysSample) is installed underneath so a hoist bug — skipping a span
// the sampler wanted — shows up as a missing span instead of silently
// passing. These tests must NOT call t.Parallel(): the OTel globals and
// samplerHoist are process state.

const (
	remoteTraceID   = "4bf92f3577b34da6a3ce929d0e0e4736"
	traceparentDrop = "00-" + remoteTraceID + "-00f067aa0ba902b7-00"
	traceparentKeep = "00-" + remoteTraceID + "-00f067aa0ba902b7-01"
)

func setHoist(t *testing.T, m hoistMode) {
	t.Helper()
	prev := samplerHoist.Swap(int32(m))
	t.Cleanup(func() { samplerHoist.Store(prev) })
}

func middlewareRequest(traceparent string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("http://bench.local/ssr/key-1")
	if traceparent != "" {
		ctx.Request.Header.Set("traceparent", traceparent)
	}
	return ctx
}

// TestFastHTTPMiddleware_HoistNoneRecordsWithAttributes pins the baseline:
// with no hoist (AlwaysSample configuration) a root request records a
// server span carrying the four HTTP attributes.
func TestFastHTTPMiddleware_HoistNoneRecordsWithAttributes(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistNone)

	inner := func(ctx *fasthttp.RequestCtx) { ctx.SetStatusCode(fasthttp.StatusOK) }
	FastHTTPMiddleware("bouine.pipeline", inner)(middlewareRequest(""))

	spans := sr.Ended()
	require.Len(t, spans, 1)
	require.Equal(t, "bouine.pipeline", spans[0].Name())
	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	assert.Equal(t, "GET", attrs["http.method"])
	assert.Equal(t, "/ssr/key-1", attrs["http.path"])
	assert.Equal(t, "bench.local", attrs["http.host"])
	assert.Equal(t, "http", attrs["http.scheme"])
}

// TestFastHTTPMiddleware_HoistAllSkipsSDK pins the NeverSample hoist: even
// with a recording sampler installed underneath, the middleware must not
// reach the SDK at all, while the parent context is still published so
// downstream helpers can read the trace chain.
func TestFastHTTPMiddleware_HoistAllSkipsSDK(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistAll)

	ctx := middlewareRequest(traceparentKeep)
	inner := func(c *fasthttp.RequestCtx) { c.SetStatusCode(fasthttp.StatusOK) }
	FastHTTPMiddleware("bouine.pipeline", inner)(ctx)

	assert.Empty(t, sr.Ended(), "hoistAll must bypass the SDK entirely")
	sc := trace.SpanContextFromContext(SpanContextFromRequest(ctx))
	require.True(t, sc.IsValid())
	assert.Equal(t, remoteTraceID, sc.TraceID().String())
}

// TestFastHTTPMiddleware_HoistUnsampledParentSkipsSDK pins the
// ParentBased hoist: an unsampled remote parent deterministically drops
// the child, so the span pipeline is skipped and the trace ID is preserved
// through the published context.
func TestFastHTTPMiddleware_HoistUnsampledParentSkipsSDK(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistUnsampledParent)

	ctx := middlewareRequest(traceparentDrop)
	inner := func(c *fasthttp.RequestCtx) { c.SetStatusCode(fasthttp.StatusOK) }
	FastHTTPMiddleware("bouine.pipeline", inner)(ctx)

	assert.Empty(t, sr.Ended(), "an unsampled parent must not create a span")
	sc := trace.SpanContextFromContext(SpanContextFromRequest(ctx))
	require.True(t, sc.IsValid(), "the extracted trace chain must still be published")
	assert.Equal(t, remoteTraceID, sc.TraceID().String())
}

// TestFastHTTPMiddleware_SampledParentRecords pins the stitching half of
// the F4 sampler change: under ParentBased a sampled remote parent always
// records (attributes set post-Start, same set as before), with the
// parent's trace ID.
func TestFastHTTPMiddleware_SampledParentRecords(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistUnsampledParent)

	ctx := middlewareRequest(traceparentKeep)
	inner := func(c *fasthttp.RequestCtx) { c.SetStatusCode(fasthttp.StatusOK) }
	FastHTTPMiddleware("bouine.pipeline", inner)(ctx)

	spans := sr.Ended()
	require.Len(t, spans, 1)
	assert.Equal(t, remoteTraceID, spans[0].SpanContext().TraceID().String())
	attrs := map[string]string{}
	for _, kv := range spans[0].Attributes() {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	assert.Equal(t, "GET", attrs["http.method"])
	assert.Equal(t, "/ssr/key-1", attrs["http.path"])
}

// TestStartOriginSpan_HoistAllSkipsSDK pins the origin-span hoist under
// NeverSample: no span, no attributes, the parent context returned as-is.
func TestStartOriginSpan_HoistAllSkipsSDK(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistAll)

	type ctxKey struct{}
	parent := context.WithValue(context.Background(), ctxKey{}, "parent")
	ctx, span := StartOriginSpan(parent, []byte("GET"), []byte("/x"), "ssr", "r")
	assert.False(t, span.IsRecording())
	assert.Empty(t, sr.Ended())
	assert.Equal(t, "parent", ctx.Value(ctxKey{}), "the parent context must be returned unchanged")
}

// TestStartOriginSpan_UnsampledParentSkipsSDK pins the origin-span hoist
// for an unsampled parent: origin spans are only ever children, so an
// unsampled parent proves the child is dropped.
func TestStartOriginSpan_UnsampledParentSkipsSDK(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistUnsampledParent)

	parent := extractParent(t, traceparentDrop)
	ctx, span := StartOriginSpan(parent, []byte("GET"), []byte("/x"), "ssr", "r")
	assert.False(t, span.IsRecording())
	assert.Empty(t, sr.Ended())
	sc := trace.SpanContextFromContext(ctx)
	assert.Equal(t, remoteTraceID, sc.TraceID().String(), "the trace chain must survive")
}

// TestStartOriginSpan_SampledParentRecords pins that a sampled parent's
// origin span still records with its attributes (method, path, pool, route).
func TestStartOriginSpan_SampledParentRecords(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	cleanup := newTestTracerProvider(t, sr)
	defer cleanup()
	setHoist(t, hoistUnsampledParent)

	parent := extractParent(t, traceparentKeep)
	_, span := StartOriginSpan(parent, []byte("GET"), []byte("/x"), "ssr", "r")
	require.True(t, span.IsRecording())
	span.End()
	attrs := map[string]string{}
	for _, kv := range recordedAttributes(t, sr, "bouine.origin") {
		attrs[string(kv.Key)] = kv.Value.String()
	}
	assert.Equal(t, "GET", attrs["http.method"])
	assert.Equal(t, "/x", attrs["http.path"])
	assert.Equal(t, "ssr", attrs["upstream_pool"])
	assert.Equal(t, "r", attrs["http.route"])
}

// TestInitTracer_ParentBasedStitching pins the production configuration
// end to end: InitTracer(0.01) installs ParentBased(TraceIDRatioBased) and
// publishes the matching hoist mode, so a sampled incoming traceparent
// always records (the stitching fix) and an unsampled one is skipped
// without touching the SDK.
func TestInitTracer_ParentBasedStitching(t *testing.T) {
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer sink.Close()

	shutdown, err := InitTracer(context.Background(), TracingConfig{
		Endpoint:     sink.URL,
		ServiceName:  "test",
		SamplingRate: 0.01,
	})
	require.NoError(t, err)

	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		shutdown()
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		tracerEnabled.Store(false)
		samplerHoist.Store(int32(hoistNone))
	})

	ctx := middlewareRequest(traceparentKeep)
	inner := func(c *fasthttp.RequestCtx) { c.SetStatusCode(fasthttp.StatusOK) }
	FastHTTPMiddleware("bouine.pipeline", inner)(ctx)
	sc := trace.SpanContextFromContext(SpanContextFromRequest(ctx))
	assert.True(t, sc.IsSampled(), "a sampled remote parent must force recording under ParentBased")
	assert.Equal(t, remoteTraceID, sc.TraceID().String())

	dropped := middlewareRequest(traceparentDrop)
	FastHTTPMiddleware("bouine.pipeline", inner)(dropped)
	dsc := trace.SpanContextFromContext(SpanContextFromRequest(dropped))
	assert.False(t, dsc.IsSampled())
	assert.Equal(t, remoteTraceID, dsc.TraceID().String(), "the dropped request keeps its trace chain")
}

func extractParent(t *testing.T, traceparent string) context.Context {
	t.Helper()
	req := &fasthttp.RequestCtx{}
	req.Request.Header.Set("traceparent", traceparent)
	return ExtractFastHTTP(context.Background(), &req.Request.Header)
}

func recordedAttributes(t *testing.T, sr *tracetest.SpanRecorder, name string) []attribute.KeyValue {
	t.Helper()
	for _, sp := range sr.Ended() {
		if sp.Name() == name {
			return sp.Attributes()
		}
	}
	t.Fatalf("no recorded span named %q", name)
	return nil
}
