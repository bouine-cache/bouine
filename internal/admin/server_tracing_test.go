package admin

import (
	"io"
	"log/slog"
	"testing"

	tracingtest "github.com/bouine-cache/bouine/internal/observability/tracing/tracingtest"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/valyala/fasthttp"
)

// TestAdmin_PropagatesTraceContext verifies the admin server joins the
// incoming W3C trace: the span started for POST /v1/ban inherits the
// traceparent sent by the invalidation caller (cache-lifecycle).
func TestAdmin_PropagatesTraceContext(t *testing.T) {
	rec := &tracingtest.SpanRecorder{}
	setupTracingForTest(t, rec)

	s := New(Config{
		Token:  "test",
		Logger: slog.New(slog.NewJSONHandler(io.Discard, nil)),
		BanFn:  func(expr api.BanExpr) (int, error) { return 1, nil },
	})

	ctx := testCtxWithBodyAuth("POST", "/v1/ban", []byte(`{"surrogate_key":"product-123"}`), "test")
	// Simulate the caller propagating W3C trace context.
	reqTP := "00-11111111111111111111111111111111-2222222222222222-01"
	ctx.Request.Header.Set("traceparent", reqTP)
	s.Handler()(ctx)
	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode())

	var adminSpan sdktrace.ReadOnlySpan
	for _, sd := range rec.Ended() {
		if sd.Name() == "bouine.admin" {
			adminSpan = sd
		}
	}
	require.NotNil(t, adminSpan, "expected a bouine.admin span")
	require.Equal(t, "11111111111111111111111111111111", adminSpan.SpanContext().TraceID().String(),
		"admin span must join the caller's trace")
}

// setupTracingForTest swaps the global tracer provider and the W3C propagator
// for the test so spans are actually created and exported, and restores both
// when the test ends (tracing's own defaults stay untouched for other tests).
func setupTracingForTest(t *testing.T, rec *tracingtest.SpanRecorder) {
	t.Helper()
	tracingtest.Setup(t, rec)
}
