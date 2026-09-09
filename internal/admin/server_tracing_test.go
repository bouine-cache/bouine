package admin

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"

	tracingtest "github.com/bouine-cache/bouine/internal/observability/tracing/tracingtest"
	"github.com/bouine-cache/bouine/pkg/api"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"

	"github.com/valyala/fasthttp"
)

// spanRecorder is an in-memory sdktrace.SpanExporter for tests.
type spanRecorder struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (r *spanRecorder) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spans...)
	return nil
}

func (r *spanRecorder) Shutdown(_ context.Context) error { return nil }

// TestAdmin_PropagatesTraceContext verifies the admin server joins the
// incoming W3C trace: the span started for POST /v1/ban inherits the
// traceparent sent by the invalidation caller (cache-lifecycle).
func TestAdmin_PropagatesTraceContext(t *testing.T) {
	rec := &spanRecorder{}
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
	rec.mu.Lock()
	for _, sd := range rec.spans {
		if sd.Name() == "bouine.admin" {
			adminSpan = sd
		}
	}
	rec.mu.Unlock()
	require.NotNil(t, adminSpan, "expected a bouine.admin span")
	require.Equal(t, "11111111111111111111111111111111", adminSpan.SpanContext().TraceID().String(),
		"admin span must join the caller's trace")
}

// setupTracingForTest swaps the global tracer provider and the W3C propagator
// for the test so spans are actually created and exported, and restores both
// when the test ends (tracing's own defaults stay untouched for other tests).
func setupTracingForTest(t *testing.T, rec *spanRecorder) {
	t.Helper()
	tracingtest.Setup(t, rec)
}
