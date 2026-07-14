package tracing

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"go.opentelemetry.io/otel"
)

// BenchmarkTracingMiddleware_Noop measures the tracing middleware
// overhead with the default no-op tracer. The middleware itself is
// benchmarked for regression detection — when tracing is not configured,
// buildHandler skips this middleware entirely (task 1.2).
// Gate: 0 allocs/op, ns/op < 50.
func BenchmarkTracingMiddleware_Noop(b *testing.B) {
	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	mw := HTTPMiddleware("bench", next)
	req := httptest.NewRequest("GET", "http://example.com/path?query=1", nil)
	rec := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		mw.ServeHTTP(rec, req)
	}
}

// BenchmarkTracingMiddleware_Active measures the tracing middleware
// overhead with a real (non-no-op) tracer after the r.URL.String() fix.
// Uses a no-op TracerProvider (default) which still creates spans but
// does not export them — this isolates the middleware overhead from
// exporter cost. The request includes a query string to verify the
// r.URL.Path path is truly zero-alloc.
// Gate: 0 allocs/op, ns/op < 80.
func BenchmarkTracingMiddleware_Active(b *testing.B) {
	// The default OTel TracerProvider creates no-op spans (no exporter).
	// This means span creation is cheap but the middleware code path
	// (Start, WithAttributes, span.End) is still exercised.
	tp := otel.GetTracerProvider()
	b.Cleanup(func() { otel.SetTracerProvider(tp) })

	tracingEnabled.Store(true)
	b.Cleanup(func() { tracingEnabled.Store(false) })

	next := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	mw := HTTPMiddleware("bench", next)
	req := httptest.NewRequest("GET", "http://example.com/path?query=1&sort=name&page=2", nil)
	rec := httptest.NewRecorder()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		mw.ServeHTTP(rec, req)
	}
}
