// Package tracingtest provides helpers for other packages' tests to install
// a real tracer provider and W3C propagator for the tracing package. The
// import of the testing package makes it unsuitable for production binaries;
// it is only imported from _test.go files of other packages.
package tracingtest

import (
	"context"
	"testing"

	"github.com/bouine-cache/bouine/internal/observability/tracing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"
)

// SpanExporter is the subset of sdktrace.SpanExporter needed by tests to
// collect exported spans.
type SpanExporter = sdktrace.SpanExporter

// Setup installs an always-sampling tracer provider whose spans are exported
// synchronously to exp, sets the W3C TraceContext propagator, and restores
// the previous globals when the test ends.
func Setup(t *testing.T, exp SpanExporter) {
	t.Helper()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(sdkresource.NewWithAttributes(
			semconv.SchemaURL, semconv.ServiceName("bouine"))),
		sdktrace.WithSyncer(exp),
	)
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
	))
	EnableForTest(true)
	t.Cleanup(func() {
		EnableForTest(false)
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})
}

// EnableForTest toggles span creation in the tracing package for tests that
// installed their own tracer provider via Setup. Tracing.EnableForTest is the
// underlying toggle; keep the two in sync.
func EnableForTest(enabled bool) {
	tracing.EnableForTest(enabled)
}
