// Package tracing provides OpenTelemetry span instrumentation for
// bouine's data-plane layers. Each layer wraps its inbound handler
// with a single span so distributed traces show L1 → L2 → L4 → L5
// as nested children.
//
// A nil tracer is always safe to use — all helpers no-op when the
// SDK is not configured, so single-node deployments without a trace
// exporter work unchanged.
package tracing

import (
	"context"
	"net/http"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "bouine"

// Tracer returns the global tracer under the bouine instrumentation name.
func Tracer() trace.Tracer {
	return otel.Tracer(tracerName)
}

// HTTPMiddleware wraps an http.Handler with an OTel span named spanName.
// The span inherits any trace context propagated in the incoming HTTP
// headers via W3C TraceContext (http.Header propagation).
func HTTPMiddleware(spanName string, next http.Handler) http.Handler {
	t := Tracer()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := t.Start(r.Context(), spanName,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("http.method", r.Method),
				attribute.String("http.url", r.URL.String()),
				attribute.String("http.host", r.Host),
			),
		)
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// StartSpan is a thin helper that starts a child span in ctx and
// returns the enriched context. The caller is responsible for calling
// span.End().
func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	ctx, span := Tracer().Start(ctx, name,
		trace.WithAttributes(attrs...),
	)
	return ctx, span
}

// RecordError records err on span and sets the span status to Error.
// Safe to call with a nil span.
func RecordError(span trace.Span, err error) {
	if err == nil || span == nil {
		return
	}
	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
