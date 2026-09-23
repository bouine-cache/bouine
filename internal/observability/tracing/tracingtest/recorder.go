package tracingtest

import (
	"context"
	"sync"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// SpanRecorder is an in-memory sdktrace.SpanExporter for tests: every
// exported span is retained for assertion. It replaces the per-package
// copies of the same struct that had accumulated in admin and cache
// tests. spans before mu satisfies fieldalignment: a 24-byte slice
// header followed by an 8-byte mutex packs with zero padding.
type SpanRecorder struct {
	spans []sdktrace.ReadOnlySpan
	mu    sync.Mutex
}

// ExportSpans implements sdktrace.SpanExporter.
func (r *SpanRecorder) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.spans = append(r.spans, spans...)
	return nil
}

// Shutdown implements sdktrace.SpanExporter.
func (r *SpanRecorder) Shutdown(_ context.Context) error { return nil }

// Ended returns all spans exported so far.
func (r *SpanRecorder) Ended() []sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(r.spans))
	copy(out, r.spans)
	return out
}

// Named returns the first exported span with the given name, or nil.
func (r *SpanRecorder) Named(name string) sdktrace.ReadOnlySpan {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, sp := range r.spans {
		if sp.Name() == name {
			return sp
		}
	}
	return nil
}

// Attr returns the value of key on span, or "" when absent.
func (r *SpanRecorder) Attr(span sdktrace.ReadOnlySpan, key string) string {
	for _, kv := range span.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.String()
		}
	}
	return ""
}
