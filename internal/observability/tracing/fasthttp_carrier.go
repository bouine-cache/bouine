package tracing

import (
	"context"

	"github.com/valyala/fasthttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// fasthttpHeaderCarrier adapts *fasthttp.RequestHeader to otel's
// propagation.TextMapCarrier interface. It allows the W3C TraceContext
// propagator to inject and extract trace headers (traceparent,
// tracestate, baggage) from fasthttp's byte-slice-based header type
// without converting to net/http's map[string][]string.
//
// Peek returns the first value for the key (case-insensitive).
// Set replaces all values for the key with a single value.
// This matches the semantics of propagation.HeaderCarrier on
// http.Header when there is a single value per key, which is the
// case for traceparent and tracestate.
type fasthttpHeaderCarrier struct {
	h *fasthttp.RequestHeader
}

// Ensure fasthttpHeaderCarrier implements propagation.TextMapCarrier.
var _ propagation.TextMapCarrier = fasthttpHeaderCarrier{}

// Get returns the value for the given key. Returns "" if the key
// is not present.
func (c fasthttpHeaderCarrier) Get(key string) string {
	return string(c.h.Peek(key))
}

// Set sets the value for the given key, replacing any existing value.
func (c fasthttpHeaderCarrier) Set(key, value string) {
	c.h.Set(key, value)
}

// Keys returns all header keys. This is used by the propagator for
// debugging and baggage extraction. fasthttp.RequestHeader does not
// expose a Keys method, so we visit all headers and collect keys.
func (c fasthttpHeaderCarrier) Keys() []string {
	var keys []string
	for k := range c.h.All() {
		keys = append(keys, string(k))
	}
	return keys
}

// InjectFastHTTP stamps W3C TraceContext (traceparent / tracestate)
// and Baggage headers into req so the upstream origin can continue
// the trace. It is the fasthttp equivalent of [InjectHTTP].
//
// It is a no-op when no tracer is configured or the context has no
// active span, so callers do not need to guard against unconfigured
// tracing.
func InjectFastHTTP(ctx context.Context, req *fasthttp.Request) {
	if !tracerEnabled.Load() {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, fasthttpHeaderCarrier{h: &req.Header})
}

// ExtractFastHTTP extracts W3C TraceContext from an incoming
// *fasthttp.RequestHeader and returns a context enriched with the
// span context. It is the fasthttp equivalent of extracting from
// http.Header in [HTTPMiddleware].
//
// It is a no-op when no propagator is configured.
func ExtractFastHTTP(ctx context.Context, h *fasthttp.RequestHeader) context.Context {
	return otel.GetTextMapPropagator().Extract(ctx, fasthttpHeaderCarrier{h: h})
}
