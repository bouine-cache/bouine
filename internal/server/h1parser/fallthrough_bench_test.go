package h1parser

import (
	"bytes"
	"io"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/api"
)

// BenchmarkH1Parse_FallThrough measures the full fall-through request
// cost: rebuildRequestHead, prefixConn, pooled *bufio.Reader checkout,
// fasthttp re-parse, and response write. Tracked for allocation
// regressions on the miss path — the hit path has its own zero-alloc
// gate (BenchmarkGate_H1Parse_Get). The reader checkout must not
// allocate in the steady state: the 16 KiB bufio.Reader is pooled
// (Parser.fallReaderPool), where it previously dominated allocation
// volume under load (~50% of all allocated bytes at a ~43% fall-through
// rate). Not a BenchmarkGate_*: the miss path is outside the hit-path
// alloc budget by design.
func BenchmarkH1Parse_FallThrough(b *testing.B) {
	raw := []byte("GET /api/v1/users/42 HTTP/1.1\r\nHost: example.com\r\nAccept: application/json\r\nUser-Agent: Bouine-Test/1.0\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n")
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
		ctx.SetBody(raw)
	}
	parser := New(nil, handler)
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/api/v1/users/42",
		HTTPVersion: "HTTP/1.1",
		Host:        "example.com",
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "application/json"},
			{Key: "User-Agent", Value: "Bouine-Test/1.0"},
			{Key: "X-Forwarded-For", Value: "10.0.0.1"},
		},
		NHeaders: 4,
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// Fresh conn per iteration: mockConn's reader aliases nothing,
		// and the fall-through prefix (rebuilt head) is self-contained,
		// so each iteration is a complete request lifecycle.
		conn := &mockConn{r: bytes.NewReader(raw)}
		if _, err := parser.handleFallThrough(conn, req, nil); err != nil {
			b.Fatalf("handleFallThrough: %v", err)
		}
	}
}

// BenchmarkH1Parse_FallThroughRaw measures the oversize-header
// fall-through (64 KiB pooled reader) the same way. Not a gate.
func BenchmarkH1Parse_FallThroughRaw(b *testing.B) {
	buffered := append(
		[]byte("GET /big HTTP/1.1\r\nHost: example.com\r\nX-Oversize: "),
		bytes.Repeat([]byte("x"), 20*1024)...,
	)
	buffered = append(buffered, "\r\n\r\n"...)
	handler := func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusOK)
	}
	parser := New(nil, handler)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		conn := &mockConn{r: bytes.NewReader(nil), w: io.Discard}
		parser.handleFallThroughRaw(conn, buffered)
	}
}
