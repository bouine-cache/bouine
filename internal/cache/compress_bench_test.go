package cache

import (
	"strconv"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// benchCompressibleHTML builds a deterministic, compressible HTML-ish body
// of the requested size (repeated markup lines) — the fill-compression
// benchmark's workload.
func benchCompressibleHTML(size int) []byte {
	line := []byte("<div class=\"product-card\"><h2>Product name</h2><p>Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor.</p></div>\n")
	out := make([]byte, 0, size+len(line))
	for len(out) < size {
		out = append(out, line...)
	}
	return out[:size]
}

func newCompressionBenchHandler(b *testing.B, body []byte) *Handler {
	b.Helper()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/html; charset=utf-8")
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(body)
	}
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	return NewHandler(HandlerConfig{
		Upstream:   upstream,
		FastClient: &testFastClient{handler: upstream},
		Store:      store,
		Compression: CompressionSpec{
			Enabled:      true,
			ContentTypes: []string{"text/html"},
		},
	})
}

// BenchmarkGate_FillCompress_Gzip1MiB measures a cold cacheable miss fill
// of a 1 MiB HTML body with gzip compression on the fill path. Time-driven;
// alloc budget enforced by bench/run.sh BUDGETS.
func BenchmarkGate_FillCompress_Gzip1MiB(b *testing.B) {
	body := benchCompressibleHTML(1 << 20)
	h := newCompressionBenchHandler(b, body)
	base := "http://bench.local/compress/gzip/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := rctxPool.Get().(*fasthttp.RequestCtx)
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(base + strconv.Itoa(i))
		ctx.Request.Header.Set(header.AcceptEncoding, "gzip")
		h.ServeRequest(ctx)
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.ResetUserValues()
		rctxPool.Put(ctx)
	}
}

// BenchmarkGate_FillCompress_Zstd1MiB is the zstd variant of the 1 MiB fill.
func BenchmarkGate_FillCompress_Zstd1MiB(b *testing.B) {
	body := benchCompressibleHTML(1 << 20)
	h := newCompressionBenchHandler(b, body)
	base := "http://bench.local/compress/zstd/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := rctxPool.Get().(*fasthttp.RequestCtx)
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(base + strconv.Itoa(i))
		ctx.Request.Header.Set(header.AcceptEncoding, "gzip, zstd")
		h.ServeRequest(ctx)
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.ResetUserValues()
		rctxPool.Put(ctx)
	}
}

// BenchmarkGate_FillCompress_Br64KiB is the brotli variant on a 64 KiB body.
func BenchmarkGate_FillCompress_Br64KiB(b *testing.B) {
	body := benchCompressibleHTML(64 * 1024)
	h := newCompressionBenchHandler(b, body)
	base := "http://bench.local/compress/br/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := rctxPool.Get().(*fasthttp.RequestCtx)
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(base + strconv.Itoa(i))
		ctx.Request.Header.Set(header.AcceptEncoding, "gzip, deflate, br")
		h.ServeRequest(ctx)
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.ResetUserValues()
		rctxPool.Put(ctx)
	}
}

// BenchmarkGate_FillSkip_Disabled measures the skip path: compression
// configured but the request buckets to identity — the fill must pay only
// the eligibility checks, no compression work. Alloc budget must match the
// passthrough miss budget.
func BenchmarkGate_FillSkip_Disabled(b *testing.B) {
	body := benchCompressibleHTML(64 * 1024)
	h := newCompressionBenchHandler(b, body)
	base := "http://bench.local/compress/skip/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := rctxPool.Get().(*fasthttp.RequestCtx)
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(base + strconv.Itoa(i))
		// No Accept-Encoding: bucket is identity, compression skipped.
		h.ServeRequest(ctx)
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.ResetUserValues()
		rctxPool.Put(ctx)
	}
}

// BenchmarkGate_FillSkip_Encoded measures the skip path when the origin
// already set Content-Encoding: passthrough, no recompression.
func BenchmarkGate_FillSkip_Encoded(b *testing.B) {
	body := benchCompressibleHTML(64 * 1024)
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "text/html")
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.ContentEncoding, "gzip")
		ctx.SetStatusCode(200)
		gz, _ := compressBytes("gzip", body)
		_, _ = ctx.Write(gz)
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		FastClient:  &testFastClient{handler: upstream},
		Store:       store,
		Compression: CompressionSpec{Enabled: true, ContentTypes: []string{"text/html"}},
	})
	base := "http://bench.local/compress/enc/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := rctxPool.Get().(*fasthttp.RequestCtx)
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(base + strconv.Itoa(i))
		ctx.Request.Header.Set(header.AcceptEncoding, "gzip")
		h.ServeRequest(ctx)
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.ResetUserValues()
		rctxPool.Put(ctx)
	}
}
