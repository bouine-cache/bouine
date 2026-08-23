package cache

import (
	"strconv"
	"strings"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func BenchmarkGate_Handler_CacheHit_ReusableWriter(b *testing.B) {
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.ETag, `"bench"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 1024))
	}
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, FastClient: &testFastClient{handler: upstream}, Store: store})

	ctx := testCtx("GET", "http://bench.local/hit")
	serveRequest(h, ctx)
	if respHeader(ctx, header.XCache) != "MISS" {
		b.Fatal("warmup should be MISS")
	}

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx := testCtx("GET", "http://bench.local/hit")
		h.ServeRequest(ctx)
	}
}

func BenchmarkHandler_CacheMiss(b *testing.B) {
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 1024))
	}
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, FastClient: &testFastClient{handler: upstream}, Store: store})

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		ctx := testCtx("GET", "http://bench.local/miss")
		h.ServeRequest(ctx)
	}
}

func BenchmarkGate_Handler_CacheMiss_Cacheable(b *testing.B) {
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.ETag, `"bench"`)
		ctx.Response.Header.Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		ctx.Response.Header.Set("Content-Type", "application/json")
		ctx.Response.Header.Set("X-Content-Type-Options", "nosniff")
		ctx.Response.Header.Set("X-Frame-Options", "DENY")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 1024))
	}
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, FastClient: &testFastClient{handler: upstream}, Store: store})

	base := "http://bench.local/miss/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := testCtx("GET", base+strconv.Itoa(i))
		h.ServeRequest(ctx)
	}
}

func BenchmarkHandler_CacheMiss_Vary(b *testing.B) {
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
		ctx.Response.Header.Set(header.ETag, `"bench"`)
		ctx.Response.Header.Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		ctx.Response.Header.Set("Content-Type", "application/json")
		ctx.Response.Header.Set("X-Content-Type-Options", "nosniff")
		ctx.Response.Header.Set("X-Frame-Options", "DENY")
		ctx.Response.Header.Set(header.Vary, "Accept-Encoding")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write(make([]byte, 1024))
	}
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, FastClient: &testFastClient{handler: upstream}, Store: store})

	base := "http://bench.local/vary/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := testCtx("GET", base+strconv.Itoa(i))
		ctx.Request.Header.Set("Accept-Encoding", "gzip")
		h.ServeRequest(ctx)
	}
}

func BenchmarkBuildKey_LongURL(b *testing.B) {
	longPath := strings.Repeat("a", 5000)
	ri := requestInfoFromURL("GET", "http://example.com/"+longPath+"?b=2&a=1&c=3&d=4")

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = BuildKey(ri, nil)
	}
}

func BenchmarkGate_Evaluate_Hit(b *testing.B) {
	ri := requestInfoFromURL("GET", "http://example.com/")
	obj := freshObj(60)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = Evaluate(ri, obj, obj.StoredAt)
	}
}

var _ storage.Store = (*storage.HotStore)(nil)
