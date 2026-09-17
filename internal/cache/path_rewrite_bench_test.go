package cache

import (
	"strconv"
	"testing"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// BenchmarkPathRewriteURI measures the per-call cost of the regex
// rewrite on the miss path (the only path it runs on): query split,
// FindSubmatchIndex, Expand with one capture group, query re-append.
// Two variants: a matching request (the production payment-callback
// shape) and a non-matching one (the cheap FindSubmatchIndex nil
// return).
func BenchmarkPathRewriteURI(b *testing.B) {
	rw := NewPathRewrite(`^/payment/orchestrator/callback/(.*)$`, "/scrooge/callback/$1")
	uri := []byte("/payment/orchestrator/callback/payin_0123456789?sig=abc")
	nomatch := []byte("/unrelated/path/0123456789?sig=abc")

	b.Run("match", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out := rw.RewriteURI(uri)
			if string(out[:7]) != "/scroog" {
				b.Fatal("unexpected rewrite result")
			}
		}
	})
	b.Run("nomatch", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			out := rw.RewriteURI(nomatch)
			if &out[0] != &nomatch[0] {
				b.Fatal("non-matching URI must pass through")
			}
		}
	})
}

// BenchmarkPathRewrite_MissWithRewrite measures the full handler miss
// with the rewrite active, so the regex cost is visible in context
// against BenchmarkGate_Handler_CacheMiss_Cacheable (which has no
// rewrite). Not a gate benchmark: the rewrite is miss-path-only, and
// the hit-path zero-alloc budget is enforced by the existing gates
// (the rewrite is never reached on a hit).
func BenchmarkPathRewrite_MissWithRewrite(b *testing.B) {
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
	h := NewHandler(HandlerConfig{
		Upstream:    upstream,
		FastClient:  &benchFastClient{handler: upstream},
		Store:       store,
		PathRewrite: NewPathRewrite(`^/payment/orchestrator/callback/(.*)$`, "/scrooge/callback/$1"),
	})

	base := "http://bench.local/payment/orchestrator/callback/"

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		ctx := rctxPool.Get().(*fasthttp.RequestCtx)
		ctx.Request.Header.SetMethod("GET")
		ctx.Request.SetRequestURI(base + strconv.Itoa(i))
		h.ServeRequest(ctx)
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.ResetUserValues()
		rctxPool.Put(ctx)
	}
}
