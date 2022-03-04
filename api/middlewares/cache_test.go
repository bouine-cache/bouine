package middlewares

import (
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cache"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/thylong/bouine/api/handlers"
	"github.com/valyala/fasthttp"
)

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_500 -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_500(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(handlers.DefaultHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusInternalServerError, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_502 -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_502(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(badGatewayHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusBadGateway, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_503 -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_503(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(serviceUnavailableHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusServiceUnavailable, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_504 -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_504(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(gatewayTimeoutHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusGatewayTimeout, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_CacheControlNoStore -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_CacheControlNoStore(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(nostoreCacheControlHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusOK, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_CacheControlPrivate -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_CacheControlPrivate(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(privateCacheControlHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusOK, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}
