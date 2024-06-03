package core

import (
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	fiber "github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cache"
	"github.com/gofiber/fiber/v2/utils"
	"github.com/valyala/fasthttp"
)

func Test_Cache_CustomExpirationGenerator_Default(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	cacheCfg := cache.Config{
		Next:                CacheSkippable,
		Expiration:          1 * time.Minute,
		CacheControl:        true,
		ExpirationGenerator: CustomExpirationGenerator,
	}
	app.Use(cache.New(cacheCfg))
	app.Use(publicCacheControlHandler) // Expires header is set by the handler

	// first request should go through but doesn't hit the cache
	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	fmt.Printf("%v\n", resp.Header)

	// Second request is expected to be served from the cache
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	fmt.Printf("%v\n", resp.Header)
	utils.AssertEqual(t, "public, max-age=60", resp.Header.Get("Cache-Control"))
	utils.AssertEqual(t, "hit", resp.Header.Get("X-Cache"))
}

// FIX: broken max-age
// func Test_Cache_CustomExpirationGenerator_ExpiresHeader(t *testing.T) {
// 	t.Parallel()
//
// 	app := fiber.New()
//
// 	cacheCfg := Config{
// 		Next:                CacheSkippable,
// 		Expiration:          1 * time.Minute,
// 		CacheControl:        true,
// 		ExpirationGenerator: CustomExpirationGenerator,
// 	}
// 	app.Use(New(cacheCfg))
// 	app.Use(expiresHeaderHandler) // Expires header is set by the handler
//
// 	// first request should go through but doesn't hit the cache
// 	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
// 	utils.AssertEqual(t, nil, err)
// 	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
// 	utils.AssertEqual(t, "miss", resp.Header.Get("X-Cache"))
//
// 	fmt.Printf("%v\n", resp.Header)
//
// 	// Second request is expected to be served from the cache
// 	resp, err = app.Test(httptest.NewRequest("GET", "/", nil))
// 	utils.AssertEqual(t, nil, err)
// 	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
//
// 	fmt.Printf("%v\n", resp.Header)
// 	utils.AssertEqual(t, "public, max-age=120", resp.Header.Get("Cache-Control"))
// 	utils.AssertEqual(t, "hit", resp.Header.Get("X-Cache"))
// }

func Test_Cache_CustomExpirationGenerator_MaxAgeCacheControlHeader(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	cacheCfg := cache.Config{
		Next:                CacheSkippable,
		Expiration:          1 * time.Minute,
		CacheControl:        true,
		ExpirationGenerator: CustomExpirationGenerator,
	}
	app.Use(cache.New(cacheCfg))
	app.Use(publicMaxAgeCacheControlHandler) // Cache-Control header is set by the handler

	// first request should go through but doesn't hit the cache
	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
	utils.AssertEqual(t, "miss", resp.Header.Get("X-Cache"))

	fmt.Printf("%v\n", resp.Header)

	// Second request is expected to be served from the cache
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	fmt.Printf("%v\n", resp.Header)
	utils.AssertEqual(t, "public, max-age=1337", resp.Header.Get("Cache-Control"))
	utils.AssertEqual(t, "hit", resp.Header.Get("X-Cache"))
}

func Test_Cache_CustomExpirationGenerator_SMaxAgeCacheControlHeader(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	cacheCfg := cache.Config{
		Next:                CacheSkippable,
		Expiration:          1 * time.Minute,
		CacheControl:        true,
		ExpirationGenerator: CustomExpirationGenerator,
	}
	app.Use(cache.New(cacheCfg))
	app.Use(publicSMaxAgeCacheControlHandler) // Cache-Control header is set by the handler

	// first request should go through but doesn't hit the cache
	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
	utils.AssertEqual(t, "miss", resp.Header.Get("X-Cache"))

	fmt.Printf("%v\n", resp.Header)

	// Second request is expected to be served from the cache
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	utils.AssertEqual(t, "hit", resp.Header.Get("X-Cache"))
	utils.AssertEqual(t, "public, max-age=666", resp.Header.Get("Cache-Control"))
}

// go test -v -run=^$ -bench=Benchmark_Cache -benchmem -count=4.
func Benchmark_Fiber_Cache(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New())

	app.Get("/demo", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusTeapot)
	})

	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod(fiber.MethodGet)
	fctx.Request.SetRequestURI("/demo")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusTeapot, fctx.Response.Header.StatusCode())
}

// go test -v -run=^$ -bench=Benchmark_Cache_Core -benchmem -count=4.
func Benchmark_Cache_Core(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New())

	app.Get("/demo", func(c *fiber.Ctx) error {
		return c.SendStatus(fiber.StatusTeapot)
	})

	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod(fiber.MethodGet)
	fctx.Request.SetRequestURI("/demo")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusTeapot, fctx.Response.Header.StatusCode())
}

func Test_Cache_CustomExpirationGenerator_E2EHeaders(t *testing.T) {
	t.Parallel()

	app := fiber.New()

	cacheCfg := cache.Config{
		Next:                 CacheSkippable,
		Expiration:           1 * time.Minute,
		CacheControl:         true,
		StoreResponseHeaders: true,
		ExpirationGenerator:  CustomExpirationGenerator,
	}
	app.Use(cache.New(cacheCfg))
	app.Get("/", func(c *fiber.Ctx) error {
		c.Response().Header.Add("X-Foobar", "foobar")
		return c.SendString("hi")
	})

	// first request should go through but doesn't hit the cache
	resp, err := app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)
	utils.AssertEqual(t, "miss", resp.Header.Get("X-Cache"))
	utils.AssertEqual(t, "foobar", resp.Header.Get("X-Foobar"))

	fmt.Printf("%v\n", resp.Header)

	// Second request is expected to be served from the cache
	resp, err = app.Test(httptest.NewRequest("GET", "/", nil))
	utils.AssertEqual(t, nil, err)
	utils.AssertEqual(t, fiber.StatusOK, resp.StatusCode)

	utils.AssertEqual(t, "hit", resp.Header.Get("X-Cache"))
	utils.AssertEqual(t, "foobar", resp.Header.Get("X-Foobar"))
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_500 -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_404(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))

	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusNotFound, fctx.Response.Header.StatusCode())
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

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_CacheControlPublic -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_CacheControlPublic(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(publicCacheControlHandler)
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
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_basicAuthAndPublicCacheControl -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_basicAuthAndPublicCacheControl(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(publicCacheControlHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.Header.Set("Authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusOK, fctx.Response.Header.StatusCode())
}

// go test -v -run=^$ -bench=Benchmark_Cache_CacheSkippable_basicAuth -benchmem -count=4.
func Benchmark_Cache_CacheSkippable_basicAuth(b *testing.B) {
	app := fiber.New()

	app.Use(cache.New(cache.Config{
		Next: CacheSkippable,
	}))
	app.Use(okHandler)
	h := app.Handler()

	fctx := &fasthttp.RequestCtx{}
	fctx.Request.Header.SetMethod("GET")
	fctx.Request.Header.Set("Authorization", "Basic YWxhZGRpbjpvcGVuc2VzYW1l")
	fctx.Request.SetRequestURI("/foobar")

	b.ReportAllocs()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		h(fctx)
	}

	utils.AssertEqual(b, fiber.StatusOK, fctx.Response.Header.StatusCode())
	utils.AssertEqual(b, "unreachable", string(fctx.Response.Header.Peek("X-Cache")))
}
