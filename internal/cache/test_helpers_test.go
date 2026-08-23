package cache

import (
	"context"
	"sync"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

var rctxPool = sync.Pool{
	New: func() any { return &fasthttp.RequestCtx{} },
}

// testFastClient wraps a fasthttp.RequestHandler as a FastClient for tests.
// It copies the request into a pooled RequestCtx, invokes the handler, and
// copies the response back into the provided *fasthttp.Response.
type testFastClient struct {
	handler fasthttp.RequestHandler
}

func (c *testFastClient) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rctx := rctxPool.Get().(*fasthttp.RequestCtx)
	req.CopyTo(&rctx.Request)
	done := make(chan struct{}, 1)
	var panicVal any
	go func() {
		defer func() {
			if r := recover(); r != nil {
				panicVal = r
			}
			done <- struct{}{}
		}()
		c.handler(rctx)
	}()
	select {
	case <-done:
		if panicVal != nil {
			rctx.Request.Reset()
			rctx.Response.Reset()
			rctx.ResetUserValues()
			rctxPool.Put(rctx)
			panic(panicVal)
		}
		rctx.Response.CopyTo(resp)
		rctx.Request.Reset()
		rctx.Response.Reset()
		rctx.ResetUserValues()
		rctxPool.Put(rctx)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// benchFastClient is a zero-overhead FastClient for benchmarks. It calls
// the handler directly without spawning a goroutine or allocating a channel,
// eliminating scheduler overhead that would dominate the profile and mask
// real handler cost. Tests that need context cancellation use testFastClient.
type benchFastClient struct {
	handler fasthttp.RequestHandler
}

func (c *benchFastClient) Do(_ context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	rctx := rctxPool.Get().(*fasthttp.RequestCtx)
	req.CopyTo(&rctx.Request)
	c.handler(rctx)
	rctx.Response.CopyTo(resp)
	rctx.Request.Reset()
	rctx.Response.Reset()
	rctx.ResetUserValues()
	rctxPool.Put(rctx)
	return nil
}

// testCtx creates a *fasthttp.RequestCtx from method and URL.
func testCtx(method, url string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI(url)
	return ctx
}

// testCtxWithBody creates a *fasthttp.RequestCtx with a request body.
func testCtxWithBody(method, url string, body []byte) *fasthttp.RequestCtx {
	ctx := testCtx(method, url)
	ctx.Request.SetBody(body)
	return ctx
}

// testCtxWithHeader creates a *fasthttp.RequestCtx with a request header set.
func testCtxWithHeader(method, url, key, value string) *fasthttp.RequestCtx {
	ctx := testCtx(method, url)
	ctx.Request.Header.Set(key, value)
	return ctx
}

// serveRequest calls h.ServeRequest(ctx).
func serveRequest(h *Handler, ctx *fasthttp.RequestCtx) {
	h.ServeRequest(ctx)
}

// respCode returns the response status code from a RequestCtx.
func respCode(ctx *fasthttp.RequestCtx) int {
	return ctx.Response.StatusCode()
}

// respHeader returns a response header value from a RequestCtx.
func respHeader(ctx *fasthttp.RequestCtx, key string) string {
	return string(ctx.Response.Header.Peek(key))
}

// respBody returns the response body as a string from a RequestCtx.
func respBody(ctx *fasthttp.RequestCtx) string {
	return string(ctx.Response.Body())
}

// headerMap builds a header.Map from key-value pairs.
func headerMap(kvs ...string) header.Map {
	m := header.NewMap(len(kvs) / 2)
	for i := 0; i+1 < len(kvs); i += 2 {
		m.Set(kvs[i], kvs[i+1])
	}
	return m
}
