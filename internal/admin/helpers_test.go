package admin

import (
	"github.com/valyala/fasthttp"
)

// testCtx creates a fasthttp.RequestCtx for testing.
func testCtx(method, path string) *fasthttp.RequestCtx {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI("http://test" + path)
	ctx.Request.Header.SetHost("test")
	return ctx
}

// testCtxWithBody creates a fasthttp.RequestCtx with a body.
func testCtxWithBody(method, path string, body []byte) *fasthttp.RequestCtx {
	ctx := testCtx(method, path)
	ctx.Request.SetBody(body)
	return ctx
}

// testCtxWithAuth creates a fasthttp.RequestCtx with auth header.
//
//nolint:unparam // test helper
func testCtxWithAuth(method, path, token string) *fasthttp.RequestCtx {
	ctx := testCtx(method, path)
	if token != "" {
		ctx.Request.Header.Set("Authorization", "Bearer "+token)
	}
	return ctx
}

// testCtxWithBodyAuth creates a fasthttp.RequestCtx with body and auth.
//
//nolint:unparam // test helper
func testCtxWithBodyAuth(method, path string, body []byte, token string) *fasthttp.RequestCtx {
	ctx := testCtxWithBody(method, path, body)
	if token != "" {
		ctx.Request.Header.Set("Authorization", "Bearer "+token)
	}
	return ctx
}

// respBody returns the response body as a string.
func respBody(ctx *fasthttp.RequestCtx) string {
	return string(ctx.Response.Body())
}
