package origin

import (
	"github.com/valyala/fasthttp"
)

// newEchoHandler returns a fasthttp.RequestHandler that echoes the
// request body and sets X-Echo-Host and X-Echo-Path headers.
func newEchoHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set("X-Echo-Host", string(ctx.Host()))
		ctx.Response.Header.Set("X-Echo-Path", string(ctx.Path()))
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write(ctx.PostBody())
	}
}

// new5xxHandler returns a fasthttp.RequestHandler that always returns 500.
func new5xxHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
	}
}
