package cache

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func TestHandler_PUTProxiesBodyCorrectly(t *testing.T) {
	t.Parallel()
	var gotMethod, gotPath, gotBody string
	upstream := func(ctx *fasthttp.RequestCtx) {
		gotMethod = string(ctx.Method())
		gotPath = string(ctx.Path())
		gotBody = string(ctx.Request.Body())
		ctx.SetStatusCode(201)
		_, _ = ctx.Write([]byte("OK"))
	}
	h := testHandler(t, upstream)

	body := `{"response_headers":[[header.CacheControl,"max-age=60"]]}`
	rr := testCtxWithBody("PUT", "http://example.com/config/test-uuid", []byte(body))
	rr.Request.Header.Set(header.ContentType, "application/json")
	h.ServeRequest(rr)

	require.Equal(t, 201, respCode(rr))
	require.Equal(t, "PUT", gotMethod)
	require.Equal(t, "/config/test-uuid", gotPath)
	require.Equal(t, body, gotBody)
}

func TestHandler_GETAfterPUTConfigSetup(t *testing.T) {
	t.Parallel()
	var configuredBody string
	upstream := func(ctx *fasthttp.RequestCtx) {
		switch {
		case string(ctx.Method()) == "PUT" && len(ctx.Path()) > 8 && string(ctx.Path()[:8]) == "/config/":
			configuredBody = string(ctx.Request.Body())
			ctx.SetStatusCode(201)
			_, _ = ctx.Write([]byte("OK"))
		case string(ctx.Method()) == "GET" && len(ctx.Path()) > 6 && string(ctx.Path()[:6]) == "/test/":
			ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("test-response"))
		default:
			ctx.SetStatusCode(404)
		}
	}
	h := testHandler(t, upstream)

	putRR := testCtxWithBody("PUT", "http://example.com/config/abc123", []byte(`{"test":"data"}`))
	putRR.Request.Header.Set(header.ContentType, "application/json")
	h.ServeRequest(putRR)
	require.Equal(t, 201, respCode(putRR))
	require.Equal(t, `{"test":"data"}`, configuredBody)

	getRR := testCtx("GET", "http://example.com/test/abc123")
	h.ServeRequest(getRR)
	require.Equal(t, 200, respCode(getRR))
	require.Equal(t, "test-response", respBody(getRR))
}
