package cache

import (
	"context"
	"net/http"
	"net/http/httptest"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

// wrapUpstream wraps an http.Handler as fasthttp.RequestHandler for tests.
func wrapUpstream(h http.Handler) fasthttp.RequestHandler {
	return fasthttpadaptor.NewFastHTTPHandler(h)
}

// wrapUpstreamFunc wraps an http.HandlerFunc as fasthttp.RequestHandler for tests.
func wrapUpstreamFunc(f func(http.ResponseWriter, *http.Request)) fasthttp.RequestHandler {
	return fasthttpadaptor.NewFastHTTPHandlerFunc(f)
}

// ServeHTTPCompat is a test-only shim that wraps ServeRequest for tests
// using httptest.ResponseRecorder. Not for production use.
func (h *Handler) ServeHTTPCompat(rr *httptest.ResponseRecorder, r *http.Request) {
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(r.Method)
	ctx.Request.SetRequestURI(r.URL.String())
	ctx.Request.Header.SetHost(r.Host)
	for k, vs := range r.Header {
		for _, v := range vs {
			ctx.Request.Header.Add(k, v)
		}
	}
	h.ServeRequest(ctx)
	rr.WriteHeader(ctx.Response.StatusCode())
	for k, v := range ctx.Response.Header.All() {
		rr.Header().Add(string(k), string(v))
	}
	_, _ = rr.Write(ctx.Response.Body())
}

// urlFastClient implements FastClient by rewriting the request URI to baseURL.
type urlFastClient struct {
	client  *fasthttp.Client
	baseURL string
}

func (c *urlFastClient) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	uri := req.URI()
	req.SetRequestURI(c.baseURL + string(uri.Path()) + "?" + string(uri.QueryString()))
	return c.client.Do(req, resp)
}
