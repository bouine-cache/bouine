package cache

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttp/fasthttpadaptor"
)

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
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		ctx.Request.SetBody(body)
	}
	//nolint:contextcheck // test shim — RequestCtx carries its own context
	h.ServeRequest(ctx)
	rr.WriteHeader(ctx.Response.StatusCode())
	//nolint:staticcheck // VisitAll deprecated but functional
	ctx.Response.Header.VisitAll(func(k, v []byte) {
		rr.Header().Add(string(k), string(v))
	})
	_, _ = rr.Write(ctx.Response.Body())
}

// wrapUpstream wraps an http.Handler as fasthttp.RequestHandler for tests.
func wrapUpstream(h http.Handler) fasthttp.RequestHandler {
	return fasthttpadaptor.NewFastHTTPHandler(h)
}

// wrapUpstreamFunc wraps an http.HandlerFunc as fasthttp.RequestHandler for tests.
func wrapUpstreamFunc(f func(http.ResponseWriter, *http.Request)) fasthttp.RequestHandler {
	return fasthttpadaptor.NewFastHTTPHandlerFunc(f)
}

// handlerFastClient wraps an http.Handler as a FastClient for tests.
// It converts the fasthttp.Request to an http.Request, calls the handler,
// and copies the response into the fasthttp.Response.
type handlerFastClient struct {
	handler http.Handler
}

func (c *handlerFastClient) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	uri := req.URI()
	requestURI := string(uri.RequestURI())
	rURL, err := url.ParseRequestURI(requestURI)
	if err != nil {
		return err
	}

	body := req.Body()
	httpReq := &http.Request{
		Method:        string(req.Header.Method()),
		URL:           rURL,
		Host:          string(uri.Host()),
		Body:          io.NopCloser(bytes.NewReader(body)),
		Header:        make(http.Header),
		RequestURI:    requestURI,
		ContentLength: int64(len(body)),
	}
	httpReq = httpReq.WithContext(ctx)
	//nolint:staticcheck // VisitAll deprecated but functional
	req.Header.VisitAll(func(k, v []byte) {
		httpReq.Header.Add(string(k), string(v))
	})

	rec := httptest.NewRecorder()
	done := make(chan struct{})
	var abortErr error
	var panicVal any
	go func() {
		defer func() {
			if r := recover(); r != nil {
				if r == http.ErrAbortHandler {
					abortErr = http.ErrAbortHandler
				} else {
					panicVal = r
				}
			}
			close(done)
		}()
		c.handler.ServeHTTP(rec, httpReq)
	}()

	select {
	case <-done:
		if panicVal != nil {
			panic(panicVal)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	if abortErr != nil {
		return abortErr
	}

	resp.SetStatusCode(rec.Code)
	for k, vs := range rec.Header() {
		for _, v := range vs {
			resp.Header.Add(k, v)
		}
	}
	if rec.Body.Len() > 0 {
		body := make([]byte, rec.Body.Len())
		copy(body, rec.Body.Bytes())
		resp.SetBody(body)
	}
	return nil
}
