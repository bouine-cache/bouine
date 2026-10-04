package cache

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"
)

// testHandlerBypassOnCookie builds a Handler with
// cache.bypass_on_cookie enabled (ADR-0054).
func testHandlerBypassOnCookie(t *testing.T, upstream fasthttp.RequestHandler) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:       upstream,
		FastClient:     &testFastClient{handler: upstream},
		Store:          store,
		BypassOnCookie: true,
	})
}

// A cookied request on a bypass_on_cookie route must never be served
// from the store: user A's cached page must not reach user B even
// though user B's request URL matches.
func TestCookieBypass_CookiedRequestNeverServedFromCache(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerBypassOnCookie(t, func(ctx *fasthttp.RequestCtx) {
		// The origin echoes the request cookie: different users get
		// different bodies — the SSR personalization shape.
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("page-for-" + string(ctx.Request.Header.Peek(header.Cookie))))
	})

	// Anonymous request populates the cache.
	anon := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon)
	require.Equal(t, 200, respCode(anon))
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Cookied request for the same URL: must bypass — never the
	// stored anonymous copy.
	cookied := testCtx("GET", "http://example.com/page")
	cookied.Request.Header.Set(header.Cookie, "sid=user-b")
	h.ServeRequest(cookied)
	require.Equal(t, 200, respCode(cookied))
	assert.Equal(t, "BYPASS", respHeader(cookied, header.XCache))
	assert.Equal(t, "page-for-sid=user-b", respBody(cookied))

	// A second cookied request also bypasses (nothing was stored
	// that could serve it, and the bypass branch does not store).
	cookied2 := testCtx("GET", "http://example.com/page")
	cookied2.Request.Header.Set(header.Cookie, "sid=user-c")
	h.ServeRequest(cookied2)
	assert.Equal(t, "BYPASS", respHeader(cookied2, header.XCache))
	assert.Equal(t, "page-for-sid=user-c", respBody(cookied2))

	// Anonymous requests still HIT the anonymous variant.
	anon2 := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon2)
	assert.Equal(t, "HIT", respHeader(anon2, header.XCache))

	// One anonymous MISS + two cookied bypasses = 3 origin calls; the
	// second anonymous request HITs (0 calls).
	assert.Equal(t, int32(3), originCalls.Load())
}

// A cookied request's response must never be stored: a subsequent
// anonymous request must not receive the cookied user's body.
func TestCookieBypass_CookiedResponseNeverStored(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookie(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body-from-" + string(ctx.Request.Header.Peek(header.Cookie))))
	})

	// Cookied request fills nothing.
	cookied := testCtx("GET", "http://example.com/x")
	cookied.Request.Header.Set(header.Cookie, "sid=user-a")
	h.ServeRequest(cookied)
	require.Equal(t, "BYPASS", respHeader(cookied, header.XCache))

	// Anonymous request for the same key: must be a MISS fetching
	// its own body — not user-a's.
	anon := testCtx("GET", "http://example.com/x")
	h.ServeRequest(anon)
	assert.Equal(t, "MISS", respHeader(anon, header.XCache))
	assert.Equal(t, "body-from-", respBody(anon))
}

// Concurrent cookied misses must not share in-flight fetches: the
// leader-follower singleflight handoff must never deliver one user's
// body to another. Each cookied request pays its own origin fetch.
func TestCookieBypass_CookiedRequestsNeverCollapse(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookie(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body-" + string(ctx.Request.Header.Peek(header.Cookie))))
	})

	done := make(chan *fasthttp.RequestCtx, 2)
	for _, sid := range []string{"user-a", "user-b"} {
		go func(sid string) {
			ctx := testCtx("GET", "http://example.com/concurrent")
			ctx.Request.Header.Set(header.Cookie, "sid="+sid)
			h.ServeRequest(ctx)
			done <- ctx
		}(sid)
	}

	first := <-done
	second := <-done
	// Each response must carry its own cookie's body.
	bodies := map[string]bool{
		string(first.Response.Body()):  true,
		string(second.Response.Body()): true,
	}
	require.Len(t, bodies, 2, "each cookied request must receive its own origin response")
	for _, sid := range []string{"user-a", "user-b"} {
		assert.True(t, bodies["body-sid="+sid], "user %s's body missing", sid)
	}
}

// Flag-off routes keep RFC 9111 semantics: the cache-tests other-cookie
// optimal case (a cookied request is served a stored fresh response)
// must keep passing on default routes.
func TestCookieBypass_OffKeepsOtherCookieConformance(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandler(t, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		origin200("shared")(ctx)
	})

	// Anonymous fill.
	anon := testCtx("GET", "http://example.com/opt")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Cookied request served the cached response (other-cookie).
	cookied := testCtx("GET", "http://example.com/opt")
	cookied.Request.Header.Set(header.Cookie, "a=b")
	h.ServeRequest(cookied)
	assert.Equal(t, "HIT", respHeader(cookied, header.XCache))
	assert.Equal(t, "shared", respBody(cookied))
	assert.Equal(t, int32(1), originCalls.Load())
}

// An empty Cookie header value must NOT trigger the bypass: fasthttp
// normalizes a bare "Cookie:" to no header, and the guard treats any
// non-empty value as the trigger. A cookied request on an SSE-intent
// fetch keeps handleSSE semantics (live stream, never buffered).
func TestCookieBypass_EmptyCookieDoesNotBypass(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookie(t, origin200("shared"))

	anon := testCtx("GET", "http://example.com/e")
	h.ServeRequest(anon)

	empty := testCtx("GET", "http://example.com/e")
	empty.Request.Header.Set(header.Cookie, "")
	h.ServeRequest(empty)
	assert.Equal(t, "HIT", respHeader(empty, header.XCache))
}

// The fast path must decline cookied requests on bypass_on_cookie
// routes so the h1parser falls through to the slow path's bypass
// branch — never serving a stored hit.
func TestCookieBypass_FastPathDeclinesCookiedRequests(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookie(t, origin200("shared"))

	// Populate the store via the slow path.
	anon := testCtx("GET", "http://example.com/fp")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	fp := NewFastPathHandler(h)

	// Cookied request: TryHit must decline.
	cookied := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	cookied.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "sid=u"}
	cookied.NHeaders = 1
	_, ok := fp.TryHit(cookied, time.Now())
	require.False(t, ok, "TryHit must decline a cookied request on a bypass_on_cookie route")

	// Anonymous request: TryHit serves the stored hit.
	anonReq := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	resp, ok := fp.TryHit(anonReq, time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	fp.Release(resp)

	// Flag-off handler: cookied requests take the normal fast path
	// (other-cookie conformance preserved).
	hOff := testHandler(t, origin200("shared"))
	anonOff := testCtx("GET", "http://example.com/off")
	hOff.ServeRequest(anonOff)
	fpOff := NewFastPathHandler(hOff)
	cookiedOff := &api.RawRequest{
		Method:      "GET",
		Path:        "/off",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	cookiedOff.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "sid=u"}
	cookiedOff.NHeaders = 1
	resp, ok = fpOff.TryHit(cookiedOff, time.Now())
	require.True(t, ok, "flag-off route must serve cookied requests normally")
	require.NotNil(t, resp)
	fpOff.Release(resp)
}
