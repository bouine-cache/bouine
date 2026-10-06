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

// testHandlerBypassOnCookieNames builds a Handler with
// cache.bypass_on_cookie_names enabled (issue #768): only requests
// carrying one of the listed cookie names bypass.
func testHandlerBypassOnCookieNames(t *testing.T, upstream fasthttp.RequestHandler, names []string) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:            upstream,
		FastClient:          &testFastClient{handler: upstream},
		Store:               store,
		BypassOnCookieNames: names,
	})
}

// A request carrying one of the listed cookie names must bypass: never
// served from the store, never stored, even though an anonymous variant
// exists for the same URL.
func TestCookieNameBypass_ListedCookieBypasses(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerBypassOnCookieNames(t, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("page-for-" + string(ctx.Request.Header.Peek(header.Cookie))))
	}, []string{"session_id", "debug_bypass"})

	// Anonymous fill.
	anon := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Request with a listed cookie name: bypasses, gets its own body.
	cookied := testCtx("GET", "http://example.com/page")
	cookied.Request.Header.Set(header.Cookie, "session_id=user-a; theme=dark")
	h.ServeRequest(cookied)
	assert.Equal(t, "BYPASS", respHeader(cookied, header.XCache))
	assert.Equal(t, "page-for-session_id=user-a; theme=dark", respBody(cookied))

	// Its response was not stored: the next anonymous request still HITs
	// the anonymous variant, not the cookied body.
	anon2 := testCtx("GET", "http://example.com/page")
	h.ServeRequest(anon2)
	assert.Equal(t, "HIT", respHeader(anon2, header.XCache))
	assert.Equal(t, "page-for-", respBody(anon2))
}

// A request carrying only UNLISTED cookie names must NOT bypass: it
// participates in the cache per RFC 9111 (other-cookie conformance
// shape). This is the whole point of the knob — analytics cookies do
// not kill the hit rate.
func TestCookieNameBypass_UnlistedCookieHits(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	// Anonymous fill.
	anon := testCtx("GET", "http://example.com/opt")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	// Unlisted cookie: served the stored response (other-cookie optimal
	// case keeps applying on this route).
	unlisted := testCtx("GET", "http://example.com/opt")
	unlisted.Request.Header.Set(header.Cookie, "analytics=abc; theme=dark")
	h.ServeRequest(unlisted)
	assert.Equal(t, "HIT", respHeader(unlisted, header.XCache))
	assert.Equal(t, "shared", respBody(unlisted))
}

// Cookie-name matching must be case-insensitive (cookie names are
// case-insensitive per RFC 6265 §4.1.1) and must match the name token,
// never a substring or a value.
func TestCookieNameBypass_MatchingSemantics(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"Session_ID"})

	// Same name, different case: bypasses.
	cookied := testCtx("GET", "http://example.com/case")
	cookied.Request.Header.Set(header.Cookie, "SESSION_ID=xyz")
	h.ServeRequest(cookied)
	assert.Equal(t, "BYPASS", respHeader(cookied, header.XCache))

	// A cookie whose NAME merely contains the listed name as a
	// substring must NOT match ("session_id_v2", "mysession_id").
	partial := testCtx("GET", "http://example.com/case")
	partial.Request.Header.Set(header.Cookie, "session_id_v2=1; mysession_id=2")
	h.ServeRequest(partial)
	assert.NotEqual(t, "BYPASS", respHeader(partial, header.XCache))

	// The listed name appearing only as a VALUE must not match.
	valueOnly := testCtx("GET", "http://example.com/case")
	valueOnly.Request.Header.Set(header.Cookie, "ref=session_id")
	h.ServeRequest(valueOnly)
	assert.NotEqual(t, "BYPASS", respHeader(valueOnly, header.XCache))
}

// A request with no Cookie header at all is untouched by the knob.
func TestCookieNameBypass_NoCookieUnaffected(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	anon := testCtx("GET", "http://example.com/n")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	anon2 := testCtx("GET", "http://example.com/n")
	h.ServeRequest(anon2)
	assert.Equal(t, "HIT", respHeader(anon2, header.XCache))
}

// The fast path must decline requests carrying a listed cookie name so
// the h1parser falls through to the slow path's bypass branch — but
// keep serving requests with only unlisted cookies.
func TestCookieNameBypass_FastPathParity(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	anon := testCtx("GET", "http://example.com/fp")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	fp := NewFastPathHandler(h)

	listed := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	listed.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "session_id=u"}
	listed.NHeaders = 1
	_, ok := fp.TryHit(listed, time.Now())
	require.False(t, ok, "TryHit must decline a listed-cookie request")

	unlisted := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	unlisted.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "analytics=1"}
	unlisted.NHeaders = 1
	resp, ok := fp.TryHit(unlisted, time.Now())
	require.True(t, ok, "TryHit must serve requests with only unlisted cookies")
	require.NotNil(t, resp)
	fp.Release(resp)

	// Anonymous requests unaffected.
	noCookie := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	resp, ok = fp.TryHit(noCookie, time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	fp.Release(resp)
}

// The fast path must see a listed cookie spread across MULTIPLE Cookie
// header lines (the h1parser keeps every line; fasthttp joins them, so
// the slow path cannot exercise this shape).
func TestCookieNameBypass_FastPathMultipleCookieLines(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnCookieNames(t, origin200("shared"), []string{"session_id"})

	anon := testCtx("GET", "http://example.com/fpm")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	fp := NewFastPathHandler(h)
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/fpm",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	req.Headers[0] = api.RawHeader{Key: header.Cookie, Value: "analytics=1"}
	req.Headers[1] = api.RawHeader{Key: header.Cookie, Value: "session_id=u"}
	req.NHeaders = 2
	_, ok := fp.TryHit(req, time.Now())
	require.False(t, ok, "TryHit must decline when a listed cookie hides on the second Cookie line")
}
