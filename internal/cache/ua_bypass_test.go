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

// testHandlerBypassOnUA builds a Handler with cache.bypass_on_user_agent
// patterns compiled in (issue #771, ADR-0055).
func testHandlerBypassOnUA(t *testing.T, patterns []string, upstream fasthttp.RequestHandler) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	return NewHandler(HandlerConfig{
		Upstream:          upstream,
		FastClient:        &testFastClient{handler: upstream},
		Store:             store,
		BypassOnUserAgent: patterns,
	})
}

// TestUABypass_Matcher pins the glob matching model: exact patterns
// match the whole User-Agent string (never a substring), `*` matches
// any run of bytes including `/`, and comparison is ASCII-case-
// insensitive. Both entry points (bytes: slow path, string: fast
// path) must agree.
func TestUABypass_Matcher(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		pattern string
		ua      string
		want    bool
	}{
		{"exact match", "ShoppingFeedBot", "ShoppingFeedBot", true},
		{"exact is not substring", "ShoppingFeedBot", "Mozilla/5.0 ShoppingFeedBot/1.0", false},
		{"case-insensitive exact", "shoppingfeedbot", "ShoppingFeedBot/1.0", false},
		{"prefix glob", "ShoppingFeedBot*", "ShoppingFeedBot/1.0 (+https://example.com)", true},
		{"prefix glob case-insensitive", "shoppingfeedbot/*", "ShoppingFeedBot/1.0", true},
		{"suffix glob", "*ShoppingFeedBot", "feed ShoppingFeedBot", true},
		{"substring glob", "*ShoppingFeedBot*", "Mozilla/5.0 (compatible; ShoppingFeedBot/1.0)", true},
		{"substring glob case-insensitive", "*shoppingfeedbot*", "Mozilla/5.0 SHOPPINGFEEDBOT/2.1", true},
		{"star matches slash", "Bot/*", "Bot/2.1", true},
		{"two-sided glob", "*Bot*", "SomeBot/1", true},
		{"middle segment in order", "Mozilla/*ShoppingFeedBot*", "Mozilla/5.0 (compatible; ShoppingFeedBot/1.0)", true},
		{"middle segment missing", "Mozilla/*OtherBot*", "Mozilla/5.0 (compatible; ShoppingFeedBot/1.0)", false},
		{"middle segments order matters", "*a*b*", "xaxb", true},
		{"middle segments out of order", "*b*a*", "xaxb", false},
		{"no match different product", "*Googlebot*", "Mozilla/5.0 (compatible; ShoppingFeedBot/1.0)", false},
		{"empty UA never matches exact", "ShoppingFeedBot", "", false},
		{"empty UA never matches substring glob", "*Bot*", "", false},
		{"multi-segment glob full coverage", "*a*a*", "aaa", true},
		{"multi-segment glob exhausted", "*a*a*", "a", false},
		{"non-ASCII compared byte-exact", "*Café*", "Café Bot", true},
		{"non-ASCII fold does not over-match", "*CAFÉ*", "Café Bot", false},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			m := compileUABypass([]string{tc.pattern})
			require.NotNil(t, m)
			assert.Equal(t, tc.want, m.matchBytes([]byte(tc.ua)), "matchBytes(%q)", tc.ua)
			assert.Equal(t, tc.want, m.matchString(tc.ua), "matchString(%q)", tc.ua)
		})
	}
}

// compileUABypass must return a nil matcher (route unchanged) for an
// empty list, and must defensively skip empty patterns and any
// pattern made only of `*` wildcards (lone `*`, `**`, `***`) that
// bypassed config validation (hand-built HandlerConfig) — none of
// them may compile into a match-everything glob.
func TestUABypass_CompileNilAndDefensive(t *testing.T) {
	t.Parallel()
	assert.Nil(t, compileUABypass(nil))
	assert.Nil(t, compileUABypass([]string{}))
	assert.Nil(t, compileUABypass([]string{"", "*", "**", "***"}))
	// A wildcard-only entry is dropped, not fatal: the real pattern
	// beside it still compiles and nothing matches everything.
	m := compileUABypass([]string{"**", "Bot"})
	require.NotNil(t, m)
	assert.True(t, m.matchString("Bot"))
	assert.False(t, m.matchString("Other"))
	assert.False(t, m.matchString("anything at all"), "no compiled glob may match every request")
}

// A matching-UA request on a bypass_on_user_agent route must never be
// served from the store: the crawler always sees a fresh origin
// response, and the crawler's response is never stored (a later
// normal request must not receive the crawler's body).
func TestUABypass_MatchingRequestNeverServedFromCache(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerBypassOnUA(t, []string{"*ShoppingFeedBot*"}, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		if string(ctx.Request.Header.Peek(header.UserAgent)) != "" {
			_, _ = ctx.Write([]byte("fresh-for-crawler"))
		} else {
			_, _ = ctx.Write([]byte("shared"))
		}
	})

	// Crawler request: bypass, fresh origin body, attributed BYPASS.
	crawler := testCtxWithHeader("GET", "http://example.com/feed.xml", header.UserAgent, "ShoppingFeedBot/1.0")
	h.ServeRequest(crawler)
	require.Equal(t, 200, respCode(crawler))
	assert.Equal(t, "BYPASS", respHeader(crawler, header.XCache))
	assert.Equal(t, "fresh-for-crawler", respBody(crawler))

	// A second crawler request also bypasses: nothing was stored that
	// could serve it, and the bypass branch does not store.
	crawler2 := testCtxWithHeader("GET", "http://example.com/feed.xml", header.UserAgent, "ShoppingFeedBot/2.0")
	h.ServeRequest(crawler2)
	assert.Equal(t, "BYPASS", respHeader(crawler2, header.XCache))
	assert.Equal(t, "fresh-for-crawler", respBody(crawler2))

	// Normal request: MISS with its own body — the crawler's response
	// was never stored under the shared key.
	normal := testCtx("GET", "http://example.com/feed.xml")
	h.ServeRequest(normal)
	assert.Equal(t, "MISS", respHeader(normal, header.XCache))
	assert.Equal(t, "shared", respBody(normal))

	// Second normal request HITs, and a further crawler request still
	// bypasses the now-warm entry.
	normal2 := testCtx("GET", "http://example.com/feed.xml")
	h.ServeRequest(normal2)
	assert.Equal(t, "HIT", respHeader(normal2, header.XCache))
	crawler3 := testCtxWithHeader("GET", "http://example.com/feed.xml", header.UserAgent, "ShoppingFeedBot/1.0")
	h.ServeRequest(crawler3)
	assert.Equal(t, "BYPASS", respHeader(crawler3, header.XCache))
	assert.Equal(t, "fresh-for-crawler", respBody(crawler3))

	// 3 crawler fetches (each bypass fetches) + 1 normal miss = 4
	// origin calls; the second normal request HITs (no call).
	assert.Equal(t, int32(4), originCalls.Load())
}

// Non-matching User-Agents on a configured route keep full cache
// semantics (MISS then HIT), and pattern-less routes are unchanged
// even when the request carries a User-Agent.
func TestUABypass_NonMatchAndFlagOffCacheNormally(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerBypassOnUA(t, []string{"*ShoppingFeedBot*"}, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		origin200("shared")(ctx)
	})

	first := testCtxWithHeader("GET", "http://example.com/p", header.UserAgent, "Mozilla/5.0 (compatible; Googlebot/2.1)")
	h.ServeRequest(first)
	require.Equal(t, "MISS", respHeader(first, header.XCache))

	second := testCtxWithHeader("GET", "http://example.com/p", header.UserAgent, "Mozilla/5.0 (compatible; Googlebot/2.1)")
	h.ServeRequest(second)
	assert.Equal(t, "HIT", respHeader(second, header.XCache))
	assert.Equal(t, int32(1), originCalls.Load())

	// Pattern-less route: a UA-carrying request participates in the
	// cache exactly as before (no UA-conditioned behavior at all).
	var offCalls atomic.Int32
	hOff := testHandler(t, func(ctx *fasthttp.RequestCtx) {
		offCalls.Add(1)
		origin200("shared")(ctx)
	})
	offFirst := testCtxWithHeader("GET", "http://example.com/off", header.UserAgent, "ShoppingFeedBot/1.0")
	hOff.ServeRequest(offFirst)
	require.Equal(t, "MISS", respHeader(offFirst, header.XCache))
	offSecond := testCtxWithHeader("GET", "http://example.com/off", header.UserAgent, "ShoppingFeedBot/1.0")
	hOff.ServeRequest(offSecond)
	assert.Equal(t, "HIT", respHeader(offSecond, header.XCache))
	assert.Equal(t, int32(1), offCalls.Load())
}

// Concurrent matching-UA requests must not share in-flight fetches:
// the bypass branch runs before the miss/collapse machinery, so every
// crawler request performs its own origin fetch.
func TestUABypass_MatchingRequestsNeverCollapse(t *testing.T) {
	t.Parallel()
	var originCalls atomic.Int32
	h := testHandlerBypassOnUA(t, []string{"*Bot*"}, func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("fresh"))
	})

	done := make(chan *fasthttp.RequestCtx, 4)
	for i := 0; i < 4; i++ {
		go func() {
			ctx := testCtxWithHeader("GET", "http://example.com/concurrent", header.UserAgent, "Bot/1.0")
			h.ServeRequest(ctx)
			done <- ctx
		}()
	}
	for i := 0; i < 4; i++ {
		ctx := <-done
		assert.Equal(t, "BYPASS", respHeader(ctx, header.XCache))
	}
	assert.Equal(t, int32(4), originCalls.Load(), "each matching request must perform its own origin fetch")
}

// Invalidating methods (POST/PUT/DELETE) must keep invalidating the
// shared GET key regardless of a matching User-Agent (ADR-0054 keeps
// the same ordering): serveInvalidating runs before the bypass branch
// by design, and the UA bypass must not weaken it.
func TestUABypass_InvalidatingMethodStillInvalidates(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnUA(t, []string{"*Bot*"}, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60, public")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("v1"))
	})

	// Fill the GET key.
	get := testCtx("GET", "http://example.com/r")
	h.ServeRequest(get)
	require.Equal(t, "MISS", respHeader(get, header.XCache))

	// POST with a matching UA: proxied AND invalidating — the stored
	// GET entry must be purged, not preserved.
	post := testCtxWithBody("POST", "http://example.com/r", []byte("x"))
	post.Request.Header.Set(header.UserAgent, "Bot/1.0")
	h.ServeRequest(post)
	require.Equal(t, 200, respCode(post))

	// The next GET must be a MISS (re-fetch), not a HIT of the purged
	// entry.
	get2 := testCtx("GET", "http://example.com/r")
	h.ServeRequest(get2)
	assert.Equal(t, "MISS", respHeader(get2, header.XCache))
}

// The fast path must decline matching-UA requests on configured routes
// so the h1parser falls through to the slow path's bypass branch —
// never serving a stored hit. Non-matching UAs and pattern-less routes
// keep the normal fast path.
func TestUABypass_FastPathDeclinesMatchingRequests(t *testing.T) {
	t.Parallel()
	h := testHandlerBypassOnUA(t, []string{"*ShoppingFeedBot*"}, origin200("shared"))

	// Populate the store via the slow path.
	anon := testCtx("GET", "http://example.com/fp")
	h.ServeRequest(anon)
	require.Equal(t, "MISS", respHeader(anon, header.XCache))

	fp := NewFastPathHandler(h)

	// Matching UA: TryHit must decline.
	matching := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	matching.Headers[0] = api.RawHeader{Key: header.UserAgent, Value: "ShoppingFeedBot/1.0"}
	matching.NHeaders = 1
	_, ok := fp.TryHit(matching, time.Now())
	require.False(t, ok, "TryHit must decline a matching-UA request on a bypass_on_user_agent route")

	// Non-matching UA: TryHit serves the stored hit.
	other := &api.RawRequest{
		Method:      "GET",
		Path:        "/fp",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	other.Headers[0] = api.RawHeader{Key: header.UserAgent, Value: "Googlebot/2.1"}
	other.NHeaders = 1
	resp, ok := fp.TryHit(other, time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	fp.Release(resp)

	// Pattern-less handler: UA-carrying requests take the normal fast
	// path.
	hOff := testHandler(t, origin200("shared"))
	anonOff := testCtx("GET", "http://example.com/off")
	hOff.ServeRequest(anonOff)
	fpOff := NewFastPathHandler(hOff)
	uReq := &api.RawRequest{
		Method:      "GET",
		Path:        "/off",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	uReq.Headers[0] = api.RawHeader{Key: header.UserAgent, Value: "ShoppingFeedBot/1.0"}
	uReq.NHeaders = 1
	resp, ok = fpOff.TryHit(uReq, time.Now())
	require.True(t, ok, "pattern-less route must serve UA-carrying requests normally")
	require.NotNil(t, resp)
	fpOff.Release(resp)
}

// The zero-alloc gate (AGENTS.md §2.4): pattern-less routes' hit path
// is unchanged — one nil check, no Peek, no allocation. Not parallel:
// AllocsPerRun cannot run during parallel tests.
func TestUABypass_FlagOffHitPathZeroAllocs(t *testing.T) {
	upstream := origin200("shared")
	h := testHandler(t, upstream)

	ctx := testCtxWithHeader("GET", "http://example.com/za", header.UserAgent, "Mozilla/5.0")
	h.ServeRequest(ctx)
	require.Equal(t, "MISS", respHeader(ctx, header.XCache))

	allocs := testing.AllocsPerRun(100, func() {
		ctx.Response.Reset()
		h.ServeRequest(ctx)
	})
	assert.Equal(t, float64(0), allocs, "flag-off hit path must stay zero-alloc with a User-Agent present")
}
