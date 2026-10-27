package cache

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// swr_identity_regression_test.go pins issue #846 (audit AUD-01): a
// stale-while-revalidate background revalidation must never replay the
// triggering client's identity (Authorization, Cookie) to the origin,
// because the refreshed response is stored under the shared key. A
// session-aware origin would render personalized content for the
// triggering client, and every later client would be served it.

// TestSWRRevalidation_CookiedStaleHitDoesNotPoisonSharedEntry proves the
// slow-path gate: a cookied client receiving a stale hit must NOT trigger
// the background revalidation. The origin personalizes every response on
// the session cookie (SSR shape: session already established, so no
// Set-Cookie comes back) and marks the result explicitly cacheable — the
// exact shape that poisoned the shared entry before the gate. The stale
// entry is still served, and no fetch ever carries the client's cookie;
// the next anonymous request performs the refresh.
func TestSWRRevalidation_CookiedStaleHitDoesNotPoisonSharedEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var originCookies []string
		origin := func(ctx *fasthttp.RequestCtx) {
			cookie := string(ctx.Request.Header.Peek(header.Cookie))
			if cookie == "" {
				cookie = "anonymous"
			}
			originCookies = append(originCookies, cookie)
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("user:" + cookie)
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:   origin,
			FastClient: &testFastClient{handler: origin},
			Store:      store,
		})
		defer h.Close(context.Background())

		key := BuildKey(requestInfoFromURL("GET", "http://poison.example.com/profile"), nil)
		stale := &api.Object{
			Key:        key,
			StatusCode: 200,
			Header:     headerMap(header.CacheControl, "max-age=60"),
			Body:       []byte("user:anonymous"),
			BodySize:   int64(len("user:anonymous")),
			StoredAt:   time.Now().Add(-70 * time.Second),
			TTL:        60 * time.Second,
			// SWR window still open: the stale hit would normally spawn
			// the background revalidation.
			StaleWhileRevalidate: 60 * time.Second,
		}
		_ = h.store.Put(context.Background(), key, stale)

		// Client A carries a session cookie and is served the stale
		// shared copy (RFC 9111 permits this) — but must not trigger a
		// revalidation that replays its cookie.
		a := testCtx("GET", "http://poison.example.com/profile")
		a.Request.Header.Set(header.Cookie, "sid=A-secret-canary")
		h.ServeRequest(a)
		require.Equal(t, "STALE", respHeader(a, header.XCache))
		require.Equal(t, "user:anonymous", respBody(a), "client A is served the shared stale copy")

		// Let any (wrongly triggered) background revalidation run.
		synctest.Sleep(200 * time.Millisecond)
		require.Empty(t, originCookies,
			"the cookied client's stale hit must not trigger a background fetch")

		// Client B, anonymous, hits the same key. It must never see
		// client A's canary in status, headers, or body.
		b := testCtx("GET", "http://poison.example.com/profile")
		h.ServeRequest(b)
		assert.NotContains(t, respBody(b), "A-secret-canary",
			"client B received client A's personalized body via a SWR-refreshed shared entry")

		// The anonymous request may trigger the refresh itself — that
		// fetch is safe, but its response must still never carry the
		// canary.
		synctest.Sleep(200 * time.Millisecond)
		for _, c := range originCookies {
			assert.NotEqual(t, "sid=A-secret-canary", c,
				"a background fetch carried client A's session cookie")
		}
	})
}

// TestSWRRevalidation_AuthorizedStaleHitDoesNotTrigger pins the
// Authorization half of the gate: an authorized client receiving a stale
// hit does not trigger the revalidation, even when the origin's response
// opts into shared-cache storage (RFC 9111 §3.5 public) — an opted-in
// origin can still embed per-user content, and the refresh stores the
// result under the shared key.
func TestSWRRevalidation_AuthorizedStaleHitDoesNotTrigger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var originAuth []string
		origin := func(ctx *fasthttp.RequestCtx) {
			originAuth = append(originAuth, string(ctx.Request.Header.Peek(header.Authorization)))
			ctx.Response.Header.Set(header.CacheControl, "public, max-age=60")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("user:" + string(ctx.Request.Header.Peek(header.Authorization)))
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:   origin,
			FastClient: &testFastClient{handler: origin},
			Store:      store,
		})
		defer h.Close(context.Background())

		key := BuildKey(requestInfoFromURL("GET", "http://poison.example.com/data"), nil)
		_ = h.store.Put(context.Background(), key, &api.Object{
			Key:                  key,
			StatusCode:           200,
			Header:               headerMap(header.CacheControl, "public, max-age=60"),
			Body:                 []byte("user:anonymous"),
			BodySize:             int64(len("user:anonymous")),
			StoredAt:             time.Now().Add(-70 * time.Second),
			TTL:                  60 * time.Second,
			StaleWhileRevalidate: 60 * time.Second,
		})

		a := testCtx("GET", "http://poison.example.com/data")
		a.Request.Header.Set(header.Authorization, "Bearer A-secret-canary")
		h.ServeRequest(a)
		require.Equal(t, "STALE", respHeader(a, header.XCache))

		synctest.Sleep(200 * time.Millisecond)
		require.Empty(t, originAuth,
			"the authorized client's stale hit must not trigger a background fetch")

		b := testCtx("GET", "http://poison.example.com/data")
		h.ServeRequest(b)
		assert.NotContains(t, respBody(b), "A-secret-canary")
	})
}

// TestSWRRevalidation_CookiePresenceRouteStillRefreshes pins the
// exemption: on a cookie-presence-keyed route (cache.key.cookie_presence,
// issue #768) the Cookie header is a declared variant dimension, so the
// SWR refresh must keep carrying it — otherwise the refresh fetch would
// land in the wrong variant. The cookied client's stale hit still
// triggers the revalidation, the fetch carries the cookie, and the
// refreshed variant stays in place.
func TestSWRRevalidation_CookiePresenceRouteStillRefreshes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var originCookies []string
		origin := func(ctx *fasthttp.RequestCtx) {
			cookie := string(ctx.Request.Header.Peek(header.Cookie))
			if cookie == "" {
				cookie = "anonymous"
			}
			originCookies = append(originCookies, cookie)
			ctx.Response.Header.Set(header.CacheControl, "max-age=2, stale-while-revalidate=60")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("render:" + cookie)
		}
		h := testHandlerCookiePresence(t, origin, []string{"session"})
		// testHandlerCookiePresence owns the store; both background
		// loops (handler revalidation pool, store sweeper/reaper) must
		// be joined before the synctest bubble exits.
		defer h.store.Close(context.Background())
		defer h.Close(context.Background())

		// Fill the session-present variant through a real request.
		first := testCtx("GET", "http://example.com/page")
		first.Request.Header.Set(header.Cookie, "session=yes; analytics=1")
		h.ServeRequest(first)
		require.Equal(t, "MISS", respHeader(first, header.XCache))
		require.Equal(t, "render:session=yes; analytics=1", respBody(first))

		// Let the variant go stale inside its SWR window.
		synctest.Sleep(3 * time.Second)

		// The cookied client's stale hit triggers the refresh — the
		// fetch must carry the cookie (declared dimension).
		second := testCtx("GET", "http://example.com/page")
		second.Request.Header.Set(header.Cookie, "session=yes; analytics=1")
		h.ServeRequest(second)
		require.Equal(t, "STALE", respHeader(second, header.XCache))

		synctest.Sleep(200 * time.Millisecond)
		require.Len(t, originCookies, 2, "the presence-keyed refresh must have fetched")
		require.Equal(t, "session=yes; analytics=1", originCookies[1],
			"the presence-keyed refresh must replay the Cookie header (declared variant dimension)")

		// The refreshed variant is served from the store to the same
		// presence bits. Presence keying ignores cookie VALUES (issue
		// #768), so a different session value with the same presence
		// bit still lands on the session-present variant — the body
		// is the refreshed render, not a re-fetch.
		third := testCtx("GET", "http://example.com/page")
		third.Request.Header.Set(header.Cookie, "session=other-value")
		h.ServeRequest(third)
		require.Equal(t, "HIT", respHeader(third, header.XCache))
		require.Equal(t, "render:session=yes; analytics=1", respBody(third))
	})
}

// TestSWRTriggerAllowed pins the gate's truth table directly: anonymous
// requests and presence-route cookies pass, Authorization and plain
// cookies on non-presence routes are refused.
func TestSWRTriggerAllowed(t *testing.T) {
	t.Parallel()
	newH := func(policy *KeyPolicy) *Handler {
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		t.Cleanup(func() { _ = store.Close(context.Background()) })
		return NewHandler(HandlerConfig{Store: store, Policy: policy})
	}
	plain := newH(nil)
	presence := newH(NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"session"}))

	ri := func(kvs ...string) RequestInfo {
		m := header.NewMap(len(kvs) / 2)
		for i := 0; i+1 < len(kvs); i += 2 {
			m.Set(kvs[i], kvs[i+1])
		}
		return RequestInfo{Method: "GET", Header: m}
	}

	assert.True(t, plain.swrTriggerAllowed(ri()), "anonymous requests may trigger")
	assert.False(t, plain.swrTriggerAllowed(ri(header.Cookie, "sid=x")),
		"a cookied request on a non-presence route must not trigger")
	assert.False(t, plain.swrTriggerAllowed(ri(header.Authorization, "Bearer t")),
		"an authorized request must not trigger")
	assert.True(t, presence.swrTriggerAllowed(ri(header.Cookie, "session=1")),
		"a cookied request on a presence-keyed route may trigger (declared dimension)")
	assert.False(t, presence.swrTriggerAllowed(ri(header.Authorization, "Bearer t")),
		"an authorized request must not trigger even on a presence-keyed route")
}
