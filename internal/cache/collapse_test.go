package cache

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// collapse_test.go pins the request-collapsing gate (ADR-0052): a
// request carrying Authorization never shares an in-flight origin
// response with any other request. The storage gates (RFC 9111 §3.5)
// never governed this path; these tests are the in-flight equivalent.

func TestCollapseDenied_AnonymousRequestsCollapse(t *testing.T) {
	t.Parallel()
	// No Authorization — the gate must not apply; anonymous traffic
	// keeps today's behavior bit-for-bit.
	require.False(t, collapseDenied(RequestInfo{}))
	require.False(t, collapseDenied(RequestInfo{Header: header.Map{}}))
}

func TestCollapseDenied_AuthorizedRequestsNeverCollapse(t *testing.T) {
	t.Parallel()
	// Any Authorization value — shared service JWT or per-user bearer —
	// removes the request from the collapsing space entirely. The value
	// itself is irrelevant: the strict rule does not try to prove two
	// identical credentials interchangeable.
	for _, cred := range []string{"Bearer token-a", "Bearer shared-service-jwt", "Basic dXNlcjpwYXNz"} {
		ri := RequestInfo{}
		ri.Header.Set(header.Authorization, cred)
		require.True(t, collapseDenied(ri), "Authorization %q must never collapse", cred)
	}
}

// TestCollapseDenied_UnsafeMethodsNeverCollapse pins the method half of
// the gate (ADR-0052, extended): POST/PUT/DELETE/PATCH requests never
// share a flight, in both RequestInfo forms. They never reach the
// collapsing paths today (ServeRequest dispatches them to
// invalidateAndProxy, which fetches directly), but that guarantee is
// purely structural — the gate makes it local so a future dispatcher
// refactor cannot silently coalesce mutations. The flight key is the
// cache key (method included), so two identical POSTs would otherwise
// merge onto one origin mutation and drop the follower's body.
func TestCollapseDenied_UnsafeMethodsNeverCollapse(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"POST", "PUT", "DELETE", "PATCH"} {
		ri := RequestInfo{Method: method}
		require.True(t, collapseDenied(ri), "%s (string form) must never collapse", method)

		// Byte form — what requestInfoFromCtx populates on the live
		// miss path; GetMethod() is never called (alloc budget).
		ctx := testCtx(method, "http://example.com/mut")
		riBytes := requestInfoFromCtx(ctx)
		require.True(t, collapseDenied(riBytes), "%s (byte form) must never collapse", method)
	}
}

// TestCollapseDenied_SafeMethodsStillEligible pins the safe set: GET,
// HEAD (which shares flight space with GET by key construction), and
// OPTIONS stay eligible for collapsing when anonymous — and stay
// eligible for the Authorization check (safe method + Authorization
// still denies).
func TestCollapseDenied_SafeMethodsStillEligible(t *testing.T) {
	t.Parallel()
	for _, method := range []string{"GET", "HEAD", "OPTIONS"} {
		require.False(t, collapseDenied(RequestInfo{Method: method}),
			"anonymous %s must stay eligible for collapsing", method)

		ri := RequestInfo{Method: method}
		ri.Header.Set(header.Authorization, "Bearer t")
		require.True(t, collapseDenied(ri), "%s + Authorization must never collapse", method)

		ctx := testCtx(method, "http://example.com/x")
		require.False(t, collapseDenied(requestInfoFromCtx(ctx)),
			"anonymous %s (byte form) must stay eligible for collapsing", method)
	}
}

// TestFetchAndStore_NoCrossCredentialCollapse pins the in-flight gate
// end to end: two concurrent misses for the same URI carrying different
// Authorization values must each perform their own origin fetch and
// each receive their own credentials' response. The origin echoes the
// Authorization back in the body, so a collapsed (broken) run returns
// the leader's body to the follower and fails the assertion.
func TestFetchAndStore_NoCrossCredentialCollapse(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		n := fetches.Add(1)
		if n <= 2 {
			// Hold the first two fetches open until both callers have
			// issued theirs. Escape keeps a broken run failing on the
			// fetch-count assertion instead of hanging the suite.
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("cred:" + string(ctx.Request.Header.Peek(header.Authorization)))
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, cred := range []string{"token-a", "token-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/private")
			ctx.Request.Header.Set(header.Authorization, cred)
			serveRequest(h, ctx)
			bodies[i] = respBody(ctx)
		}()
	}
	// Both callers park before either fetch resolves; open the gate once
	// both have started (poll the fetch counter).
	for fetches.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	wg.Wait()

	require.Equal(t, int64(2), fetches.Load(),
		"callers with different Authorization must not share a flight")
	require.Equal(t, "cred:token-a", bodies[0])
	require.Equal(t, "cred:token-b", bodies[1])
}

// TestFetchAndStore_SameCredentialAlsoNeverCollapses pins the strict
// rule: even identical Authorization values must not share a flight.
// A service-scoped credential can impersonate per-tenant callers
// selected by an undeclared custom header — the incident shape — and
// the cache cannot verify the credential is genuinely shared. The
// origin echoes the merchant header, so a collapsed (broken) run
// delivers the leader's tenant to the follower.
func TestFetchAndStore_SameCredentialAlsoNeverCollapses(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		n := fetches.Add(1)
		if n <= 2 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("merchant:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, merchant := range []string{"14", "66"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/ws/orders?state=8")
			// The production incident shape: one shared service JWT,
			// byte-identical URI, tenant selected by a custom header the
			// cache key does not include.
			ctx.Request.Header.Set(header.Authorization, "Bearer shared-service-jwt")
			ctx.Request.Header.Set("X-Tenant-Id", merchant)
			serveRequest(h, ctx)
			bodies[i] = respBody(ctx)
		}()
	}
	for fetches.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	wg.Wait()

	require.Equal(t, int64(2), fetches.Load(),
		"identical Authorization must still not share a flight: the strict rule assumes nothing about credential sharing")
	require.Equal(t, "merchant:14", bodies[0])
	require.Equal(t, "merchant:66", bodies[1])
}

// TestFetchAndStore_AnonymousUnchanged pins the default behavior: no
// Authorization — concurrent misses collapse to one fetch (regression
// guard for the fix; anonymous traffic is bit-for-bit unchanged).
func TestFetchAndStore_AnonymousUnchanged(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 1 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("public")
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/public")
			serveRequest(h, ctx)
		}()
	}
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	require.Equal(t, int64(1), fetches.Load(),
		"anonymous callers must keep collapsing")
}

// TestInvalidatingMethods_EachRequestFetchesItsOwnCopy pins the
// dispatcher-level behavior end to end: concurrent POST/PUT/DELETE
// requests for the same URI each perform their own origin fetch and
// each receive their own mutation's result. The origin echoes the
// request body back, so a collapsed (broken) run would deliver the
// leader's body to the follower and fail the assertion. This pins the
// dispatch (serveInvalidating → invalidateAndProxy fetches directly);
// the gate itself is pinned below with the dispatcher bypassed.
func TestInvalidatingMethods_EachRequestFetchesItsOwnCopy(t *testing.T) {
	t.Parallel()

	for _, method := range []string{"POST", "PUT", "DELETE"} {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			var fetches atomic.Int64
			release := make(chan struct{})
			origin := func(ctx *fasthttp.RequestCtx) {
				n := fetches.Add(1)
				if n <= 2 {
					select {
					case <-release:
					case <-time.After(2 * time.Second):
					}
				}
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("mutated:" + string(ctx.Request.Body()))
			}
			h := testHandler(t, origin)

			var wg sync.WaitGroup
			bodies := make([]string, 2)
			for i, body := range []string{"item-a", "item-b"} {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx := testCtxWithBody(method, "http://example.com/items", []byte(body))
					serveRequest(h, ctx)
					bodies[i] = respBody(ctx)
				}()
			}
			for fetches.Load() < 2 {
				time.Sleep(5 * time.Millisecond)
			}
			close(release)
			wg.Wait()

			require.Equal(t, int64(2), fetches.Load(),
				"each %s must perform its own origin fetch", method)
			require.Equal(t, "mutated:item-a", bodies[0])
			require.Equal(t, "mutated:item-b", bodies[1])
		})
	}
}

// TestFetchAndStore_UnsafeMethodNeverParksOnSharedFlight pins the
// collapse gate behind the dispatcher: fetchAndStore is the miss-path
// entry a dispatcher refactor would have to route a POST through, so
// the test calls it directly with an unsafe method and proves the
// follower never receives the leader's response. The origin echoes the
// body, so a collapsed (broken) run returns the leader's body to the
// follower.
func TestFetchAndStore_UnsafeMethodNeverParksOnSharedFlight(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		n := fetches.Add(1)
		if n <= 2 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body:" + string(ctx.Request.Body()))
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, body := range []string{"payload-1", "payload-2"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtxWithBody("POST", "http://example.com/owned", []byte(body))
			primaryKey, lookupKey, _, _ := h.lookup(ctx)
			h.fetchAndStore(ctx, primaryKey, lookupKey, requestInfoFromCtx(ctx))
			bodies[i] = respBody(ctx)
		}()
	}
	for fetches.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	close(release)
	wg.Wait()

	require.Equal(t, int64(2), fetches.Load(),
		"the gate must keep unsafe methods off the shared flight even behind the dispatcher")
	require.Equal(t, "body:payload-1", bodies[0])
	require.Equal(t, "body:payload-2", bodies[1])
}

// TestFetchAndStore_IncludeHeaderRouteAnonymousStillCollapses pins
// that include_headers alone does not disable collapsing for anonymous
// callers: the gate is Authorization-only (ADR-0052 scope note).
func TestFetchAndStore_IncludeHeaderRouteAnonymousStillCollapses(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Tenant-Id"}, false)

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 1 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("same-for-all")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		Policy:     policy,
	})

	var wg sync.WaitGroup
	for _, merchant := range []string{"14", "66"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/public?x=1")
			ctx.Request.Header.Set("X-Tenant-Id", merchant)
			serveRequest(h, ctx)
		}()
	}
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	require.Equal(t, int64(1), fetches.Load(),
		"anonymous callers on include_headers routes must keep collapsing")
}

// TestRevalidate_NoCrossCredentialCollapse pins the foreground
// revalidation path: two concurrent conditional requests with different
// Authorization must not share a revalidation flight.
func TestRevalidate_NoCrossCredentialCollapse(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	origin := func(ctx *fasthttp.RequestCtx) {
		fetches.Add(1)
		// First fill stores the object; subsequent requests revalidate
		// (no-cache forces the conditional path on every hit).
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body")
	}
	h := testHandler(t, origin)

	seed := testCtx("GET", "http://example.com/reval")
	serveRequest(h, seed)
	require.Equal(t, int64(1), fetches.Load())

	// Two concurrent revalidations with different credentials.
	var wg sync.WaitGroup
	for _, cred := range []string{"token-a", "token-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/reval")
			ctx.Request.Header.Set(header.Authorization, cred)
			serveRequest(h, ctx)
		}()
	}
	wg.Wait()

	// Seed + two revalidations: 3 fetches total (no shared flight).
	require.Equal(t, int64(3), fetches.Load(),
		"revalidation flights must split on different Authorization")
}

// TestRevalidate_SameCredentialAlsoNeverCollapses pins the strict rule
// on the revalidation path: identical credentials do not share a
// revalidation flight either.
func TestRevalidate_SameCredentialAlsoNeverCollapses(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	origin := func(ctx *fasthttp.RequestCtx) {
		fetches.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body")
	}
	h := testHandler(t, origin)

	seed := testCtx("GET", "http://example.com/reval")
	serveRequest(h, seed)
	require.Equal(t, int64(1), fetches.Load())

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/reval")
			ctx.Request.Header.Set(header.Authorization, "Bearer shared-jwt")
			serveRequest(h, ctx)
		}()
	}
	wg.Wait()

	require.Equal(t, int64(3), fetches.Load(),
		"identical Authorization must still not share a revalidation flight")
}
