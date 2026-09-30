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
		_, _ = ctx.WriteString("merchant:" + string(ctx.Request.Header.Peek("X-BM-Merchant-Id")))
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
			ctx.Request.Header.Set("X-BM-Merchant-Id", merchant)
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

// TestFetchAndStore_IncludeHeaderRouteAnonymousStillCollapses pins
// that include_headers alone does not disable collapsing for anonymous
// callers: the gate is Authorization-only (ADR-0052 scope note).
func TestFetchAndStore_IncludeHeaderRouteAnonymousStillCollapses(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-BM-Merchant-Id"}, false)

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
			ctx.Request.Header.Set("X-BM-Merchant-Id", merchant)
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
