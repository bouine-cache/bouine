package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestCollapseFlightKey pins the composite flight-key rule (ADR-0057) in
// isolation: a flight is shared only under a key that encodes every
// dimension the response is known to vary on. A stored object (with or
// without Vary) is the origin's own declaration — warm flights keep the
// lookup key; cold flights on a declared route extend the primary key
// with the declared headers (hashed as the storage variant key hashes
// them); cold flights with no declaration are refused (the zero key).
func TestCollapseFlightKey(t *testing.T) {
	t.Parallel()

	tenantPolicy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Tenant-Id"}, false)

	primary := api.NewKeyFromBytes([16]byte{1})
	variant := api.NewKeyFromBytes([16]byte{2})
	stored := &api.Object{}

	ri14 := RequestInfo{}
	ri14.Header.Set("X-Tenant-Id", "14")
	ri66 := RequestInfo{}
	ri66.Header.Set("X-Tenant-Id", "66")

	t.Run("warm flight keys on the lookup key, Vary or not", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(HandlerConfig{Policy: tenantPolicy})
		require.Equal(t, variant, collapseFlightKey(h, primary, variant, stored, ri14),
			"a stored variant's key already encodes the declared dimensions")
		require.Equal(t, primary, collapseFlightKey(h, primary, primary, stored, ri14),
			"a stored Vary-less object declares no selector header: the primary key is a proven identity")
		h = NewHandler(HandlerConfig{})
		require.Equal(t, primary, collapseFlightKey(h, primary, primary, stored, ri14),
			"undeclared route, stored Vary-less object: warm flights keep collapsing")
	})

	t.Run("cold flight on a declared route extends the key with the declared dimensions", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(HandlerConfig{Policy: tenantPolicy})
		k14 := collapseFlightKey(h, primary, primary, nil, ri14)
		k66 := collapseFlightKey(h, primary, primary, nil, ri66)
		require.NotEqual(t, api.Key{}, k14, "a declared route keeps a shared flight")
		require.NotEqual(t, k14, k66,
			"different declared-dimension callers must never share one flight")
		require.NotEqual(t, primary, k14,
			"the flight key must not be the bare primary key on a declared route")
		require.Equal(t, k14, collapseFlightKey(h, primary, primary, nil, ri14),
			"the flight key is a pure function of the declared dimensions")
	})

	t.Run("cold flight on an undeclared route is refused", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(HandlerConfig{})
		require.Equal(t, api.Key{}, collapseFlightKey(h, primary, primary, nil, ri14),
			"no shared key can be proven safe before anything declared the variation surface")
	})

	t.Run("declared but absent headers share one dimension-keyed flight", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(HandlerConfig{Policy: tenantPolicy})
		// Every declared header absent: each field still hashes a stable
		// "field=;" pair, so all absent-header callers share one flight
		// key — distinct from the primary key and from any value-carrying
		// flight.
		k := collapseFlightKey(h, primary, primary, nil, RequestInfo{Header: header.Map{}})
		require.NotEqual(t, api.Key{}, k)
		require.NotEqual(t, primary, k)
		require.Equal(t, k, collapseFlightKey(h, primary, primary, nil, RequestInfo{Header: header.Map{}}),
			"absent declared headers must be one deterministic flight key")
		require.NotEqual(t, k, collapseFlightKey(h, primary, primary, nil, ri14),
			"absent and present dimension values must never share a flight")
	})

	t.Run("flight key matches the storage variant key for the same dimensions", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(HandlerConfig{Policy: tenantPolicy})
		// The flight key must be the variant key the storage path would
		// compute for the same declared dimensions — the parity that
		// makes a shared flight provably serve one stored variant.
		//
		// NORMALIZATION INVARIANT (ADR-0057 §Risks, the language-
		// normalization trap): this parity is the ONLY thing that makes
		// dimension equality safe, and it holds because the flight key
		// routes every declared field through the SAME varyHeaderValue
		// normalization the storage variant key uses (bucketed
		// Accept-Encoding/Accept-Language, legacy lowercase+sort
		// otherwise). Any future change that normalizes values
		// DIFFERENTLY on one path — e.g. folding "fr-FR" and
		// "fr-fr" together for flight keying but not for storage, or
		// teaching one path a language tag equivalence the other does
		// not know — breaks the proof here: two requests whose
		// declared-dimension values differ only in the folded class
		// would share a flight whose stored variants are DISTINCT, and
		// the follower receives the leader's variant. The invariant is
		// semantic, not just byte parity: varyHeaderValue must remain
		// the single value-normalization authority for BOTH keys. A
		// change to varyHeaderValue's equivalence classes is safe; a
		// normalization that exists on only one path never is.
		require.Equal(t,
			VariantKey(primary, "x-tenant-id", ri14.Header, tenantPolicy),
			collapseFlightKey(h, primary, primary, nil, ri14))
	})

	t.Run("variant-miss flight keys on the stored variant key, not the include-only dimensions", func(t *testing.T) {
		t.Parallel()
		h := NewHandler(HandlerConfig{Policy: tenantPolicy})
		// A stored Vary resolver exists but this variant was evicted:
		// lookup returns obj=nil with the variant lookup key. The
		// variant key was derived from the STORED VaryValue (origin Vary
		// + includes), so it encodes strictly more than the include-only
		// dimensions — the flight must key on it, or callers the stored
		// Vary distinguishes (different origin-Vary field values) would
		// collapse onto one leader despite the includes matching.
		require.Equal(t, variant, collapseFlightKey(h, primary, variant, nil, ri14),
			"a variant-miss flight must key on the stored variant key")
	})
}

// TestCollapseFlightKey_CookiePresence pins the cookie-presence half of
// the declared-dimension keying: a route that keys stored variants on
// cookie presence (issue #768) must key the cold flight on the same
// dimension — presence bits only, never Cookie values — and callers
// with different presence sets must never share one flight.
func TestCollapseFlightKey_CookiePresence(t *testing.T) {
	t.Parallel()

	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false).WithCookiePresence([]string{"session", "consent"})

	primary := api.NewKeyFromBytes([16]byte{1})

	none := RequestInfo{Header: header.Map{}}
	session := RequestInfo{Header: header.Map{}}
	session.Header.Set(header.Cookie, "session=abc; other=x")
	consent := RequestInfo{Header: header.Map{}}
	consent.Header.Set(header.Cookie, "consent=1")

	h := NewHandler(HandlerConfig{Policy: policy})

	kNone := collapseFlightKey(h, primary, primary, nil, none)
	kSession := collapseFlightKey(h, primary, primary, nil, session)
	kConsent := collapseFlightKey(h, primary, primary, nil, consent)

	require.NotEqual(t, api.Key{}, kNone, "a presence-keyed route keeps a shared cold flight")
	require.NotEqual(t, primary, kNone, "the flight key must carry the presence dimension")
	require.NotEqual(t, kNone, kSession, "absent and present cookies must key different flights")
	require.NotEqual(t, kSession, kConsent, "different presence sets must key different flights")
	// Presence bits only: two callers with different Cookie VALUES but
	// the same presence set share one flight.
	samePresence := RequestInfo{Header: header.Map{}}
	samePresence.Header.Set(header.Cookie, "session=zzz; other=y")
	require.Equal(t, kSession, collapseFlightKey(h, primary, primary, nil, samePresence),
		"presence-bit keying must ignore cookie values")
	// Parity with the storage variant key for the same dimensions.
	require.Equal(t,
		VariantKey(primary, "x-bouine-cookie-presence", session.Header, policy),
		kSession)
}

// collapse_test.go pins the request-collapsing gate (ADR-0052 for
// Authorization, ADR-0054 for Cookie): a request carrying Authorization
// or Cookie never shares an in-flight origin response with any other
// request. The storage gates (RFC 9111 §3.5) never governed this path;
// these tests are the in-flight equivalent.

func TestCollapseDenied_AnonymousRequestsCollapse(t *testing.T) {
	t.Parallel()
	// No Authorization, no Cookie — the gate must not apply; anonymous
	// traffic keeps today's behavior bit-for-bit.
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

func TestCollapseDenied_CookiedRequestsNeverCollapse(t *testing.T) {
	t.Parallel()
	// ADR-0054: any Cookie value removes the request from the collapsing
	// space — a session cookie selects per-user SSR content, and "same
	// cookie string ⇒ same user" is exactly the assumption a shared
	// cache cannot verify (the ADR-0052 argument, applied to cookies).
	for _, c := range []string{"sid=abc", "session=x; theme=dark", "a=b"} {
		ri := RequestInfo{}
		ri.Header.Set(header.Cookie, c)
		require.True(t, collapseDenied(ri), "Cookie %q must never collapse", c)
	}
	// The gate composes: Authorization OR Cookie, either alone suffices.
	ri := RequestInfo{}
	ri.Header.Set(header.Authorization, "Bearer t")
	require.True(t, collapseDenied(ri))
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

// TestFetchAndStore_NoCrossCookieCollapse pins the ADR-0054 in-flight
// gate end to end: two concurrent misses for the same URI carrying
// different session cookies must each perform their own origin fetch
// and each receive their own user's SSR render. The origin echoes the
// Cookie back in the body, so a collapsed (broken) run returns the
// leader's page to the follower. This runs on a DEFAULT route — no
// bypass_on_cookie — because the collapse refusal is unconditional:
// it is the in-flight half of the cookie contract, independent of the
// per-route pass flag (the cache-tests other-cookie case is sequential
// serving from store, which this does not touch).
func TestFetchAndStore_NoCrossCookieCollapse(t *testing.T) {
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
		_, _ = ctx.WriteString("page:" + string(ctx.Request.Header.Peek(header.Cookie)))
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, sid := range []string{"sid=user-a", "sid=user-b"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/profile")
			ctx.Request.Header.Set(header.Cookie, sid)
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
		"callers with different Cookie values must not share a flight")
	require.Equal(t, "page:sid=user-a", bodies[0])
	require.Equal(t, "page:sid=user-b", bodies[1])
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

// TestFetchAndStore_UndeclaredRouteWarmFlightsStillCollapse pins the
// preserved behavior: the composite flight gate refuses only COLD misses
// on undeclared routes (no shared key can be proven safe before the
// origin's Vary is known). Once a fill stores the object — with or
// without Vary — warm flights collapse as before, so steady-state
// anonymous traffic keeps its dedup. First fill (1 fetch, sequential),
// then two concurrent revalidations of the same key collapse to one
// conditional request.
func TestFetchAndStore_UndeclaredRouteWarmFlightsStillCollapse(t *testing.T) {
	// synctest, not wall-clock sleeps: the assertion only holds if both
	// revalidations overlap the leader's in-flight singleflight entry,
	// whose window is the fetch's own duration. With a real clock the
	// follower's parking raced that microscopic window on loaded CI
	// runners (observed: 3 fetches). Here the origin blocks the second
	// fetch on a channel, so the bubble's fake clock cannot advance past
	// the park point and the sequence is deterministic: fill → expire →
	// both requests park → release → one shared fetch.
	synctest.Test(t, func(t *testing.T) {
		var fetches atomic.Int64
		release := make(chan struct{})
		origin := func(ctx *fasthttp.RequestCtx) {
			if fetches.Add(1) == 2 {
				// Hold the collapsed revalidation open until both
				// callers are parked (singleflight leader + follower).
				// The 5s timeout is a broken-run escape: a non-collapsed
				// run fails on the fetch-count assertion instead of
				// deadlocking the bubble.
				select {
				case <-release:
				case <-time.After(5 * time.Second):
				}
			}
			ctx.Response.Header.Set(header.CacheControl, "max-age=1, stale-while-revalidate=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("public")
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:   origin,
			FastClient: &testFastClient{handler: origin},
			Store:      store,
		})
		defer h.Close(context.Background())

		// Cold fill — sequential, allowed to fetch alone.
		seed := testCtx("GET", "http://example.com/public")
		serveRequest(h, seed)
		synctest.Wait()
		require.Equal(t, int64(1), fetches.Load())

		// Advance past the TTL so the two requests below are warm
		// revalidations (flight key = the stored key).
		time.Sleep(2 * time.Second)

		var wg sync.WaitGroup
		for range 2 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ctx := testCtx("GET", "http://example.com/public")
				serveRequest(h, ctx)
			}()
		}
		// Both callers are now durably blocked — the leader inside the
		// origin handler, the follower on the singleflight entry — so
		// Wait returns and releasing cannot race the park point.
		synctest.Wait()
		close(release)
		wg.Wait()

		require.Equal(t, int64(2), fetches.Load(),
			"concurrent warm revalidations of one key must collapse to a single origin fetch")
	})
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
			h.fetchAndStore(ctx, lookupKey, primaryKey, nil, requestInfoFromCtx(ctx))
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

// TestFetchAndStore_IncludeHeaderRouteColdMissNeverCrossesDimensions pins
// the declared-dimension half of the flight gate: on a route whose
// cache.key.include_headers names request headers, a cold miss (no stored
// object, so the shared key would be the primary key, which does not
// encode those headers) must not share a flight ACROSS dimension values.
// The origin echoes the tenant header, so a collapsed (broken) run would
// deliver the leader's tenant body to the follower. The two requests
// differ only in the include-listed header — byte-identical URI and Host
// — the exact shape the gate covers.
func TestFetchAndStore_IncludeHeaderRouteColdMissNeverCrossesDimensions(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Tenant-Id"}, false)

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
		_, _ = ctx.WriteString("tenant:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		Policy:     policy,
	})

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, tenant := range []string{"14", "66"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/public?x=1")
			ctx.Request.Header.Set("X-Tenant-Id", tenant)
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
		"a cold miss on an include_headers route must never share a flight across declared-dimension values")
	require.Equal(t, "tenant:14", bodies[0])
	require.Equal(t, "tenant:66", bodies[1])
}

// TestFetchAndStore_IncludeHeaderRouteSameDimensionsStillCollapse pins
// the dedup-preserving half of the composite rule: on the same declared
// route, two concurrent cold misses with the SAME declared-dimension
// values still collapse to one origin fetch. The flight key extends the
// primary key with the declared dimensions, so same-dimension callers
// meet on one flight; this is the cold-burst dedup option A restores.
func TestFetchAndStore_IncludeHeaderRouteSameDimensionsStillCollapse(t *testing.T) {
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
		_, _ = ctx.WriteString("same-variant")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		Policy:     policy,
	})

	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/public?x=1")
			ctx.Request.Header.Set("X-Tenant-Id", "14")
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
		"same-dimension cold misses must keep sharing one flight")
}

// TestFetchAndStore_UndeclaredRouteColdMissNeverCollapses pins the
// refusal half of the composite rule: on a route with NO include_headers,
// a cold miss never shares a flight — the origin's Vary is unknowable at
// flight time, so no shared key can be proven to encode the origin's
// variation surface. The two requests differ only in a header the route
// never declared (the undeclared-selector shape). Once the first fill
// stores the Vary resolver, warm traffic collapses on the variant key as
// before (covered by the revalidate/stayin-alive suites).
func TestFetchAndStore_UndeclaredRouteColdMissNeverCollapses(t *testing.T) {
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
		ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("tenant:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, tenant := range []string{"14", "66"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/public?x=1")
			ctx.Request.Header.Set("X-Tenant-Id", tenant)
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
		"a cold miss on an undeclared route must never share a flight: the origin's Vary is unknowable at flight time")
	require.Equal(t, "tenant:14", bodies[0])
	require.Equal(t, "tenant:66", bodies[1])
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
