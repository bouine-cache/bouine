package cache

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// This file pins the client-state rule (ADR-0067): a response carrying
// Set-Cookie or Set-Login is minted for the requesting client's session
// and may be served ONLY to the client whose request produced it —
// never stored unstripped, never handed to another caller through any
// collapse surface:
//
//   - fill flights (ADR-0066): followers never receive leader bytes;
//     under the default client-state gate the fill stores nothing and
//     the follower fetches its own, under allow_set_cookie the store
//     carries only the stripped copy.
//   - byte-handoff flights (warm and declared-dimension, ADR-0057):
//     the leader publishes ErrStreamUnshareable and each follower
//     fetches its own — the same release the streaming path uses for
//     SSE (ADR-0042). The warm byte-handoff flight shares these exact
//     publish sites (streamMissBuffered / teeStreamToClient) with the
//     declared-dimension flight pinned here.
//   - revalidation flights (revalKeySuffix singleflight): a 200 answer
//     carrying client state makes every shared caller re-issue its own
//     conditional fetch; a 304 can never leak client state because
//     MergeHeaders304 excludes it from the merged stored object.

// TestClientState_FillFollowerGetsOwnSetCookie pins the fill-flight
// surface (ADR-0066) under the default gate: a cold include-free burst
// against an origin that mints a session cookie on every response. The
// fill stores nothing (ADR-0012 gate), so the follower re-enters the
// pipeline, misses, and fetches its OWN copy — its Set-Cookie is its
// own origin answer, never the leader's.
func TestClientState_FillFollowerGetsOwnSetCookie(t *testing.T) {
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
		ctx.Response.Header.Set(header.SetCookie, fmt.Sprintf("sid=%d", fetches.Load()))
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("session-page")
	}
	h := testHandler(t, origin)

	leaderCookie := make(chan string, 1)
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/fill-cookie")
		serveRequest(h, ctx)
		leaderCookie <- respHeader(ctx, header.SetCookie)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan string, 1)
	go func() {
		ctx := testCtx("GET", "http://example.com/fill-cookie")
		serveRequest(h, ctx)
		followerDone <- respHeader(ctx, header.SetCookie)
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park on the fill flight
	close(release)
	<-leaderDone

	select {
	case got := <-followerDone:
		require.Equal(t, int64(2), fetches.Load(),
			"the fill stored nothing usable; the follower must fetch its own copy")
		require.Equal(t, "sid=1", <-leaderCookie)
		require.Equal(t, "sid=2", got,
			"a fill follower must receive its own session cookie, never the leader's")
	case <-time.After(5 * time.Second):
		t.Fatal("fill follower did not terminate")
	}
}

// TestClientState_FillFollowerGetsOwnSetLogin pins the same surface for
// Set-Login (WICG Sign-In Cookies): the login state the follower
// observes is its own origin answer, not the leader's.
func TestClientState_FillFollowerGetsOwnSetLogin(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		n := fetches.Add(1)
		if n == 1 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.SetLogin, fmt.Sprintf("logged-in-%d", n))
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("profile")
	}
	h := testHandler(t, origin)

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/fill-login")
		serveRequest(h, ctx)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan string, 1)
	go func() {
		ctx := testCtx("GET", "http://example.com/fill-login")
		serveRequest(h, ctx)
		followerDone <- respHeader(ctx, header.SetLogin)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	<-leaderDone

	select {
	case got := <-followerDone:
		require.Equal(t, int64(2), fetches.Load(),
			"a Set-Login response stores nothing by default; the follower fetches its own")
		require.Equal(t, "logged-in-2", got,
			"a fill follower must observe its own login state, never the leader's")
	case <-time.After(5 * time.Second):
		t.Fatal("fill follower did not terminate")
	}
}

// TestClientState_DeclaredFlightNeverHandsOffSetCookie pins the
// byte-handoff surface (ADR-0057 declared-dimension flight): two
// same-dimension anonymous cold misses share one flight, but the
// origin's per-request Set-Cookie (session rotation on every response,
// a standard anti-fixation measure) must reach each caller only from
// its own fetch. Before ADR-0067 the follower received the leader's
// Set-Cookie through the buffered hand-off.
func TestClientState_DeclaredFlightNeverHandsOffSetCookie(t *testing.T) {
	t.Parallel()

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
		ctx.Response.Header.Set(header.SetCookie, fmt.Sprintf("sid=%d", fetches.Load()))
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("shared")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		Policy:     policy,
	})

	cookies := make([]string, 2)
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/declared-cookie")
		ctx.Request.Header.Set("X-Tenant-Id", "14")
		serveRequest(h, ctx)
		cookies[0] = respHeader(ctx, header.SetCookie)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		ctx := testCtx("GET", "http://example.com/declared-cookie")
		ctx.Request.Header.Set("X-Tenant-Id", "14")
		serveRequest(h, ctx)
		cookies[1] = respHeader(ctx, header.SetCookie)
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park on the byte-handoff flight
	close(release)
	<-leaderDone
	<-followerDone

	require.Equal(t, int64(2), fetches.Load(),
		"a client-state response must never be handed to a second caller: the follower fetches its own")
	require.Equal(t, "sid=1", cookies[0])
	require.Equal(t, "sid=2", cookies[1],
		"the follower's session cookie must be its own origin answer, never the leader's")
}

// TestClientState_DeclaredFlightNeverHandsOffSetLogin pins the same
// byte-handoff surface for Set-Login.
func TestClientState_DeclaredFlightNeverHandsOffSetLogin(t *testing.T) {
	t.Parallel()

	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Tenant-Id"}, false)
	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		n := fetches.Add(1)
		if n == 1 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.SetLogin, fmt.Sprintf("logged-in-%d", n))
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("shared")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		Policy:     policy,
	})

	states := make([]string, 2)
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/declared-login")
		ctx.Request.Header.Set("X-Tenant-Id", "14")
		serveRequest(h, ctx)
		states[0] = respHeader(ctx, header.SetLogin)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		ctx := testCtx("GET", "http://example.com/declared-login")
		ctx.Request.Header.Set("X-Tenant-Id", "14")
		serveRequest(h, ctx)
		states[1] = respHeader(ctx, header.SetLogin)
	}()
	time.Sleep(50 * time.Millisecond)
	close(release)
	<-leaderDone
	<-followerDone

	require.Equal(t, int64(2), fetches.Load(),
		"a Set-Login response must never be handed to a second caller in-flight")
	require.Equal(t, "logged-in-1", states[0])
	require.Equal(t, "logged-in-2", states[1],
		"the follower must observe its own login state, never the leader's")
}

// TestClientState_RevalidateFollowerGetsOwnSetCookie pins the
// revalidation flight (collapsedRevalidateBg): concurrent anonymous
// revalidations share one conditional fetch, but the origin's 200
// replacement answer carries a per-request session cookie — every
// shared caller must re-issue its own conditional fetch instead of
// serving the flight's answer. The shared fetch's cookie must not
// reach any client.
func TestClientState_RevalidateFollowerGetsOwnSetCookie(t *testing.T) {
	t.Parallel()

	var fetches atomic.Int64
	release := make(chan struct{})
	origin := func(ctx *fasthttp.RequestCtx) {
		n := fetches.Add(1)
		if len(ctx.Request.Header.Peek(header.IfNoneMatch)) > 0 {
			if n == 2 {
				// Hold the shared revalidation flight open until the
				// follower has joined it.
				select {
				case <-release:
				case <-time.After(2 * time.Second):
				}
			}
			ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.Response.Header.Set(header.SetCookie, fmt.Sprintf("sid=%d", n))
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("body")
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body")
	}
	h := testHandler(t, origin)

	seed := testCtx("GET", "http://example.com/reval-cookie")
	serveRequest(h, seed)
	require.Equal(t, int64(1), fetches.Load())

	cookies := make([]string, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/reval-cookie")
			serveRequest(h, ctx)
			cookies[i] = respHeader(ctx, header.SetCookie)
		}()
	}
	for fetches.Load() < 2 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the follower join the revalidation flight
	close(release)
	wg.Wait()

	// Seed (1) + shared revalidation fetch (2) + one own re-fetch per
	// shared caller (3, 4): the flight's sid=2 cookie reaches no client.
	require.Equal(t, int64(4), fetches.Load(),
		"every shared caller of a client-state revalidation answer must re-fetch its own")
	require.ElementsMatch(t, []string{"sid=3", "sid=4"}, cookies,
		"each revalidating client must receive its own session cookie, never the shared flight's")
}

// TestClientState_TeeFillNeverHandsOffSetCookie pins the streaming tee
// publish site under the allow_set_cookie opt-in: the leader's client
// keeps its own Set-Cookie, followers fetch their own, and the stored
// copy is stripped so later HITs replay no session state.
func TestClientState_TeeFillNeverHandsOffSetCookie(t *testing.T) {
	t.Parallel()

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
		ctx.Response.Header.Set(header.SetCookie, fmt.Sprintf("sid=%d", fetches.Load()))
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("streamed-shared")
	}
	h := NewHandler(HandlerConfig{
		Upstream:       origin,
		FastClient:     &streamFastClient{handler: origin},
		Store:          storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		Policy:         policy,
		AllowSetCookie: true,
	})

	cookies := make([]string, 2)
	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/tee-cookie")
		ctx.Request.Header.Set("X-Tenant-Id", "14")
		serveRequest(h, ctx)
		_ = respBody(ctx) // materializes the stream writer (the tee publish)
		cookies[0] = respHeader(ctx, header.SetCookie)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan struct{})
	go func() {
		defer close(followerDone)
		ctx := testCtx("GET", "http://example.com/tee-cookie")
		ctx.Request.Header.Set("X-Tenant-Id", "14")
		serveRequest(h, ctx)
		_ = respBody(ctx)
		cookies[1] = respHeader(ctx, header.SetCookie)
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park on the byte-handoff flight
	close(release)
	<-leaderDone
	<-followerDone

	require.Equal(t, int64(2), fetches.Load(),
		"a client-state response is never handed to a second caller, even on the streaming tee path")
	require.Equal(t, "sid=1", cookies[0])
	require.Equal(t, "sid=2", cookies[1])

	// The stored copy is stripped: a later request HITs without any
	// session state.
	ctx := testCtx("GET", "http://example.com/tee-cookie")
	ctx.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx)
	require.Equal(t, "HIT", respHeader(ctx, header.XCache))
	require.Equal(t, "", respHeader(ctx, header.SetCookie),
		"the stored copy must never replay a session cookie (ADR-0012 strip)")
}

// TestClientState_AllowSetCookieFollowerServesStrippedCopy pins the
// fill-flight store-mediated serve under the allow_set_cookie opt-in:
// the leader's fill is cacheable and stored STRIPPED, so the follower
// re-enters and serves the clean stored copy — never the leader's
// Set-Cookie / Set-Login, and never its own either (sequential HIT
// behavior: the opt-in caches the response with client state removed).
func TestClientState_AllowSetCookieFollowerServesStrippedCopy(t *testing.T) {
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
		ctx.Response.Header.Set(header.SetCookie, "sid=1")
		ctx.Response.Header.Set(header.SetLogin, "logged-in")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("session-page")
	}
	h := NewHandler(HandlerConfig{
		Upstream:       origin,
		FastClient:     &testFastClient{handler: origin},
		Store:          storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		AllowSetCookie: true,
	})

	leaderDone := make(chan *fasthttp.RequestCtx, 1)
	go func() {
		ctx := testCtx("GET", "http://example.com/optin-cookie")
		serveRequest(h, ctx)
		leaderDone <- ctx
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan *fasthttp.RequestCtx, 1)
	go func() {
		ctx := testCtx("GET", "http://example.com/optin-cookie")
		serveRequest(h, ctx)
		followerDone <- ctx
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park on the fill flight
	close(release)

	leader := <-leaderDone
	require.Equal(t, "MISS", respHeader(leader, header.XCache))
	require.Equal(t, "sid=1", respHeader(leader, header.SetCookie),
		"the fill leader receives its own session cookie")
	require.Equal(t, "logged-in", respHeader(leader, header.SetLogin))

	follower := <-followerDone
	require.Equal(t, "HIT", respHeader(follower, header.XCache),
		"the fill follower serves the stripped stored copy through the store")
	require.Equal(t, "", respHeader(follower, header.SetCookie),
		"the follower must never receive the leader's session cookie")
	require.Equal(t, "", respHeader(follower, header.SetLogin),
		"the follower must never observe the leader's login state")
	require.Equal(t, "session-page", respBody(follower))

	require.Equal(t, int64(1), fetches.Load(),
		"the stripped fill is storable under allow_set_cookie: the follower serves it from the store, no second fetch")

	third := testCtx("GET", "http://example.com/optin-cookie")
	serveRequest(h, third)
	require.Equal(t, "HIT", respHeader(third, header.XCache))
	require.Equal(t, "", respHeader(third, header.SetCookie))
	require.Equal(t, "", respHeader(third, header.SetLogin))
}

// TestClientState_PostResponseSetLoginNotStored pins the POST
// store-to-GET-key gate (RFC 7234 §3, kept by design): a successful
// POST answer carrying Set-Login is per-user and must never be stored
// under the GET key, where every subsequent client would observe
// another user's login state.
func TestClientState_PostResponseSetLoginNotStored(t *testing.T) {
	t.Parallel()

	h := testHandler(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		if string(ctx.Method()) == "POST" {
			ctx.Response.Header.Set(header.ContentLocation, string(ctx.Path()))
			ctx.Response.Header.Set(header.SetLogin, "logged-in")
		}
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	})

	post := testCtx("POST", "http://example.com/post-login")
	h.ServeRequest(post)
	require.Equal(t, 200, respCode(post))

	// The POST answer carried Set-Login: nothing is stored under the
	// GET key, so the next GET is its own miss, not a replay.
	get := testCtx("GET", "http://example.com/post-login")
	h.ServeRequest(get)
	require.Equal(t, "MISS", respHeader(get, header.XCache),
		"a POST answer carrying Set-Login must not be stored under the GET key")
	require.Equal(t, "", respHeader(get, header.SetLogin),
		"no client may observe another client's login state from the cache")
}
