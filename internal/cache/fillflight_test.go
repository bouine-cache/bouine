package cache

import (
	"context"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// This file pins the fill-flight rule (ADR-0066): an anonymous,
// safe-method, cold include-free miss parks concurrent callers on one
// leader fill, but the follower never receives the leader's bytes — it
// waits for the fill to settle in the store, re-enters the request
// pipeline, and is served through the same gates as a sequential
// request (variant selection, RFC 9111 §3.5 storage, freshness,
// conditionals), or fetches its own.
//
// The core safety property is sequential equivalence
// (TestFetchAndStore_FillFollowerSequentialEquivalence): a fill
// follower's response is byte-identical to the response the same
// request would have received had it arrived after the leader's fill
// completed. No client can observe, concurrently, any content it could
// not have observed sequentially.

// fillPair holds one request's observable response.
type fillPair struct {
	status int
	body   string
	xCache string
}

func fillPairFrom(ctx *fasthttp.RequestCtx) fillPair {
	return fillPair{
		status: ctx.Response.StatusCode(),
		body:   respBody(ctx),
		xCache: respHeader(ctx, header.XCache),
	}
}

// runFillPairConcurrent forces the fill-flight parking shape: the
// leader request starts first and its fetch is held open until the
// follower has had time to park on the registered flight, so the
// follower deterministically waits on the leader's fill (not on a
// flight of its own).
func runFillPairConcurrent(t *testing.T, origin fasthttp.RequestHandler, leaderHeaders, followerHeaders [][2]string) fillPair {
	t.Helper()

	var fetches atomic.Int64
	release := make(chan struct{})
	gated := func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 1 {
			select {
			case <-release:
			case <-time.After(2 * time.Second):
			}
		}
		origin(ctx)
	}
	h := testHandler(t, gated)

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/pair")
		for _, kv := range leaderHeaders {
			ctx.Request.Header.Set(kv[0], kv[1])
		}
		serveRequest(h, ctx)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan fillPair, 1)
	go func() {
		ctx := testCtx("GET", "http://example.com/pair")
		for _, kv := range followerHeaders {
			ctx.Request.Header.Set(kv[0], kv[1])
		}
		serveRequest(h, ctx)
		followerDone <- fillPairFrom(ctx)
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park on the fill flight
	close(release)
	<-leaderDone

	select {
	case got := <-followerDone:
		return got
	case <-time.After(5 * time.Second):
		t.Fatal("fill follower did not terminate")
		return fillPair{}
	}
}

// runFillPairSequential serves the leader first, waits for it to
// finish, then serves the follower: the response the follower would
// have received had it arrived after the fill.
func runFillPairSequential(t *testing.T, origin fasthttp.RequestHandler, leaderHeaders, followerHeaders [][2]string) fillPair {
	t.Helper()
	h := testHandler(t, origin)

	lctx := testCtx("GET", "http://example.com/pair")
	for _, kv := range leaderHeaders {
		lctx.Request.Header.Set(kv[0], kv[1])
	}
	serveRequest(h, lctx)

	fctx := testCtx("GET", "http://example.com/pair")
	for _, kv := range followerHeaders {
		fctx.Request.Header.Set(kv[0], kv[1])
	}
	serveRequest(h, fctx)
	return fillPairFrom(fctx)
}

// TestFetchAndStore_FillFollowerSequentialEquivalence is the core
// safety proof of ADR-0066: for every response shape, a follower
// parked on a fill flight receives byte-identically what the same
// request would have received had it arrived after the leader's fill
// — the same status, body, and X-Cache. If a follower could ever
// observe content a sequential request could not, this test fails.
func TestFetchAndStore_FillFollowerSequentialEquivalence(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name           string
		origin         fasthttp.RequestHandler
		leaderHeaders  [][2]string
		followerHeader [][2]string
	}{
		{
			name: "no vary, identical requests",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("shared")
			},
		},
		{
			name: "vary declaring origin, differing selector",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("tenant:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
			},
			leaderHeaders:  [][2]string{{"X-Tenant-Id", "14"}},
			followerHeader: [][2]string{{"X-Tenant-Id", "66"}},
		},
		{
			name: "vary declaring origin, matching selector",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("tenant:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
			},
			leaderHeaders:  [][2]string{{"X-Tenant-Id", "14"}},
			followerHeader: [][2]string{{"X-Tenant-Id", "14"}},
		},
		{
			name: "no-store response",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("t:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
			},
			leaderHeaders:  [][2]string{{"X-Tenant-Id", "14"}},
			followerHeader: [][2]string{{"X-Tenant-Id", "66"}},
		},
		{
			name: "private response",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "private, max-age=60")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("t:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
			},
			leaderHeaders:  [][2]string{{"X-Tenant-Id", "14"}},
			followerHeader: [][2]string{{"X-Tenant-Id", "66"}},
		},
		{
			name: "uncacheable status",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.SetStatusCode(500)
				_, _ = ctx.WriteString("t:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
			},
			leaderHeaders:  [][2]string{{"X-Tenant-Id", "14"}},
			followerHeader: [][2]string{{"X-Tenant-Id", "66"}},
		},
		{
			name: "conditional follower after etag fill",
			origin: func(ctx *fasthttp.RequestCtx) {
				if len(ctx.Request.Header.Peek(header.IfNoneMatch)) > 0 {
					ctx.Response.Header.Set(header.ETag, `"v1"`)
					ctx.SetStatusCode(fasthttp.StatusNotModified)
					return
				}
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.Response.Header.Set(header.ETag, `"v1"`)
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("shared")
			},
			followerHeader: [][2]string{{header.IfNoneMatch, `"v1"`}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			concurrent := runFillPairConcurrent(t, tc.origin, tc.leaderHeaders, tc.followerHeader)
			sequential := runFillPairSequential(t, tc.origin, tc.leaderHeaders, tc.followerHeader)
			require.Equal(t, sequential, concurrent,
				"a fill follower must receive byte-identically what the same request would receive after the fill")
		})
	}
}

// TestFetchAndStore_UndeclaredRouteColdMissCollapsesToOneFetch pins the
// dedup half of ADR-0066: N concurrent identical anonymous cold misses
// on an include-free route collapse to ONE origin fetch, and every
// caller is served (the leader from the fill, the followers from the
// store after re-entry). This is the nginx proxy_cache_lock parity
// case that ADR-0057 §3 refused.
func TestFetchAndStore_UndeclaredRouteColdMissCollapsesToOneFetch(t *testing.T) {
	t.Parallel()

	const callers = 16
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
		_, _ = ctx.WriteString("burst")
	}
	h := testHandler(t, origin)

	bodies := make([]string, callers)
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/burst")
			serveRequest(h, ctx)
			bodies[i] = respBody(ctx)
		}()
	}
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the followers park
	close(release)
	wg.Wait()

	require.Equal(t, int64(1), fetches.Load(),
		"a cold burst on an include-free route must collapse to one origin fetch")
	for _, b := range bodies {
		require.Equal(t, "burst", b, "every caller in the burst must be served")
	}
}

// TestFetchAndStore_PersonalColdMissStillNeverSharesFlights pins that
// ADR-0066 does not relax ADR-0052/ADR-0054: concurrent cold misses
// carrying Authorization or Cookie on an include-free route still never
// share any flight — each fetches its own, each receives its own
// credentials' content. The origin echoes the credential back, so a
// shared (broken) run fails on both the fetch count and the bodies.
func TestFetchAndStore_PersonalColdMissStillNeverSharesFlights(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		hdr    string
		values []string
	}{
		{"authorization", header.Authorization, []string{"token-a", "token-b"}},
		{"cookie", header.Cookie, []string{"sid=abc", "sid=xyz"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				_, _ = ctx.WriteString("who:" + string(ctx.Request.Header.Peek(tc.hdr)))
			}
			h := testHandler(t, origin)

			var wg sync.WaitGroup
			bodies := make([]string, 2)
			for i, v := range tc.values {
				wg.Add(1)
				go func() {
					defer wg.Done()
					ctx := testCtx("GET", "http://example.com/personal")
					ctx.Request.Header.Set(tc.hdr, v)
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
				"personal requests must never share a flight (ADR-0052/ADR-0054, unchanged by ADR-0066)")
			require.Equal(t, "who:"+tc.values[0], bodies[0])
			require.Equal(t, "who:"+tc.values[1], bodies[1])
		})
	}
}

// TestFetchAndStore_FillFollowerTerminatesForEachVerdict pins the
// stored-signal lifecycle: for every leader verdict — cacheable fill,
// no-store, private, uncacheable status, oversize, transport error — a
// parked fill follower terminates (the leader closes inflight.stored on
// every path; a missed close would park the follower forever).
func TestFetchAndStore_FillFollowerTerminatesForEachVerdict(t *testing.T) {
	t.Parallel()

	verdicts := []struct {
		name           string
		origin         fasthttp.RequestHandler
		transportError bool
	}{
		{
			name: "cacheable fill",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("ok")
			},
		},
		{
			name: "no-store fill",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("ok")
			},
		},
		{
			name: "private fill",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "private, max-age=60")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("ok")
			},
		},
		{
			name: "uncacheable status fill",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.SetStatusCode(500)
				_, _ = ctx.WriteString("boom")
			},
		},
		{
			name:           "transport error",
			transportError: true,
		},
	}

	for _, v := range verdicts {
		t.Run(v.name, func(t *testing.T) {
			t.Parallel()

			release := make(chan struct{})
			rawOrigin := v.origin
			origin := rawOrigin
			var client FastClient
			if v.transportError {
				// A leader flight that stays registered and open until it
				// resolves with a transport error: the leader blocks
				// inside Do, so the follower parks deterministically.
				client = &blockingErrFastClient{release: release}
			} else {
				// Hold the leader's (only) fetch open until the follower
				// has parked on the registered flight.
				var fetches atomic.Int64
				gated := func(ctx *fasthttp.RequestCtx) {
					if fetches.Add(1) == 1 {
						select {
						case <-release:
						case <-time.After(2 * time.Second):
						}
					}
					rawOrigin(ctx)
				}
				origin = gated
				client = &testFastClient{handler: gated}
			}
			store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
			h := NewHandler(HandlerConfig{
				Upstream:   origin,
				FastClient: client,
				Store:      store,
			})

			leaderDone := make(chan struct{})
			go func() {
				defer close(leaderDone)
				ctx := testCtx("GET", "http://example.com/verdict")
				serveRequest(h, ctx)
			}()
			time.Sleep(50 * time.Millisecond) // the leader registers its flight, then holds it open

			followerDone := make(chan struct{})
			go func() {
				defer close(followerDone)
				ctx := testCtx("GET", "http://example.com/verdict")
				serveRequest(h, ctx)
			}()
			time.Sleep(50 * time.Millisecond) // let the follower park on the held flight
			close(release)                    // resolve the leader's verdict
			<-leaderDone

			select {
			case <-followerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("fill follower never terminated: inflight.stored was not closed on this verdict path")
			}
		})
	}
}

// blockingErrFastClient is a FastClient whose fetch blocks on the
// release channel and then fails — a leader flight that stays
// registered and open until it resolves with a transport error.
type blockingErrFastClient struct {
	release chan struct{}
}

func (c *blockingErrFastClient) Do(context.Context, *fasthttp.Request, *fasthttp.Response) error {
	select {
	case <-c.release:
	case <-time.After(2 * time.Second):
	}
	return errors.New("origin unreachable")
}

func (c *blockingErrFastClient) DoDeadline(*fasthttp.Request, *fasthttp.Response, time.Time) error {
	select {
	case <-c.release:
	case <-time.After(2 * time.Second):
	}
	return errors.New("origin unreachable")
}

// TestFetchAndStore_FillFollowerDoesNotRepark pins the re-entry guard:
// when a fill follower's variant is absent (unstorable fill or foreign
// variant), its own fetch must not park on the (already completed)
// leader's flight again — bounded to exactly one follower fetch, no
// spin.
func TestFetchAndStore_FillFollowerDoesNotRepark(t *testing.T) {
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
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("t:" + string(ctx.Request.Header.Peek("X-Tenant-Id")))
	}
	h := testHandler(t, origin)

	var wg sync.WaitGroup
	bodies := make([]string, 2)
	for i, tenant := range []string{"14", "66"} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/repark")
			ctx.Request.Header.Set("X-Tenant-Id", tenant)
			serveRequest(h, ctx)
			bodies[i] = respBody(ctx)
		}()
	}
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the follower park
	close(release)
	wg.Wait()

	require.Equal(t, int64(2), fetches.Load(),
		"an unstorable fill resolves to exactly one follower fetch: the re-entry guard must prevent re-parking and spinning")
	require.Equal(t, "t:14", bodies[0])
	require.Equal(t, "t:66", bodies[1])
}

// TestFetchAndStore_FillFollowerHITReportsHitFromStore documents the
// observable contract of a fill-follower serve: the follower is served
// from the store after re-entry, so X-Cache reports HIT (the leader's
// fill reports MISS) — sequential-equivalent by construction.
func TestFetchAndStore_FillFollowerHITReportsHitFromStore(t *testing.T) {
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
		_, _ = ctx.WriteString("ok")
	}
	h := testHandler(t, origin)

	results := make([]fillPair, 2)
	var wg sync.WaitGroup
	for i := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com/hit")
			serveRequest(h, ctx)
			results[i] = fillPairFrom(ctx)
		}()
	}
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let the follower park
	close(release)
	wg.Wait()

	require.Equal(t, int64(1), fetches.Load())
	xc := []string{results[0].xCache, results[1].xCache}
	sort.Strings(xc)
	require.Equal(t, []string{"HIT", "MISS"}, xc,
		"one caller is the fill leader (MISS), the other serves from the store after re-entry (HIT)")
	for _, r := range results {
		require.Equal(t, 200, r.status)
		require.Equal(t, "ok", r.body)
	}
}

// TestFetchAndStore_FillFollowerReentryReasksOwner pins the cluster half
// of the store-mediated serve: the follower re-enters as a
// freshly-arrived request, which on a non-owner node means re-asking
// the owner. The H1 fast path's pre-fill owner hints
// (api.OwnerMissContextKey and siblings) are stale once the leader's
// fill has populated the owner — a sequential request arriving after
// the fill peer-fetches and HITs, and the re-entering follower must do
// the same instead of skipping the peer lookup and fetching its own
// origin copy.
func TestFetchAndStore_FillFollowerReentryReasksOwner(t *testing.T) {
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
		_, _ = ctx.WriteString("from-origin")
	}
	// The owner's post-fill state: what a peer-fetch returns once the
	// leader's fill has been forwarded (peerPut is not wired here; the
	// mock supplies the owner's answer directly).
	ownerObj := &api.Object{
		StatusCode: 200,
		Body:       []byte("from-owner"),
		Header:     headerMap(header.ContentType, "text/plain"),
		StoredAt:   time.Now(),
		TTL:        time.Minute,
	}
	var peerFetchCalls atomic.Int32
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false // this node is not the owner
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key, _ string) (*api.Object, error) {
			peerFetchCalls.Add(1)
			return ownerObj, nil
		},
	})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		ctx := testCtx("GET", "http://example.com/reask")
		// The fast path already asked the owner and got a definitive
		// miss before this request reached the slow path.
		ctx.SetUserValue(api.OwnerMissContextKey, true)
		serveRequest(h, ctx)
	}()
	for fetches.Load() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	followerDone := make(chan *fasthttp.RequestCtx, 1)
	go func() {
		ctx := testCtx("GET", "http://example.com/reask")
		// The follower's own fast-path pass also got an owner miss —
		// before the leader's fill settled.
		ctx.SetUserValue(api.OwnerMissContextKey, true)
		serveRequest(h, ctx)
		followerDone <- ctx
	}()
	time.Sleep(50 * time.Millisecond) // let the follower park on the fill flight
	close(release)
	<-leaderDone
	require.Equal(t, int32(0), peerFetchCalls.Load(),
		"the hinted leader and the hinted first pass skip the owner RPC as before")

	select {
	case follower := <-followerDone:
		require.Equal(t, int32(1), peerFetchCalls.Load(),
			"the re-entry must re-ask the owner: the pre-fill miss hint is stale once the fill has populated the owner")
		require.Equal(t, int64(1), fetches.Load(),
			"the follower serves the fill's object from the owner instead of fetching its own origin copy")
		require.Equal(t, "from-owner", respBody(follower))
	case <-time.After(5 * time.Second):
		t.Fatal("fill follower did not terminate")
	}
}
