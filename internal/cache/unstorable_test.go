package cache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// This file pins the unstorable-fill memo (ADR-0066 amendment #2): a
// fill settling unstorable by explicit origin directive (no-store /
// private) suppresses fill flights for its key for a bounded window, so
// subsequent anonymous cold include-free misses fetch their own
// immediately — the sequential behavior — instead of parking on fills
// that provably store nothing shareable and paying the leader-fill wait
// plus the released-wave stagger (PR #838). No content-sharing surface
// changes: a memoized request is a sequential request.

// memoEntries counts live (unexpired) entries across all memo shards.
func memoEntries(m *unstorableMemo) int {
	now := time.Now().UnixNano()
	n := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.RLock()
		for _, exp := range s.m {
			if exp > now {
				n++
			}
		}
		s.mu.RUnlock()
	}
	return n
}

// TestUnstorableMemo_WindowAndExpiry pins the memo's lifecycle on the
// synctest fake clock: a noted key bypasses, the bypass lapses once the
// window elapses, and a fresh note re-arms it.
func TestUnstorableMemo_WindowAndExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var m unstorableMemo
		key := BuildKey(mustRequestInfo(t, "http://example.com/a"), nil)

		require.False(t, m.bypass(key), "a never-noted key never bypasses")
		m.note(key)
		require.True(t, m.bypass(key), "a noted key bypasses within the window")

		time.Sleep(unstorableMemoWindow)
		require.False(t, m.bypass(key), "the bypass lapses when the window elapses")

		m.note(key)
		require.True(t, m.bypass(key), "a fresh note re-arms the window")

		other := BuildKey(mustRequestInfo(t, "http://example.com/b"), nil)
		require.False(t, m.bypass(other), "only the noted key bypasses")
		require.Equal(t, 1, memoEntries(&m))
	})
}

// TestUnstorableMemo_BoundedFailOpen pins the bound: a full shard first
// reclaims expired entries, then refuses new notes — fail-open back to
// ADR-0066 collapse, never unbounded growth.
func TestUnstorableMemo_BoundedFailOpen(t *testing.T) {
	t.Parallel()

	var m unstorableMemo
	// Noting 16*512 distinct keys guarantees every one of the 16
	// shards has been offered more than its 512-slot cap; live entries
	// cannot be swept yet, so the overflow writes are refused.
	for i := range 16 * unstorableMemoPerShard {
		m.note(BuildKey(mustRequestInfo(t, fmt.Sprintf("http://example.com/k%d", i)), nil))
	}
	for i := range m.shards {
		m.shards[i].mu.RLock()
		require.LessOrEqual(t, len(m.shards[i].m), unstorableMemoPerShard,
			"every shard stays within its cap")
		m.shards[i].mu.RUnlock()
	}

	// The refused writes (shards already full of live entries) must not
	// panic and must fail open: an un-noted key keeps ADR-0066 collapse.
	fresh := BuildKey(mustRequestInfo(t, "http://example.com/fresh"), nil)
	require.False(t, m.bypass(fresh))
}

// mustRequestInfo builds a RequestInfo for key derivation in tests.
func mustRequestInfo(t *testing.T, url string) RequestInfo {
	t.Helper()
	ri := requestInfoFromURL("GET", url)
	require.NotEmpty(t, ri.Host)
	return ri
}

// unstorableHarness is one no-store/private origin with per-fetch unique
// bodies plus arrival-spread tracking, under the synctest fake clock.
type unstorableHarness struct {
	t         *testing.T
	h         *Handler
	fetches   atomic.Int64
	mu        sync.Mutex
	arrivals  []time.Time
	origin    fasthttp.RequestHandler
	holdFirst bool
	release   chan struct{}
}

func newUnstorableHarness(t *testing.T, cacheControl string) *unstorableHarness {
	t.Helper()
	x := &unstorableHarness{t: t, release: make(chan struct{})}
	x.origin = func(ctx *fasthttp.RequestCtx) {
		n := x.fetches.Add(1)
		if x.holdFirst && n == 1 {
			select {
			case <-x.release:
			case <-time.After(5 * time.Second):
			}
		}
		x.mu.Lock()
		x.arrivals = append(x.arrivals, time.Now())
		x.mu.Unlock()
		if cacheControl != "" {
			ctx.Response.Header.Set(header.CacheControl, cacheControl)
		}
		ctx.SetStatusCode(200)
		_, _ = fmt.Fprintf(ctx, "body-%d", n)
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	t.Cleanup(func() { store.Close(context.Background()) })
	x.h = NewHandler(HandlerConfig{
		Upstream:            x.origin,
		FastClient:          &testFastClient{handler: x.origin},
		Store:               store,
		MaxFetchConcurrency: 64,
	})
	t.Cleanup(func() { x.h.Close(context.Background()) })
	return x
}

func (x *unstorableHarness) burst(n int, path string) []string {
	x.t.Helper()
	bodies := make([]string, n)
	started := make(chan struct{})
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := testCtx("GET", "http://example.com"+path)
			<-started
			serveRequest(x.h, ctx)
			require.Equal(x.t, 200, respCode(ctx))
			bodies[i] = respBody(ctx)
		}()
	}
	close(started)
	wg.Wait()
	return bodies
}

func (x *unstorableHarness) arrivalSpan() time.Duration {
	x.mu.Lock()
	defer x.mu.Unlock()
	if len(x.arrivals) < 2 {
		return 0
	}
	return x.arrivals[len(x.arrivals)-1].Sub(x.arrivals[0])
}

// TestFetchAndStore_UnstorableFillMemoizesBypass pins the memo's effect:
// the FIRST cold wave on a no-store key collapses (leader + parked
// followers, staggered release — unchanged ADR-0066/#838 behavior), and
// the fill's settle memoizes the key so a SECOND concurrent burst skips
// the fill flight entirely: every caller fetches its own immediately
// (zero arrival spread — no fill wait, no release stagger) and receives
// its own unique body (no byte handoff, ever).
func TestFetchAndStore_UnstorableFillMemoizesBypass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers = 8
		x := newUnstorableHarness(t, "no-store")

		// First burst: memo empty, so the wave parks on one leader fill.
		first := x.burst(callers, "/memo")
		synctest.Wait()
		require.Equal(t, int64(callers), x.fetches.Load(),
			"first wave: one fetch per caller (leader + released followers)")
		require.Equal(t, callers, len(uniqueBodies(first)),
			"every caller receives its own body: the leader's no-store bytes are never handed to a follower")
		require.Greater(t, x.arrivalSpan(), time.Duration(0),
			"first wave: the released followers' own fetches are staggered (PR #838), not one burst")
		require.Equal(t, 1, memoEntries(&x.h.unstorableMemo),
			"the no-store settle memoizes the key")

		base := x.fetches.Load()
		before := len(x.arrivals)

		// Second burst within the memo window: no parking, no waiting.
		second := x.burst(callers, "/memo")
		synctest.Wait()
		require.Equal(t, base+int64(callers), x.fetches.Load(),
			"second burst: every caller fetches its own (sequential fetch count, unchanged)")
		require.Equal(t, callers, len(uniqueBodies(second)),
			"no caller ever receives another caller's bytes")
		x.mu.Lock()
		span := x.arrivals[len(x.arrivals)-1].Sub(x.arrivals[before])
		x.mu.Unlock()
		require.Equal(t, time.Duration(0), span,
			"second burst: own fetches start immediately — no fill wait, no release stagger")
		require.Equal(t, 1, memoEntries(&x.h.unstorableMemo))
	})
}

// TestFetchAndStore_PrivateFillMemoizesBypass pins the same effect for
// Cache-Control: private — the other directive that refuses shared
// storage outright (RFC 9111 §5.2.2.7).
func TestFetchAndStore_PrivateFillMemoizesBypass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers = 4
		x := newUnstorableHarness(t, "private")
		x.burst(callers, "/private")
		synctest.Wait()
		require.Equal(t, 1, memoEntries(&x.h.unstorableMemo),
			"a private settle memoizes the key")
		base := x.fetches.Load()
		x.burst(callers, "/private")
		synctest.Wait()
		require.Equal(t, base+int64(callers), x.fetches.Load(),
			"within the window every caller fetches its own")
	})
}

// TestFetchAndStore_MemoExpiresRefillsCollapse pins the window's other
// edge: once the memo lapses, the next cold wave collapses onto a fill
// flight again (the memo suppresses flights, it never permanently
// disables ADR-0066's dedup).
func TestFetchAndStore_MemoExpiresRefillsCollapse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const callers = 4
		x := newUnstorableHarness(t, "no-store")
		x.burst(callers, "/expiry")
		synctest.Wait()
		require.Equal(t, 1, memoEntries(&x.h.unstorableMemo))

		time.Sleep(unstorableMemoWindow)
		require.Equal(t, 0, memoEntries(&x.h.unstorableMemo),
			"the memo lapses after the window")

		// The next wave must park on a leader fill again: hold the
		// first fetch open until the followers are durably parked, so
		// the fetch count proves the collapse (1 fetch, not `callers`).
		base := x.fetches.Load()
		x.holdFirst = true
		parked := make(chan struct{})
		go func() {
			defer close(parked)
			x.burst(callers, "/expiry")
		}()
		synctest.Wait()
		require.Equal(t, int64(1), x.fetches.Load()-base,
			"post-expiry: the wave collapses onto one leader fill again (the parked followers have not fetched)")
		close(x.release)
		<-parked
		synctest.Wait()
		require.Equal(t, int64(callers), x.fetches.Load()-base,
			"the released wave own-fetches (sequential count, unchanged)")
	})
}

// TestFetchAndStore_MemoExclusions pins that only explicit shared-
// storage-refusing directives memoize. Every other unstorable settle
// keeps the settle-then-stagger flow: errors are transient (a
// struggling origin needs collapse's dedup on recovery), client-state
// responses are ADR-0067's surface, absent freshness is not a policy
// statement, HEAD-led fills store nothing by method, and storable
// fills have nothing to memoize at all.
func TestFetchAndStore_MemoExclusions(t *testing.T) {
	cases := []struct {
		name    string
		origin  func(ctx *fasthttp.RequestCtx)
		method  string
		wantLen int
	}{
		{
			name: "no-store memoizes",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
				ctx.SetStatusCode(200)
			},
			wantLen: 1,
		},
		{
			name: "private memoizes",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "private")
				ctx.SetStatusCode(200)
			},
			wantLen: 1,
		},
		{
			name: "cdn-cache-control no-store memoizes (RFC 9213 precedence)",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CDNCacheControl, "no-store")
				ctx.Response.Header.Set(header.CacheControl, "max-age=3600")
				ctx.SetStatusCode(200)
			},
			wantLen: 1,
		},
		{
			name: "error status does not memoize",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(503)
			},
			wantLen: 0,
		},
		{
			name: "client-state gate does not memoize (no directive)",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.Response.Header.Set(header.SetCookie, "sid=leader")
				ctx.SetStatusCode(200)
			},
			wantLen: 0,
		},
		{
			// A genuine directive memoizes even when client-state rides
			// along: the memo only makes each caller fetch its own
			// immediately — exactly ADR-0067's outcome for every caller,
			// minus the wave wait.
			name: "no-store with Set-Cookie memoizes",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
				ctx.Response.Header.Set(header.SetCookie, "sid=leader")
				ctx.SetStatusCode(200)
			},
			wantLen: 1,
		},
		{
			name: "no directives at all does not memoize",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.SetStatusCode(200)
			},
			wantLen: 0,
		},
		{
			name:   "HEAD-led no-store fill does not memoize",
			method: "HEAD",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
				ctx.SetStatusCode(200)
			},
			wantLen: 0,
		},
		{
			name: "storable fill does not memoize",
			origin: func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
				ctx.SetStatusCode(200)
			},
			wantLen: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				if tc.method == "" {
					tc.method = "GET"
				}
				origin := tc.origin
				store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
				defer store.Close(context.Background())
				h := NewHandler(HandlerConfig{
					Upstream:   origin,
					FastClient: &testFastClient{handler: origin},
					Store:      store,
				})
				defer h.Close(context.Background())
				serveRequest(h, testCtx(tc.method, "http://example.com/"+tc.name))
				synctest.Wait()
				require.Equal(t, tc.wantLen, memoEntries(&h.unstorableMemo))
			})
		})
	}
}

// TestFetchAndStore_UpstreamFallbackNoStoreMemoizes covers the memo's
// upstream-fallback branch (sf.resp == nil, issue #598): a buffered
// Upstream-only handler settling no-store memoizes the key too.
func TestFetchAndStore_UpstreamFallbackNoStoreMemoizes(t *testing.T) {
	t.Parallel()

	origin := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "no-store")
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("fallback")
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		Upstream: origin,
		Store:    store,
	})
	defer h.Close(context.Background())
	serveRequest(h, testCtx("GET", "http://example.com/fallback"))
	require.Equal(t, 1, memoEntries(&h.unstorableMemo),
		"the upstream-fallback no-store settle memoizes the key")
}

// TestRefusesSharedStorage pins the directive predicate against
// isCacheBlocked's must-understand escape: no-store on an understood
// status WITH must-understand does not refuse shared storage (the
// operator's default-TTL policy may still store it), and without the
// escape it does.
func TestRefusesSharedStorage(t *testing.T) {
	t.Parallel()

	require.True(t, refusesSharedStorage(ParseCacheControl("no-store"), 200))
	require.True(t, refusesSharedStorage(ParseCacheControl("private"), 200))
	require.True(t, refusesSharedStorage(ParseCacheControl("no-store, must-understand"), 599),
		"RFC 9111 §5.2.2.3: must-understand does not escape the block on a status the cache does not understand")
	require.False(t, refusesSharedStorage(ParseCacheControl("no-store, must-understand"), 200),
		"RFC 9111 §5.2.2.3: must-understand + an understood status escapes the no-store block")
	require.False(t, refusesSharedStorage(ParseCacheControl("max-age=60"), 200))
	require.False(t, refusesSharedStorage(Directives{}, 200))
}

// TestFetchAndStore_MemoSelfHealsOnStorableFlip pins the safety net
// for the one cost the memo can charge: an origin that flips
// no-store to storable while the window is armed. While unstorable,
// every memoized caller fetches its own unique body (no sharing,
// ever). The moment the origin answers storable, the first caller to
// receive it stores it and every subsequent request serves that stored
// object from the store — never another caller's in-flight bytes — and
// the memo goes inert (store hits never consult it).
func TestFetchAndStore_MemoSelfHealsOnStorableFlip(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fetches atomic.Int64
		var mu sync.Mutex
		storable := false
		origin := func(ctx *fasthttp.RequestCtx) {
			n := fetches.Add(1)
			mu.Lock()
			flip := storable
			mu.Unlock()
			if flip {
				ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			} else {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
			}
			ctx.SetStatusCode(200)
			_, _ = fmt.Fprintf(ctx, "body-%d", n)
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:            origin,
			FastClient:          &testFastClient{handler: origin},
			Store:               store,
			MaxFetchConcurrency: 64,
		})
		defer h.Close(context.Background())

		// While unstorable: sequential callers, all memoized after the
		// first, each fetching its own unique body.
		const unstorableCallers = 8
		for range unstorableCallers {
			ctx := testCtx("GET", "http://example.com/flip")
			serveRequest(h, ctx)
			require.Equal(t, 200, respCode(ctx))
			require.Equal(t, "MISS", respHeader(ctx, header.XCache))
		}
		synctest.Wait()
		require.Equal(t, int64(unstorableCallers), fetches.Load(),
			"every unstorable caller fetched its own (sequential count)")
		require.Equal(t, 1, memoEntries(&h.unstorableMemo))

		// The origin flips storable mid-window: the next caller still
		// skips parking (the memo is armed), receives and STORES the
		// storable response...
		mu.Lock()
		storable = true
		mu.Unlock()
		ctx := testCtx("GET", "http://example.com/flip")
		serveRequest(h, ctx)
		synctest.Wait()
		require.Equal(t, int64(unstorableCallers+1), fetches.Load())
		require.Equal(t, "MISS", respHeader(ctx, header.XCache))

		// ...and every subsequent caller serves the STORED object —
		// no fetch, no in-flight handoff, the memo inert.
		for range 4 {
			next := testCtx("GET", "http://example.com/flip")
			serveRequest(h, next)
			require.Equal(t, "HIT", respHeader(next, header.XCache))
			require.Equal(t, respBody(ctx), respBody(next),
				"the store-served copy is byte-identical to the stored original")
		}
		synctest.Wait()
		require.Equal(t, int64(unstorableCallers+1), fetches.Load(),
			"post-flip callers never fetch: the store answers")
	})
}

// TestFetchAndStore_CookiedNoStoreDoesNotArmMemo pins the memo's
// evidence rule: only a request that was itself eligible to share
// flights may arm it. A cookied or authorized request's no-store
// response is not evidence about the anonymous population's storage
// policy — origins vary Cache-Control by request state, and anonymous
// collapse behavior stays driven by anonymous evidence.
func TestFetchAndStore_CookiedNoStoreDoesNotArmMemo(t *testing.T) {
	cases := []struct {
		name string
		hdr  [2]string
	}{
		{"cookie", [2]string{"Cookie", "sid=mine"}},
		{"authorization", [2]string{"Authorization", "Bearer token"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			origin := func(ctx *fasthttp.RequestCtx) {
				ctx.Response.Header.Set(header.CacheControl, "no-store")
				ctx.SetStatusCode(200)
				_, _ = ctx.WriteString("personal")
			}
			store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
			defer store.Close(context.Background())
			h := NewHandler(HandlerConfig{
				Upstream:   origin,
				FastClient: &testFastClient{handler: origin},
				Store:      store,
			})
			defer h.Close(context.Background())
			ctx := testCtx("GET", "http://example.com/cookied")
			ctx.Request.Header.Set(tc.hdr[0], tc.hdr[1])
			serveRequest(h, ctx)
			require.Equal(t, 200, respCode(ctx))
			require.Equal(t, 0, memoEntries(&h.unstorableMemo),
				"a personal request's no-store settle must not arm the memo")
		})
	}
}

// TestFetchAndStore_DeclaredDimensionsUnaffectedByMemo pins the memo's
// read boundary: it is consulted only by COLD INCLUDE-FREE fill
// flights. A declared-dimensions route's cold flight is a byte-handoff
// flight under the proven-key rule (ADR-0057), so it keeps collapsing —
// follower receives the leader's bytes — even while the same key's
// memo entry is armed by the anonymous no-store settle.
func TestFetchAndStore_DeclaredDimensionsUnaffectedByMemo(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var fetches atomic.Int64
		release1 := make(chan struct{})
		release2 := make(chan struct{})
		origin := func(ctx *fasthttp.RequestCtx) {
			n := fetches.Add(1)
			switch n {
			case 1:
				select {
				case <-release1:
				case <-time.After(5 * time.Second):
				}
			case 2:
				select {
				case <-release2:
				case <-time.After(5 * time.Second):
				}
			}
			ctx.Response.Header.Set(header.CacheControl, "no-store")
			ctx.SetStatusCode(200)
			_, _ = fmt.Fprintf(ctx, "body-%d", n)
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			Upstream:   origin,
			FastClient: &testFastClient{handler: origin},
			Store:      store,
			Policy:     NewKeyPolicy(nil, nil, nil, nil, false, false, []string{"X-Tenant-Id"}, false),
		})
		defer h.Close(context.Background())

		// Pair 1: the leader's fetch is held open while the follower
		// parks on the byte-handoff flight, then released — the
		// follower must receive the leader's bytes (ADR-0057
		// proven-key handoff, unchanged), and the anonymous settle
		// arms the memo.
		pair := func(release chan struct{}) (leader, follower string) {
			leaderDone := make(chan string, 1)
			go func() {
				ctx := testCtx("GET", "http://example.com/tenant")
				ctx.Request.Header.Set("X-Tenant-Id", "14")
				serveRequest(h, ctx)
				leaderDone <- respBody(ctx)
			}()
			synctest.Wait() // the leader is durably blocked in the origin
			followerDone := make(chan string, 1)
			go func() {
				ctx := testCtx("GET", "http://example.com/tenant")
				ctx.Request.Header.Set("X-Tenant-Id", "14")
				serveRequest(h, ctx)
				followerDone <- respBody(ctx)
			}()
			synctest.Wait() // the follower is durably parked on the flight
			close(release)
			return <-leaderDone, <-followerDone
		}

		l1, f1 := pair(release1)
		synctest.Wait()
		require.Equal(t, l1, f1,
			"a declared-dimension cold flight keeps byte handoff: the follower receives the leader's bytes")
		require.Equal(t, int64(1), fetches.Load(),
			"the pair collapsed onto one fetch (leader held, follower parked on the handoff)")
		require.Equal(t, 1, memoEntries(&h.unstorableMemo),
			"the anonymous no-store settle arms the memo")

		// Pair 2 on the same key, memo armed: still a byte-handoff
		// cold flight — the memo is never consulted here.
		l2, f2 := pair(release2)
		synctest.Wait()
		require.Equal(t, l2, f2)
		require.Equal(t, int64(2), fetches.Load(),
			"pair 2 collapses onto one fetch too: the memo never gates declared-dimension flights")
	})
}

// uniqueBodies deduplicates response bodies; len == n proves every
// caller received a distinct (own-fetched) response.
func uniqueBodies(bodies []string) map[string]struct{} {
	set := make(map[string]struct{}, len(bodies))
	for _, b := range bodies {
		set[b] = struct{}{}
	}
	return set
}
