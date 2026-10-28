package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// This file pins the experimental peer near-cache (ADR-0071, audit
// finding F2): admission guards (variant/resolver/empty-body/stale
// refusal, popularity threshold, TTL bounding, grace zeroing),
// fresh-only serving, and the invalidation surfaces (purge, refresh,
// ban) that ride the existing cluster broadcast.

type nearCounter struct{ n atomic.Int64 }

func (c *nearCounter) Inc() { c.n.Add(1) }

func nearTestObject(key api.Key, ttl time.Duration) *api.Object {
	return &api.Object{
		Key:        key,
		StatusCode: 200,
		// X-Bouine-Host/Path are stamped on every stored object by the
		// owner's buildObject; the wire codec carries them, so peer
		// objects reach the near cache with the ban-subject headers
		// intact (the near store's Get evaluates bans against them
		// exactly like the main store's).
		Header: headerMap(
			header.CacheControl, "public, max-age=600",
			header.XBouineHost, "example.com",
			header.XBouinePath, "/guarded",
		),
		Body:     []byte("peer-body"),
		BodySize: 9,
		StoredAt: time.Now(),
		TTL:      ttl,
	}
}

// armAdmitMemo satisfies the popularity gate for key without going
// through two peer fetches: the memo is package-internal state and the
// gate's window semantics are pinned by their own unit tests.
func armAdmitMemo(n *NearCache, key api.Key) {
	n.admit.note(key, nearAdmitWindow)
}

func TestNearCache_AdmitGuards(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/guarded"), nil)
	now := time.Now()

	variant := nearTestObject(key, time.Minute)
	variant.VaryKey = "v"
	variant.VaryValue = "Accept-Encoding"

	resolver := nearTestObject(key, time.Minute)
	resolver.VaryValue = "Accept-Encoding"
	resolver.Body = nil

	empty := nearTestObject(key, time.Minute)
	empty.Body = nil

	stale := nearTestObject(key, time.Minute)
	stale.StoredAt = now.Add(-2 * time.Minute)

	for name, obj := range map[string]*api.Object{
		"variant": variant, "resolver": resolver, "empty body": empty, "stale": stale,
	} {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, obj, now)
		_, ok := n.Lookup(t.Context(), key, now)
		assert.False(t, ok, "%s objects must never be admitted", name)
	}
}

func TestNearCache_PopularityThreshold(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/threshold"), nil)
	now := time.Now()
	n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
	obj := nearTestObject(key, time.Minute)

	// First sight: arms the gate, does not admit.
	n.Admit(t.Context(), key, obj, now)
	_, ok := n.Lookup(t.Context(), key, now)
	require.False(t, ok, "first peer fetch must not admit")

	// Second sight within the window: admits.
	n.Admit(t.Context(), key, obj, now)
	got, ok := n.Lookup(t.Context(), key, now)
	require.True(t, ok, "second peer fetch must admit")
	assert.Equal(t, obj.Body, got.Body)
}

func TestNearCache_TTLBoundedAndGraceZeroed(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/bounded"), nil)
	now := time.Now()

	t.Run("origin TTL shorter than max_ttl wins", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Hour})
		armAdmitMemo(n, key)
		obj := nearTestObject(key, 10*time.Second)
		obj.StoredAt = now
		obj.StaleWhileRevalidate = time.Minute
		obj.StaleIfError = time.Minute
		n.Admit(t.Context(), key, obj, now)
		got, ok := n.Lookup(t.Context(), key, now)
		require.True(t, ok)
		assert.Equal(t, 10*time.Second, got.TTL)
		assert.Zero(t, got.StaleWhileRevalidate, "near entries must never serve stale")
		assert.Zero(t, got.StaleIfError, "near entries must never serve stale")
	})

	t.Run("max_ttl caps a longer origin TTL", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: 5 * time.Second})
		armAdmitMemo(n, key)
		obj := nearTestObject(key, time.Hour)
		obj.StoredAt = now
		n.Admit(t.Context(), key, obj, now)
		got, ok := n.Lookup(t.Context(), key, now)
		require.True(t, ok)
		assert.Equal(t, 5*time.Second, got.TTL)
	})

	t.Run("expired entry is refused and removed", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
		later := now.Add(2 * time.Minute)
		_, ok := n.Lookup(t.Context(), key, later)
		assert.False(t, ok, "an expired near entry must never serve")
		// The entry was reclaimed, not just skipped.
		obj, _, err := n.store.Get(t.Context(), key)
		require.NoError(t, err)
		assert.Nil(t, obj, "expired near entries are deleted on lookup")
	})
}

func TestNearCache_InvalidationSurfaces(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/invalidated"), nil)
	now := time.Now()

	t.Run("purge removes the near entry", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
		require.True(t, mustNearLookup(t, n, key, now))
		n.Purge(t.Context(), key)
		assert.False(t, mustNearLookup(t, n, key, now), "purge must remove the near copy")
	})

	t.Run("refresh removes the near entry", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
		n.Refresh(t.Context(), key)
		assert.False(t, mustNearLookup(t, n, key, now),
			"refresh must remove the near copy: near entries have no grace to mark stale")
	})

	t.Run("ban blocks the near entry exactly like the main store", func(t *testing.T) {
		invalidations := &nearCounter{}
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute,
			Counters: NearCacheCounters{Invalidations: invalidations}})
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
		require.True(t, mustNearLookup(t, n, key, now))
		// The engine-facing surface (wired next to the engine's store.Ban
		// when a ban broadcasts), not the raw store.
		n.Ban(t.Context(), api.BanExpr{PathRegex: "/guarded"})
		assert.False(t, mustNearLookup(t, n, key, now),
			"a banned key must miss in the near cache the moment it would miss on the owner")
		assert.EqualValues(t, 1, invalidations.n.Load(), "a ban-driven removal counts as an invalidation")
	})
}

// TestNearCache_AdmitKeepsOwnerSideStoredAt pins the ban-exemption
// parity of admission: the near store's Get exempts entries whose
// StoredAt is after the ban's creation time (a refilled copy is fresh,
// bans.go subjectTo), so admission must keep the OWNER-side StoredAt.
// Re-stamping it with the admission time would exempt every entry
// from bans created before its admission — including a ban that
// landed while the peer answer was in flight and already killed the
// owner's copy — resurrecting banned bytes locally for the entry's
// TTL. The stored TTL carries the same absolute expiry either way.
func TestNearCache_AdmitKeepsOwnerSideStoredAt(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/ban-race"), nil)
	now := time.Now()

	t.Run("ban created while the peer answer was in flight kills the admitted copy", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		// The owner stored the object before the ban arrived; the ban
		// lands (broadcast) while the peer answer is in flight, i.e.
		// before this node admits it.
		obj := nearTestObject(key, 10*time.Minute)
		obj.StoredAt = time.Now()
		n.Ban(t.Context(), api.BanExpr{PathRegex: "/guarded"})
		n.Admit(t.Context(), key, obj, time.Now())
		assert.False(t, mustNearLookup(t, n, key, time.Now()),
			"an admitted copy of a banned object must stay banned: exemption compares the owner-side StoredAt, not the admission time")
	})

	t.Run("ban older than the object's storage does not block admission", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		// The owner refilled after the ban (its new copy is exempt, the
		// documented refill semantics) — the near copy must be exempt
		// the same way, or near hits would under-serve refilled keys.
		n.Ban(t.Context(), api.BanExpr{PathRegex: "/guarded"})
		obj := nearTestObject(key, 10*time.Minute)
		obj.StoredAt = time.Now()
		n.Admit(t.Context(), key, obj, time.Now())
		assert.True(t, mustNearLookup(t, n, key, time.Now()),
			"an exempt refill must admit and serve exactly like the owner's exempt copy")
	})

	t.Run("stored TTL preserves the absolute expiry and the owner-side StoredAt", func(t *testing.T) {
		n := NewNearCache(NearCacheConfig{MaxTTL: 30 * time.Second})
		armAdmitMemo(n, key)
		// Stored 10 s ago, fresh for another 50 s, older than max_ttl:
		// the entry must expire at now+max_ttl (the same instant a
		// re-stamped entry computed) while keeping the owner's clock.
		obj := nearTestObject(key, time.Hour)
		obj.StoredAt = now.Add(-10 * time.Second)
		n.Admit(t.Context(), key, obj, now)
		got, ok := n.Lookup(t.Context(), key, now)
		require.True(t, ok)
		assert.Equal(t, obj.StoredAt, got.StoredAt,
			"admission keeps the owner-side StoredAt (ban exemption and Age parity)")
		assert.Equal(t, now.Add(30*time.Second), got.StoredAt.Add(got.TTL),
			"the entry expires at admission-now + min(remaining freshness, max_ttl)")
		assert.True(t, mustNearLookup(t, n, key, now.Add(29*time.Second)),
			"fresh inside the bounded window")
		assert.False(t, mustNearLookup(t, n, key, now.Add(31*time.Second)),
			"expired past the bounded window")
	})
}

func TestNearCache_NilReceiverIsDisabled(t *testing.T) {
	t.Parallel()
	var n *NearCache
	key := BuildKey(mustRequestInfo(t, "http://example.com/nil"), nil)
	now := time.Now()

	// Every method is nil-safe: the disabled deployment shape.
	_, ok := n.Lookup(t.Context(), key, now)
	assert.False(t, ok)
	n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
	n.Purge(t.Context(), key)
	n.Refresh(t.Context(), key)
	n.Ban(t.Context(), api.BanExpr{})
	n.RecordHit()
	assert.Zero(t, n.Hits())
}

func TestNearCache_Counters(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/counted"), nil)
	now := time.Now()
	hits, admits, invalidations := &nearCounter{}, &nearCounter{}, &nearCounter{}
	n := NewNearCache(NearCacheConfig{
		MaxTTL: time.Minute,
		Counters: NearCacheCounters{
			Hits: hits, Admissions: admits, Invalidations: invalidations,
		},
	})
	armAdmitMemo(n, key)
	n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
	require.EqualValues(t, 1, admits.n.Load())

	// A fresh lookup alone is not a serve: the counter must stay flat
	// until the serve site records the hit.
	_, ok := n.Lookup(t.Context(), key, now)
	require.True(t, ok)
	require.Zero(t, hits.n.Load(), "a gate-pending lookup must not count as a serve")
	n.RecordHit()
	require.EqualValues(t, 1, hits.n.Load())
	require.EqualValues(t, 1, n.Hits())

	n.Refresh(t.Context(), key)
	assert.EqualValues(t, 1, invalidations.n.Load())
}

func mustNearLookup(t *testing.T, n *NearCache, key api.Key, now time.Time) bool {
	t.Helper()
	_, ok := n.Lookup(t.Context(), key, now)
	return ok
}

// TestFastPathPeerFetch_NearCacheServesWithoutAsk pins the fast-path
// integration end to end: two peer fetches admit the key, the third
// request serves from the near cache with no RPC, and the served
// source is "near" (X-Cache-Source parity with the slow path).
func TestFastPathPeerFetch_NearCacheServesWithoutAsk(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(fasthttp.StatusOK)
			_, _ = ctx.Write([]byte("from-origin"))
		},
		Store:     store,
		NearCache: NewNearCache(NearCacheConfig{MaxTTL: time.Minute}),
	})
	fp := NewFastPathHandler(h)

	var asks atomic.Int32
	fp.WithPeerFetch(
		func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		func(_ context.Context, _ api.PeerInfo, key api.Key, _ string) (*api.Object, error) {
			asks.Add(1)
			obj := nearTestObject(key, time.Minute)
			return obj, nil
		},
	)

	req := func() *api.RawRequest {
		return &api.RawRequest{
			Method:      "GET",
			Path:        "/near-hit",
			Host:        "example.com",
			Scheme:      "http",
			HTTPVersion: "HTTP/1.1",
		}
	}

	// First fetch: serves from the owner, arms the popularity gate
	// (no admission yet).
	now := time.Now()
	resp, ok := fp.TryHit(req(), now)
	require.True(t, ok, "the first peer answer serves like today")
	require.Equal(t, string(api.SourcePeer), resp.Source)
	require.EqualValues(t, 1, asks.Load())

	// Second fetch: serves from the owner again and admits into the
	// near cache.
	resp, ok = fp.TryHit(req(), now)
	require.True(t, ok)
	require.Equal(t, string(api.SourcePeer), resp.Source)
	require.EqualValues(t, 2, asks.Load())

	// Third request: near-cache serve, no RPC.
	resp, ok = fp.TryHit(req(), now)
	require.True(t, ok, "the admitted near entry must serve")
	require.NotNil(t, resp)
	assert.Equal(t, "HIT", resp.CacheResult)
	assert.Equal(t, string(api.SourceNear), resp.Source)
	assert.EqualValues(t, 2, asks.Load(), "a near hit must not re-ask the owner")
	assert.EqualValues(t, 1, h.nearCache.Hits(), "a near serve must be recorded")
}

// TestFastPathPeerFetch_MemoGatePrecedesNear pins the ordering: the
// unstorable-fill memo (finding F12) fires BEFORE the near lookup, so
// an origin that just settled no-store is not answered from a stale
// near copy even inside its freshness window.
func TestFastPathPeerFetch_MemoGatePrecedesNear(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
		Store:     store,
		NearCache: NewNearCache(NearCacheConfig{MaxTTL: time.Minute}),
	})
	fp := NewFastPathHandler(h)

	req := &api.RawRequest{
		Method: "GET", Path: "/memo-over-near", Host: "example.com",
		Scheme: "http", HTTPVersion: "HTTP/1.1",
	}
	key := buildKeyFromRaw(req, nil)

	// Admit a fresh near entry, then arm the unstorable memo for the
	// same key (an origin flip to no-store inside the near window).
	armAdmitMemo(h.nearCache, key)
	h.nearCache.Admit(t.Context(), key, nearTestObject(key, time.Minute), time.Now())
	h.unstorableMemo.note(key)

	var asks atomic.Int32
	fp.WithPeerFetch(
		func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Addr: "owner:8080"}, false },
		func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			asks.Add(1)
			return nil, nil
		},
	)

	resp, ok := fp.TryHit(req, time.Now())
	require.False(t, ok)
	assert.Nil(t, resp)
	assert.EqualValues(t, 0, asks.Load(), "the memo gate must suppress the ask")
	assert.True(t, req.OwnerMiss, "the slow path must learn the ask is futile")
}

// TestFastPathPeerFetch_StalePeerAnswerNotAdmitted pins that only a
// FRESH owner answer is a near-cache admission candidate: a stale peer
// object serves (SWR parity with today's behavior) but is never
// remembered.
func TestFastPathPeerFetch_StalePeerAnswerNotAdmitted(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
		Store:     store,
		NearCache: NewNearCache(NearCacheConfig{MaxTTL: time.Minute}),
	})
	fp := NewFastPathHandler(h)

	req := &api.RawRequest{
		Method: "GET", Path: "/stale-peer", Host: "example.com",
		Scheme: "http", HTTPVersion: "HTTP/1.1",
	}
	key := buildKeyFromRaw(req, nil)

	stale := nearTestObject(key, time.Minute)
	stale.StoredAt = time.Now().Add(-2 * time.Minute)
	stale.StaleWhileRevalidate = 5 * time.Minute

	fp.WithPeerFetch(
		func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Addr: "owner:8080"}, false },
		func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			return stale, nil
		},
	)
	for range 2 {
		resp, ok := fp.TryHit(req, time.Now())
		require.True(t, ok, "a stale peer answer still serves (SWR parity)")
		assert.Equal(t, "STALE", resp.CacheResult)
	}
	_, fresh := h.nearCache.Lookup(t.Context(), key, time.Now())
	assert.False(t, fresh, "a stale answer must not be admitted into the near cache")
}

// TestHandleCacheMiss_NearCacheServesAndInvalidates pins the slow-path
// integration: a near entry serves without the owner RPC, Purge removes
// it (the peer broadcast applies through this surface on every node),
// and the next request re-asks the owner.
func TestHandleCacheMiss_NearCacheServesAndInvalidates(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var peerFetchCalls, originCalls atomic.Int32
	originUpstream := func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("from-origin"))
	}
	near := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
	h := NewHandler(HandlerConfig{
		Upstream:   originUpstream,
		FastClient: &testFastClient{handler: originUpstream},
		Store:      store,
		NearCache:  near,
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		PeerFetch: func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			peerFetchCalls.Add(1)
			return nil, nil
		},
	})

	key := BuildKey(mustRequestInfo(t, "http://example.com/slow-near"), nil)
	armAdmitMemo(near, key)
	near.Admit(t.Context(), key, nearTestObject(key, time.Minute), time.Now())

	// Near serve: no peer RPC, no origin fetch.
	r := testCtx("GET", "http://example.com/slow-near")
	h.ServeRequest(r)
	require.Equal(t, 200, respCode(r))
	assert.Contains(t, respBody(r), "peer-body")
	assert.EqualValues(t, 0, peerFetchCalls.Load(), "the near copy must serve without the owner ask")
	assert.EqualValues(t, 0, originCalls.Load())
	require.EqualValues(t, 1, near.Hits(), "a near serve must be recorded")

	// Purge (what the peer broadcast applies on every node): the near
	// copy dies with the owner's copy, and the next request re-asks.
	_, err := h.Purge(t.Context(), key)
	require.NoError(t, err)
	r = testCtx("GET", "http://example.com/slow-near")
	h.ServeRequest(r)
	require.Equal(t, 200, respCode(r))
	assert.EqualValues(t, 1, peerFetchCalls.Load(), "a purged near copy must not serve")
	assert.EqualValues(t, 1, originCalls.Load(), "the definitive owner miss falls through to origin")
}

// TestHandleCacheMiss_NearEntryNotServedForOwnedKeys pins the slow
// path's ownership gate: a near entry for a key this node OWNS can
// only be a leftover from a ring change (admission runs on
// non-owners only), and serving it would skip this node's
// authoritative fill for up to the entry's TTL — the owner would 404
// its peers into origin while the leftover lives out its clock. The
// fast path already gates on isLocal before its near branch
// (tryPeerFetch returns first); the slow path must match it.
func TestHandleCacheMiss_NearEntryNotServedForOwnedKeys(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var peerFetchCalls, originCalls atomic.Int32
	originUpstream := func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("from-origin"))
	}
	near := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
	h := NewHandler(HandlerConfig{
		Upstream:   originUpstream,
		FastClient: &testFastClient{handler: originUpstream},
		Store:      store,
		NearCache:  near,
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			// This node owns the key: the ring moved ownership here
			// after the near entry was admitted on a node that owned
			// nothing at the time (a scale-up rebalance, a peer
			// restart).
			return api.PeerInfo{Addr: "self:8443"}, true
		},
		PeerFetch: func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			peerFetchCalls.Add(1)
			return nil, nil
		},
	})

	key := BuildKey(mustRequestInfo(t, "http://example.com/owned-near"), nil)
	armAdmitMemo(near, key)
	near.Admit(t.Context(), key, nearTestObject(key, time.Minute), time.Now())
	require.True(t, mustNearLookup(t, near, key, time.Now()), "precondition: the leftover near entry is fresh")

	r := testCtx("GET", "http://example.com/owned-near")
	h.ServeRequest(r)
	require.Equal(t, 200, respCode(r))
	assert.Equal(t, "from-origin", respBody(r),
		"an owned key must fill its authoritative copy from origin, not from a leftover near entry")
	assert.EqualValues(t, 1, originCalls.Load())
	assert.EqualValues(t, 0, peerFetchCalls.Load(), "the owner never asks itself")
	assert.EqualValues(t, 0, near.Hits(), "a near entry for an owned key must not serve on the slow path")
}

// getErrStore wraps a working store with a failing Get, so the near
// cache's error paths are pinned without a crashy fake.
type getErrStore struct {
	storage.Store
}

func (getErrStore) Get(context.Context, api.Key) (*api.Object, api.Source, error) {
	return nil, "", errors.New("get boom")
}

// putErrStore wraps a working store with a failing Put.
type putErrStore struct {
	storage.Store
}

func (putErrStore) Put(context.Context, api.Key, *api.Object) error {
	return errors.New("put boom")
}

// TestNearCache_StoreFailuresDegradGracefully pins the error paths: a
// failing store Get reports a near miss (the request falls through to
// the ordinary peer ask), and a failing store Put refuses admission
// without panicking or counting.
func TestNearCache_StoreFailuresDegradeGracefully(t *testing.T) {
	t.Parallel()
	key := BuildKey(mustRequestInfo(t, "http://example.com/storefail"), nil)
	now := time.Now()

	t.Run("Get error is a miss", func(t *testing.T) {
		t.Parallel()
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		n.store = getErrStore{Store: n.store}
		obj, ok := n.Lookup(t.Context(), key, now)
		assert.False(t, ok, "a store error must not surface as a near hit")
		assert.Nil(t, obj)
	})

	t.Run("Put error refuses admission", func(t *testing.T) {
		t.Parallel()
		admits := &nearCounter{}
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute,
			Counters: NearCacheCounters{Admissions: admits}})
		n.store = putErrStore{Store: n.store}
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
		assert.Zero(t, admits.n.Load(), "a refused admission must not count")
		_, ok := n.Lookup(t.Context(), key, now)
		assert.False(t, ok)
	})
}

// TestFastPathPeerFetch_NearEntrySurvivesGateRefusal pins the
// fall-through contract: a request whose own freshness gate refuses the
// fresh near entry (request Cache-Control: max-age=0) neither serves
// from nor evicts it — the ask proceeds and the next plain request
// still serves near.
func TestFastPathPeerFetch_NearEntrySurvivesGateRefusal(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(fasthttp.StatusOK)
		},
		Store:     store,
		NearCache: NewNearCache(NearCacheConfig{MaxTTL: time.Minute}),
	})
	fp := NewFastPathHandler(h)

	var asks atomic.Int32
	fp.WithPeerFetch(
		func(api.Key) (api.PeerInfo, bool) { return api.PeerInfo{Addr: "owner:8080"}, false },
		func(_ context.Context, _ api.PeerInfo, key api.Key, _ string) (*api.Object, error) {
			asks.Add(1)
			obj := nearTestObject(key, time.Minute)
			// A real peer answer carries the OWNER's store time, already
			// in the past when the requester evaluates it.
			obj.StoredAt = time.Now().Add(-100 * time.Millisecond)
			return obj, nil
		},
	)

	plain := &api.RawRequest{
		Method: "GET", Path: "/gate-refusal", Host: "example.com",
		Scheme: "http", HTTPVersion: "HTTP/1.1",
	}
	key := buildKeyFromRaw(plain, nil)

	// Admit via the popularity gate (two peer fetches).
	for range 2 {
		_, ok := fp.TryHit(plain, time.Now())
		require.True(t, ok)
	}
	require.EqualValues(t, 2, asks.Load())
	_, fresh := h.nearCache.Lookup(t.Context(), key, time.Now())
	require.True(t, fresh, "precondition: the near entry is admitted")

	// max-age=0: the request's own freshness clock refuses the fresh
	// near entry, so the near lookup must fall through to the peer
	// ask. The owner's answer fails the same request clock and, with no
	// validators to revalidate against, the whole fast path falls
	// through to the slow path/origin — the strict request is served by
	// neither the near copy nor the peer answer, and the refusal must
	// not evict the entry. The header travels the real path:
	// RecomputeScanFlags derives CacheControlRaw from the header array,
	// not the other way round.
	strict := &api.RawRequest{
		Method: "GET", Path: "/gate-refusal", Host: "example.com",
		Scheme: "http", HTTPVersion: "HTTP/1.1",
	}
	strict.Headers[0] = api.RawHeader{Key: "Cache-Control", Value: "max-age=0"}
	strict.NHeaders = 1
	strict.RecomputeScanFlags()
	require.Equal(t, "max-age=0", strict.CacheControlRaw)
	resp, ok := fp.TryHit(strict, time.Now())
	require.False(t, ok, "a gate-refused near entry must not serve; the strict request falls through")
	require.Nil(t, resp)
	assert.EqualValues(t, 3, asks.Load(), "the refusal still falls through to the peer ask")

	// The near entry survived the refusal: the next plain request
	// still serves from local memory.
	resp, ok = fp.TryHit(plain, time.Now())
	require.True(t, ok)
	assert.Equal(t, string(api.SourceNear), resp.Source)
	assert.EqualValues(t, 3, asks.Load(), "the near entry must still serve after a gate refusal")
}

// TestNearCache_ZeroConfigAppliesDefaults pins the built-in bounds: an
// all-zero config (the config layer's zero values) yields the 16 MiB /
// 10 s defaults, not a zero-byte store or an entry that dies at once,
// and the near store's lazy-ban TTL is capped at max_ttl plus the
// admission window (a ban older than that can never bite a near entry).
func TestNearCache_ZeroConfigAppliesDefaults(t *testing.T) {
	t.Parallel()
	n := NewNearCache(NearCacheConfig{})
	require.NotNil(t, n.store)

	hot, ok := n.store.(*storage.HotStore)
	require.True(t, ok, "the near store is a hot store")
	assert.Equal(t, nearMaxTTLDefault+nearAdmitWindow, hot.BanTTL(),
		"the near store prunes bans at max_ttl + admission window, not the 24h main-store default")

	key := BuildKey(mustRequestInfo(t, "http://example.com/defaults"), nil)
	now := time.Now()
	armAdmitMemo(n, key)
	obj := nearTestObject(key, time.Hour)
	// Pin the owner-side StoredAt to the test clock: the stored TTL is
	// a duration-since-StoredAt, so an unpinned one drifts by the
	// nanoseconds between the two time.Now() calls.
	obj.StoredAt = now
	n.Admit(t.Context(), key, obj, now)

	// The entry survives past the 10 s default cap only because the
	// origin TTL (1 h) is bounded by maxTTL: the stored TTL must be the
	// 10 s default, not the object's hour.
	got, ok := n.Lookup(t.Context(), key, now)
	require.True(t, ok)
	require.Equal(t, nearMaxTTLDefault, got.TTL, "max_ttl zero applies the built-in default")
}

// TestNearCache_BanTTLScalesWithMaxTTL pins the ban-TTL cap for an
// explicit max_ttl: the bound is max_ttl plus the admission window (an
// answer fetched before a ban can be admitted one peer-fetch round
// trip later, and dies at admission + max_ttl), so a ban that could
// still bite is never pruned early — and nothing is kept anywhere
// near the main store's 24 h.
func TestNearCache_BanTTLScalesWithMaxTTL(t *testing.T) {
	t.Parallel()
	n := NewNearCache(NearCacheConfig{MaxTTL: 45 * time.Second})
	hot, ok := n.store.(*storage.HotStore)
	require.True(t, ok)
	assert.Equal(t, 45*time.Second+nearAdmitWindow, hot.BanTTL())
	assert.Less(t, hot.BanTTL(), storage.DefaultBanTTL,
		"the near store's ban list must not live on the main store's 24h clock")
}
