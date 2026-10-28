package cache

import (
	"context"
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
		n := NewNearCache(NearCacheConfig{MaxTTL: time.Minute})
		armAdmitMemo(n, key)
		n.Admit(t.Context(), key, nearTestObject(key, time.Minute), now)
		require.True(t, mustNearLookup(t, n, key, now))
		_, err := n.store.Ban(t.Context(), api.BanExpr{PathRegex: "/guarded"})
		require.NoError(t, err)
		assert.False(t, mustNearLookup(t, n, key, now),
			"a banned key must miss in the near cache the moment it would miss on the owner")
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
