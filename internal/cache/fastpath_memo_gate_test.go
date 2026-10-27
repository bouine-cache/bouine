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

// This file pins finding F12: the unstorable-fill memo (ADR-0066
// amendment #2) gates the fast path's bare-key peer ask — a key whose
// last local fill settled no-store/private cannot be held by the owner
// either, so the ask would deterministically 404. The slow path's own
// owner-first ask is gated by the same verdict (policied routes, where
// the OwnerMiss hint does not bind, included).

// TestFastPathPeerFetch_UnstorableMemoSkipsAsk pins the fast-path gate:
// a memoized key must not pay the bare-key peer RPC, and the slow path
// must learn the ask is futile via the OwnerMiss hint.
func TestFastPathPeerFetch_UnstorableMemoSkipsAsk(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(fasthttp.StatusOK)
			_, _ = ctx.Write([]byte("from-origin"))
		},
		Store: store,
	})
	fp := NewFastPathHandler(h)

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/memoized",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	key := buildKeyFromRaw(req, nil)
	h.unstorableMemo.note(key)

	var asks atomic.Int32
	fp.WithPeerFetch(
		func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			asks.Add(1)
			return nil, nil
		},
	)

	resp, ok := fp.TryHit(req, time.Now())
	require.False(t, ok, "a memoized unstorable key must fall through to the slow path")
	assert.Nil(t, resp)
	assert.Equal(t, int32(0), asks.Load(), "the memo must suppress the bare-key peer ask")
	assert.True(t, req.OwnerMiss, "the slow path must be told the owner ask is futile")
}

// TestFastPathPeerFetch_NoMemoStillAsks pins the negative: without a memo
// verdict the bare-key ask runs as before and a peer hit is served.
func TestFastPathPeerFetch_NoMemoStillAsks(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	h := NewHandler(HandlerConfig{
		Upstream: func(ctx *fasthttp.RequestCtx) {
			ctx.Response.Header.Set(header.CacheControl, "max-age=60")
			ctx.SetStatusCode(fasthttp.StatusOK)
			_, _ = ctx.Write([]byte("from-origin"))
		},
		Store: store,
	})
	fp := NewFastPathHandler(h)

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/fresh-miss",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	key := buildKeyFromRaw(req, nil)
	peerObj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap(header.CacheControl, "max-age=60"),
		Body:       []byte("from-peer"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}

	var asks atomic.Int32
	fp.WithPeerFetch(
		func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			asks.Add(1)
			return peerObj, nil
		},
	)

	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok, "an unmemoized remote key is served by the owner")
	require.NotNil(t, resp)
	assert.Equal(t, "HIT", resp.CacheResult)
	assert.Equal(t, int32(1), asks.Load())
	assert.False(t, req.OwnerMiss)
}

// TestHandleCacheMiss_UnstorableMemoSkipsPeerAsk pins the slow-path gate:
// even without the fast path's hint (this test enters via ServeRequest),
// a memoized key must not pay the owner-first peer RPC — it goes straight
// to origin.
func TestHandleCacheMiss_UnstorableMemoSkipsPeerAsk(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var peerFetchCalls, originCalls atomic.Int32
	originUpstream := func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("from-origin"))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   originUpstream,
		FastClient: &testFastClient{handler: originUpstream},
		Store:      store,
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		PeerFetch: func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			peerFetchCalls.Add(1)
			return nil, nil
		},
	})

	r := testCtx("GET", "http://example.com/memoized-slow")
	h.unstorableMemo.note(BuildKey(mustRequestInfo(t, "http://example.com/memoized-slow"), nil))
	h.ServeRequest(r)

	require.Equal(t, 200, respCode(r))
	assert.Equal(t, "from-origin", respBody(r))
	assert.Equal(t, int32(0), peerFetchCalls.Load(), "the memo must gate the slow-path owner ask")
	assert.Equal(t, int32(1), originCalls.Load())
}

// TestHandleCacheMiss_NoMemoStillPeerFetches pins the negative on the
// slow path: an unmemoized remote-owned key still asks the owner
// (TestHandleCacheMiss_PeerFetch pins the serving half; this pins the
// call-count half with a definitive miss answer).
func TestHandleCacheMiss_NoMemoStillPeerFetches(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	var peerFetchCalls, originCalls atomic.Int32
	originUpstream := func(ctx *fasthttp.RequestCtx) {
		originCalls.Add(1)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_, _ = ctx.Write([]byte("from-origin"))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   originUpstream,
		FastClient: &testFastClient{handler: originUpstream},
		Store:      store,
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		PeerFetch: func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			peerFetchCalls.Add(1)
			return nil, nil
		},
	})

	r := testCtx("GET", "http://example.com/unmemoized-slow")
	h.ServeRequest(r)

	require.Equal(t, 200, respCode(r))
	assert.Equal(t, "from-origin", respBody(r))
	assert.Equal(t, int32(1), peerFetchCalls.Load(), "an unmemoized key must still ask the owner")
	assert.Equal(t, int32(1), originCalls.Load())
}
