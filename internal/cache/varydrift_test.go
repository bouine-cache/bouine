package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// driftCounter is a minimal varyDriftInc collector for tests.
type driftCounter struct{ n atomic.Int64 }

func (c *driftCounter) Inc() { c.n.Add(1) }

// TestDetectVaryDrift pins the drift detector's trigger table in
// isolation (ADR-0058): only a stored non-empty declaration that
// differs from the fresh response's effectiveVary union counts as
// drift, and drift purges the primary key (resolver and variants).
func TestDetectVaryDrift(t *testing.T) {
	t.Parallel()

	policy := NewKeyPolicy(nil, nil, nil, nil, false, false, nil, false)
	ri := RequestInfo{Method: "GET", Host: "example.com", Path: "/page", URI: "/page"}
	primary := BuildKey(ri, policy)

	newHandler := func(counter *driftCounter, store storage.Store) *Handler {
		return NewHandler(HandlerConfig{
			Store:     store,
			Policy:    policy,
			VaryDrift: counter,
		})
	}

	t.Run("no drift: same declaration is a no-op", func(t *testing.T) {
		t.Parallel()
		counter := &driftCounter{}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		h := newHandler(counter, store)
		stored := &api.Object{Key: primary, VaryValue: "accept-language"}
		h.detectVaryDrift(context.Background(), stored, ri, "accept-language")
		require.Equal(t, int64(0), counter.n.Load(), "a matching declaration must not signal")
	})

	t.Run("no drift: field order and case normalize equal", func(t *testing.T) {
		t.Parallel()
		counter := &driftCounter{}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		h := newHandler(counter, store)
		stored := &api.Object{Key: primary, VaryValue: "accept-encoding, accept-language"}
		h.detectVaryDrift(context.Background(), stored, ri, "Accept-Language, Accept-Encoding")
		require.Equal(t, int64(0), counter.n.Load(),
			"effectiveVary sorts and lowercases: reordered or re-cased fields are the same surface")
	})

	t.Run("no drift: empty stored declaration never signals", func(t *testing.T) {
		t.Parallel()
		counter := &driftCounter{}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		h := newHandler(counter, store)
		stored := &api.Object{Key: primary}
		h.detectVaryDrift(context.Background(), stored, ri, "accept-language")
		require.Equal(t, int64(0), counter.n.Load(),
			"an empty stored VaryValue is either genuinely Vary-less or a legacy warm blob re-deriving on next store")
	})

	t.Run("nil stale object never signals", func(t *testing.T) {
		t.Parallel()
		counter := &driftCounter{}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		h := newHandler(counter, store)
		h.detectVaryDrift(context.Background(), nil, ri, "accept-language")
		require.Equal(t, int64(0), counter.n.Load())
	})

	t.Run("drift: changed declaration purges the resolver and signals", func(t *testing.T) {
		t.Parallel()
		counter := &driftCounter{}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		h := newHandler(counter, store)
		// A resolver entry under the primary key and one tracked variant
		// under a variant key — the purge must remove both.
		variantKey := api.NewKeyFromBytes([16]byte{9})
		resolver := &api.Object{Key: primary, VaryValue: "accept-language", TTL: time.Minute}
		variant := &api.Object{Key: variantKey, VaryValue: "accept-language", TTL: time.Minute}
		require.NoError(t, store.Put(context.Background(), primary, resolver))
		require.NoError(t, store.Put(context.Background(), variantKey, variant))

		stored := &api.Object{Key: variantKey, VaryValue: "accept-language"}
		h.detectVaryDrift(context.Background(), stored, ri, "accept-language, x-tenant-id")
		require.Equal(t, int64(1), counter.n.Load(), "a changed declaration must signal exactly once")

		got, _, err := store.Get(context.Background(), primary)
		require.NoError(t, err)
		require.Nil(t, got, "the resolver entry must be purged on drift")
	})

	t.Run("drift: dropping a field purges too (surface changed either way)", func(t *testing.T) {
		t.Parallel()
		counter := &driftCounter{}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		h := newHandler(counter, store)
		resolver := &api.Object{Key: primary, VaryValue: "accept-language", TTL: time.Minute}
		require.NoError(t, store.Put(context.Background(), primary, resolver))
		stored := &api.Object{Key: primary, VaryValue: "accept-language, x-tenant-id"}
		h.detectVaryDrift(context.Background(), stored, ri, "accept-language")
		require.Equal(t, int64(1), counter.n.Load(),
			"the origin narrowing its declaration also invalidates the stored variant set")
	})
}

// TestRevalidate_VaryDriftPurgesAndRefills is the end-to-end pin: a
// route whose origin declares Vary on the first fill, then declares a
// DIFFERENT surface on revalidation (the origin added a selector
// header), must detect the drift, purge the stale resolver and its
// variants, and re-store under the fresh declaration — so the next
// lookup computes variant keys from the NEW surface, not the
// abandoned one. The revalidation is forced deterministically with
// no-cache + ETag (the same trigger the revalidate-collapse tests
// use): every request after the fill takes the conditional path, no
// clock games needed. The origin answers the revalidation with a
// full 200 carrying the new Vary — a 200 is the observable way for
// an origin to change its declaration mid-life (the response body
// changed with it).
func TestRevalidate_VaryDriftPurgesAndRefills(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	counter := &driftCounter{}

	var declares atomic.Bool
	var fetches atomic.Int64
	origin := func(ctx *fasthttp.RequestCtx) {
		if fetches.Add(1) == 2 {
			// The second fetch is the revalidation: flip the
			// declaration so the fresh 200 declares a wider surface.
			declares.Store(true)
		}
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.SetStatusCode(200)
		if declares.Load() {
			ctx.Response.Header.Set(header.Vary, "X-Tenant-Id, X-Region")
			ctx.Response.Header.Set(header.ETag, `"v2"`)
			_, _ = ctx.WriteString("body-v2")
			return
		}
		ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		_, _ = ctx.WriteString("body-v1")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		VaryDrift:  counter,
	})

	// Fill: stores the resolver with VaryValue "x-tenant-id".
	ctx := testCtx("GET", "http://example.com/page")
	ctx.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx)
	require.Contains(t, respBody(ctx), "body-v1")

	// Revalidate: no-cache forces the conditional path; the origin's
	// fresh 200 declares the wider surface.
	ctx2 := testCtx("GET", "http://example.com/page")
	ctx2.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx2)
	require.Contains(t, respBody(ctx2), "body-v2", "the revalidation must serve the fresh body")
	require.Equal(t, int64(2), fetches.Load(), "the second request must have revalidated, not bypassed")
	require.Equal(t, int64(1), counter.n.Load(),
		"the changed declaration must have been detected on the revalidate")

	// The store must now hold the fresh declaration: a request in a
	// region the old surface ignored must resolve to a MISS (no stored
	// variant under the new keys), not to the old tenant variant served
	// as a HIT.
	ctx3 := testCtx("GET", "http://example.com/page")
	ctx3.Request.Header.Set("X-Tenant-Id", "14")
	ctx3.Request.Header.Set("X-Region", "eu")
	serveRequest(h, ctx3)
	require.Equal(t, "MISS", string(ctx3.Response.Header.Peek(header.XCache)),
		"a region the fresh surface distinguishes must not resolve onto the old variant")
	require.Contains(t, respBody(ctx3), "body-v2",
		"the re-filled store must serve the fresh body under the new declaration")
}
