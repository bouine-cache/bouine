package cache

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// The ADR-0058 trigger table — stated in the PR description, the ADR, the
// changelog, and detectVaryDrift's own doc comment — is that ONLY a good
// fresh response (304 or cacheable 2xx) can signal drift: an origin
// under duress (5xx/4xx) or an uncacheable response is not a declaration
// source, and a stale declaration must never be purged by a response that
// will not itself be stored. These tests pin the table on the FOREGROUND
// revalidate path, where the call used to sit before the store gate.

// fillTenantVariant fills and returns the primary/lookup key pair for a
// no-cache Vary-carrying route, so every later request revalidates.
func fillTenantVariant(t *testing.T, h *Handler) {
	t.Helper()
	ctx := testCtx("GET", "http://example.com/page")
	ctx.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx)
	require.Contains(t, respBody(ctx), "body-v1")
}

func TestRevalidate_VaryDrift_5xxNeverSignals(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	counter := &driftCounter{}

	var fail atomic.Bool
	origin := func(ctx *fasthttp.RequestCtx) {
		if fail.Load() {
			ctx.SetStatusCode(fasthttp.StatusBadGateway)
			_, _ = ctx.WriteString("origin down")
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body-v1")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		VaryDrift:  counter,
	})

	fillTenantVariant(t, h)

	// A 502 that no stale-fallback gate intercepts (no-cache forbids
	// serving stale) must reach the client as an error without ever
	// treating the error response as a declaration.
	fail.Store(true)
	ctx2 := testCtx("GET", "http://example.com/page")
	ctx2.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx2)
	require.Equal(t, 502, ctx2.Response.StatusCode(),
		"no-cache forbids stale serving: the client must see the 502")
	require.Equal(t, int64(0), counter.n.Load(),
		"a 5xx must never signal drift: an origin under duress is not a declaration source")

	// The stored resolver and variant must survive the outage.
	ctx3 := testCtx("GET", "http://example.com/page")
	ctx3.Request.Header.Set("X-Tenant-Id", "14")
	primaryKey, lookupKey, obj, _ := h.lookup(ctx3)
	require.NotNil(t, obj, "the stored object must survive a 502 revalidation")
	require.NotEqual(t, primaryKey, lookupKey, "the Vary resolver must survive a 502 revalidation")
}

func TestRevalidate_VaryDrift_404NeverSignals(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	counter := &driftCounter{}

	var gone atomic.Bool
	origin := func(ctx *fasthttp.RequestCtx) {
		if gone.Load() {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body-v1")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		VaryDrift:  counter,
	})

	fillTenantVariant(t, h)

	// A 404 (content transiently deleted behind a flaky deploy) is not a
	// declaration source, with or without negative caching.
	gone.Store(true)
	ctx2 := testCtx("GET", "http://example.com/page")
	ctx2.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx2)
	require.Equal(t, int64(0), counter.n.Load(),
		"a 404 must never signal drift: content deletion is not a declaration change")
}

func TestRevalidate_VaryDrift_Uncacheable200NeverSignals(t *testing.T) {
	t.Parallel()

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	counter := &driftCounter{}

	var uncacheable atomic.Bool
	origin := func(ctx *fasthttp.RequestCtx) {
		if uncacheable.Load() {
			ctx.Response.Header.Set(header.CacheControl, "no-store")
			ctx.Response.Header.Set(header.Vary, "X-Tenant-Id, X-Extra")
			ctx.SetStatusCode(200)
			_, _ = ctx.WriteString("body-v2")
			return
		}
		ctx.Response.Header.Set(header.CacheControl, "no-cache, max-age=60")
		ctx.Response.Header.Set(header.Vary, "X-Tenant-Id")
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.WriteString("body-v1")
	}
	h := NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      store,
		VaryDrift:  counter,
	})

	fillTenantVariant(t, h)

	// A 200 that will not be stored (no-store) must not purge the stored
	// surface either: nothing fresh lands under the new declaration,
	// so the purge would only destroy live variants.
	uncacheable.Store(true)
	ctx2 := testCtx("GET", "http://example.com/page")
	ctx2.Request.Header.Set("X-Tenant-Id", "14")
	serveRequest(h, ctx2)
	require.Equal(t, int64(0), counter.n.Load(),
		"an uncacheable 200 must never signal drift: the response that would anchor the new declaration is not stored")
}
