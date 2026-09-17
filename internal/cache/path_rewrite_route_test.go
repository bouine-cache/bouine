package cache

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// rewritePattern/rewriteReplace are the production nginx migration
// shape shared by every test in this file: the /payment/orchestrator/
// callback/<x> public path rewritten to /scrooge/callback/<x>.
const (
	rewritePattern = `^/payment/orchestrator/callback/(.*)$`
	rewriteReplace = "/scrooge/callback/$1"
)

// TestPathRewriteRoute_MissSendsRewrittenURIToOrigin pins the core
// contract: the origin receives the regex-rewritten path (query kept),
// while the cache key keeps the original public path.
func TestPathRewriteRoute_MissSendsRewrittenURIToOrigin(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		PathRewrite: NewPathRewrite(rewritePattern, rewriteReplace),
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/payment/orchestrator/callback/payin123?sig=1")
	h.ServeRequest(rr)

	require.Equal(t, 200, respCode(rr))
	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/scrooge/callback/payin123?sig=1", uris[0],
		"origin must receive the rewritten URI with the query preserved")
}

// TestPathRewriteRoute_CacheKeyUsesOriginalPath pins that the object is
// stored and served under the original public path — a second request
// is a HIT with no second origin fetch, and the cached body comes from
// the rewritten-path origin response.
func TestPathRewriteRoute_CacheKeyUsesOriginalPath(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		PathRewrite: NewPathRewrite(rewritePattern, rewriteReplace),
	})
	defer h.Close(context.Background())

	url := "/payment/orchestrator/callback/payin123"
	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, "MISS", respHeader(rr, header.XCache))

	rr2 := testCtx("GET", url)
	h.ServeRequest(rr2)
	assert.Equal(t, "HIT", respHeader(rr2, header.XCache))
	assert.Len(t, cap.uris(), 1, "no second origin fetch on the hit")
	assert.Equal(t, "origin-body", string(rr2.Response.Body()),
		"cached body must come from the rewritten-path origin response")
}

// TestPathRewriteRoute_BypassAndInvalidateSendRewrittenURI covers the
// BYPASS path (Cache-Control: no-cache) and the invalidating-proxy path
// (POST): both send the rewritten URI to the origin.
func TestPathRewriteRoute_BypassAndInvalidateSendRewrittenURI(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		PathRewrite: NewPathRewrite(rewritePattern, rewriteReplace),
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/payment/orchestrator/callback/bypass")
	rr.Request.Header.Set(header.CacheControl, "no-cache")
	h.ServeRequest(rr)

	rr = testCtx("POST", "/payment/orchestrator/callback/payin123")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 2)
	assert.Equal(t, "/scrooge/callback/bypass", uris[0])
	assert.Equal(t, "/scrooge/callback/payin123", uris[1])
}

// TestPathRewriteRoute_RevalidateSendsRewrittenURI covers the
// foreground revalidation path (conditional request after stale hit).
func TestPathRewriteRoute_RevalidateSendsRewrittenURI(t *testing.T) {
	t.Parallel()
	var capture stripCapture
	origin := func(ctx *fasthttp.RequestCtx) {
		if len(ctx.Request.Header.Peek(header.IfNoneMatch)) > 0 {
			capture.add(ctx.RequestURI())
			ctx.SetStatusCode(304)
			return
		}
		capture.add(ctx.RequestURI())
		ctx.Response.Header.Set(header.CacheControl, "max-age=0, must-revalidate")
		ctx.Response.Header.Set(header.ETag, `"rw-v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		PathRewrite: NewPathRewrite(rewritePattern, rewriteReplace),
	})
	defer h.Close(context.Background())

	url := "/payment/orchestrator/callback/reval"
	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, "MISS", respHeader(rr, header.XCache))

	rr = testCtx("GET", url)
	h.ServeRequest(rr)
	assert.Equal(t, "REVALIDATED", respHeader(rr, header.XCache))

	uris := capture.uris()
	require.Len(t, uris, 2)
	assert.Equal(t, "/scrooge/callback/reval", uris[0], "initial fetch must be rewritten")
	assert.Equal(t, "/scrooge/callback/reval", uris[1], "revalidation must be rewritten")
}

// TestPathRewriteRoute_WithoutRewritePassthrough pins the zero-config
// route: no path_rewrite means the origin sees the original path.
func TestPathRewriteRoute_WithoutRewritePassthrough(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient: &testFastClient{handler: origin},
		Store:      store,
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/payment/orchestrator/callback/x")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/payment/orchestrator/callback/x", uris[0])
}

// TestPathRewriteRoute_NoMatchPassthrough pins that a request whose
// path does not match the pattern is forwarded unmodified — the regex
// is scoped per route, but the handler must never corrupt a non-matching
// URI.
func TestPathRewriteRoute_NoMatchPassthrough(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		PathRewrite: NewPathRewrite(rewritePattern, rewriteReplace),
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/unrelated/path")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/unrelated/path", uris[0])
}

// TestPathRewriteRoute_BgRevalidateSendsRewrittenURI covers the SWR
// background revalidation path (triggerBgRevalidate →
// doBackgroundRevalidate), which reads the RequestInfo captured at
// stale-hit time.
func TestPathRewriteRoute_BgRevalidateSendsRewrittenURI(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var capture stripCapture
		origin := func(ctx *fasthttp.RequestCtx) {
			if string(ctx.Request.Header.Peek(header.IfNoneMatch)) != "" {
				capture.add(ctx.RequestURI())
				ctx.Response.Header.Set(header.CacheControl, "max-age=120")
				ctx.SetStatusCode(304)
				return
			}
			capture.add(ctx.RequestURI())
			ctx.Response.Header.Set(header.CacheControl, "max-age=60, stale-while-revalidate=3600")
			ctx.Response.Header.Set(header.ETag, `"rw-v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			FastClient:  &testFastClient{handler: origin},
			Store:       store,
			PathRewrite: NewPathRewrite(rewritePattern, rewriteReplace),
		})
		defer h.Close(context.Background())

		url := "/payment/orchestrator/callback/swr"
		rr := testCtx("GET", url)
		h.ServeRequest(rr)
		require.Equal(t, "MISS", respHeader(rr, header.XCache))

		time.Sleep(61 * time.Second)
		rr = testCtx("GET", url)
		h.ServeRequest(rr)
		require.Equal(t, "STALE", respHeader(rr, header.XCache))

		synctest.Wait()
		uris := capture.uris()
		require.Len(t, uris, 2)
		assert.Equal(t, "/scrooge/callback/swr", uris[0])
		assert.Equal(t, "/scrooge/callback/swr", uris[1], "background SWR revalidation must send the rewritten URI")
	})
}

// TestPathRewriteRoute_RefreshSendsRewrittenURI covers the
// refresh-before-expiry path: the registry stores the original URI; the
// refresh fetch must rewrite.
func TestPathRewriteRoute_RefreshSendsRewrittenURI(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		FastClient:          &testFastClient{handler: origin},
		Store:               store,
		PathRewrite:         NewPathRewrite(rewritePattern, rewriteReplace),
		RefreshBeforeExpiry: true,
		RefreshMinHits:      1,
		RefreshMargin:       6 * time.Second,
		RefreshTimeout:      5 * time.Second,
		RefreshConcurrency:  4,
	})
	defer h.Close(context.Background())

	url := "/payment/orchestrator/callback/page"
	rr := testCtx("GET", url)
	h.ServeRequest(rr)
	rr = testCtx("GET", url)
	h.ServeRequest(rr)
	require.Equal(t, "HIT", respHeader(rr, header.XCache))

	key := h.buildKey(testCtx("GET", url))
	obj, _, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj)

	h.doBackgroundRefresh(context.Background(), key, obj, 1)

	uris := cap.uris()
	require.Len(t, uris, 2)
	assert.Equal(t, "/scrooge/callback/page", uris[0])
	assert.Equal(t, "/scrooge/callback/page", uris[1],
		"refresh-before-expiry fetch must send the rewritten URI")
}
