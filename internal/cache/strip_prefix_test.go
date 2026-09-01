package cache

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// stripCapture records the URIs the fake origin received.
type stripCapture struct {
	uri []string
	mu  sync.Mutex
}

func (c *stripCapture) add(uri []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.uri = append(c.uri, string(uri))
}

func (c *stripCapture) uris() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.uri...)
}

// stripOrigin returns a cacheable origin handler that records request URIs.
func stripOrigin(cap *stripCapture) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		cap.add(ctx.RequestURI())
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ETag, `"strip-v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("origin-body"))
	}
}

// TestStripPrefix_MissSendsStrippedURIToOrigin pins the issue #595
// contract: the origin receives the stripped path (with query), while
// the cache key keeps the original path.
func TestStripPrefix_MissSendsStrippedURIToOrigin(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		StripPrefix: "/api/v1",
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/api/v1/users?q=1")
	h.ServeRequest(rr)

	require.Equal(t, 200, respCode(rr))
	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/users?q=1", uris[0], "origin must receive the stripped URI")
}

// TestStripPrefix_CacheKeyUsesOriginalPath verifies that the stored key
// is derived from the original (un-stripped) URI per the config contract
// (internal/config/config.go RouteRequest.StripPrefix doc).
func TestStripPrefix_CacheKeyUsesOriginalPath(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		StripPrefix: "/api/v1",
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/api/v1/users")
	h.ServeRequest(rr)

	key := h.buildKey(testCtx("GET", "/api/v1/users"))
	obj, src, err := store.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, obj, "object must be stored under the ORIGINAL-path key")
	require.Equal(t, api.SourceHot, src)

	// Second request is a HIT under the same key — the miss→hit round
	// trip works end to end without a second origin call.
	rr2 := testCtx("GET", "/api/v1/users")
	h.ServeRequest(rr2)
	assert.Equal(t, "HIT", respHeader(rr2, header.XCache))
	assert.Len(t, cap.uris(), 1, "no second origin fetch")
}

// TestStripPrefix_BypassAndInvalidateSendStrippedURI covers the BYPASS
// and POST-invalidation origin paths.
func TestStripPrefix_BypassAndInvalidateSendStrippedURI(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		StripPrefix: "/api/v1",
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/api/v1/bypass")
	rr.Request.Header.Set(header.CacheControl, "no-cache")
	h.ServeRequest(rr)

	rr = testCtx("POST", "/api/v1/users")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 2)
	assert.Equal(t, "/bypass", uris[0])
	assert.Equal(t, "/users", uris[1])
}

// TestStripPrefix_RevalidateSendsStrippedURI covers the foreground
// revalidation path (conditional request after stale hit).
func TestStripPrefix_RevalidateSendsStrippedURI(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	var calls int
	origin := func(ctx *fasthttp.RequestCtx) {
		calls++
		if len(ctx.Request.Header.Peek(header.IfNoneMatch)) > 0 {
			cap.add(ctx.RequestURI())
			ctx.SetStatusCode(304)
			return
		}
		cap.add(ctx.RequestURI())
		ctx.Response.Header.Set(header.CacheControl, "max-age=0, must-revalidate")
		ctx.Response.Header.Set(header.ETag, `"strip-v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body"))
	}
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		StripPrefix: "/api/v1",
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/api/v1/reval")
	h.ServeRequest(rr)
	require.Equal(t, "MISS", respHeader(rr, header.XCache))

	rr = testCtx("GET", "/api/v1/reval")
	h.ServeRequest(rr)
	assert.Equal(t, "REVALIDATED", respHeader(rr, header.XCache))

	uris := cap.uris()
	require.Len(t, uris, 2)
	assert.Equal(t, "/reval", uris[0], "initial fetch must be stripped")
	assert.Equal(t, "/reval", uris[1], "revalidation must be stripped")
}

// TestStripPrefix_BgRevalidateSendsStrippedURI covers the SWR background
// revalidation path (triggerBgRevalidate → doBackgroundRevalidate), which
// reads the RequestInfo captured at stale-hit time.
func TestStripPrefix_BgRevalidateSendsStrippedURI(t *testing.T) {
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
			ctx.Response.Header.Set(header.ETag, `"v1"`)
			ctx.SetStatusCode(200)
			_, _ = ctx.Write([]byte("body"))
		}
		store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
		defer store.Close(context.Background())
		h := NewHandler(HandlerConfig{
			FastClient:  &testFastClient{handler: origin},
			Store:       store,
			StripPrefix: "/api/v1",
		})
		defer h.Close(context.Background())

		rr := testCtx("GET", "/api/v1/swr")
		h.ServeRequest(rr)
		require.Equal(t, "MISS", respHeader(rr, header.XCache))

		time.Sleep(61 * time.Second)
		rr = testCtx("GET", "/api/v1/swr")
		h.ServeRequest(rr)
		require.Equal(t, "STALE", respHeader(rr, header.XCache))

		synctest.Wait()
		uris := capture.uris()
		require.Len(t, uris, 2)
		assert.Equal(t, "/swr", uris[0])
		assert.Equal(t, "/swr", uris[1], "background SWR revalidation must send the stripped URI")
	})
}

// TestStripPrefix_EmptyPathBecomesSlash pins the request-line guard: a
// request whose path equals the prefix must not produce an origin
// request with an empty path.
func TestStripPrefix_EmptyPathBecomesSlash(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		StripPrefix: "/api/v1",
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/api/v1?q=1")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/?q=1", uris[0])
}

// TestStripPrefix_NonMatchingPrefixPassthrough: requests that do not
// carry the prefix are forwarded unmodified.
func TestStripPrefix_NonMatchingPrefixPassthrough(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	defer store.Close(context.Background())
	h := NewHandler(HandlerConfig{
		FastClient:  &testFastClient{handler: origin},
		Store:       store,
		StripPrefix: "/api/v1",
	})
	defer h.Close(context.Background())

	rr := testCtx("GET", "/other/path")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/other/path", uris[0])
}

// TestStripPrefix_WithoutConfigPassthrough: when StripPrefix is empty
// the origin URI is untouched.
func TestStripPrefix_WithoutConfigPassthrough(t *testing.T) {
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

	rr := testCtx("GET", "/api/v1/users")
	h.ServeRequest(rr)

	uris := cap.uris()
	require.Len(t, uris, 1)
	assert.Equal(t, "/api/v1/users", uris[0])
}

// TestStripPrefix_RefreshSendsStrippedURI covers the refresh-before-expiry
// path: the registry stores the original URI; the refresh fetch must strip.
func TestStripPrefix_RefreshSendsStrippedURI(t *testing.T) {
	t.Parallel()
	cap := &stripCapture{}
	origin := stripOrigin(cap)
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		FastClient:          &testFastClient{handler: origin},
		Store:               store,
		StripPrefix:         "/api/v1",
		RefreshBeforeExpiry: true,
		RefreshMinHits:      1,
		RefreshMargin:       6 * time.Second,
		RefreshTimeout:      5 * time.Second,
		RefreshConcurrency:  4,
	})
	defer h.Close(context.Background())

	url := "/api/v1/page"
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
	assert.Equal(t, "/page", uris[0])
	assert.Equal(t, "/page", uris[1], "refresh-before-expiry fetch must send the stripped URI")
}
