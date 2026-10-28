package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Issue #857: in strong mode, a non-owner that serves a STALE object
// fetched from the owner must start the stale-while-revalidate refresh
// itself and forward the fresh object to the owner. The owner never
// learns that a peer served its stale copy, so without this the object
// stays stale for the whole SWR window whenever client requests do not
// reach the owner directly.

const peerStaleWait = 5 * time.Second

// peerStaleFixture is a non-owner Handler whose owner answers every peer
// fetch with a stale-within-SWR object, and whose origin answers with a
// fresh body. Every write-to-owner RPC is delivered on puts.
type peerStaleFixture struct {
	h     *Handler
	stale *api.Object
	puts  chan *api.Object
	// respond writes the origin response; replace it before the first
	// request to change what the origin answers.
	respond     fasthttp.RequestHandler
	originCalls atomic.Int32
}

func newPeerStaleFixture(t *testing.T, url string) *peerStaleFixture {
	t.Helper()
	f := &peerStaleFixture{puts: make(chan *api.Object, 8)}
	key := BuildKey(requestInfoFromURL("GET", url), nil)
	f.stale = &api.Object{
		Key:                  key,
		StatusCode:           200,
		Header:               headerMap(header.CacheControl, "max-age=30, stale-while-revalidate=3600"),
		Body:                 []byte("old"),
		BodySize:             3,
		StoredAt:             time.Now().Add(-90 * time.Second),
		TTL:                  30 * time.Second,
		StaleWhileRevalidate: time.Hour,
	}
	f.respond = func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=30, stale-while-revalidate=3600")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("new"))
	}
	origin := func(ctx *fasthttp.RequestCtx) {
		f.originCalls.Add(1)
		f.respond(ctx)
	}
	f.h = NewHandler(HandlerConfig{
		Upstream:   origin,
		FastClient: &testFastClient{handler: origin},
		Store:      storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2}),
		OwnerFn: func(api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false // never local
		},
		PeerFetch: func(context.Context, api.PeerInfo, api.Key, string) (*api.Object, error) {
			return f.stale, nil
		},
		PeerPut: func(_ context.Context, _ api.PeerInfo, obj *api.Object) {
			f.puts <- obj
		},
	})
	t.Cleanup(func() { _ = f.h.Close(context.Background()) })
	return f
}

// waitPut returns the next object forwarded to the owner, failing the
// test if none arrives.
func (f *peerStaleFixture) waitPut(t *testing.T) *api.Object {
	t.Helper()
	select {
	case obj := <-f.puts:
		return obj
	case <-time.After(peerStaleWait):
		require.FailNow(t, "no write-to-owner RPC after serving a peer STALE")
		return nil
	}
}

func TestServePeerHit_StaleTriggersRevalidateAndForwardsToOwner(t *testing.T) {
	t.Parallel()
	const url = "http://example.com/peer-stale-slow"
	f := newPeerStaleFixture(t, url)

	ctx := testCtx("GET", url)
	f.h.ServeRequest(ctx)
	require.Equal(t, 200, respCode(ctx))
	assert.Equal(t, "STALE", respHeader(ctx, header.XCache))
	assert.Equal(t, string(api.SourcePeer), respHeader(ctx, header.XCacheSource))
	assert.Equal(t, "old", respBody(ctx), "the stale copy is served while revalidating")

	fresh := f.waitPut(t)
	assert.Equal(t, f.stale.Key, fresh.Key, "the refreshed object keeps the stale object's key")
	assert.Equal(t, "new", string(fresh.Body))
	assert.True(t, fresh.Fresh(time.Now()), "the forwarded object must be fresh")
	assert.Equal(t, int32(1), f.originCalls.Load())

	// The non-owner still does not store the object locally (issue #509).
	obj, _, _ := f.h.store.Get(context.Background(), f.stale.Key)
	assert.Nil(t, obj, "a non-owner must not keep the refreshed object")
}

func TestServePeerHit_StaleVariantForwardKeepsPairedKey(t *testing.T) {
	t.Parallel()
	const url = "http://example.com/peer-stale-variant"
	f := newPeerStaleFixture(t, url)
	reqHeader := headerMap("X-Variant", "a")
	primary := BuildKey(requestInfoFromURL("GET", url), nil)
	f.stale.Key = VariantKey(primary, "X-Variant", reqHeader, nil)
	f.stale.VaryValue = "X-Variant"
	f.stale.VaryKey = BuildVaryKey("X-Variant", reqHeader, nil)
	f.stale.PairedKey = primary
	f.stale.Header.Set(header.Vary, "X-Variant")
	f.respond = func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=30, stale-while-revalidate=3600")
		ctx.Response.Header.Set(header.Vary, "X-Variant")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("new"))
	}

	ctx := testCtxWithHeader("GET", url, "X-Variant", "a")
	f.h.ServeRequest(ctx)
	require.Equal(t, "STALE", respHeader(ctx, header.XCache))

	fresh := f.waitPut(t)
	assert.Equal(t, f.stale.Key, fresh.Key, "the variant is forwarded under its variant key")
	assert.Equal(t, primary, fresh.PairedKey, "the owner needs the primary key to track the variant")
	assert.Equal(t, "new", string(fresh.Body))
}

func TestServePeerHit_Stale304ForwardsRefreshedObject(t *testing.T) {
	t.Parallel()
	const url = "http://example.com/peer-stale-304"
	f := newPeerStaleFixture(t, url)
	f.stale.ETag = `"v1"`
	f.stale.Header.Set(header.ETag, `"v1"`)
	f.respond = func(ctx *fasthttp.RequestCtx) {
		assert.Equal(t, `"v1"`, string(ctx.Request.Header.Peek(header.IfNoneMatch)))
		ctx.Response.Header.Set(header.CacheControl, "max-age=30, stale-while-revalidate=3600")
		ctx.SetStatusCode(fasthttp.StatusNotModified)
	}

	ctx := testCtx("GET", url)
	f.h.ServeRequest(ctx)
	require.Equal(t, "STALE", respHeader(ctx, header.XCache))

	fresh := f.waitPut(t)
	assert.Equal(t, f.stale.Key, fresh.Key)
	assert.Equal(t, "old", string(fresh.Body), "a 304 keeps the stored body")
	assert.True(t, fresh.Fresh(time.Now()), "a 304 renews the freshness clock")
}

func TestFastPathPeerStale_TriggersRevalidateAndForwardsToOwner(t *testing.T) {
	t.Parallel()
	const url = "http://example.com/peer-stale-fast"
	f := newPeerStaleFixture(t, url)
	fp := NewFastPathHandler(f.h).
		WithPeerFetch(f.h.ownerFn, f.h.peerFetch).
		WithOnPeerStale(f.h.TriggerBgRevalidateFromPeerStale)

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/peer-stale-fast",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	assert.Equal(t, "STALE", resp.CacheResult)
	assert.Equal(t, string(api.SourcePeer), resp.Source)
	fp.Release(resp)

	fresh := f.waitPut(t)
	assert.Equal(t, f.stale.Key, fresh.Key)
	assert.Equal(t, "new", string(fresh.Body))
	assert.Equal(t, int32(1), f.originCalls.Load())
}

// A local stale leftover on a non-owner (after a ring change) keeps the
// owner gate: refreshing it would forward to the owner but leave the
// local copy stale, so every request would hit the origin.
func TestTriggerBgRevalidateFromFastPath_NonOwnerLocalStaleStaysGated(t *testing.T) {
	t.Parallel()
	const url = "http://example.com/local-leftover"
	f := newPeerStaleFixture(t, url)
	req := &api.RawRequest{Method: "GET", Path: "/local-leftover", Host: "example.com", Scheme: "http"}

	f.h.TriggerBgRevalidateFromFastPath(req, f.stale.Key, f.stale)
	require.NoError(t, f.h.Close(context.Background()))

	assert.Empty(t, f.puts)
	assert.Equal(t, int32(0), f.originCalls.Load())
}

// requestInfoFromRaw must not alias the connection's read buffer: the
// background revalidation reads the RequestInfo after the next
// keep-alive request has overwritten that buffer.
func TestRequestInfoFromRaw_DoesNotAliasReadBuffer(t *testing.T) {
	t.Parallel()
	buf := []byte("GET example.com /path q=1")
	view := func(from, to int) string { return unsafe.String(&buf[from], to-from) }
	req := &api.RawRequest{
		Method: view(0, 3),
		Host:   view(4, 15),
		Path:   view(16, 21),
		Scheme: "http",
	}
	reqNoQuery := *req

	req.Query = view(22, 25)
	ri := requestInfoFromRaw(req)
	riNoQuery := requestInfoFromRaw(&reqNoQuery)
	for i := range buf {
		buf[i] = 'x' // the next request reuses the buffer
	}

	assert.Equal(t, "GET", ri.GetMethod())
	assert.Equal(t, "example.com", ri.GetHost())
	assert.Equal(t, "/path", ri.GetPath())
	assert.Equal(t, "/path?q=1", ri.GetURI())
	assert.Equal(t, "/path", riNoQuery.GetURI())
}
