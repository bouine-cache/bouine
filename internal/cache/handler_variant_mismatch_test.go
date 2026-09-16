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

// TestHandleCacheMiss_PeerFetchVariantMismatchMetric verifies that the
// consumer-side rejection of a foreign-variant peer object fires the
// OnPeerVariantMismatch callback (wired to
// bouine_peer_fetch_variant_mismatch_total{side="consumer"} by the
// engine) and that a matching-variant peer hit does not fire it.
func TestHandleCacheMiss_PeerFetchVariantMismatchMetric(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})

	riFr := requestInfoFromHTTP("http://example.com/vary-metric", "/vary-metric",
		headerMap("BM-Market", "fr"))
	frObj := &api.Object{
		StatusCode: 200,
		Header:     headerMap(header.CacheControl, "max-age=60", header.Vary, "BM-Market"),
		Body:       []byte("market=fr"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		VaryValue:  "BM-Market",
		VaryKey:    BuildVaryKey("BM-Market", riFr.Header, nil),
	}
	frObj.CacheControl = "max-age=60"

	var mismatches atomic.Int32
	originUpstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "BM-Market")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("market=" + string(ctx.Request.Header.Peek("BM-Market"))))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   originUpstream,
		FastClient: &testFastClient{handler: originUpstream},
		Store:      store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key, _ string) (*api.Object, error) {
			return frObj, nil
		},
		OnPeerVariantMismatch: func() { mismatches.Add(1) },
	})

	// A us request against an fr-only peer: the gate rejects, origin
	// fills, and the callback fires exactly once.
	rUs := testCtxWithHeader("GET", "http://example.com/vary-metric", "BM-Market", "us")
	h.ServeRequest(rUs)
	require.Equal(t, "MISS", respHeader(rUs, header.XCache))
	require.Equal(t, "market=us", respBody(rUs))
	require.Equal(t, int32(1), mismatches.Load())

	// A fr request selects the peer's stored variant: served as a peer
	// hit, no callback.
	rFr := testCtxWithHeader("GET", "http://example.com/vary-metric", "BM-Market", "fr")
	h.ServeRequest(rFr)
	require.Equal(t, "HIT", respHeader(rFr, header.XCache))
	require.Equal(t, "peer", respHeader(rFr, header.XCacheSource))
	require.Equal(t, "market=fr", respBody(rFr))
	assert.Equal(t, int32(1), mismatches.Load(), "matching variant must not fire the callback")

	// A second us request after the origin fill stores the us variant
	// locally... but this non-owner does not store; it peer-fetches
	// again and the gate rejects again.
	rUs2 := testCtxWithHeader("GET", "http://example.com/vary-metric", "BM-Market", "us")
	h.ServeRequest(rUs2)
	require.Equal(t, "market=us", respBody(rUs2))
	assert.Equal(t, int32(2), mismatches.Load())
}

// TestHandleCacheMiss_PeerFetchMismatchNilCallback pins nil-safety: a
// handler without OnPeerVariantMismatch must behave exactly as before
// the metric existed (reject, fall through to origin).
func TestHandleCacheMiss_PeerFetchMismatchNilCallback(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	riFr := requestInfoFromHTTP("http://example.com/vary-nil-cb", "/vary-nil-cb",
		headerMap("BM-Market", "fr"))
	frObj := &api.Object{
		StatusCode: 200,
		Header:     headerMap(header.CacheControl, "max-age=60", header.Vary, "BM-Market"),
		Body:       []byte("market=fr"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		VaryValue:  "BM-Market",
		VaryKey:    BuildVaryKey("BM-Market", riFr.Header, nil),
	}
	frObj.CacheControl = "max-age=60"
	originUpstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "BM-Market")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("market=us"))
	}
	h := NewHandler(HandlerConfig{
		Upstream:   originUpstream,
		FastClient: &testFastClient{handler: originUpstream},
		Store:      store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "owner:8080"}, false
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key, _ string) (*api.Object, error) {
			return frObj, nil
		},
	})

	rUs := testCtxWithHeader("GET", "http://example.com/vary-nil-cb", "BM-Market", "us")
	h.ServeRequest(rUs)
	require.Equal(t, "MISS", respHeader(rUs, header.XCache))
	require.Equal(t, "market=us", respBody(rUs))
}
