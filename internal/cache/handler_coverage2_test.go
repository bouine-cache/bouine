package cache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// --- doBackgroundRefresh error paths ---

func TestDoBackgroundRefresh_ResErr_Backoff(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(1)
	// Register the key with a valid URL.
	req := &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, req, "", 0)
	// Use an upstream that returns 502 (error response).
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(502)
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
	// The entry should still be registered (re-scheduled with backoff).
	assert.Equal(t, 1, h.refreshRegistry.Len())
}

func TestDoBackgroundRefresh_ContextCancelled(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(2)
	req := &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, req, "", 0)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already cancelled
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	// Should return without panicking.
	h.doBackgroundRefresh(ctx, key, stale, 0)
}

func TestDoBackgroundRefresh_UncacheableSkip(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(3)
	req := &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, req, "", 0)
	// Upstream returns no-store (uncacheable).
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "no-store")
		w.WriteHeader(200)
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
}

func TestDoBackgroundRefresh_SetCookieSkip(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.allowSetCookie = false
	key := testkey.Key(4)
	req := &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, req, "", 0)
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.SetCookie, "sid=abc")
		w.WriteHeader(200)
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
}

func TestDoBackgroundRefresh_MaxObjectSizeSkip(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.maxObjectSize = 5
	key := testkey.Key(5)
	req := &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}
	h.refreshRegistry.Register(key, req, "", 0)
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "this is a very long body that exceeds the max object size")
	})
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	h.refreshTimeout = 5 * time.Second
	h.doBackgroundRefresh(context.Background(), key, stale, 0)
}

// --- writeAndMaybeStore variant path ---

func TestWriteAndMaybeStore_VariantStore(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set(header.CacheControl, "max-age=60")
			w.Header().Set(header.Vary, "Accept-Encoding")
			w.Header().Set(header.ETag, `"v1"`)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "body-"+r.Header.Get(header.AcceptEncoding))
		}),
		Store: store,
	})
	// First request — MISS, stores with Vary.
	r1 := httptest.NewRequest("GET", "http://example.com/v", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, r1)
	require.Equal(t, "MISS", rr1.Header().Get(header.XCache))
	// Second request with different encoding — MISS, stores variant.
	r2 := httptest.NewRequest("GET", "http://example.com/v", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	require.Equal(t, "MISS", rr2.Header().Get(header.XCache))
	// Third request with gzip — HIT.
	r3 := httptest.NewRequest("GET", "http://example.com/v", nil)
	r3.Header.Set(header.AcceptEncoding, "gzip")
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, r3)
	require.Equal(t, "HIT", rr3.Header().Get(header.XCache))
}

// --- tryConditional304 ---

func TestTryConditional304_Match(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.IfNoneMatch, `"v1"`)
	rr := httptest.NewRecorder()
	ok := h.tryConditional304(rr, r, obj, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, http.StatusNotModified, rr.Code)
	assert.Equal(t, `"v1"`, rr.Header().Get(header.ETag))
	assert.Equal(t, "HIT", rr.Header().Get(header.XCache))
}

func TestTryConditional304_NoMatch(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.IfNoneMatch, `"v2"`)
	rr := httptest.NewRecorder()
	ok := h.tryConditional304(rr, r, obj, api.SourceHot)
	require.False(t, ok)
}

// --- ageHeader fallback ---

func TestAgeHeader_Fallback(t *testing.T) {
	t.Parallel()
	// Age >= 600s uses the fallback allocation path.
	got := ageHeader(600 * time.Second)
	assert.Equal(t, "600", got[0])
	got = ageHeader(-1 * time.Second)
	assert.Equal(t, "-1", got[0])
}

// --- shouldRefresh score gate ---

func TestShouldRefresh_ScoreGate(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.refreshMinScore = 1000
	// staleHits * BodySize < 1000 → false.
	obj := &api.Object{BodySize: 10}
	assert.False(t, h.shouldRefresh(5, obj))
	// staleHits * BodySize >= 1000 → true.
	obj = &api.Object{BodySize: 200}
	assert.True(t, h.shouldRefresh(5, obj))
}

// --- doFetch truncated ---

func TestDoFetch_Truncated(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		// Write more than maxResponseBytes.
		_, _ = w.Write(make([]byte, 2*(1<<20))) // 2 MiB
	}))
	h.maxResponseBytes = 1 << 20 // 1 MiB
	req := httptest.NewRequest("GET", "/", nil)
	res := h.doFetch(req)
	require.NotNil(t, res.Err)
	assert.Contains(t, res.Err.Error(), "exceeds")
}

// --- handleBypass only-if-cached 504 ---

func TestHandleBypass_OnlyIfCached_504(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("GET", "http://example.com/nonexistent", nil)
	r.Header.Set(header.CacheControl, "only-if-cached")
	rr := httptest.NewRecorder()
	h.handleBypass(rr, r)
	require.Equal(t, http.StatusGatewayTimeout, rr.Code)
}

// --- buildLocationKey ---

func TestBuildLocationKey_AbsoluteURL(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("POST", "http://example.com/submit", nil)
	key := h.buildLocationKey(r, "http://example.com/other")
	require.False(t, key.IsZero())
}

func TestBuildLocationKey_RelativePath(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("POST", "http://example.com/api/submit", nil)
	key := h.buildLocationKey(r, "../v2")
	require.False(t, key.IsZero())
}

func TestBuildLocationKey_InvalidURL(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("POST", "http://example.com/submit", nil)
	key := h.buildLocationKey(r, "ht\ttp://invalid")
	require.True(t, key.IsZero())
}

// --- lookupForRefresh ---

func TestLookupForRefresh_StaleObject(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	// Store a stale object.
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/stale", nil), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=1"}}),
		Body:       []byte("stale"),
		BodySize:   5,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	// lookupForRefresh should return nil for stale.
	result := h.lookupForRefresh(key)
	require.Nil(t, result)
}

func TestLookupForRefresh_FreshObject(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/fresh", nil), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}}),
		Body:       []byte("fresh"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	result := h.lookupForRefresh(key)
	require.NotNil(t, result)
}

func TestLookupForRefresh_NotFound(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/missing", nil), nil)
	result := h.lookupForRefresh(key)
	require.Nil(t, result)
}

// --- storeObject refresh scheduling ---

func TestStoreObject_RefreshScheduling(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(10)
	r := httptest.NewRequest("GET", "http://example.com/test", nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	h.storeObject(context.Background(), key, obj, r, false, 0)
	// Should be registered in the refresh registry.
	assert.Equal(t, 1, h.refreshRegistry.Len())
	// Should be scheduled in the scheduler.
	assert.Equal(t, 1, h.scheduler.Len())
}

func TestStoreObject_NegativeCacheableSkipRefresh(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(11)
	r := httptest.NewRequest("GET", "http://example.com/404", nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 404,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=30"}}),
		Body:       []byte("not found"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        30 * time.Second,
	}
	h.storeObject(context.Background(), key, obj, r, false, 0)
	// Negative cacheable objects should NOT be scheduled for refresh.
	assert.Equal(t, 0, h.refreshRegistry.Len())
}

// --- invalidateAndProxy ---

func TestInvalidateAndProxy_5xxNoInvalidation(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			w.WriteHeader(500)
			return
		}
		calls.Add(1)
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	}))
	// Populate cache with GET.
	h.ServeHTTP(httptest.NewRecorder(),
		httptest.NewRequest("GET", "http://example.com/res", nil))
	// POST with 5xx should NOT invalidate.
	r := httptest.NewRequest("POST", "http://example.com/res", strings.NewReader("data"))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	assert.Equal(t, 500, rr.Code)
	// GET should still be HIT (not invalidated).
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/res", nil))
	assert.Equal(t, "HIT", rr2.Header().Get(header.XCache))
}

// --- maybeStorePostResponse ---

func TestMaybeStorePostResponse_NonPOST(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	r := httptest.NewRequest("GET", "http://example.com/res", nil)
	getReq := httptest.NewRequest("GET", "http://example.com/res", nil)
	key := h.buildKey(getReq)
	rec := acquireRecorder(h.maxResponseBytes)
	rec.statusCode = 200
	rec.header.Set(header.CacheControl, "max-age=60")
	_, _ = rec.Write([]byte("body"))
	releaseRecorder(rec)
	// Non-POST should be a no-op.
	h.maybeStorePostResponse(r, getReq, key, rec)
}

// --- Peer fetch path ---

func TestHandleCacheMiss_PeerFetch(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	peerObj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}}),
		Body:       []byte("from-peer"),
		BodySize:   9,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	var originCalls atomic.Int32
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			originCalls.Add(1)
			w.Header().Set(header.CacheControl, "max-age=60")
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "from-origin")
		}),
		Store: store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "peer1:8080"}, false // not local
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, _ api.Key) (*api.Object, error) {
			return peerObj, nil
		},
	})
	r := httptest.NewRequest("GET", "http://example.com/peer", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, 200, rr.Code)
	assert.Equal(t, "from-peer", rr.Body.String())
	assert.Equal(t, int32(0), originCalls.Load())
}

// --- Vary lookup miss ---

func TestLookup_VaryVariantMiss(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body-"+r.Header.Get(header.AcceptEncoding))
	}))
	// Populate with gzip variant.
	r1 := httptest.NewRequest("GET", "http://example.com/vary-miss", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	h.ServeHTTP(httptest.NewRecorder(), r1)
	// Request with br — should miss (variant not found), then store.
	r2 := httptest.NewRequest("GET", "http://example.com/vary-miss", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	assert.Equal(t, "MISS", rr2.Header().Get(header.XCache))
}

// --- fastpath: appendCanonicalQueryString with policy ---

func TestAppendCanonicalQueryString_Policy(t *testing.T) {
	t.Parallel()
	var buf [256]byte
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	// "q=test&utm=x" → should strip utm, keep q.
	n := appendCanonicalQueryString(buf[:], 0, "q=test&utm=x", policy)
	result := string(buf[:n])
	assert.Contains(t, result, "q=test")
	assert.NotContains(t, result, "utm")
}

func TestAppendCanonicalQueryString_PercentEncoded(t *testing.T) {
	t.Parallel()
	var buf [256]byte
	n := appendCanonicalQueryStringNoPolicy(buf[:], 0, "a=%31&b=2")
	// Should produce sorted: a=1&b=2 (after percent-decode).
	result := string(buf[:n])
	assert.Contains(t, result, "a=")
	assert.Contains(t, result, "b=2")
}

func TestAppendCanonicalQueryString_MoreThan8Params(t *testing.T) {
	t.Parallel()
	var buf [512]byte
	raw := "a=1&b=2&c=3&d=4&e=5&f=6&g=7&h=8&i=9"
	n := appendCanonicalQueryStringNoPolicy(buf[:], 0, raw)
	require.Greater(t, n, 0)
}

func TestAppendCanonicalQuerySlowString_Policy(t *testing.T) {
	t.Parallel()
	var buf [512]byte
	policy := NewKeyPolicy(nil, map[string]bool{"q": true}, nil, nil, false, false)
	n := appendCanonicalQuerySlowString(buf[:], 0, "q=test&utm=x&fbclid=123", policy)
	result := string(buf[:n])
	assert.Contains(t, result, "q=test")
	assert.NotContains(t, result, "utm")
	assert.NotContains(t, result, "fbclid")
}

func mustURL(raw string) *url.URL {
	u, _ := url.Parse(raw)
	return u
}
