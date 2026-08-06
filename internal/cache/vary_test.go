package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestVariantKey_NoVary(t *testing.T) {
	t.Parallel()
	primary := api.NewKeyFromUint64(uint64(100))
	got := VariantKey(primary, "", nil, nil)
	require.Equal(t, primary, got)
}

func TestVariantKey_DifferentHeaders(t *testing.T) {
	t.Parallel()
	primary := api.NewKeyFromUint64(uint64(100))
	h1 := http.Header{header.AcceptEncoding: {"gzip"}}
	h2 := http.Header{header.AcceptEncoding: {"br"}}
	k1 := VariantKey(primary, "Accept-Encoding", h1, nil)
	k2 := VariantKey(primary, "Accept-Encoding", h2, nil)
	require.NotEqual(t, k2, k1)
	if k1 == primary || k2 == primary {
		t.Fatal("variant key should differ from primary")
	}
}

func TestVariantKey_SameHeaders(t *testing.T) {
	t.Parallel()
	primary := api.NewKeyFromUint64(uint64(100))
	h := http.Header{header.AcceptEncoding: {"gzip"}}
	k1 := VariantKey(primary, "Accept-Encoding", h, nil)
	k2 := VariantKey(primary, "Accept-Encoding", h, nil)
	require.Equal(t, k2, k1)
}

func TestVariantKey_VaryStar(t *testing.T) {
	t.Parallel()
	primary := api.NewKeyFromUint64(uint64(100))
	h1 := http.Header{header.Accept: {"text/html"}}
	h2 := http.Header{header.Accept: {"application/json"}}
	k1 := VariantKey(primary, "*", h1, nil)
	k2 := VariantKey(primary, "*", h2, nil)
	require.NotEqual(t, k2, k1)
}

func TestVariantKey_ExcludeCaseInsensitive(t *testing.T) {
	t.Parallel()
	primary := api.NewKeyFromUint64(uint64(100))
	// Exclude map uses lowercase; Vary header uses mixed case.
	// VariantKey lowercases Vary fields before lookup, so this should
	// match.
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := http.Header{"X-Request-ID": {"abc"}}
	h2 := http.Header{"X-Request-ID": {"xyz"}}
	k1 := VariantKey(primary, "X-Request-ID", h1, excludePolicy)
	k2 := VariantKey(primary, "X-Request-ID", h2, excludePolicy)
	require.Equal(t, k2, k1)
	require.Equal(t, primary, k1)

	// Partial exclude: non-excluded Vary field must still produce a
	// variant key distinct from primary.
	hGzip := http.Header{header.AcceptEncoding: {"gzip"}, "X-Request-ID": {"abc"}}
	kPartial := VariantKey(primary, "Accept-Encoding, X-Request-ID", hGzip, excludePolicy)
	require.NotEqual(t, primary, kPartial)
}

func TestHandler_VaryAwareStorage(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.Header().Set(header.ContentEncoding, r.Header.Get(header.AcceptEncoding))
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body-" + r.Header.Get(header.AcceptEncoding)))
	})
	h := testHandler(t, upstream)

	// gzip request.
	r1 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, r1)
	require.Equal(t, "MISS", rr1.Header().Get(header.XCache))

	// br request — different variant, should MISS.
	r2 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	require.Equal(t, "MISS", rr2.Header().Get(header.XCache))

	// gzip again — should HIT.
	r3 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r3.Header.Set(header.AcceptEncoding, "gzip")
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, r3)
	require.Equal(t, "HIT", rr3.Header().Get(header.XCache))
	require.Equal(t, "body-gzip", rr3.Body.String())
}

func TestHandler_RangeOnCachedObject(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"full"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("Hello, Range World!"))
	})
	h := testHandler(t, upstream)

	// Populate cache with full body.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/range", nil))
	require.Equal(t, 200, rr.Code)

	// Range request on cached object.
	rangeReq := httptest.NewRequest("GET", "http://example.com/range", nil)
	rangeReq.Header.Set(header.Range, "bytes=0-4")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, rangeReq)
	require.Equal(t, http.StatusPartialContent, rr2.Code)
	require.Equal(t, "Hello", rr2.Body.String())
	xc := rr2.Header().Get(header.XCache)
	require.Equal(t, "HIT", xc)
}

func TestHandler_RangeOnStaleObject(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=1, stale-while-revalidate=60")
		w.Header().Set(header.ETag, `"full"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("Hello, Range World!"))
	})
	h := testHandler(t, upstream)

	url := "http://example.com/stale-range"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	key := BuildKey(httptest.NewRequest("GET", url, nil), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-2 * time.Second)
	_ = h.store.Put(context.Background(), key, stale)

	rangeReq := httptest.NewRequest("GET", url, nil)
	rangeReq.Header.Set(header.Range, "bytes=0-4")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, rangeReq)

	require.Equal(t, http.StatusPartialContent, rr.Code)
	require.Equal(t, "Hello", rr.Body.String())
	xc := rr.Header().Get(header.XCache)
	require.Equal(t, "STALE", xc)
	w := rr.Header().Get(header.Warning)
	require.True(t, strings.HasPrefix(w, "110"))
}
