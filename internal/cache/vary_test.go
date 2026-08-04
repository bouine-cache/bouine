package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestVariantKey_NoVary(t *testing.T) {
	t.Parallel()
	primary := api.Key(100)
	got := VariantKey(primary, "", nil, nil)
	if got != primary {
		t.Fatalf("no Vary should return primary key")
	}
}

func TestVariantKey_DifferentHeaders(t *testing.T) {
	t.Parallel()
	primary := api.Key(100)
	h1 := http.Header{header.AcceptEncoding: {"gzip"}}
	h2 := http.Header{header.AcceptEncoding: {"br"}}
	k1 := VariantKey(primary, "Accept-Encoding", h1, nil)
	k2 := VariantKey(primary, "Accept-Encoding", h2, nil)
	if k1 == k2 {
		t.Fatal("different Accept-Encoding should produce different keys")
	}
	if k1 == primary || k2 == primary {
		t.Fatal("variant key should differ from primary")
	}
}

func TestVariantKey_SameHeaders(t *testing.T) {
	t.Parallel()
	primary := api.Key(100)
	h := http.Header{header.AcceptEncoding: {"gzip"}}
	k1 := VariantKey(primary, "Accept-Encoding", h, nil)
	k2 := VariantKey(primary, "Accept-Encoding", h, nil)
	if k1 != k2 {
		t.Fatal("same headers should produce same variant key")
	}
}

func TestVariantKey_VaryStar(t *testing.T) {
	t.Parallel()
	primary := api.Key(100)
	h1 := http.Header{header.Accept: {"text/html"}}
	h2 := http.Header{header.Accept: {"application/json"}}
	k1 := VariantKey(primary, "*", h1, nil)
	k2 := VariantKey(primary, "*", h2, nil)
	if k1 == k2 {
		t.Fatal("Vary: * with different headers should differ")
	}
}

func TestVariantKey_ExcludeCaseInsensitive(t *testing.T) {
	t.Parallel()
	primary := api.Key(100)
	// Exclude map uses lowercase; Vary header uses mixed case.
	// VariantKey lowercases Vary fields before lookup, so this should
	// match.
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := http.Header{"X-Request-ID": {"abc"}}
	h2 := http.Header{"X-Request-ID": {"xyz"}}
	k1 := VariantKey(primary, "X-Request-ID", h1, excludePolicy)
	k2 := VariantKey(primary, "X-Request-ID", h2, excludePolicy)
	if k1 != k2 {
		t.Fatal("exclude lookup should be case-insensitive")
	}
	if k1 != primary {
		t.Fatal("excluding all Vary fields should collapse to primary key")
	}

	// Partial exclude: non-excluded Vary field must still produce a
	// variant key distinct from primary.
	hGzip := http.Header{header.AcceptEncoding: {"gzip"}, "X-Request-ID": {"abc"}}
	kPartial := VariantKey(primary, "Accept-Encoding, X-Request-ID", hGzip, excludePolicy)
	if kPartial == primary {
		t.Fatal("non-excluded Vary field should still produce a variant key")
	}
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
	if rr1.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("gzip first: X-Cache = %q", rr1.Header().Get(header.XCache))
	}

	// br request — different variant, should MISS.
	r2 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, r2)
	if rr2.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("br first: X-Cache = %q", rr2.Header().Get(header.XCache))
	}

	// gzip again — should HIT.
	r3 := httptest.NewRequest("GET", "http://example.com/vary", nil)
	r3.Header.Set(header.AcceptEncoding, "gzip")
	rr3 := httptest.NewRecorder()
	h.ServeHTTP(rr3, r3)
	if rr3.Header().Get(header.XCache) != "HIT" {
		t.Fatalf("gzip second: X-Cache = %q, want HIT", rr3.Header().Get(header.XCache))
	}
	if rr3.Body.String() != "body-gzip" {
		t.Fatalf("body = %q, want body-gzip", rr3.Body.String())
	}
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
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}

	// Range request on cached object.
	rangeReq := httptest.NewRequest("GET", "http://example.com/range", nil)
	rangeReq.Header.Set(header.Range, "bytes=0-4")
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, rangeReq)
	if rr2.Code != http.StatusPartialContent {
		t.Fatalf("range status = %d, want 206", rr2.Code)
	}
	if rr2.Body.String() != "Hello" {
		t.Fatalf("range body = %q, want Hello", rr2.Body.String())
	}
	if xc := rr2.Header().Get(header.XCache); xc != "HIT" {
		t.Fatalf("range X-Cache = %q, want HIT", xc)
	}
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
	if obj == nil {
		t.Fatal("object not stored")
	}
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-2 * time.Second)
	_ = h.store.Put(context.Background(), key, stale)

	rangeReq := httptest.NewRequest("GET", url, nil)
	rangeReq.Header.Set(header.Range, "bytes=0-4")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, rangeReq)

	if rr.Code != http.StatusPartialContent {
		t.Fatalf("status = %d, want 206", rr.Code)
	}
	if rr.Body.String() != "Hello" {
		t.Fatalf("body = %q, want Hello", rr.Body.String())
	}
	if xc := rr.Header().Get(header.XCache); xc != "STALE" {
		t.Fatalf("stale range X-Cache = %q, want STALE", xc)
	}
	if w := rr.Header().Get(header.Warning); !strings.HasPrefix(w, "110") {
		t.Fatalf("Warning = %q, want 110 prefix", w)
	}
}
