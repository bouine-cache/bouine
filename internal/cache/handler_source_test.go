package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

func TestHandler_XCacheSource_MissThenHit(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	// MISS → origin
	rr1 := httptest.NewRecorder()
	h.ServeHTTP(rr1, httptest.NewRequest("GET", "http://example.com/foo", nil))
	if got := rr1.Header().Get(header.XCacheSource); got != string(api.SourceOrigin) {
		t.Fatalf("MISS source = %q, want %q", got, api.SourceOrigin)
	}

	// HIT → hot
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/foo", nil))
	if got := rr2.Header().Get(header.XCacheSource); got != string(api.SourceHot) {
		t.Fatalf("HIT source = %q, want %q", got, api.SourceHot)
	}
}

func TestHandler_XCacheSource_Bypass(t *testing.T) {
	t.Parallel()
	// Request no-store → Bypass decision → X-Cache: BYPASS, source empty.
	h := testHandler(t, origin200("body"))

	req := httptest.NewRequest("GET", "http://example.com/bypass", nil)
	req.Header.Set(header.CacheControl, "no-store")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if got := rr.Header().Get(header.XCache); got != "BYPASS" {
		t.Fatalf("X-Cache = %q, want BYPASS", got)
	}
	// BYPASS → source should be empty (origin was contacted but not
	// attributed to a cache tier).
	if got := rr.Header().Get(header.XCacheSource); got != "" {
		t.Fatalf("BYPASS source = %q, want empty", got)
	}
}

func TestHandler_XCacheSource_OnlyIfCached_504(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	// only-if-cached with no cached object → 504, source empty
	req := httptest.NewRequest("GET", "http://example.com/missing", nil)
	req.Header.Set(header.CacheControl, "only-if-cached")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 504 {
		t.Fatalf("status = %d, want 504", rr.Code)
	}
	if got := rr.Header().Get(header.XCache); got != "MISS" {
		t.Fatalf("X-Cache = %q, want MISS", got)
	}
	if got := rr.Header().Get(header.XCacheSource); got != "" {
		t.Fatalf("504 source = %q, want empty", got)
	}
}

func TestHandler_XCacheSource_InvalidateAndProxy_Origin(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	// POST → invalidateAndProxy → source=origin
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "http://example.com/res", strings.NewReader("data")))
	if got := rr.Header().Get(header.XCacheSource); got != string(api.SourceOrigin) {
		t.Fatalf("POST source = %q, want %q", got, api.SourceOrigin)
	}
}

func TestHandler_XCacheSource_FetchAndStore_Error_Origin(t *testing.T) {
	t.Parallel()
	// An upstream that returns a body larger than maxResponseBytes triggers
	// the truncation error path in doFetch (fetchResult.Err != nil), which
	// makes fetchAndStore set X-Cache-Source: origin before the 502.
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(200)
			_, _ = w.Write(make([]byte, 10<<20)) // 10 MiB > 4 MiB default
		}),
		Store: storage.NewHotStore(storage.HotConfig{
			MaxBytes:  1 << 20,
			NumShards: 2,
		}),
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/fail", nil))

	if rr.Code != 502 {
		t.Fatalf("status = %d, want 502", rr.Code)
	}
	if got := rr.Header().Get(header.XCacheSource); got != string(api.SourceOrigin) {
		t.Fatalf("fetch error source = %q, want %q", got, api.SourceOrigin)
	}
}

func TestHandler_XCacheSource_Conditional304_Hot(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	// Populate cache.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://example.com/304", nil))

	// Conditional GET → 304, source=hot
	req := httptest.NewRequest("GET", "http://example.com/304", nil)
	req.Header.Set(header.IfNoneMatch, `"v1"`)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 304 {
		t.Fatalf("status = %d, want 304", rr.Code)
	}
	if got := rr.Header().Get(header.XCache); got != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", got)
	}
	if got := rr.Header().Get(header.XCacheSource); got != string(api.SourceHot) {
		t.Fatalf("304 source = %q, want %q", got, api.SourceHot)
	}
}

func TestHandler_XCacheSource_PeerHit(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  1 << 20,
		NumShards: 2,
	})
	h := NewHandler(HandlerConfig{
		Upstream: origin200("body"),
		Store:    store,
		OwnerFn: func(key api.Key) (api.PeerInfo, bool) {
			return api.PeerInfo{Addr: "peer:1"}, false // always remote
		},
		PeerFetch: func(_ context.Context, _ api.PeerInfo, key api.Key) (*api.Object, error) {
			return &api.Object{
				Key:        key,
				StatusCode: 200,
				Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}}),
				Body:       []byte("peer-body"),
				BodySize:   9,
				TTL:        time.Minute,
				StoredAt:   time.Now(),
			}, nil
		},
	})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/peer", nil))

	if got := rr.Header().Get(header.XCache); got != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", got)
	}
	if got := rr.Header().Get(header.XCacheSource); got != string(api.SourcePeer) {
		t.Fatalf("peer hit source = %q, want %q", got, api.SourcePeer)
	}
}

func TestHandler_XCacheSource_Range_Hot(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ContentType, "text/plain")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("0123456789"))
	}))

	// Populate cache.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://example.com/range", nil))

	// Range request → 206, source=hot
	req := httptest.NewRequest("GET", "http://example.com/range", nil)
	req.Header.Set(header.Range, "bytes=0-4")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 206 {
		t.Fatalf("status = %d, want 206", rr.Code)
	}
	if got := rr.Header().Get(header.XCache); got != "HIT" {
		t.Fatalf("X-Cache = %q, want HIT", got)
	}
	if got := rr.Header().Get(header.XCacheSource); got != string(api.SourceHot) {
		t.Fatalf("range source = %q, want %q", got, api.SourceHot)
	}
}

// TestHandler_XCacheSource_InvalidateAndProxy_SpoofPrevention verifies
// that an origin-supplied X-Cache-Source header cannot override bouine's
// source=origin attribution on the POST/PUT/DELETE proxy path.
func TestHandler_XCacheSource_InvalidateAndProxy_SpoofPrevention(t *testing.T) {
	t.Parallel()
	// Upstream attempts to spoof the source label by sending
	// X-Cache-Source: hot.
	spoofUpstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCacheSource, "hot")
		w.Header().Set(header.XCache, "HIT")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
	h := testHandler(t, spoofUpstream)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("POST", "http://example.com/spoof", strings.NewReader("data")))

	if got := rr.Header().Get(header.XCacheSource); got != string(api.SourceOrigin) {
		t.Fatalf("origin spoof: X-Cache-Source = %q, want %q (origin must overwrite spoofed value)", got, api.SourceOrigin)
	}
	if got := rr.Header().Get(header.XCache); got != "MISS" {
		t.Fatalf("origin spoof: X-Cache = %q, want MISS", got)
	}
}

// TestHandler_XCacheSource_Bypass_SpoofPrevention verifies that an
// origin-supplied X-Cache-Source header is stripped on the BYPASS path,
// preventing metric label spoofing when the upstream writes directly to
// the client.
func TestHandler_XCacheSource_Bypass_SpoofPrevention(t *testing.T) {
	t.Parallel()
	spoofUpstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.XCacheSource, "hot")
		w.Header().Set(header.XCache, "HIT")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
	h := testHandler(t, spoofUpstream)

	req := httptest.NewRequest("GET", "http://example.com/bypass-spoof", nil)
	req.Header.Set(header.CacheControl, "no-store")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if got := rr.Header().Get(header.XCache); got != "BYPASS" {
		t.Fatalf("bypass spoof: X-Cache = %q, want BYPASS", got)
	}
	if got := rr.Header().Get(header.XCacheSource); got != "" {
		t.Fatalf("bypass spoof: X-Cache-Source = %q, want empty (origin spoof must be stripped)", got)
	}
}

// TestHandler_Conditional304_ETagCanonical verifies that the ETag header
// on a 304 response is stored under the canonical key so that
// http.Header.Get can find it.
func TestHandler_Conditional304_ETagCanonical(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	// Populate cache.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "http://example.com/etag304", nil))

	// Conditional GET → 304
	req := httptest.NewRequest("GET", "http://example.com/etag304", nil)
	req.Header.Set(header.IfNoneMatch, `"v1"`)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != 304 {
		t.Fatalf("status = %d, want 304", rr.Code)
	}
	// Header.Get canonicalises the key — this will fail if the header
	// was stored under a non-canonical key like "ETag" instead of "Etag".
	if got := rr.Header().Get(header.ETag); got != `"v1"` {
		t.Fatalf("ETag = %q, want %q (header must be stored under canonical key)", got, `"v1"`)
	}
}
