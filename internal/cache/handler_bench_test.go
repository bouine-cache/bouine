package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thylong/bouine/internal/storage"
)

func BenchmarkHandler_CacheHit(b *testing.B) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600")
		w.Header().Set("ETag", `"bench"`)
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 1024))
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})

	// Warm the cache.
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://bench.local/hit", nil))
	if rr.Header().Get("X-Cache") != "MISS" {
		b.Fatal("warmup should be MISS")
	}

	req := httptest.NewRequest("GET", "http://bench.local/hit", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}

func BenchmarkHandler_CacheMiss(b *testing.B) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 1024))
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})
	req := httptest.NewRequest("GET", "http://bench.local/miss", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, req)
	}
}

func BenchmarkBuildKey(b *testing.B) {
	req := httptest.NewRequest("GET", "http://example.com/api/v1/users?page=1&sort=name", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = BuildKey(req)
	}
}

func BenchmarkBuildKey_LongURL(b *testing.B) {
	// Exercises the heap fallback path: canonical key exceeds the 4 KB
	// stack buffer. This URL is ~5 KB of path + query.
	longPath := strings.Repeat("a", 5000)
	req := httptest.NewRequest("GET", "http://example.com/"+longPath+"?b=2&a=1&c=3&d=4", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = BuildKey(req)
	}
}

func BenchmarkEvaluate_Hit(b *testing.B) {
	req := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(60)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = Evaluate(req, obj, obj.StoredAt)
	}
}

// Ensure HotStore satisfies storage.Store at compile time.
var _ storage.Store = (*storage.HotStore)(nil)
