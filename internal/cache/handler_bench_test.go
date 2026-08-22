package cache

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

func BenchmarkGate_Handler_CacheHit_ReusableWriter(b *testing.B) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.ETag, `"bench"`)
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 1024))
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://bench.local/hit", nil))
	if rr.Header().Get(header.XCache) != "MISS" {
		b.Fatal("warmup should be MISS")
	}

	req := httptest.NewRequest("GET", "http://bench.local/hit", nil)
	w := newBenchResponseWriter()

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		w.Reset()
		h.ServeHTTP(w, req)
	}
}

type benchResponseWriter struct {
	header http.Header
	status int
	bytes  int
}

func newBenchResponseWriter() *benchResponseWriter {
	return &benchResponseWriter{header: make(http.Header, 16), status: 200}
}

func (w *benchResponseWriter) Header() http.Header { return w.header }

func (w *benchResponseWriter) WriteHeader(code int) { w.status = code }

func (w *benchResponseWriter) Write(p []byte) (int, error) {
	w.bytes += len(p)
	return len(p), nil
}

func (w *benchResponseWriter) Reset() {
	for k := range w.header {
		delete(w.header, k)
	}
	w.status = 200
	w.bytes = 0
}

func BenchmarkHandler_CacheMiss(b *testing.B) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "no-store")
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

// BenchmarkGate_Handler_CacheMiss_Cacheable measures the full cacheable miss
// path: origin fetch → response copy → cacheability check → buildObject
// → storeObject. Unlike BenchmarkHandler_CacheMiss (no-store), this
// exercises the storage path and is the primary benchmark for the
// miss-path performance plan (see notes/Bouine/miss-path-perf-plan.md).
//
// The upstream returns 6 headers — a realistic subset that exercises
// item 1.1's ownership transfer (savings scale with header count).
func BenchmarkGate_Handler_CacheMiss_Cacheable(b *testing.B) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.ETag, `"bench"`)
		w.Header().Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 1024))
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})

	// Use a unique URL per iteration to guarantee a miss every time.
	// The hit path benchmarks reuse the same URL; here we need freshness.
	// Use benchResponseWriter (reusable) instead of httptest.NewRecorder
	// to eliminate ~8 harness allocs/op that mask the miss-path savings.
	base := "http://bench.local/miss/"
	w := newBenchResponseWriter()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		w.Reset()
		req := httptest.NewRequest("GET", base+strconv.Itoa(i), nil)
		h.ServeHTTP(w, req)
	}
}

// BenchmarkHandler_CacheMiss_Vary measures the cacheable miss path with a
// Vary header, exercising item 1.3's shallow-copy optimization in
// writeAndMaybeStore. When the upstream sets Vary, the handler stores the
// response under both a variant key and the primary key — the shallow copy
// avoids a second full buildObject call (~5 allocs).
func BenchmarkHandler_CacheMiss_Vary(b *testing.B) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.ETag, `"bench"`)
		w.Header().Set(header.LastModified, "Mon, 01 Jan 2024 00:00:00 GMT")
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 1024))
	})
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store})

	base := "http://bench.local/vary/"
	w := newBenchResponseWriter()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		w.Reset()
		req := httptest.NewRequest("GET", base+strconv.Itoa(i), nil)
		req.Header.Set("Accept-Encoding", "gzip")
		h.ServeHTTP(w, req)
	}
}

func BenchmarkBuildKey_LongURL(b *testing.B) {
	// Exercises the heap fallback path: canonical key exceeds the
	// 512-byte stack buffer. This URL is ~5 KB of path + query.
	longPath := strings.Repeat("a", 5000)
	req := httptest.NewRequest("GET", "http://example.com/"+longPath+"?b=2&a=1&c=3&d=4", nil)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = BuildKey(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), nil)
	}
}

func BenchmarkGate_Evaluate_Hit(b *testing.B) {
	req := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(60)

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_ = Evaluate(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), obj, obj.StoredAt)
	}
}

// Ensure HotStore satisfies storage.Store at compile time.
var _ storage.Store = (*storage.HotStore)(nil)
