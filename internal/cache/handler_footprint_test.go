package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/thylong/bouine/internal/storage"
	"github.com/thylong/bouine/pkg/header"
)

// chunkedOrigin returns an upstream that writes a body of bodySize bytes in
// chunkSize-byte writes, mimicking a streaming origin. Multiple writes make
// the recorder's bytes.Buffer grow by doubling, leaving slack capacity —
// the exact waste the right-sized copy in doFetch reclaims.
func chunkedOrigin(bodySize, chunkSize int) http.Handler {
	payload := make([]byte, bodySize)
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600")
		w.Header().Set(header.ETag, `"footprint"`)
		w.WriteHeader(200)
		for off := 0; off < len(payload); off += chunkSize {
			_, _ = w.Write(payload[off:min(off+chunkSize, len(payload))])
		}
	})
}

// TestFetchStoresRightSizedBody proves that a stored object's body slice has
// no slack capacity: cap(Body) == len(Body). Before doFetch copied the body
// out of the recorder, obj.Body aliased the recorder's bytes.Buffer, whose
// capacity over-allocates (especially with chunked writes), pinning that
// slack for the object's entire — long — lifetime. This test fails on the
// pre-fix code and guards against a regression.
func TestFetchStoresRightSizedBody(t *testing.T) {
	t.Parallel()
	// A size that lands mid-doubling for bytes.Buffer (not on a power-of-two
	// boundary) so the pre-fix aliased body carries real slack capacity.
	const bodySize = 100_000
	h := testHandler(t, chunkedOrigin(bodySize, 8<<10))

	req := httptest.NewRequest("GET", "http://example.com/right-sized", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Header().Get(header.XCache) != "MISS" {
		t.Fatalf("expected MISS, got %q", rr.Header().Get(header.XCache))
	}

	key := BuildKey(req)
	obj, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("stored object not found: obj=%v err=%v", obj, err)
	}
	if len(obj.Body) != bodySize {
		t.Fatalf("stored body len = %d, want %d", len(obj.Body), bodySize)
	}
	slack := cap(obj.Body) - len(obj.Body)
	t.Logf("stored body: len=%d cap=%d slack=%d bytes (%.1f%%)",
		len(obj.Body), cap(obj.Body), slack, 100*float64(slack)/float64(len(obj.Body)))
	if cap(obj.Body) != len(obj.Body) {
		t.Errorf("stored body has %d bytes of slack capacity; expected a right-sized copy (cap == len)", slack)
	}
}

// TestFetchProducesRightSizedBody proves that doFetch produces a
// fetchResult.Body that is right-sized (cap == len) and does not
// alias the recorder's backing array. The fix adds Content-Length
// pre-sizing in WriteHeader to avoid bytes.Buffer doubling
// over-allocation under concurrent miss traffic (issue #141).
func TestFetchProducesRightSizedBody(t *testing.T) {
	t.Parallel()
	const bodySize = 100_000
	h := testHandler(t, chunkedOrigin(bodySize, 8<<10))

	req := httptest.NewRequest("GET", "http://example.com/transfer", nil)
	res := h.doFetch(req)
	if res.Err != nil {
		t.Fatalf("doFetch: %v", res.Err)
	}
	if len(res.Body) != bodySize {
		t.Fatalf("body len = %d, want %d", len(res.Body), bodySize)
	}
	if cap(res.Body) != len(res.Body) {
		t.Errorf("body cap = %d, len = %d; expected right-sized copy (cap == len)",
			cap(res.Body), len(res.Body))
	}
}

// TestWriteHeaderPreSizesBuffer proves that WriteHeader pre-allocates the
// body buffer to the exact Content-Length, eliminating append-doubling
// over-allocation. This is the core fix for issue #141: when the origin
// sends Content-Length, the recorder grows the buffer once to the exact
// size instead of doubling through multiple capacity tiers.
func TestWriteHeaderPreSizesBuffer(t *testing.T) {
	t.Parallel()
	const bodySize = 100_000
	rec := acquireRecorder(defaultMaxResponseBytes)
	t.Cleanup(func() { releaseRecorder(rec) })
	rec.header.Set("Content-Length", strconv.Itoa(bodySize))
	rec.WriteHeader(200)
	if rec.body.Cap() < bodySize {
		t.Fatalf("buffer cap = %d, want >= %d after Content-Length pre-sizing",
			rec.body.Cap(), bodySize)
	}
}

// BenchmarkStoreFootprint exercises the miss path that actually stores the
// response (the existing CacheMiss bench uses no-store and never reaches the
// store path). Each iteration fetches and stores a distinct key from a
// chunked origin; a bounded cache budget reaches a steady state via eviction.
//
// Run with -benchmem for per-op allocations, or capture a heap profile to
// quantify the retained-memory (right-sizing) win:
//
//	go test -run=NONE -bench=StoreFootprint -benchmem \
//	  -memprofile=mem.prof ./internal/cache/
//	go tool pprof -inuse_space -top -nodecount=12 mem.prof
//
// The fix pre-sizes the buffer via Content-Length and keeps the right-sized
// copy, so inuse_space attributed to bytes.Buffer's over-allocated backing
// array drops and total live heap for the same cache budget falls.
func BenchmarkStoreFootprint(b *testing.B) {
	const (
		bodySize  = 64 << 10
		chunkSize = 16 << 10
	)
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: chunkedOrigin(bodySize, chunkSize), Store: store})

	i := 0
	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		req := httptest.NewRequest("GET", "http://example.com/obj/"+itoa(i), nil)
		h.ServeHTTP(httptest.NewRecorder(), req)
		i++
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	n := len(buf)
	for i > 0 {
		n--
		buf[n] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[n:])
}
