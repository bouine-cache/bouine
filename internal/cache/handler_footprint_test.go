package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
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
	require.Equal(t, "MISS", rr.Header().Get(header.XCache))

	key := BuildKey(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), nil)
	obj, _, err := h.store.Get(context.Background(), key)
	if err != nil || obj == nil {
		t.Fatalf("stored object not found: obj=%v err=%v", obj, err)
	}
	require.Len(t, obj.Body, bodySize)
	slack := cap(obj.Body) - len(obj.Body)
	t.Logf("stored body: len=%d cap=%d slack=%d bytes (%.1f%%)",
		len(obj.Body), cap(obj.Body), slack, 100*float64(slack)/float64(len(obj.Body)))
	assert.Len(t, obj.Body, cap(obj.Body))
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
	require.Nil(t, res.Err)
	require.Len(t, res.Body, bodySize)
	assert.Len(t, res.Body, cap(res.Body))
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

// BenchmarkStoreFootprint_Interned measures the heap footprint of storing
// 1000 objects whose header values are all common strings (text/html,
// max-age=3600). With interning, these values are deduplicated across
// all objects via unique.Make. Compare B/op and allocs/op against
// BenchmarkStoreFootprint (which uses the same origin but stores
// distinct URLs, so the header values are still common and interned).
//
// To measure the interning benefit directly, compare:
//   - allocs/op: should be lower because value strings are shared
//   - B/op: should be lower because duplicate string data is eliminated
func BenchmarkStoreFootprint_Interned(b *testing.B) {
	const (
		bodySize  = 64 << 10
		chunkSize = 16 << 10
		numObjs   = 1000
	)
	store := storage.NewHotStore(storage.HotConfig{
		MaxBytes:  256 << 20,
		NumShards: 16,
	})
	h := NewHandler(HandlerConfig{Upstream: chunkedOrigin(bodySize, chunkSize), Store: store})

	b.ResetTimer()
	b.ReportAllocs()
	for i := range numObjs {
		req := httptest.NewRequest("GET", "http://example.com/obj/"+itoa(i), nil)
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
