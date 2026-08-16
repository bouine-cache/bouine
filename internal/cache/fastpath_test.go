package cache

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestFastPathHandler_TryHit(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	// Build a RawRequest that matches the cached object's key.
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}

	// Compute key the same way TryHit does.
	key := buildKeyFromRaw(req, nil)

	// Populate the store with a cached object.
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"13"},
		}),
		Body:     []byte("Hello, World!"),
		BodySize: 13,
		StoredAt: time.Now(),
		TTL:      60 * time.Second,
	}
	err := store.Put(context.Background(), key, obj)
	require.NoError(t, err, "Put failed")

	now := time.Now()
	resp, ok := fp.TryHit(req, now)
	require.True(t, ok)
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "HIT", resp.CacheResult)
	assert.Equal(t, 13, resp.BytesOut)

	// Verify the response contains the body.
	if len(resp.Buffers) < 3 {
		t.Fatalf("Buffers has %d elements, want >= 3", len(resp.Buffers))
	}
	body := resp.Buffers[2]
	assert.Equal(t, "Hello, World!", string(body))

	// Release the pooled response.
	fp.Release(resp)
}

func TestFastPathHandler_Miss(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	req := &api.RawRequest{
		Method: "GET",
		Path:   "/nonexistent",
		Host:   "example.com",
		Scheme: "http",
	}

	resp, ok := fp.TryHit(req, time.Now())
	require.False(t, ok)
	require.Nil(t, resp)
}

func TestFastPathHandler_ConditionalHeadersFallthrough(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	// Request with If-None-Match should not qualify for fast path.
	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}
	req.Headers[0] = api.RawHeader{Key: "If-None-Match", Value: `"abc123"`}
	req.NHeaders = 1

	resp, ok := fp.TryHit(req, time.Now())
	require.False(t, ok)
	require.Nil(t, resp)
}

func TestFastPathHandler_NoCacheFallthrough(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}
	req.Headers[0] = api.RawHeader{Key: "Cache-Control", Value: "no-cache"}
	req.NHeaders = 1

	resp, ok := fp.TryHit(req, time.Now())
	require.False(t, ok)
	_ = resp
}

func TestFastPathHandler_HEADRequest(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"13"},
		}),
		Body:     []byte("Hello, World!"),
		BodySize: 13,
		StoredAt: time.Now(),
		TTL:      60 * time.Second,
	}
	// Build key matching "HEAD /" — HEAD is normalized to GET for key building.
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "HEAD",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj.Key = key
	err := store.Put(context.Background(), key, obj)
	require.NoError(t, err, "Put failed")

	req := &api.RawRequest{
		Method: "HEAD",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}

	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	assert.Equal(t, 0, resp.BytesOut)
	// Body should be nil for HEAD.
	if len(resp.Buffers) >= 3 && resp.Buffers[2] != nil {
		t.Errorf("body should be nil for HEAD request")
	}
	fp.Release(resp)
}

func TestFastPathHandler_StaleHit(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	// Object that is stale but within SWR window.
	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"5"},
		}),
		Body:                 []byte("stale"),
		BodySize:             5,
		StoredAt:             time.Now().Add(-10 * time.Second),
		TTL:                  1 * time.Second,
		StaleWhileRevalidate: 60 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj.Key = key
	err := store.Put(context.Background(), key, obj)
	require.NoError(t, err, "Put failed")

	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}

	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	assert.Equal(t, "STALE", resp.CacheResult)
	fp.Release(resp)
}

func BenchmarkGate_FastPath_Hit(b *testing.B) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"13"},
		}),
		Body:     []byte("Hello, World!"),
		BodySize: 13,
		StoredAt: time.Now(),
		TTL:      600 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj.Key = key
	if err := store.Put(context.Background(), key, obj); err != nil {
		b.Fatalf("Put failed: %v", err)
	}

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		resp, ok := fp.TryHit(req, now)
		if !ok {
			b.Fatal("TryHit returned false")
		}
		fp.Release(resp)
	}
}

func BenchmarkFastPath_Fallthrough(b *testing.B) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/nonexistent",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_, ok := fp.TryHit(req, now)
		if ok {
			b.Fatal("TryHit returned true for miss")
		}
	}
}

func BenchmarkBuildKeyFromRaw(b *testing.B) {
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/api/v1/products",
		Query:       "page=1&sort=price",
		Host:        "example.com",
		Scheme:      "https",
		HTTPVersion: "HTTP/1.1",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		_ = buildKeyFromRaw(req, nil)
	}
}

// BenchmarkFastPath_ParseAndHit measures the combined cost of request
// parsing (simulated by constructing a RawRequest) + TryHit + Release.
// This approximates the full H1 fast-path hit cost excluding network I/O.
// The h1parser package adds ~114 ns for actual byte-level parsing on top.
func BenchmarkFastPath_ParseAndHit(b *testing.B) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"13"},
		}),
		Body:     []byte("Hello, World!"),
		BodySize: 13,
		StoredAt: time.Now(),
		TTL:      600 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj.Key = key
	if err := store.Put(context.Background(), key, obj); err != nil {
		b.Fatalf("Put failed: %v", err)
	}

	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		// Simulate parsed request (h1parser adds ~114ns for real parsing).
		req := &api.RawRequest{
			Method:      "GET",
			Path:        "/",
			Host:        "example.com",
			Scheme:      "http",
			HTTPVersion: "HTTP/1.1",
		}
		resp, ok := fp.TryHit(req, now)
		if !ok {
			b.Fatal("TryHit returned false")
		}
		fp.Release(resp)
	}
}

// TestFastPathHandler_WriteAndReuse verifies that after WriteTo consumes
// the Buffers slice, the response can be released to the pool and reused
// on the next TryHit without allocating a new Buffers backing array.
// This is the regression test for the net.Buffers.WriteTo consume bug.
func TestFastPathHandler_WriteAndReuse(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"13"},
		}),
		Body:     []byte("Hello, World!"),
		BodySize: 13,
		StoredAt: time.Now(),
		TTL:      600 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj.Key = key
	err := store.Put(context.Background(), key, obj)
	require.NoError(t, err, "Put failed")

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	for i := 0; i < 3; i++ {
		resp, ok := fp.TryHit(req, now)
		require.True(t, ok)

		// Simulate WriteTo via a pipe — this consumes the Buffers slice.
		r, w := net.Pipe()
		go func() {
			_, err := resp.Buffers.WriteTo(w)
			w.Close()
			if err != nil {
				t.Errorf("WriteTo error on iteration %d: %v", i, err)
			}
		}()

		written, err := io.ReadAll(r)
		r.Close()
		require.NoErrorf(t, err, "ReadAll error on iteration %d", i)

		// After WriteTo, Buffers should be consumed (len=0).
		assert.Len(t, resp.Buffers, 0)

		// Verify the response bytes are correct.
		assert.True(t, bytes.Contains(written, []byte("Hello, World!")))
		assert.True(t, bytes.Contains(written, []byte("HTTP/1.1 200")))

		fp.Release(resp)
	}
}

// BenchmarkGate_FastPath_HitWithWrite measures the full hit path including
// net.Buffers.WriteTo — the operation that consumes the Buffers slice.
// This is the real production path: TryHit → WriteTo → Release → reuse.
// allocs/op must be 0 to prove pool reuse survives WriteTo consumption.
func BenchmarkGate_FastPath_HitWithWrite(b *testing.B) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		StatusCode: 200,
		Header: header.FromHTTP(http.Header{
			"Content-Type":   []string{"text/html"},
			"Content-Length": []string{"13"},
		}),
		Body:     []byte("Hello, World!"),
		BodySize: 13,
		StoredAt: time.Now(),
		TTL:      600 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj.Key = key
	if err := store.Put(context.Background(), key, obj); err != nil {
		b.Fatalf("Put failed: %v", err)
	}

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	// Pre-warm the pool with one cycle through WriteTo.
	resp, ok := fp.TryHit(req, now)
	if !ok {
		b.Fatal("TryHit returned false")
	}
	r, w := net.Pipe()
	go func() { _, _ = resp.Buffers.WriteTo(w); w.Close() }()
	_, _ = io.ReadAll(r)
	r.Close()
	fp.Release(resp)

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		resp, ok := fp.TryHit(req, now)
		if !ok {
			b.Fatal("TryHit returned false")
		}
		// WriteTo consumes the Buffers slice — this is what the production
		// path does and what BenchmarkFastPath_Hit fails to test.
		_, err := resp.Buffers.WriteTo(io.Discard)
		if err != nil {
			b.Fatalf("WriteTo error: %v", err)
		}
		fp.Release(resp)
	}
}

func TestEvaluateFromRaw_NoStore(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "no-store"}
	req.NHeaders = 1
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Bypass, d.Decision)
}

func TestEvaluateFromRaw_NoCache(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "no-cache"}
	req.NHeaders = 1
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second, ETag: `"v1"`}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Revalidate, d.Decision)
}

func TestEvaluateFromRaw_MustRevalidate(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StoredAt:     time.Now().Add(-2 * time.Second),
		TTL:          time.Second,
		CacheControl: "max-age=1, must-revalidate",
	}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Revalidate, d.Decision)
}

func TestEvaluateFromRaw_MaxStale(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StoredAt: time.Now().Add(-10 * time.Second),
		TTL:      time.Second,
	}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "max-stale=60"}
	req.NHeaders = 1
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, StaleHit, d.Decision)
}

func TestEvaluateFromRaw_Fresh(t *testing.T) {
	t.Parallel()
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Hit, d.Decision)
}

func TestEvaluateFromRaw_NilObj(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, nil, time.Now())
	assert.Equal(t, Miss, d.Decision)
}

func TestVariantKeyFromRaw_VaryStar(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	assert.Equal(t, primary, variantKeyFromRaw(primary, "*", req, nil))
}

func TestVariantKeyFromRaw_EmptyVary(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	assert.Equal(t, primary, variantKeyFromRaw(primary, "", req, nil))
}

func TestVariantKeyFromRaw_DifferentHeaders(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req1 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req1.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	req1.NHeaders = 1
	req2 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req2.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "br"}
	req2.NHeaders = 1
	k1 := variantKeyFromRaw(primary, "Accept-Encoding", req1, nil)
	k2 := variantKeyFromRaw(primary, "Accept-Encoding", req2, nil)
	assert.NotEqual(t, k2, k1)
	assert.NotEqual(t, primary, k1)
}

func TestVariantKeyFromRaw_PolicyExclusion(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	policy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	req1 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req1.Headers[0] = api.RawHeader{Key: "X-Request-Id", Value: "abc"}
	req1.NHeaders = 1
	req2 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req2.Headers[0] = api.RawHeader{Key: "X-Request-Id", Value: "xyz"}
	req2.NHeaders = 1
	k1 := variantKeyFromRaw(primary, "X-Request-Id", req1, policy)
	k2 := variantKeyFromRaw(primary, "X-Request-Id", req2, policy)
	assert.Equal(t, k2, k1)
	assert.Equal(t, primary, k1)
}

func TestVariantKeyFromRaw_TooManyFields(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	// >16 Vary fields → falls back to primary.
	vary := ""
	for i := range 20 {
		if i > 0 {
			vary += ", "
		}
		vary += "X-H" + string(rune('0'+i))
	}
	assert.Equal(t, primary, variantKeyFromRaw(primary, vary, req, nil))
}

func TestQualifiesForFastPath_IfRange(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: "If-Range", Value: `"etag"`}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestQualifiesForFastPath_IfMatch(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: "If-Match", Value: `"etag"`}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestQualifiesForFastPath_TransferEncoding(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.TransferEncoding, Value: "chunked"}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestQualifiesForFastPath_PragmaNoCache(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.Pragma, Value: "no-cache"}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestShouldSkipHeader(t *testing.T) {
	t.Parallel()
	noCache := map[string]bool{"Set-Cookie": true}
	assert.True(t, shouldSkipHeader(header.XBouinePath, noCache))
	assert.True(t, shouldSkipHeader(header.Connection, noCache))
	assert.True(t, shouldSkipHeader(header.TE, noCache))
	assert.True(t, shouldSkipHeader(header.Trailer, noCache))
	assert.True(t, shouldSkipHeader(header.Upgrade, noCache))
	assert.True(t, shouldSkipHeader(header.Age, noCache))
	assert.True(t, shouldSkipHeader("Set-Cookie", noCache))
	assert.False(t, shouldSkipHeader("Content-Type", noCache))
}

func TestSkipStaticHeader(t *testing.T) {
	t.Parallel()
	noCache := map[string]bool{}
	assert.True(t, skipStaticHeader(header.XCache, noCache))
	assert.True(t, skipStaticHeader(header.XCacheSource, noCache))
	assert.True(t, skipStaticHeader(header.Warning, noCache))
	assert.False(t, skipStaticHeader("Content-Type", noCache))
}

func TestParseNoCacheFieldNames(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, parseNoCacheFieldNames(""))
	})
	t.Run("no_no_cache", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, parseNoCacheFieldNames("max-age=60"))
	})
	t.Run("with_fields", func(t *testing.T) {
		t.Parallel()
		m := parseNoCacheFieldNames(`no-cache="Set-Cookie, Content-Encoding"`)
		require.NotNil(t, m)
		assert.True(t, m["Set-Cookie"])
		assert.True(t, m["Content-Encoding"])
	})
}

func TestAppendCanonicalPathString_Empty(t *testing.T) {
	t.Parallel()
	var buf [64]byte
	n := appendCanonicalPathString(buf[:], 0, "")
	assert.Equal(t, "/", string(buf[:n]))
}

func TestAppendCanonicalPathString_DuplicateSlashes(t *testing.T) {
	t.Parallel()
	var buf [64]byte
	n := appendCanonicalPathString(buf[:], 0, "/a//b")
	assert.Equal(t, "/a/b", string(buf[:n]))
}

func TestRelease_NilResponse(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	fp := NewFastPathHandlerFromStore(store)
	fp.Release(nil) // must not panic
}

func TestRelease_OversizedBuffer(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	fp := NewFastPathHandlerFromStore(store)
	// Create a response with an oversized buffer.
	resp := &api.FastPathResponse{
		StatusCode: 200,
	}
	resp.HeaderBuf = make([]byte, 2*1024*1024) // 2 MiB > maxFastPathHeaderBytes
	fp.Release(resp)
}

func newTestStore() *storage.HotStore {
	return storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
}

func TestSerializeResponse_HeadTooLarge(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)
	// Create an object with many headers to exceed the max header bytes.
	hdr := http.Header{}
	for i := range 200 {
		hdr.Set("X-Huge-"+string(rune('a'+i%26))+string(rune('a'+i/26)), strings.Repeat("x", 100))
	}
	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header:     header.FromHTTP(hdr),
		Body:       []byte("hello"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	_ = store.Put(context.Background(), testkey.Key(1), obj)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	resp, ok := fp.TryHit(req, time.Now())
	// Should still get a response (fallback path).
	if ok && resp != nil {
		fp.Release(resp)
	}
}

func TestAppendResponseHeaders_Fallback(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)
	// Create an object with a serialized head that is nil (not pre-computed).
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	key := buildKeyFromRaw(req, nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/html"}, header.ContentLength: {"5"}}),
		Body:       []byte("hello"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	// SerializedHead is nil by default (not pre-computed) — forces fallback path.
	_ = store.Put(context.Background(), key, obj)
	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.StatusCode)
	fp.Release(resp)
}

func TestNewFastPathHandler(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	fp := NewFastPathHandler(h)
	require.NotNil(t, fp)
	// Verify it shares the same store.
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/test", nil), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentLength: {"5"}}),
		Body:       []byte("hello"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	req := &api.RawRequest{Method: "GET", Path: "/test", Host: "example.com", Scheme: "http"}
	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	require.NotNil(t, resp)
	fp.Release(resp)
}
