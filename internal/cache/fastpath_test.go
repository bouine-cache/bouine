package cache

import (
	"bytes"
	"context"
	"io"
	"net"
	"strings"
	"sync"
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
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
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
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
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
		Key:                  testkey.Key(1),
		StatusCode:           200,
		Header:               headerMap("Content-Type", "text/html", "Content-Length", "5"),
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

// TestFastPathHandler_StaleHitTriggersSWR verifies the fast-path SWR
// hook: a StaleHit on an object with a stale-while-revalidate window
// must invoke onStale with the lookup key, so the engine-wired handler
// can trigger background revalidation. Without this, fast-path stale
// objects would never refresh (the miss path revalidates; the fast
// path previously only serialized).
func TestFastPathHandler_StaleHitTriggersSWR(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})

	var mu sync.Mutex
	var gotKey api.Key
	var gotObj *api.Object
	var callCount int
	fp := NewFastPathHandlerFromStore(store).WithOnStale(func(_ *api.RawRequest, key api.Key, obj *api.Object) {
		mu.Lock()
		defer mu.Unlock()
		callCount++
		gotKey = key
		gotObj = obj
	})

	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj := &api.Object{
		Key:                  key,
		StatusCode:           200,
		Header:               headerMap("Content-Type", "text/html", "Content-Length", "5"),
		Body:                 []byte("stale"),
		BodySize:             5,
		StoredAt:             time.Now().Add(-10 * time.Second),
		TTL:                  1 * time.Second,
		StaleWhileRevalidate: 60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))

	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}

	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	fp.Release(resp)

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, 1, callCount, "StaleHit within the SWR window must fire onStale exactly once")
	assert.Equal(t, key, gotKey, "the lookup (variant) key must be passed")
	assert.Same(t, obj, gotObj)
}

// TestFastPathHandler_HitDoesNotTriggerSWR verifies the onStale hook
// fires only on StaleHit, not on fresh HITs.
func TestFastPathHandler_HitDoesNotTriggerSWR(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})

	var mu sync.Mutex
	var calls int
	fp := NewFastPathHandlerFromStore(store).WithOnStale(func(_ *api.RawRequest, _ api.Key, _ *api.Object) {
		mu.Lock()
		defer mu.Unlock()
		calls++
	})

	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}, nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "5"),
		Body:       []byte("fresh"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), key, obj))

	req := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok)
	fp.Release(resp)

	mu.Lock()
	defer mu.Unlock()
	assert.Zero(t, calls, "a fresh HIT must not fire onStale")
}

func BenchmarkGate_FastPath_Hit(b *testing.B) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
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
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
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
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
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
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
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
	d := evaluateFromRaw(req, obj, time.Now(), Directives{NoStore: true})
	assert.Equal(t, Bypass, d.Decision)
}

func TestEvaluateFromRaw_NoCache(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "no-cache"}
	req.NHeaders = 1
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second, ETag: `"v1"`}
	d := evaluateFromRaw(req, obj, time.Now(), Directives{NoCache: true})
	assert.Equal(t, Revalidate, d.Decision)
}

func TestEvaluateFromRaw_MustRevalidate(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StoredAt:           time.Now().Add(-2 * time.Second),
		TTL:                time.Second,
		CacheControl:       "max-age=1, must-revalidate",
		RespMustRevalidate: true,
	}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, obj, time.Now(), Directives{})
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
	d := evaluateFromRaw(req, obj, time.Now(), Directives{MaxStaleSet: true, MaxStale: 60 * time.Second})
	assert.Equal(t, StaleHit, d.Decision)
}

func TestEvaluateFromRaw_Fresh(t *testing.T) {
	t.Parallel()
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, obj, time.Now(), Directives{})
	assert.Equal(t, Hit, d.Decision)
}

func TestEvaluateFromRaw_NilObj(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, nil, time.Now(), Directives{})
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
	req.RecomputeScanFlags()
	_, ok := qualifiesForFastPath(req)
	assert.False(t, ok)
}

func TestQualifiesForFastPath_IfMatch(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: "If-Match", Value: `"etag"`}
	req.NHeaders = 1
	req.RecomputeScanFlags()
	_, ok := qualifiesForFastPath(req)
	assert.False(t, ok)
}

func TestQualifiesForFastPath_TransferEncoding(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.TransferEncoding, Value: "chunked"}
	req.NHeaders = 1
	req.RecomputeScanFlags()
	_, ok := qualifiesForFastPath(req)
	assert.False(t, ok)
}

func TestQualifiesForFastPath_PragmaNoCache(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.Pragma, Value: "no-cache"}
	req.NHeaders = 1
	req.RecomputeScanFlags()
	_, ok := qualifiesForFastPath(req)
	assert.False(t, ok)
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
	hdr := header.Map{}
	for i := range 200 {
		hdr.Set("X-Huge-"+string(rune('a'+i%26))+string(rune('a'+i/26)), strings.Repeat("x", 100))
	}
	obj := &api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Header:     hdr,
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
		Header:     headerMap(header.ContentType, "text/html", header.ContentLength, "5"),
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
	key := BuildKey(requestInfoFromHTTP("GET", "http://example.com/test", "example.com", "/test", false, header.Map{}), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap(header.ContentLength, "5"),
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

// TestFastPathHandler_VaryHit verifies that TryHit resolves Vary variants
// correctly: a request matching the stored variant's Vary key is served
// from the fast path, and a request with a different Accept-Encoding
// (no matching variant) misses.
func TestFastPathHandler_VaryHit(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	// Build a primary key for "GET /vary-example host example.com".
	reqBase := &api.RawRequest{
		Method: "GET",
		Path:   "/vary-example",
		Host:   "example.com",
		Scheme: "http",
	}
	primary := buildKeyFromRaw(reqBase, nil)

	// Store the primary object with a Vary header.
	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		Header:     headerMap(header.Vary, "Accept-Encoding"),
		VaryValue:  "Accept-Encoding",
		Body:       []byte("primary"),
		BodySize:   7,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), primary, primaryObj))

	// Build a variant key for Accept-Encoding: gzip.
	reqGzip := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-example",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 1,
	}
	reqGzip.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	varyKeyGzip := variantKeyFromRaw(primary, "Accept-Encoding", reqGzip, nil)

	// Store the gzip variant under its variant key.
	gzipObj := &api.Object{
		Key:        varyKeyGzip,
		StatusCode: 200,
		Header: headerMap(header.Vary, "Accept-Encoding",
			header.ContentLength, "10"),
		VaryValue: "Accept-Encoding",
		Body:      []byte("gzip-body!"),
		BodySize:  10,
		StoredAt:  time.Now(),
		TTL:       60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), varyKeyGzip, gzipObj))

	// TryHit with Accept-Encoding: gzip — should find the variant and serve it.
	resp, ok := fp.TryHit(reqGzip, time.Now())
	require.True(t, ok, "TryHit should serve the gzip variant")
	require.NotNil(t, resp)
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "HIT", resp.CacheResult)
	assert.Equal(t, 10, resp.BytesOut)
	// Verify the body is the gzip variant, not the primary.
	require.GreaterOrEqual(t, len(resp.Buffers), 3)
	assert.Equal(t, "gzip-body!", string(resp.Buffers[2]))
	// Verify the Vary header is present in the serialized response headers.
	assert.Contains(t, string(resp.Buffers[1]), "Vary: Accept-Encoding")
	fp.Release(resp)

	// TryHit with Accept-Encoding: br — no variant stored, should miss.
	reqBr := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-example",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 1,
	}
	reqBr.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "br"}
	resp2, ok := fp.TryHit(reqBr, time.Now())
	assert.False(t, ok, "TryHit should miss for br variant (not stored)")
	assert.Nil(t, resp2)
}

// TestFastPathHandler_VaryStaleHit verifies that a stale-but-SWR variant
// is served as STALE through the fast path.
func TestFastPathHandler_VaryStaleHit(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	reqBase := &api.RawRequest{
		Method: "GET",
		Path:   "/vary-stale",
		Host:   "example.com",
		Scheme: "http",
	}
	primary := buildKeyFromRaw(reqBase, nil)

	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		Header:     headerMap(header.Vary, "Accept-Encoding"),
		VaryValue:  "Accept-Encoding",
		Body:       []byte("primary"),
		BodySize:   7,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), primary, primaryObj))

	reqGzip := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-stale",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 1,
	}
	reqGzip.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	varyKeyGzip := variantKeyFromRaw(primary, "Accept-Encoding", reqGzip, nil)

	// Store a stale-but-SWR gzip variant.
	gzipObj := &api.Object{
		Key:        varyKeyGzip,
		StatusCode: 200,
		Header: headerMap(header.Vary, "Accept-Encoding",
			header.ContentLength, "11"),
		VaryValue:            "Accept-Encoding",
		Body:                 []byte("stale-gzip!"),
		BodySize:             11,
		StoredAt:             time.Now().Add(-10 * time.Second),
		TTL:                  1 * time.Second,
		StaleWhileRevalidate: 60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), varyKeyGzip, gzipObj))

	resp, ok := fp.TryHit(reqGzip, time.Now())
	require.True(t, ok, "TryHit should serve stale gzip variant within SWR window")
	assert.Equal(t, "STALE", resp.CacheResult)
	assert.Equal(t, 11, resp.BytesOut)
	fp.Release(resp)
}

// TestFastPathHandler_VaryVariantMiss verifies that TryHit returns false
// when the primary object has a Vary header but the specific variant is
// not in the store (variant miss).
func TestFastPathHandler_VaryVariantMiss(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	reqBase := &api.RawRequest{
		Method: "GET",
		Path:   "/vary-miss",
		Host:   "example.com",
		Scheme: "http",
	}
	primary := buildKeyFromRaw(reqBase, nil)

	// Store only the primary (with Vary header) — no variants.
	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		Header:     headerMap(header.Vary, "Accept-Encoding"),
		VaryValue:  "Accept-Encoding",
		Body:       []byte("primary"),
		BodySize:   7,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), primary, primaryObj))

	// Verify the primary is in the store (so a miss means the variant
	// wasn't found, not that the primary was missing).
	gotObj, _, err := store.Get(context.Background(), primary)
	require.NoError(t, err)
	require.NotNil(t, gotObj, "primary must be in store for the miss to be meaningful")

	// Request with Accept-Encoding: gzip — variant key != primary, variant
	// not in store → TryHit should return false (miss).
	reqGzip := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-miss",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 1,
	}
	reqGzip.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	resp, ok := fp.TryHit(reqGzip, time.Now())
	assert.False(t, ok, "TryHit should miss when variant is not stored")
	assert.Nil(t, resp)
}

// TestFastPathHandler_VaryMultiField verifies that TryHit resolves
// Vary with multiple fields correctly. Two requests that differ only
// in the order of Vary field values must produce different variant
// keys if the values differ, and the same key if the values match.
func TestFastPathHandler_VaryMultiField(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	reqBase := &api.RawRequest{
		Method: "GET",
		Path:   "/vary-multi",
		Host:   "example.com",
		Scheme: "http",
	}
	primary := buildKeyFromRaw(reqBase, nil)

	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		Header:     headerMap(header.Vary, "Accept-Encoding, Accept-Language"),
		VaryValue:  "Accept-Encoding, Accept-Language",
		Body:       []byte("primary"),
		BodySize:   7,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), primary, primaryObj))

	// Build variant key for Accept-Encoding: gzip, Accept-Language: en.
	reqGzipEn := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-multi",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 2,
	}
	reqGzipEn.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	reqGzipEn.Headers[1] = api.RawHeader{Key: "Accept-Language", Value: "en"}
	varyKeyGzipEn := variantKeyFromRaw(primary, "Accept-Encoding, Accept-Language", reqGzipEn, nil)

	// Store the gzip+en variant.
	variantObj := &api.Object{
		Key:        varyKeyGzipEn,
		StatusCode: 200,
		Header: headerMap(header.Vary, "Accept-Encoding, Accept-Language",
			header.ContentLength, "8"),
		VaryValue: "Accept-Encoding, Accept-Language",
		Body:      []byte("gzip-en!"),
		BodySize:  8,
		StoredAt:  time.Now(),
		TTL:       60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), varyKeyGzipEn, variantObj))

	// TryHit with gzip+en — should serve the variant.
	resp, ok := fp.TryHit(reqGzipEn, time.Now())
	require.True(t, ok, "TryHit should serve the gzip+en variant")
	assert.Equal(t, "HIT", resp.CacheResult)
	assert.Equal(t, 8, resp.BytesOut)
	require.GreaterOrEqual(t, len(resp.Buffers), 3)
	assert.Equal(t, "gzip-en!", string(resp.Buffers[2]))
	fp.Release(resp)

	// TryHit with gzip+fr — different variant, not stored → miss.
	reqGzipFr := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-multi",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 2,
	}
	reqGzipFr.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	reqGzipFr.Headers[1] = api.RawHeader{Key: "Accept-Language", Value: "fr"}
	resp2, ok := fp.TryHit(reqGzipFr, time.Now())
	assert.False(t, ok, "TryHit should miss for gzip+fr variant (not stored)")
	assert.Nil(t, resp2)
}

// TestFastPathHandler_VarySameKey verifies that when the variant key
// equals the primary key (e.g. Vary header is empty after policy
// exclusion), TryHit serves the primary object directly without a
// second store.Get.
func TestFastPathHandler_VarySameKey(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	policy := NewKeyPolicy(nil, nil, map[string]bool{"x-trace-id": true}, nil, false, false)
	fp := &FastPathHandler{store: store, policy: policy}

	reqBase := &api.RawRequest{
		Method: "GET",
		Path:   "/vary-same",
		Host:   "example.com",
		Scheme: "http",
	}
	primary := buildKeyFromRaw(reqBase, nil)

	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		Header: headerMap(header.Vary, "X-Trace-Id",
			header.ContentLength, "4"),
		VaryValue: "X-Trace-Id",
		Body:      []byte("body"),
		BodySize:  4,
		StoredAt:  time.Now(),
		TTL:       60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), primary, primaryObj))

	// Request with X-Trace-Id — policy excludes it, so variant key == primary.
	req := &api.RawRequest{
		Method:   "GET",
		Path:     "/vary-same",
		Host:     "example.com",
		Scheme:   "http",
		NHeaders: 1,
	}
	req.Headers[0] = api.RawHeader{Key: "X-Trace-Id", Value: "abc123"}

	resp, ok := fp.TryHit(req, time.Now())
	require.True(t, ok, "TryHit should serve primary when variant key == primary")
	assert.Equal(t, "HIT", resp.CacheResult)
	assert.Equal(t, 4, resp.BytesOut)
	fp.Release(resp)
}

// TestFastPathHandler_BuildKeyFromRawOverflow verifies that buildKeyFromRaw
// correctly falls back to the heap buffer when the canonical key exceeds
// the 512-byte stack buffer.
func TestFastPathHandler_BuildKeyFromRawOverflow(t *testing.T) {
	t.Parallel()
	// Construct a path that exceeds 512 bytes when combined with scheme+host+query.
	longPath := "/" + strings.Repeat("a", 600)
	req := &api.RawRequest{
		Method: "GET",
		Path:   longPath,
		Host:   "example.com",
		Scheme: "http",
	}
	key := buildKeyFromRaw(req, nil)
	assert.False(t, key.IsZero(), "overflow key should not be zero")
}

// TestFastPathHandler_SchemeDefault verifies that an empty scheme
// defaults to "http" in buildKeyFromRaw.
func TestFastPathHandler_SchemeDefault(t *testing.T) {
	t.Parallel()
	reqHTTP := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	reqEmpty := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: ""}
	assert.Equal(t, buildKeyFromRaw(reqHTTP, nil), buildKeyFromRaw(reqEmpty, nil))
}

func TestFastPathHandler_ComposedHeadCache(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		StatusCode: 200,
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET", Path: "/", Host: "example.com", Scheme: "http",
	}, nil)
	obj.Key = key
	require.NoError(t, store.Put(context.Background(), key, obj))

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/",
		Host:        "example.com",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	// First hit composes and caches the head. Bytes are captured before
	// Release — the pooled struct may be reused by the next TryHit.
	resp1, ok := fp.TryHit(req, now)
	require.True(t, ok)
	require.Contains(t, string(resp1.BuffersArr[1]), "Connection: keep-alive")
	status1 := string(resp1.BuffersArr[0])
	headers1 := string(resp1.BuffersArr[1])
	fp.Release(resp1)

	// Second hit within the same second must reuse the cached composed
	// head: identical bytes, still a valid response, and the pool header
	// buffer untouched (BufPtr nil — the composed head owns its bytes).
	// The identical timestamp is used so the second cannot roll over.
	resp2, ok := fp.TryHit(req, now)
	require.True(t, ok)
	require.Nil(t, resp2.BufPtr, "composed-head hit must not consume a pool buffer")
	require.Nil(t, resp2.HeaderBuf, "composed-head hit must not carry a pool buffer")
	assert.Equal(t, status1, string(resp2.BuffersArr[0]))
	assert.Equal(t, headers1, string(resp2.BuffersArr[1]))
	assert.Equal(t, "Hello, World!", string(resp2.BuffersArr[2]))
	fp.Release(resp2)

	// A different second recomposes (Age changes), and a Connection:
	// close request gets the close variant — different trailer, and
	// CloseConn set so callers close after the flush.
	resp3, ok := fp.TryHit(req, now.Add(1500*time.Millisecond))
	require.True(t, ok)
	fp.Release(resp3)

	reqClose := *req
	reqClose.ConnectionClose = true
	resp4, ok := fp.TryHit(&reqClose, now.Add(1500*time.Millisecond))
	require.True(t, ok)
	assert.True(t, resp4.CloseConn, "close-request hit must carry CloseConn")
	require.Contains(t, string(resp4.BuffersArr[1]), "Connection: close")
	require.NotContains(t, string(resp4.BuffersArr[1]), "Connection: keep-alive")
	fp.Release(resp4)

	// The keep-alive variant must not have been poisoned by the close
	// variant composing on the same object.
	resp5, ok := fp.TryHit(req, now.Add(1500*time.Millisecond))
	require.True(t, ok)
	assert.False(t, resp5.CloseConn)
	require.Contains(t, string(resp5.BuffersArr[1]), "Connection: keep-alive")
	fp.Release(resp5)
}

func TestFastPathHandler_HeadRequestComposedHead(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		StatusCode: 200,
		Header:     headerMap("Content-Type", "text/html", "Content-Length", "13"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
		StoredAt:   time.Now(),
		TTL:        600 * time.Second,
	}
	key := buildKeyFromRaw(&api.RawRequest{
		Method: "GET", Path: "/", Host: "example.com", Scheme: "http",
	}, nil)
	obj.Key = key
	require.NoError(t, store.Put(context.Background(), key, obj))

	getReq := &api.RawRequest{Method: "GET", Path: "/", Host: "example.com", Scheme: "http"}
	headReq := &api.RawRequest{Method: "HEAD", Path: "/", Host: "example.com", Scheme: "http"}
	now := time.Now()

	// A GET composes the head and caches it. The status/header bytes are
	// captured before Release: the pooled response struct may be reused
	// by the very next TryHit, and reading its fields afterwards would
	// compare the struct against itself.
	respGet, ok := fp.TryHit(getReq, now)
	require.True(t, ok)
	assert.Equal(t, 13, respGet.BytesOut)
	getStatus := string(respGet.BuffersArr[0])
	getHeaders := string(respGet.BuffersArr[1])
	fp.Release(respGet)

	// A HEAD in the same second reuses the composed head bytes but
	// elides the body.
	respHead, ok := fp.TryHit(headReq, now)
	require.True(t, ok)
	assert.Equal(t, getStatus, string(respHead.BuffersArr[0]))
	assert.Equal(t, getHeaders, string(respHead.BuffersArr[1]))
	assert.Nil(t, respHead.BuffersArr[2], "HEAD must not serve a body")
	assert.Zero(t, respHead.BytesOut)
	fp.Release(respHead)
}
