package cache

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
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
	if err := store.Put(context.Background(), key, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	now := time.Now()
	resp, ok := fp.TryHit(req, now)
	if !ok {
		t.Fatal("TryHit returned false, expected hit")
	}
	if resp == nil {
		t.Fatal("TryHit returned nil response")
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode=%d want 200", resp.StatusCode)
	}
	if resp.CacheResult != "HIT" {
		t.Errorf("CacheResult=%q want HIT", resp.CacheResult)
	}
	if resp.BytesOut != 13 {
		t.Errorf("BytesOut=%d want 13", resp.BytesOut)
	}

	// Verify the response contains the body.
	if len(resp.Buffers) < 3 {
		t.Fatalf("Buffers has %d elements, want >= 3", len(resp.Buffers))
	}
	body := resp.Buffers[2]
	if string(body) != "Hello, World!" {
		t.Errorf("body=%q want %q", string(body), "Hello, World!")
	}

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
	if ok {
		t.Fatal("TryHit returned true for miss, expected false")
	}
	if resp != nil {
		t.Fatal("TryHit returned non-nil response for miss")
	}
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
	if ok {
		t.Fatal("TryHit returned true for conditional request, expected false")
	}
	if resp != nil {
		t.Fatal("TryHit returned non-nil response for conditional request")
	}
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
	if ok {
		t.Fatal("TryHit returned true for no-cache request, expected false")
	}
	_ = resp
}

func TestFastPathHandler_HEADRequest(t *testing.T) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		Key:        1,
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
	if err := store.Put(context.Background(), key, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	req := &api.RawRequest{
		Method: "HEAD",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}

	resp, ok := fp.TryHit(req, time.Now())
	if !ok {
		t.Fatal("TryHit returned false for HEAD hit, expected true")
	}
	if resp.BytesOut != 0 {
		t.Errorf("BytesOut=%d want 0 for HEAD", resp.BytesOut)
	}
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
		Key:        1,
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
	if err := store.Put(context.Background(), key, obj); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	req := &api.RawRequest{
		Method: "GET",
		Path:   "/",
		Host:   "example.com",
		Scheme: "http",
	}

	resp, ok := fp.TryHit(req, time.Now())
	if !ok {
		t.Fatal("TryHit returned false for stale-hit, expected true")
	}
	if resp.CacheResult != "STALE" {
		t.Errorf("CacheResult=%q want STALE", resp.CacheResult)
	}
	fp.Release(resp)
}

func BenchmarkFastPath_Hit(b *testing.B) {
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	obj := &api.Object{
		Key:        1,
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
