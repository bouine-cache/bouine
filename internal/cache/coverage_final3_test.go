package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Test triggerBgRefresh with a fresh object that gets refreshed via 304.
func TestTriggerBgRefresh_304Refresh(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	h := testRefreshHandler(t, 1)
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/refresh304", nil), nil)
	// Upstream returns 200 on first call, 304 on subsequent.
	h.upstream = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(header.IfNoneMatch) != "" {
			w.Header().Set(header.CacheControl, "max-age=120")
			w.WriteHeader(304)
			return
		}
		calls.Add(1)
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body"))
	})
	// Store a fresh object.
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, obj)
	h.refreshRegistry.Register(key, &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/refresh304"),
		Header: http.Header{},
	}, "", 0)
	// Schedule to fire immediately.
	h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
	// Wait for the background refresh to complete.
	time.Sleep(500 * time.Millisecond)
	// The refresh should have been called (304 path updates TTL).
	updated, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, updated)
	// TTL should be refreshed (was 60s, now 120s from 304 response).
	assert.Equal(t, 120*time.Second, updated.TTL)
}

// Test triggerBgRefresh rate limited.
func TestTriggerBgRefresh_RateLimited(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	h.refreshLimiter = newRefreshRateLimiter(0) // 0 RPS = always deny
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/rl", nil), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, obj)
	h.refreshRegistry.Register(key, &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/rl"),
		Header: http.Header{},
	}, "", 0)
	h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	// The key should still be registered (re-scheduled with jitter, not unregistered).
	assert.Equal(t, 1, h.refreshRegistry.Len())
}

// Test triggerBgRefresh semaphore full.
func TestTriggerBgRefresh_SemaphoreFull(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	// Fill the refresh semaphore.
	for range cap(h.refreshSem) {
		h.refreshSem <- struct{}{}
	}
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/sem", nil), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, obj)
	h.refreshRegistry.Register(key, &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/sem"),
		Header: http.Header{},
	}, "", 0)
	h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	// The key should still be registered (semaphore_full skip).
	assert.Equal(t, 1, h.refreshRegistry.Len())
	// Drain the semaphore so Close can proceed.
	for range cap(h.refreshSem) {
		<-h.refreshSem
	}
}

// Test headerGuard Write path (bypass with body).
func TestHeaderGuard_BypassWrite(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "text/html")
		w.WriteHeader(200)
		_, _ = w.Write([]byte("bypass-body"))
	}))
	r := httptest.NewRequest("GET", "http://example.com/bypass-write", nil)
	r.Header.Set(header.CacheControl, "no-store")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	assert.Equal(t, "BYPASS", rr.Header().Get(header.XCache))
	assert.Equal(t, "bypass-body", rr.Body.String())
}

// Test Purge with variants.
func TestPurge_WithVariants(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.Vary, "Accept-Encoding")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body-" + r.Header.Get(header.AcceptEncoding)))
	}))
	r1 := httptest.NewRequest("GET", "http://example.com/purge-var", nil)
	r1.Header.Set(header.AcceptEncoding, "gzip")
	h.ServeHTTP(httptest.NewRecorder(), r1)
	r2 := httptest.NewRequest("GET", "http://example.com/purge-var", nil)
	r2.Header.Set(header.AcceptEncoding, "br")
	h.ServeHTTP(httptest.NewRecorder(), r2)
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/purge-var", nil), nil)
	owned, err := h.Purge(context.Background(), key)
	require.NoError(t, err)
	require.True(t, owned)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", "http://example.com/purge-var", nil))
	assert.Equal(t, "MISS", rr.Header().Get(header.XCache))
}

// Test Purge not owned.
func TestPurge_NotOwned(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/nonexistent", nil), nil)
	owned, err := h.Purge(context.Background(), key)
	require.NoError(t, err)
	require.False(t, owned)
}

// Test doBackgroundRevalidate via direct call.
func TestDoBackgroundRevalidate_DirectCall(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get(header.IfNoneMatch) != "" {
			w.Header().Set(header.CacheControl, "max-age=120")
			w.WriteHeader(304)
			return
		}
		w.Header().Set(header.CacheControl, "max-age=60")
		w.Header().Set(header.ETag, `"v1"`)
		w.WriteHeader(200)
		_, _ = w.Write([]byte("body"))
	}))
	url := "http://example.com/reval-direct"
	r := httptest.NewRequest("GET", url, nil)
	key := BuildKey(r, nil)
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("old"),
		BodySize:   3,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, stale)
	h.doBackgroundRevalidate(context.Background(), r, key, stale)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
}
