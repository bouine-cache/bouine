package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Test triggerBgRefresh indirectly via the scheduler.
func TestTriggerBgRefresh_NotFound(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	// Schedule a key that doesn't exist in the store.
	key := testkey.Key(999)
	h.refreshRegistry.Register(key, &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/test"),
		Header: http.Header{},
	}, "", 0)
	// Schedule to fire immediately.
	h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
	// Wait for the scheduler to pop and triggerBgRefresh to run.
	time.Sleep(300 * time.Millisecond)
	// The key should have been unregistered (not_found path).
	assert.Equal(t, 0, h.refreshRegistry.Len())
}

func TestTriggerBgRefresh_StaleObject(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/stale", nil), nil)
	// Store a stale object.
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=1"}}),
		Body:       []byte("stale"),
		BodySize:   5,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
	}
	_ = h.store.Put(context.Background(), key, obj)
	h.refreshRegistry.Register(key, &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/stale"),
		Header: http.Header{},
	}, "", 0)
	// Schedule to fire immediately.
	h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
	time.Sleep(300 * time.Millisecond)
	// The key should have been unregistered (stale path).
	assert.Equal(t, 0, h.refreshRegistry.Len())
}

func TestTriggerBgRefresh_FreshObject(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/fresh", nil), nil)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("fresh"),
		BodySize:   5,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
		ETag:       `"v1"`,
	}
	_ = h.store.Put(context.Background(), key, obj)
	h.refreshRegistry.Register(key, &http.Request{
		Method: http.MethodGet,
		URL:    mustURL("http://example.com/fresh"),
		Header: http.Header{},
	}, "", 0)
	// Schedule to fire immediately.
	h.scheduler.Schedule(key, time.Now().Add(50*time.Millisecond))
	// Wait for the background refresh to complete.
	time.Sleep(500 * time.Millisecond)
}

// Test appendResponseHeaders via the fast path fallback.
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

// Test NewFastPathHandler (from *Handler).
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

// Test headerGuard Write auto-WriteHeader.
func TestHeaderGuard_Write(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	// Create a bypass request to test headerGuard.
	r := httptest.NewRequest("GET", "http://example.com/bypass", nil)
	r.Header.Set(header.CacheControl, "no-store")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	// The bypass path wraps the writer in headerGuard which sets X-Cache.
	assert.Equal(t, "BYPASS", rr.Header().Get(header.XCache))
}

// Test responseRecorder WriteHeader with Content-Length.
func TestResponseRecorder_WriteHeader(t *testing.T) {
	t.Parallel()
	rec := acquireRecorder(1 << 20)
	rec.WriteHeader(200)
	assert.Equal(t, 200, rec.statusCode)
	releaseRecorder(rec)
}

// Test responseRecorder Write.
func TestResponseRecorder_Write(t *testing.T) {
	t.Parallel()
	rec := acquireRecorder(1 << 20)
	_, _ = rec.Write([]byte("hello"))
	assert.Equal(t, 5, rec.body.Len())
	releaseRecorder(rec)
}

// Test fetchAndStoreStayinAlive 5xx fallback via direct call.
func TestFetchAndStoreStayinAlive_5xxFallback(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(503)
		}),
		Store:       store,
		StayinAlive: true,
	})
	// Store a stale object manually.
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/stayin5xx", nil), nil)
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=1, stale-while-revalidate=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("stale-body"),
		BodySize:   9,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
		ETag:       `"v1"`,
	}
	_ = store.Put(context.Background(), key, stale)
	// Call fetchAndStoreStayinAlive directly.
	r := httptest.NewRequest("GET", "http://example.com/stayin5xx", nil)
	rr := httptest.NewRecorder()
	h.fetchAndStoreStayinAlive(rr, r, key, key, stale, time.Now(), api.SourceHot)
	assert.Equal(t, "STALE", rr.Header().Get(header.XCache))
	assert.Equal(t, "stale-body", rr.Body.String())
}

// Test fetchAndStoreStayinAlive error fallback via direct call.
func TestFetchAndStoreStayinAlive_ErrorFallback(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	h := NewHandler(HandlerConfig{
		Upstream: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Origin down: block until context cancelled (timeout).
			<-r.Context().Done()
		}),
		Store:        store,
		StayinAlive:  true,
		FetchTimeout: 100 * time.Millisecond,
	})
	key := BuildKey(httptest.NewRequest("GET", "http://example.com/stayin-err", nil), nil)
	stale := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.CacheControl: {"max-age=1, stale-while-revalidate=60"}, header.ETag: {`"v1"`}}),
		Body:       []byte("stale-body"),
		BodySize:   9,
		StoredAt:   time.Now().Add(-10 * time.Second),
		TTL:        time.Second,
		ETag:       `"v1"`,
	}
	_ = store.Put(context.Background(), key, stale)
	r := httptest.NewRequest("GET", "http://example.com/stayin-err", nil)
	rr := httptest.NewRecorder()
	h.fetchAndStoreStayinAlive(rr, r, key, key, stale, time.Now(), api.SourceHot)
	assert.Equal(t, "STALE", rr.Header().Get(header.XCache))
}
