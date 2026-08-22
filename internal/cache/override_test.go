package cache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/header"
)

// newOverrideHandler builds a Handler with OverrideTTL set, backed by an
// in-process hot store.
func newOverrideHandler(t *testing.T, upstream http.Handler, override time.Duration) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:    wrapUpstream(upstream),
		FastClient:  &mockOriginClient{status: 200, body: []byte("body"), headers: http.Header{header.CacheControl: []string{"max-age=30"}}},
		Store:       store,
		OverrideTTL: override,
	})
}

// TestOverrideTTL_WinsOverMaxAge verifies that when OverrideTTL is set the
// stored object's TTL equals the override, not the upstream's max-age.
func TestOverrideTTL_WinsOverMaxAge(t *testing.T) {
	t.Parallel()
	const originMaxAge = 30 * time.Second
	const routeOverride = 2 * time.Hour

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=30")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, routeOverride)

	req := httptest.NewRequest("GET", "http://example.com/r", nil)
	rr := newRR()
	h.ServeHTTPCompat(rr, req)
	require.Equal(t, 200, rr.Code)

	// Retrieve the stored object and assert TTL = override (±jitter; jitter=0 here).
	key := BuildKey(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), nil)
	obj, _, err := h.store.Get(req.Context(), key)
	if err != nil || obj == nil {
		t.Fatalf("object not stored: %v", err)
	}
	assert.Equal(t, routeOverride, obj.TTL)
}

// TestOverrideTTL_ForwardsOriginalCacheControlHeader ensures the response
// sent to the downstream client carries the upstream's original Cache-Control
// header unmodified, not the override value.
func TestOverrideTTL_ForwardsOriginalCacheControlHeader(t *testing.T) {
	t.Parallel()
	const upstreamCC = "max-age=30, must-revalidate"

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, upstreamCC)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, 90*time.Minute)

	req := httptest.NewRequest("GET", "http://example.com/fwd", nil)
	rr := newRR()
	h.ServeHTTPCompat(rr, req)

	got := rr.Header().Get(header.CacheControl)
	assert.Equal(t, upstreamCC, got)

	// Second request hits cache — header is served from the stored object.
	rr2 := newRR()
	h.ServeHTTPCompat(rr2, httptest.NewRequest("GET", "http://example.com/fwd", nil))
	assert.Equal(t, upstreamCC, rr2.Header().Get(header.CacheControl))
}

// TestOverrideTTL_ZeroDisabled confirms that zero OverrideTTL leaves the
// normal TTL derivation from upstream headers intact.
func TestOverrideTTL_ZeroDisabled(t *testing.T) {
	t.Parallel()
	const originMaxAge = 45 * time.Second

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=45")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, 0) // override disabled

	req := httptest.NewRequest("GET", "http://example.com/nodis", nil)
	rr := newRR()
	h.ServeHTTPCompat(rr, req)

	key := BuildKey(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), nil)
	obj, _, err := h.store.Get(req.Context(), key)
	if err != nil || obj == nil {
		t.Fatalf("object not stored: %v", err)
	}
	assert.Equal(t, originMaxAge, obj.TTL)
}

// TestOverrideTTL_HitBeforeExpiry verifies the object is served as a cache HIT
// when the override TTL hasn't expired yet, even though the upstream's max-age
// would already consider the object stale.
func TestOverrideTTL_HitBeforeExpiry(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "max-age=1") // stale after 1 s
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	// Override: 24 h — the object stays fresh in bouine's view.
	h := newOverrideHandler(t, upstream, 24*time.Hour)

	url := "http://example.com/longttl"
	rr := newRR()
	h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))

	// Second request: must be a HIT (origin not called again), even though
	// upstream's max-age=1 has logically not elapsed yet (test runs < 1 s).
	rr = newRR()
	h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))
	assert.Equal(t, "HIT", rr.Header().Get(header.XCache))
	assert.Equal(t, 1, calls)
}

// TestOverrideTTL_NoStoreNotCached ensures no-store responses are never
// cached regardless of OverrideTTL — the boolean directive wins.
func TestOverrideTTL_NoStoreNotCached(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set(header.CacheControl, "no-store")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "private-body")
	})
	h := newOverrideHandler(t, upstream, time.Hour)

	url := "http://example.com/nostore"
	for i := 0; i < 3; i++ {
		rr := newRR()
		h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))
	}
	assert.Equal(t, 3, calls)
}

// TestOverrideTTL_ShortensUpstreamTTL verifies the override can be shorter
// than the upstream's max-age, forcing earlier revalidation.
func TestOverrideTTL_ShortensUpstreamTTL(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=3600") // 1 h from upstream
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, 5*time.Second)

	req := httptest.NewRequest("GET", "http://example.com/short", nil)
	h.ServeHTTPCompat(httptest.NewRecorder(), req)

	key := BuildKey(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), nil)
	obj, _, _ := h.store.Get(req.Context(), key)
	require.NotNil(t, obj)
	assert.Equal(t, 5*time.Second, obj.TTL)
	// Downstream still sees the upstream's 1 h header.
	assert.Equal(t, "max-age=3600", obj.Header.Get(header.CacheControl))
}

// TestOverrideTTL_WithJitter checks that jitter is applied to the override
// value (not the upstream's max-age) when JitterPercent > 0.
func TestOverrideTTL_WithJitter(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.CacheControl, "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := NewHandler(HandlerConfig{
		Upstream:      wrapUpstream(upstream),
		FastClient:    &mockOriginClient{status: 200, body: []byte("body"), headers: http.Header{header.CacheControl: []string{"max-age=60"}}},
		Store:         store,
		OverrideTTL:   time.Hour,
		JitterPercent: 10,
	})

	req := httptest.NewRequest("GET", "http://example.com/jitter", nil)
	h.ServeHTTPCompat(httptest.NewRecorder(), req)

	key := BuildKey(requestInfoFromHTTP(req.Method, req.URL.String(), req.Host, req.URL.Path, req.TLS != nil, header.FromHTTP(req.Header)), nil)
	obj, _, _ := h.store.Get(req.Context(), key)
	require.NotNil(t, obj)
	// With 10 % jitter on 1 h, TTL must be in [54 min, 66 min].
	const (
		low  = 54 * time.Minute
		high = 66 * time.Minute
	)
	if obj.TTL < low || obj.TTL > high {
		t.Errorf("jittered TTL = %v, want [%v, %v]", obj.TTL, low, high)
	}
}

// TestOverrideTTL_PreservedAfterConditionalRevalidation verifies that the
// override TTL is re-applied after a 304 Not Modified response, so the stored
// object does not revert to the upstream's (shorter) max-age.
func TestOverrideTTL_PreservedAfterConditionalRevalidation(t *testing.T) {
	t.Parallel()
	const etag = `"abc"`
	phase := 0 // 0 = 200, 1 = 304

	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch phase {
		case 0:
			w.Header().Set(header.CacheControl, "max-age=1, must-revalidate")
			w.Header().Set(header.ETag, etag)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "body")
		case 1:
			if r.Header.Get(header.IfNoneMatch) == etag {
				w.Header().Set(header.CacheControl, "max-age=1, must-revalidate")
				w.Header().Set(header.ETag, etag)
				w.WriteHeader(304)
			} else {
				t.Error("304 phase: expected If-None-Match")
				w.WriteHeader(500)
			}
		}
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	const override = 90 * time.Minute
	h := NewHandler(HandlerConfig{Upstream: wrapUpstream(upstream), Store: store, OverrideTTL: override})

	url := "http://example.com/reval"

	// Phase 0: initial MISS — store with override TTL.
	rr := newRR()
	h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))

	// Manually set StoredAt far in the past so Evaluate sees the object as
	// expired (past override TTL) and triggers revalidation.
	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	expired := obj.CloneForRefresh()
	expired.StoredAt = time.Now().Add(-(override + time.Second))
	_ = h.store.Put(context.Background(), key, expired)

	// Phase 1: 304 revalidation.
	phase = 1
	rr = newRR()
	h.ServeHTTPCompat(rr, httptest.NewRequest("GET", url, nil))

	// After the 304, the object must still carry the override TTL.
	after, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, after)
	assert.Equal(t, override, after.TTL)
	// Upstream's original Cache-Control is still forwarded verbatim.
	assert.Equal(t, "max-age=1, must-revalidate", after.Header.Get(header.CacheControl))
}
