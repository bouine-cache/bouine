package cache

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thylong/bouine/internal/storage"
)

// newOverrideHandler builds a Handler with OverrideTTL set, backed by an
// in-process hot store.
func newOverrideHandler(t *testing.T, upstream http.Handler, override time.Duration) *Handler {
	t.Helper()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	return NewHandler(HandlerConfig{
		Upstream:    upstream,
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
		w.Header().Set("Cache-Control", "max-age=30")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, routeOverride)

	req := httptest.NewRequest("GET", "http://example.com/r", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatalf("status = %d", rr.Code)
	}

	// Retrieve the stored object and assert TTL = override (±jitter; jitter=0 here).
	key := BuildKey(req)
	obj, err := h.store.Get(req.Context(), key)
	if err != nil || obj == nil {
		t.Fatalf("object not stored: %v", err)
	}
	if obj.TTL != routeOverride {
		t.Errorf("obj.TTL = %v, want %v (origin max-age was %v)", obj.TTL, routeOverride, originMaxAge)
	}
}

// TestOverrideTTL_ForwardsOriginalCacheControlHeader ensures the response
// sent to the downstream client carries the upstream's original Cache-Control
// header unmodified, not the override value.
func TestOverrideTTL_ForwardsOriginalCacheControlHeader(t *testing.T) {
	t.Parallel()
	const upstreamCC = "max-age=30, must-revalidate"

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", upstreamCC)
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, 90*time.Minute)

	req := httptest.NewRequest("GET", "http://example.com/fwd", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	got := rr.Header().Get("Cache-Control")
	if got != upstreamCC {
		t.Errorf("downstream Cache-Control = %q, want %q", got, upstreamCC)
	}

	// Second request hits cache — header is served from the stored object.
	rr2 := httptest.NewRecorder()
	h.ServeHTTP(rr2, httptest.NewRequest("GET", "http://example.com/fwd", nil))
	if rr2.Header().Get("Cache-Control") != upstreamCC {
		t.Errorf("cached Cache-Control = %q, want %q", rr2.Header().Get("Cache-Control"), upstreamCC)
	}
}

// TestOverrideTTL_ZeroDisabled confirms that zero OverrideTTL leaves the
// normal TTL derivation from upstream headers intact.
func TestOverrideTTL_ZeroDisabled(t *testing.T) {
	t.Parallel()
	const originMaxAge = 45 * time.Second

	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=45")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, 0) // override disabled

	req := httptest.NewRequest("GET", "http://example.com/nodis", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	key := BuildKey(req)
	obj, err := h.store.Get(req.Context(), key)
	if err != nil || obj == nil {
		t.Fatalf("object not stored: %v", err)
	}
	if obj.TTL != originMaxAge {
		t.Errorf("obj.TTL = %v, want %v", obj.TTL, originMaxAge)
	}
}

// TestOverrideTTL_HitBeforeExpiry verifies the object is served as a cache HIT
// when the override TTL hasn't expired yet, even though the upstream's max-age
// would already consider the object stale.
func TestOverrideTTL_HitBeforeExpiry(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "max-age=1") // stale after 1 s
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	// Override: 24 h — the object stays fresh in bouine's view.
	h := newOverrideHandler(t, upstream, 24*time.Hour)

	url := "http://example.com/longttl"
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Second request: must be a HIT (origin not called again), even though
	// upstream's max-age=1 has logically not elapsed yet (test runs < 1 s).
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest("GET", url, nil))
	if rr.Header().Get("X-Cache") != "HIT" {
		t.Errorf("X-Cache = %q, want HIT", rr.Header().Get("X-Cache"))
	}
	if calls != 1 {
		t.Errorf("origin called %d times, want 1", calls)
	}
}

// TestOverrideTTL_NoStoreNotCached ensures no-store responses are never
// cached regardless of OverrideTTL — the boolean directive wins.
func TestOverrideTTL_NoStoreNotCached(t *testing.T) {
	t.Parallel()
	calls := 0
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "private-body")
	})
	h := newOverrideHandler(t, upstream, time.Hour)

	url := "http://example.com/nostore"
	for i := 0; i < 3; i++ {
		h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))
	}
	if calls != 3 {
		t.Errorf("origin called %d times, want 3 (no-store must bypass cache even with OverrideTTL)", calls)
	}
}

// TestOverrideTTL_ShortensUpstreamTTL verifies the override can be shorter
// than the upstream's max-age, forcing earlier revalidation.
func TestOverrideTTL_ShortensUpstreamTTL(t *testing.T) {
	t.Parallel()
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=3600") // 1 h from upstream
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := newOverrideHandler(t, upstream, 5*time.Second)

	req := httptest.NewRequest("GET", "http://example.com/short", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := BuildKey(req)
	obj, _ := h.store.Get(req.Context(), key)
	if obj == nil {
		t.Fatal("object not stored")
	}
	if obj.TTL != 5*time.Second {
		t.Errorf("obj.TTL = %v, want 5s", obj.TTL)
	}
	// Downstream still sees the upstream's 1 h header.
	if obj.Header.Get("Cache-Control") != "max-age=3600" {
		t.Errorf("stored Cache-Control = %q, want max-age=3600", obj.Header.Get("Cache-Control"))
	}
}

// TestOverrideTTL_WithJitter checks that jitter is applied to the override
// value (not the upstream's max-age) when JitterPercent > 0.
func TestOverrideTTL_WithJitter(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	upstream := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		w.WriteHeader(200)
		_, _ = io.WriteString(w, "body")
	})
	h := NewHandler(HandlerConfig{
		Upstream:      upstream,
		Store:         store,
		OverrideTTL:   time.Hour,
		JitterPercent: 10,
	})

	req := httptest.NewRequest("GET", "http://example.com/jitter", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	key := BuildKey(req)
	obj, _ := h.store.Get(req.Context(), key)
	if obj == nil {
		t.Fatal("object not stored")
	}
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
			w.Header().Set("Cache-Control", "max-age=1, must-revalidate")
			w.Header().Set("ETag", etag)
			w.WriteHeader(200)
			_, _ = io.WriteString(w, "body")
		case 1:
			if r.Header.Get("If-None-Match") == etag {
				w.Header().Set("Cache-Control", "max-age=1, must-revalidate")
				w.Header().Set("ETag", etag)
				w.WriteHeader(304)
			} else {
				t.Error("304 phase: expected If-None-Match")
				w.WriteHeader(500)
			}
		}
	})

	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20, NumShards: 2})
	const override = 90 * time.Minute
	h := NewHandler(HandlerConfig{Upstream: upstream, Store: store, OverrideTTL: override})

	url := "http://example.com/reval"

	// Phase 0: initial MISS — store with override TTL.
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// Manually set StoredAt far in the past so Evaluate sees the object as
	// expired (past override TTL) and triggers revalidation.
	key := BuildKey(httptest.NewRequest("GET", url, nil))
	obj, _ := h.store.Get(context.Background(), key)
	if obj == nil {
		t.Fatal("object not stored after phase 0")
	}
	expired := *obj
	expired.StoredAt = time.Now().Add(-(override + time.Second))
	_ = h.store.Put(context.Background(), key, &expired)

	// Phase 1: 304 revalidation.
	phase = 1
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", url, nil))

	// After the 304, the object must still carry the override TTL.
	after, _ := h.store.Get(context.Background(), key)
	if after == nil {
		t.Fatal("object missing after 304 revalidation")
	}
	if after.TTL != override {
		t.Errorf("TTL after 304 = %v, want %v (override must survive revalidation)", after.TTL, override)
	}
	// Upstream's original Cache-Control is still forwarded verbatim.
	if after.Header.Get("Cache-Control") != "max-age=1, must-revalidate" {
		t.Errorf("stored Cache-Control = %q, want max-age=1, must-revalidate", after.Header.Get("Cache-Control"))
	}
}
