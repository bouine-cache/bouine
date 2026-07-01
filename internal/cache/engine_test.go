package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
)

func freshObj(ttl time.Duration) *api.Object {
	return &api.Object{
		StatusCode:   200,
		Header:       http.Header{header.CacheControl: {"max-age=60"}},
		CacheControl: "max-age=60",
		Body:         []byte("cached"),
		BodySize:     6,
		StoredAt:     time.Now(),
		TTL:          ttl,
		ETag:         `"abc"`,
	}
}

func TestEvaluate_Hit(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Hit {
		t.Fatalf("expected Hit, got %d", d.Decision)
	}
}

func TestEvaluate_Miss(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	d := Evaluate(r, nil, time.Now())
	if d.Decision != Miss {
		t.Fatalf("expected Miss, got %d", d.Decision)
	}
}

func TestEvaluate_BypassOnPost(t *testing.T) {
	t.Parallel()
	// POST goes through isInvalidating path in ServeHTTP, not Evaluate.
	// Evaluate returns Bypass for all non-GET/HEAD methods.
	r := httptest.NewRequest("POST", "/", nil)
	d := Evaluate(r, nil, time.Now())
	if d.Decision != Bypass {
		t.Fatalf("expected Bypass for POST, got %d", d.Decision)
	}
}

func TestEvaluate_BypassOnRequestNoStore(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "no-store")
	d := Evaluate(r, freshObj(time.Minute), time.Now())
	if d.Decision != Bypass {
		t.Fatalf("expected Bypass for no-store, got %d", d.Decision)
	}
}

func TestEvaluate_RevalidateOnResponseNoCache(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Minute)
	obj.Header.Set(header.CacheControl, "no-cache")
	obj.CacheControl = "no-cache"
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate for no-cache, got %d", d.Decision)
	}
}

func TestEvaluate_RevalidateOnRequestNoCache(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "no-cache")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate, got %d", d.Decision)
	}
}

func TestEvaluate_StaleWithSWR(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second) // 1s past TTL
	obj.StaleWhileRevalidate = 30 * time.Second
	d := Evaluate(r, obj, time.Now())
	if d.Decision != StaleHit {
		t.Fatalf("expected StaleHit in SWR window, got %d", d.Decision)
	}
}

func TestEvaluate_StaleWithSIE(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second)
	obj.StaleIfError = 5 * time.Minute
	obj.ETag = `"abc"` // must have validator for Revalidate path
	d := Evaluate(r, obj, time.Now())
	// RFC 5861 §4: stale-if-error requires the cache to attempt
	// revalidation first; only serve stale if origin returns an error.
	// Unlike SWR, SIE must NOT short-circuit to StaleHit.
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate (SIE must attempt origin first), got %d", d.Decision)
	}
}

func TestEvaluate_StaleWithMaxStale(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "max-stale=60")
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-10 * time.Second) // 9s past TTL
	d := Evaluate(r, obj, time.Now())
	if d.Decision != StaleHit {
		t.Fatalf("expected StaleHit with max-stale, got %d", d.Decision)
	}
}

func TestEvaluate_MustRevalidate(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second) // stale
	obj.Header.Set(header.CacheControl, "max-age=1, must-revalidate")
	obj.CacheControl = "max-age=1, must-revalidate"
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate for must-revalidate, got %d", d.Decision)
	}
}

func TestEvaluate_RequestMaxAge(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "max-age=0")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	// max-age=0 from request means the object is considered stale.
	if d.Decision == Hit {
		t.Fatal("max-age=0 from request should not produce Hit")
	}
}

func TestEvaluate_OriginAgeNotDoubleCounted(t *testing.T) {
	t.Parallel()
	// computeTTL subtracts OriginAge from TTL at store time, so freshness
	// MUST NOT re-apply it. Object is 10s into a 30s remaining lifetime and
	// must still be a Hit. Old engine: age = 10s+20s = 30s ≮ TTL 30s → not Hit.
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(30 * time.Second)
	obj.OriginAge = 20 * time.Second
	obj.StoredAt = time.Now().Add(-10 * time.Second)
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Hit {
		t.Fatalf("OriginAge double-counted: expected Hit, got %d", d.Decision)
	}
}

func TestEvaluate_MinFresh_OriginAge(t *testing.T) {
	t.Parallel()
	// Remaining lifetime is 20s, request asks for min-fresh=15 → servable.
	// Old engine computed TTL-age = 30s-30s = 0 < 15 → wrongly not Hit.
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "min-fresh=15")
	obj := freshObj(30 * time.Second)
	obj.OriginAge = 20 * time.Second
	obj.StoredAt = time.Now().Add(-10 * time.Second)
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Hit {
		t.Fatalf("min-fresh with OriginAge: expected Hit, got %d", d.Decision)
	}
}

func TestEvaluate_OnlyIfCached_Miss(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "only-if-cached")
	d := Evaluate(r, nil, time.Now())
	if d.Decision != Bypass {
		t.Fatalf("expected Bypass for only-if-cached + miss, got %d", d.Decision)
	}
}

func TestConditionalHeaders(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := &api.Object{
		ETag:         `W/"xyz"`,
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	ConditionalHeaders(r, obj)
	if r.Header.Get(header.IfNoneMatch) != `W/"xyz"` {
		t.Errorf("INM = %q", r.Header.Get(header.IfNoneMatch))
	}
	if r.Header.Get(header.IfModifiedSince) == "" {
		t.Error("IMS not set")
	}
}
