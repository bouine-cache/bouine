package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

func freshObj(ttl time.Duration) *api.Object {
	return &api.Object{
		StatusCode:   200,
		Header:       http.Header{"Cache-Control": {"max-age=60"}},
		CacheControl: "max-age=60",
		Body:         []byte("cached"),
		BodySize:     6,
		StoredAt:     time.Now(),
		TTL:          ttl,
		ETag:         `"abc"`,
	}
}

func TestEvaluate_Hit(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Hit {
		t.Fatalf("expected Hit, got %d", d.Decision)
	}
}

func TestEvaluate_Miss(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	d := Evaluate(r, nil, time.Now())
	if d.Decision != Miss {
		t.Fatalf("expected Miss, got %d", d.Decision)
	}
}

func TestEvaluate_BypassOnPost(t *testing.T) {
	r := httptest.NewRequest("POST", "/", nil)
	d := Evaluate(r, nil, time.Now())
	if d.Decision != Bypass {
		t.Fatalf("expected Bypass for POST, got %d", d.Decision)
	}
}

func TestEvaluate_BypassOnRequestNoStore(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Cache-Control", "no-store")
	d := Evaluate(r, freshObj(time.Minute), time.Now())
	if d.Decision != Bypass {
		t.Fatalf("expected Bypass for no-store, got %d", d.Decision)
	}
}

func TestEvaluate_RevalidateOnResponseNoCache(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Minute)
	obj.Header.Set("Cache-Control", "no-cache")
	obj.CacheControl = "no-cache"
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate for no-cache, got %d", d.Decision)
	}
}

func TestEvaluate_RevalidateOnRequestNoCache(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Cache-Control", "no-cache")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate, got %d", d.Decision)
	}
}

func TestEvaluate_StaleWithSWR(t *testing.T) {
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
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second)
	obj.StaleIfError = 5 * time.Minute
	d := Evaluate(r, obj, time.Now())
	if d.Decision != StaleHit {
		t.Fatalf("expected StaleHit in SIE window, got %d", d.Decision)
	}
}

func TestEvaluate_StaleWithMaxStale(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Cache-Control", "max-stale=60")
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-10 * time.Second) // 9s past TTL
	d := Evaluate(r, obj, time.Now())
	if d.Decision != StaleHit {
		t.Fatalf("expected StaleHit with max-stale, got %d", d.Decision)
	}
}

func TestEvaluate_MustRevalidate(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second) // stale
	obj.Header.Set("Cache-Control", "max-age=1, must-revalidate")
	obj.CacheControl = "max-age=1, must-revalidate"
	d := Evaluate(r, obj, time.Now())
	if d.Decision != Revalidate {
		t.Fatalf("expected Revalidate for must-revalidate, got %d", d.Decision)
	}
}

func TestEvaluate_RequestMaxAge(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Cache-Control", "max-age=0")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	// max-age=0 from request means the object is considered stale.
	if d.Decision == Hit {
		t.Fatal("max-age=0 from request should not produce Hit")
	}
}

func TestEvaluate_OnlyIfCached_Miss(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Cache-Control", "only-if-cached")
	d := Evaluate(r, nil, time.Now())
	if d.Decision != Bypass {
		t.Fatalf("expected Bypass for only-if-cached + miss, got %d", d.Decision)
	}
}

func TestConditionalHeaders(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	obj := &api.Object{
		ETag:         `W/"xyz"`,
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	ConditionalHeaders(r, obj)
	if r.Header.Get("If-None-Match") != `W/"xyz"` {
		t.Errorf("INM = %q", r.Header.Get("If-None-Match"))
	}
	if r.Header.Get("If-Modified-Since") == "" {
		t.Error("IMS not set")
	}
}
