package cache

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func freshObj(ttl time.Duration) *api.Object {
	return &api.Object{
		StatusCode:   200,
		Header:       header.FromHTTP(http.Header{header.CacheControl: {"max-age=60"}}),
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
	require.Equal(t, Hit, d.Decision)
}

func TestEvaluate_Miss(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	d := Evaluate(r, nil, time.Now())
	require.Equal(t, Miss, d.Decision)
}

func TestEvaluate_BypassOnPost(t *testing.T) {
	t.Parallel()
	// POST goes through isInvalidating path in ServeHTTP, not Evaluate.
	// Evaluate returns Bypass for all non-GET/HEAD methods.
	r := httptest.NewRequest("POST", "/", nil)
	d := Evaluate(r, nil, time.Now())
	require.Equal(t, Bypass, d.Decision)
}

func TestEvaluate_BypassOnRequestNoStore(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "no-store")
	d := Evaluate(r, freshObj(time.Minute), time.Now())
	require.Equal(t, Bypass, d.Decision)
}

func TestEvaluate_RevalidateOnResponseNoCache(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Minute)
	obj.Header.Set(header.CacheControl, "no-cache")
	obj.CacheControl = "no-cache"
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, Revalidate, d.Decision)
}

func TestEvaluate_RevalidateOnRequestNoCache(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "no-cache")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, Revalidate, d.Decision)
}

func TestEvaluate_StaleWithSWR(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second) // 1s past TTL
	obj.StaleWhileRevalidate = 30 * time.Second
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, StaleHit, d.Decision)
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
	require.Equal(t, Revalidate, d.Decision)
}

func TestEvaluate_StaleWithMaxStale(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "max-stale=60")
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-10 * time.Second) // 9s past TTL
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, StaleHit, d.Decision)
}

func TestEvaluate_MustRevalidate(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second) // stale
	obj.Header.Set(header.CacheControl, "max-age=1, must-revalidate")
	obj.CacheControl = "max-age=1, must-revalidate"
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, Revalidate, d.Decision)
}

func TestEvaluate_RequestMaxAge(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "max-age=0")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	// max-age=0 from request means the object is considered stale.
	require.NotEqual(t, Hit, d.Decision)
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
	require.Equal(t, Hit, d.Decision)
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
	require.Equal(t, Hit, d.Decision)
}

func TestEvaluate_OnlyIfCached_Miss(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "only-if-cached")
	d := Evaluate(r, nil, time.Now())
	require.Equal(t, Bypass, d.Decision)
}

func TestConditionalHeaders(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := &api.Object{
		ETag:         `W/"xyz"`,
		LastModified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	ConditionalHeaders(r, obj)
	assert.Equal(t, `W/"xyz"`, r.Header.Get(header.IfNoneMatch))
	assert.NotEqual(t, "", r.Header.Get(header.IfModifiedSince))
}

func TestEvaluate_ProxyRevalidate(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := freshObj(time.Second)
	obj.StoredAt = time.Now().Add(-2 * time.Second)
	obj.Header.Set(header.CacheControl, "max-age=1, proxy-revalidate")
	obj.CacheControl = "max-age=1, proxy-revalidate"
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, Revalidate, d.Decision)
}

func TestEvaluate_HeuristicFreshnessStaleHit(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.LastModified: {"Mon, 01 Jan 2023 00:00:00 GMT"}}),
		Body:       []byte("cached"),
		StoredAt:   time.Now().Add(-2 * time.Second),
		TTL:        time.Second, // stale
		ETag:       `"abc"`,
	}
	d := Evaluate(r, obj, time.Now())
	// No max-age/s-maxage/Expires → heuristic freshness → StaleHit.
	require.Equal(t, StaleHit, d.Decision)
}

func TestEvaluate_EvalNoCache_NoValidator(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.CacheControl, "no-cache")
	obj := freshObj(time.Minute)
	obj.ETag = ""
	obj.LastModified = time.Time{}
	obj.Header.Del(header.ETag)
	obj.Header.Del(header.LastModified)
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, Miss, d.Decision)
}

func TestEvaluate_PragmaNoCache(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Pragma, "no-cache")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	require.Equal(t, Revalidate, d.Decision)
}

func TestEvaluate_MultipleCacheControlHeaders(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Add(header.CacheControl, "max-age=0")
	r.Header.Add(header.CacheControl, "public")
	obj := freshObj(time.Minute)
	d := Evaluate(r, obj, time.Now())
	require.NotEqual(t, Hit, d.Decision)
}

func TestObjDirectives_FallbackToHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Header: header.FromHTTP(http.Header{header.CacheControl: {"max-age=30"}}),
	}
	d := objDirectives(obj)
	assert.True(t, d.MaxAgeSet)
	assert.Equal(t, 30*time.Second, d.MaxAge)
}

func TestEffectiveOriginAge_FallbackToHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		Header: header.FromHTTP(http.Header{header.Age: {"42"}}),
	}
	assert.Equal(t, 42*time.Second, effectiveOriginAge(obj))
}

func TestRevalidateOrMiss_NoValidator(t *testing.T) {
	t.Parallel()
	obj := &api.Object{}
	d := revalidateOrMiss(obj)
	require.Equal(t, Miss, d.Decision)
}

func TestRevalidateOrMiss_WithValidator(t *testing.T) {
	t.Parallel()
	obj := &api.Object{ETag: `"abc"`}
	d := revalidateOrMiss(obj)
	require.Equal(t, Revalidate, d.Decision)
}
