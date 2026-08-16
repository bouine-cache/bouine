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
