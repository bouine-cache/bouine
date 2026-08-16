package cache

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestErrorType(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "timeout", errorType(errors.New("context deadline exceeded")))
	assert.Equal(t, "connection", errorType(errors.New("connection refused")))
	assert.Equal(t, "connection", errorType(errors.New("dial tcp: EOF")))
	assert.Equal(t, "other", errorType(errors.New("something else")))
	assert.Equal(t, "unknown", errorType(nil))
}

func TestParseSurrogateKeys(t *testing.T) {
	t.Parallel()
	t.Run("surrogate_key", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("Surrogate-Key", "key1 key2")
		keys := parseSurrogateKeys(h)
		assert.Equal(t, []string{"key1", "key2"}, keys)
	})
	t.Run("cache_tag", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("Cache-Tag", "tag1,tag2")
		keys := parseSurrogateKeys(h)
		assert.Equal(t, []string{"tag1", "tag2"}, keys)
	})
	t.Run("x_cache_tags", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("X-Cache-Tags", "t1 t2, t3")
		keys := parseSurrogateKeys(h)
		assert.Equal(t, []string{"t1", "t2", "t3"}, keys)
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		assert.Nil(t, parseSurrogateKeys(h))
	})
	t.Run("dedup", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set("Surrogate-Key", "key1 key1 key2")
		keys := parseSurrogateKeys(h)
		assert.Equal(t, []string{"key1", "key2"}, keys)
	})
}

func TestStripNoCacheFields(t *testing.T) {
	t.Parallel()
	dst := http.Header{
		header.CacheControl: {"max-age=60"},
		"Set-Cookie":        {"sid=abc"},
		"Content-Encoding":  {"gzip"},
		"ETag":              {`"v1"`},
	}
	stripNoCacheFields(dst, `no-cache="Set-Cookie, Content-Encoding"`)
	assert.NotContains(t, dst, "Set-Cookie")
	assert.NotContains(t, dst, "Content-Encoding")
	assert.Contains(t, dst, "ETag")
}

func TestStripNoCacheFields_Empty(t *testing.T) {
	t.Parallel()
	dst := http.Header{"ETag": {`"v1"`}}
	stripNoCacheFields(dst, "")
	stripNoCacheFields(dst, "max-age=60")
	assert.Contains(t, dst, "ETag")
}

func TestIsInvalidating(t *testing.T) {
	t.Parallel()
	assert.True(t, isInvalidating(http.MethodPost))
	assert.True(t, isInvalidating(http.MethodPut))
	assert.True(t, isInvalidating(http.MethodDelete))
	assert.True(t, isInvalidating(http.MethodPatch))
	assert.False(t, isInvalidating(http.MethodGet))
	assert.False(t, isInvalidating(http.MethodHead))
}

func TestStaleFallbackAllowed(t *testing.T) {
	t.Parallel()
	t.Run("no_restrictions", func(t *testing.T) {
		t.Parallel()
		obj := &api.Object{CacheControl: "max-age=60"}
		assert.True(t, staleFallbackAllowed(obj))
	})
	t.Run("must_revalidate_blocks", func(t *testing.T) {
		t.Parallel()
		obj := &api.Object{CacheControl: "max-age=60, must-revalidate"}
		assert.False(t, staleFallbackAllowed(obj))
	})
	t.Run("no_cache_blocks", func(t *testing.T) {
		t.Parallel()
		obj := &api.Object{CacheControl: "no-cache"}
		assert.False(t, staleFallbackAllowed(obj))
	})
}

func TestSourceSlice(t *testing.T) {
	t.Parallel()
	assert.Equal(t, []string{"hot"}, sourceSlice(api.SourceHot))
	assert.Equal(t, []string{"warm"}, sourceSlice(api.SourceWarm))
	assert.Equal(t, []string{"peer"}, sourceSlice(api.SourcePeer))
	assert.Equal(t, []string{"origin"}, sourceSlice(api.SourceOrigin))
}

func TestComputeTTL_NegativeTTL(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	ttl := computeTTL(h, 404, Directives{}, 30*time.Second, 0, 0, 0, time.Now())
	assert.Equal(t, 30*time.Second, ttl)
}

func TestComputeTTL_HeuristicTTL(t *testing.T) {
	t.Parallel()
	h := http.Header{
		header.Date:         {"Mon, 01 Jan 2024 00:00:00 GMT"},
		header.LastModified: {"Mon, 01 Jan 2023 00:00:00 GMT"},
	}
	ttl := computeTTL(h, 200, Directives{}, 0, 0, 0, 0, time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, 876*time.Hour, ttl)
}

func TestComputeTTL_DefaultTTL(t *testing.T) {
	t.Parallel()
	h := http.Header{}
	ttl := computeTTL(h, 200, Directives{}, 0, 60*time.Second, 0, 0, time.Now())
	assert.Equal(t, 60*time.Second, ttl)
}

func TestHandler_OnlyIfCachedBypass(t *testing.T) {
	t.Parallel()
	h := testHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
	}))
	r := httptest.NewRequest("GET", "http://example.com/nonexistent", nil)
	r.Header.Set(header.CacheControl, "only-if-cached")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, r)
	require.Equal(t, http.StatusGatewayTimeout, rr.Code)
	assert.Equal(t, "MISS", rr.Header().Get(header.XCache))
}

func TestHandler_RefreshStats(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	// No refresh configured → zeros.
	scheduled, registry := h.RefreshStats()
	assert.Equal(t, 0, scheduled)
	assert.Equal(t, 0, registry)
}

func TestHandler_RouteName(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	assert.Equal(t, "", h.RouteName())
}

func TestHandler_RefreshEnabled(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))
	assert.False(t, h.RefreshEnabled())
}

func TestBuildObject_CDNCacheControl(t *testing.T) {
	t.Parallel()
	resHeader := http.Header{}
	resHeader.Set("CDN-Cache-Control", "max-age=120")
	resHeader.Set(header.ContentType, "text/html")
	res := fetchResult{
		StatusCode: 200,
		Header:     resHeader,
		Body:       []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil)
	require.NotNil(t, obj)
	assert.Equal(t, 120*time.Second, obj.TTL)
	assert.Contains(t, obj.CacheControl, "max-age=120")
}

func TestBuildObject_OverrideTTL(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header: http.Header{
			header.CacheControl: {"max-age=60"},
		},
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 300*time.Second, 0, 0, 0, nil)
	require.NotNil(t, obj)
	assert.Equal(t, 300*time.Second, obj.TTL)
}

func TestBuildObject_ContentLengthSynthesis(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header:     http.Header{header.CacheControl: {"max-age=60"}},
		Body:       []byte("hello world"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil)
	require.NotNil(t, obj)
	assert.Equal(t, "11", obj.Header.Get(header.ContentLength))
}

func TestBuildObject_DateApparentAge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	res := fetchResult{
		StatusCode: 200,
		Header: http.Header{
			header.CacheControl: {"max-age=60"},
			header.Date:         {now.Add(-10 * time.Second).Format(http.TimeFormat)},
			header.Age:          {"5"},
		},
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil)
	require.NotNil(t, obj)
	// OriginAge should be max(5s from Age header, ~10s apparent age from Date).
	assert.GreaterOrEqual(t, obj.OriginAge, 5*time.Second)
}

func TestBuildObject_LastModifiedParsed(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header: http.Header{
			header.CacheControl: {"max-age=60"},
			header.LastModified: {"Mon, 01 Jan 2024 00:00:00 GMT"},
		},
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil)
	require.NotNil(t, obj)
	assert.False(t, obj.LastModified.IsZero())
}

func TestBuildObject_SWRDefault(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header:     http.Header{header.CacheControl: {"max-age=60"}},
		Body:       []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 30*time.Second, 0, 0, nil)
	require.NotNil(t, obj)
	assert.Equal(t, 30*time.Second, obj.StaleWhileRevalidate)
}

func TestBuildObject_SIEDefault(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header:     http.Header{header.CacheControl: {"max-age=60"}},
		Body:       []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 60*time.Second, 0, nil)
	require.NotNil(t, obj)
	assert.Equal(t, 60*time.Second, obj.StaleIfError)
}

func TestBuildObject_VaryKeyComputed(t *testing.T) {
	t.Parallel()
	res := fetchResult{
		StatusCode: 200,
		Header: http.Header{
			header.CacheControl: {"max-age=60"},
			header.Vary:         {"Accept-Encoding"},
		},
		Body: []byte("hello"),
	}
	r := httptest.NewRequest("GET", "http://example.com/", nil)
	r.Header.Set(header.AcceptEncoding, "gzip")
	obj := buildObject(api.Key{}, r, res, 0, 0, 0, 0, 0, 0, nil)
	require.NotNil(t, obj)
	// VaryKey should be non-empty (the object has a Vary header).
	assert.NotEqual(t, "", obj.VaryKey)
}

func TestDoBackgroundRefresh_EntryNil(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	ctx := context.Background()
	// Call with a key that's not registered — should skip.
	h.doBackgroundRefresh(ctx, testkey.Key(999), &api.Object{
		StoredAt: time.Now(),
		TTL:      60 * time.Second,
	}, 0)
	// No panic, no store change.
}

func TestDoBackgroundRefresh_BadURL(t *testing.T) {
	t.Parallel()
	h := testRefreshHandler(t, 1)
	key := testkey.Key(1)
	// Register with a URL containing a control character that url.Parse rejects.
	h.refreshRegistry.Register(key, &http.Request{
		Method: "GET",
		URL:    &url.URL{Scheme: "http", Host: "example.com", Path: "/\x00"},
	}, "", 0)
	// This should unregister and skip without panicking.
	h.doBackgroundRefresh(context.Background(), key, &api.Object{
		StoredAt: time.Now(),
		TTL:      60 * time.Second,
	}, 0)
	assert.Equal(t, 0, h.refreshRegistry.Len())
}
