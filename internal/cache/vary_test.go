package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func TestVariantKey_NoVary(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	got := VariantKey(primary, "", header.Map{}, nil)
	require.Equal(t, primary, got)
}

func TestVariantKey_DifferentHeaders(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h1 := headerMap(header.AcceptEncoding, "gzip")
	h2 := headerMap(header.AcceptEncoding, "br")
	k1 := VariantKey(primary, "Accept-Encoding", h1, nil)
	k2 := VariantKey(primary, "Accept-Encoding", h2, nil)
	require.NotEqual(t, k2, k1)
	if k1 == primary || k2 == primary {
		t.Fatal("variant key should differ from primary")
	}
}

func TestVariantKey_SameHeaders(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h := headerMap(header.AcceptEncoding, "gzip")
	k1 := VariantKey(primary, "Accept-Encoding", h, nil)
	k2 := VariantKey(primary, "Accept-Encoding", h, nil)
	require.Equal(t, k2, k1)
}

// TestVariantKey_VaryStar verifies that Vary:* returns the primary key
// (a no-op). RFC 9111 §4.1: a stored response with Vary:* "always fails
// to match," so no variant key is computed. isCacheBlocked is the sole
// gate that prevents Vary:* responses from being stored.
func TestVariantKey_VaryStar(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h1 := headerMap(header.Accept, "text/html")
	h2 := headerMap(header.Accept, "application/json")
	require.Equal(t, primary, VariantKey(primary, "*", h1, nil))
	require.Equal(t, primary, VariantKey(primary, "*", h2, nil))

	// Fast path must match.
	raw := &api.RawRequest{NHeaders: 1}
	raw.Headers[0] = api.RawHeader{Key: "Accept", Value: "text/html"}
	require.Equal(t, primary, variantKeyFromRaw(primary, "*", raw, nil))

	// Nil header must not panic.
	require.Equal(t, primary, VariantKey(primary, "*", header.Map{}, nil))

	// Policy exclusions don't change the result — still primary.
	policy := NewKeyPolicy(nil, nil, map[string]bool{"accept": true}, nil, false, false)
	require.Equal(t, primary, VariantKey(primary, "*", h1, policy))
}

func TestVariantKey_ExcludeCaseInsensitive(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	// Exclude map uses lowercase; Vary header uses mixed case.
	// VariantKey lowercases Vary fields before lookup, so this should
	// match.
	excludePolicy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	h1 := headerMap("X-Request-ID", "abc")
	h2 := headerMap("X-Request-ID", "xyz")
	k1 := VariantKey(primary, "X-Request-ID", h1, excludePolicy)
	k2 := VariantKey(primary, "X-Request-ID", h2, excludePolicy)
	require.Equal(t, k2, k1)
	require.Equal(t, primary, k1)

	// Partial exclude: non-excluded Vary field must still produce a
	// variant key distinct from primary.
	hGzip := headerMap(header.AcceptEncoding, "gzip")
	hGzip.Set("X-Request-ID", "abc")
	kPartial := VariantKey(primary, "Accept-Encoding, X-Request-ID", hGzip, excludePolicy)
	require.NotEqual(t, primary, kPartial)
}

func TestHandler_VaryAwareStorage(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Encoding")
		ctx.Response.Header.Set(header.ContentEncoding, string(ctx.Request.Header.Peek(header.AcceptEncoding)))
		ctx.Response.Header.Set(header.ETag, `"v1"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("body-" + string(ctx.Request.Header.Peek(header.AcceptEncoding))))
	}
	h := testHandler(t, upstream)

	r1 := testCtxWithHeader("GET", "http://example.com/vary", header.AcceptEncoding, "gzip")
	h.ServeRequest(r1)
	require.Equal(t, "MISS", respHeader(r1, header.XCache))

	r2 := testCtxWithHeader("GET", "http://example.com/vary", header.AcceptEncoding, "br")
	h.ServeRequest(r2)
	require.Equal(t, "MISS", respHeader(r2, header.XCache))

	r3 := testCtxWithHeader("GET", "http://example.com/vary", header.AcceptEncoding, "gzip")
	h.ServeRequest(r3)
	require.Equal(t, "HIT", respHeader(r3, header.XCache))
	require.Equal(t, "body-gzip", respBody(r3))
}

func TestHandler_RangeOnCachedObject(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.ETag, `"full"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("Hello, Range World!"))
	}
	h := testHandler(t, upstream)

	rr := testCtx("GET", "http://example.com/range")
	h.ServeRequest(rr)
	require.Equal(t, 200, respCode(rr))

	rr2 := testCtxWithHeader("GET", "http://example.com/range", header.Range, "bytes=0-4")
	h.ServeRequest(rr2)
	require.Equal(t, fasthttp.StatusPartialContent, respCode(rr2))
	require.Equal(t, "Hello", respBody(rr2))
	require.Equal(t, "HIT", respHeader(rr2, header.XCache))
}

func TestHandler_RangeOnStaleObject(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=1, stale-while-revalidate=60")
		ctx.Response.Header.Set(header.ETag, `"full"`)
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("Hello, Range World!"))
	}
	h := testHandler(t, upstream)

	url := "http://example.com/stale-range"
	rr := testCtx("GET", url)
	h.ServeRequest(rr)

	key := BuildKey(requestInfoFromURL("GET", url), nil)
	obj, _, _ := h.store.Get(context.Background(), key)
	require.NotNil(t, obj)
	stale := obj.CloneForRefresh()
	stale.StoredAt = time.Now().Add(-2 * time.Second)
	_ = h.store.Put(context.Background(), key, stale)

	rr = testCtxWithHeader("GET", url, header.Range, "bytes=0-4")
	h.ServeRequest(rr)

	require.Equal(t, fasthttp.StatusPartialContent, respCode(rr))
	require.Equal(t, "Hello", respBody(rr))
	require.Equal(t, "STALE", respHeader(rr, header.XCache))
	w := respHeader(rr, header.Warning)
	require.True(t, strings.HasPrefix(w, "110"))
}

func TestNormalizeHeaderValue(t *testing.T) {
	t.Parallel()
	t.Run("no_comma_lowercase", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "gzip", normalizeHeaderValue("GZIP"))
	})
	t.Run("comma_separated_sorted", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "en,fr", normalizeHeaderValue("fr, en"))
	})
	t.Run("same_order_regardless_of_input", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, normalizeHeaderValue("en,FR"), normalizeHeaderValue("fr, en"))
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", normalizeHeaderValue(""))
	})
	t.Run("whitespace_trimmed", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "gzip", normalizeHeaderValue("  gzip  "))
	})
}

func TestVaryContainsStar(t *testing.T) {
	t.Parallel()
	t.Run("star_alone", func(t *testing.T) {
		t.Parallel()
		require.True(t, varyContainsStar("*"))
	})
	t.Run("star_with_spaces", func(t *testing.T) {
		t.Parallel()
		require.True(t, varyContainsStar(" * "))
	})
	t.Run("non_star", func(t *testing.T) {
		t.Parallel()
		require.False(t, varyContainsStar("Accept-Encoding"))
	})
	t.Run("accept_and_star", func(t *testing.T) {
		t.Parallel()
		require.True(t, varyContainsStar("Accept, *"))
	})
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		require.False(t, varyContainsStar(""))
	})
}

func TestVariantKeySlow_TooManyFields(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h := header.Map{}
	// >16 Vary fields triggers variantKeySlow fallback.
	vary := ""
	for i := range 20 {
		if i > 0 {
			vary += ", "
		}
		vary += "X-H" + string(rune('0'+i))
		h.Set("X-H"+string(rune('0'+i)), "val")
	}
	// Should produce a non-primary key (variantKeySlow processes all fields).
	result := VariantKey(primary, vary, h, nil)
	assert.NotEqual(t, primary, result)
}

func TestVariantKeySlow_LongValue(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	h := header.Map{}
	// A single Vary field with a very long value that exceeds the 256-byte buffer.
	h.Set("Accept-Encoding", string(make([]byte, 300)))
	result := VariantKey(primary, "Accept-Encoding", h, nil)
	// Should NOT return primary (it should hash the long value).
	assert.NotEqual(t, primary, result)
}

func TestNormaliseListHeader_Comma(t *testing.T) {
	t.Parallel()
	// "b, a" and "a, b" should produce the same output (sorted).
	assert.Equal(t, normaliseListHeader("b, a"), normaliseListHeader("a, b"))
	// Should be trimmed and sorted.
	assert.Equal(t, "a,b", normaliseListHeader(" b ,  a "))
}
