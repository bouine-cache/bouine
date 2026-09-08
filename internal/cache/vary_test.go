package cache

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
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

// TestJoinedVary_MultiLine verifies the RFC 9110 §5.2 join used for
// VaryValue and variant keys: multiple Vary field lines combine into one
// comma-joined list. Get (first line only) dropped later lines and
// collapsed distinct variants onto a single cache entry.
func TestJoinedVary_MultiLine(t *testing.T) {
	t.Parallel()
	// Build the multi-line shape via AppendEntry.
	multi := headerMap(header.Vary, "Accept-Encoding,Accept-Language")
	multi.AppendEntry(header.Vary, "BM-Market")
	require.Equal(t, "Accept-Encoding,Accept-Language, BM-Market", joinedVary(multi))
	// Single-line Vary passes through unchanged.
	single := headerMap(header.Vary, "Accept-Encoding")
	require.Equal(t, "Accept-Encoding", joinedVary(single))
	// No Vary header returns empty.
	require.Equal(t, "", joinedVary(headerMap(header.ContentType, "text/html")))
}

// TestJoinedVary_VariantKeysDistinguishLaterLines pins the regression:
// two requests differing only in a header named on the second Vary line
// must produce distinct variant keys once the joined VaryValue is used.
func TestJoinedVary_VariantKeysDistinguishLaterLines(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	vary := "Accept-Encoding,Accept-Language, BM-Market"
	fr := headerMap(header.AcceptEncoding, "gzip", header.AcceptLanguage, "en", "BM-Market", "fr")
	us := headerMap(header.AcceptEncoding, "gzip", header.AcceptLanguage, "en", "BM-Market", "us")
	kFr := VariantKey(primary, vary, fr, nil)
	kUs := VariantKey(primary, vary, us, nil)
	require.NotEqual(t, kFr, kUs)
	require.NotEqual(t, primary, kFr)
}

// TestHandler_MultiLineVaryDistinctVariants is the end-to-end regression
// for the production incident: an origin that sends Vary across two
// field lines ("Vary: Accept-Encoding,Accept-Language" +
// "Vary: BM-Market") stored objects under a variant key that ignored
// BM-Market, so a second market was served the first market's cached
// body without touching the origin.
func TestHandler_MultiLineVaryDistinctVariants(t *testing.T) {
	t.Parallel()
	var originHits int
	upstream := func(ctx *fasthttp.RequestCtx) {
		originHits++
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Encoding,Accept-Language")
		ctx.Response.Header.Add(header.Vary, "BM-Market")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("market=" + string(ctx.Request.Header.Peek("BM-Market"))))
	}
	h := testHandler(t, upstream)

	r1 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r1.Request.Header.Set("BM-Market", "fr")
	h.ServeRequest(r1)
	require.Equal(t, "MISS", respHeader(r1, header.XCache))
	require.Equal(t, "market=fr", respBody(r1))

	r2 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r2.Request.Header.Set("BM-Market", "us")
	h.ServeRequest(r2)
	require.Equal(t, "MISS", respHeader(r2, header.XCache),
		"a different BM-Market must not hit the first market's variant")
	require.Equal(t, "market=us", respBody(r2))

	r3 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r3.Request.Header.Set("BM-Market", "fr")
	h.ServeRequest(r3)
	require.Equal(t, "HIT", respHeader(r3, header.XCache))
	require.Equal(t, "market=fr", respBody(r3))

	r4 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r4.Request.Header.Set("BM-Market", "us")
	h.ServeRequest(r4)
	require.Equal(t, "HIT", respHeader(r4, header.XCache))
	require.Equal(t, "market=us", respBody(r4))
	require.Equal(t, 2, originHits, "origin must have been fetched exactly once per market")
}

// TestHandler_MultiLineVaryFastPathDistinctVariants pins the same
// variant isolation on the H1 fast path, which resolves variants from
// the stored VaryValue instead of the raw response headers.
func TestHandler_MultiLineVaryFastPathDistinctVariants(t *testing.T) {
	t.Parallel()
	upstream := func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		ctx.Response.Header.Set(header.Vary, "Accept-Encoding,Accept-Language")
		ctx.Response.Header.Add(header.Vary, "BM-Market")
		ctx.SetStatusCode(200)
		_, _ = ctx.Write([]byte("market=" + string(ctx.Request.Header.Peek("BM-Market"))))
	}
	h := testHandler(t, upstream)

	r1 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r1.Request.Header.Set("BM-Market", "fr")
	h.ServeRequest(r1)
	require.Equal(t, "MISS", respHeader(r1, header.XCache))

	r2 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r2.Request.Header.Set("BM-Market", "fr")
	h.ServeRequest(r2)
	require.Equal(t, "HIT", respHeader(r2, header.XCache))
	require.Equal(t, "market=fr", respBody(r2))

	// Distinct market must miss and fetch its own variant.
	r3 := testCtxWithHeader("GET", "http://example.com/page", header.AcceptEncoding, "gzip")
	r3.Request.Header.Set("BM-Market", "us")
	h.ServeRequest(r3)
	require.Equal(t, "MISS", respHeader(r3, header.XCache))
	require.Equal(t, "market=us", respBody(r3))
}

// TestBuildObject_MultiLineVaryValue pins VaryValue and VaryKey on the
// RFC-joined Vary value for objects built from multi-line responses.
func TestBuildObject_MultiLineVaryValue(t *testing.T) {
	t.Parallel()
	resMap := headerMap(header.CacheControl, "max-age=60", header.Vary, "Accept-Encoding,Accept-Language")
	resMap.AppendEntry(header.Vary, "BM-Market")
	res := fetchResult{StatusCode: 200, Header: fromHeaderMap(resMap), Body: []byte("body")}
	ri := requestInfoFromHTTP("http://example.com/page", "/page",
		headerMap(header.AcceptEncoding, "gzip", header.AcceptLanguage, "en", "BM-Market", "fr"))
	obj := buildObject(testkey.Key(1), ri, res, resMap, 0, 0, 0, 0, 0, 0, nil, time.Now())
	require.NotNil(t, obj)
	require.Equal(t, "Accept-Encoding,Accept-Language, BM-Market", obj.VaryValue)
	require.NotEmpty(t, obj.VaryKey)
	// The stored header map keeps both field lines; WriteToFastHTTP and
	// GetAll on the stored map reproduce the same joined value.
	require.Equal(t, obj.VaryValue, joinedVary(obj.Header))
}

// TestRefreshFrom304_MultiLineVaryValue pins the 304 revalidation path:
// when the 304 response re-sends Vary across multiple field lines, the
// refreshed object's VaryValue must be the RFC-joined list, not the
// first line only (Get), or the variant key would change shape after
// refresh and orphan the previously stored variant.
func TestRefreshFrom304_MultiLineVaryValue(t *testing.T) {
	t.Parallel()
	h := testHandler(t, origin200("body"))

	multi := headerMap(header.CacheControl, "max-age=60", header.Vary, "Accept-Encoding,Accept-Language")
	multi.AppendEntry(header.Vary, "BM-Market")
	stale := &api.Object{
		Key:        BuildKeyFromURL("http://example.com/test", nil),
		StatusCode: 200,
		Header:     multi.Clone(),
		Body:       []byte("body"),
		BodySize:   4,
		StoredAt:   time.Now().Add(-time.Minute),
		TTL:        time.Minute,
		ETag:       `"v1"`,
		VaryValue:  joinedVary(multi),
	}
	stale.CacheControl = stale.Header.Get(header.CacheControl)

	res := fetchResult{
		StatusCode: 304,
		Header:     fromHeaderMap(multi.Clone()),
	}

	refreshed := h.refreshFrom304(stale, res, time.Now())
	require.Equal(t, "Accept-Encoding,Accept-Language, BM-Market", refreshed.VaryValue)
}

// TestMultiLineVary_FastPathVariantHIT is the h1parser fast-path
// regression for the production incident: a stored object whose
// VaryValue is the RFC-9110 §5.2 join of two Vary field lines
// ("Accept-Encoding,Accept-Language" + "BM-Market") must be resolved
// via variantKeyFromRaw on the full joined list. Hashing only the
// first line's fields served one market's body to another without
// touching the origin. Mirrors TestHandler_MultiLineVaryFastPathDistinctVariants
// (handler path) on the fast path.
func TestMultiLineVary_FastPathVariantHIT(t *testing.T) {
	t.Parallel()
	store := storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
	fp := NewFastPathHandlerFromStore(store)

	reqBase := &api.RawRequest{
		Method: "GET",
		Path:   "/market-page",
		Host:   "example.com",
		Scheme: "http",
	}
	primary := buildKeyFromRaw(reqBase, nil)

	vary := "Accept-Encoding,Accept-Language, BM-Market"
	// The stored header map keeps the original two field lines, exactly
	// as the incident response carried them; the RFC-joined list lives
	// in VaryValue.
	varyLines := headerMap(header.Vary, "Accept-Encoding,Accept-Language")
	varyLines.AppendEntry(header.Vary, "BM-Market")
	primaryObj := &api.Object{
		Key:        primary,
		StatusCode: 200,
		Header:     varyLines,
		VaryValue:  vary,
		Body:       []byte("primary"),
		BodySize:   7,
		StoredAt:   time.Now(),
		TTL:        60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), primary, primaryObj))

	marketReq := func(market string) *api.RawRequest {
		req := &api.RawRequest{
			Method:   "GET",
			Path:     "/market-page",
			Host:     "example.com",
			Scheme:   "http",
			NHeaders: 3,
		}
		req.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
		req.Headers[1] = api.RawHeader{Key: "Accept-Language", Value: "en"}
		req.Headers[2] = api.RawHeader{Key: "BM-Market", Value: market}
		req.RecomputeScanFlags()
		return req
	}

	frKey := variantKeyFromRaw(primary, vary, marketReq("fr"), nil)
	require.NotEqual(t, primary, frKey,
		"the joined Vary list must produce a non-primary variant key")

	// The incident stored the fr variant under a key that ignored
	// BM-Market; pin that the key the fast path now computes is
	// market-sensitive.
	usKey := variantKeyFromRaw(primary, vary, marketReq("us"), nil)
	require.NotEqual(t, frKey, usKey, "distinct markets must hash to distinct variant keys")

	frObj := &api.Object{
		Key:        frKey,
		StatusCode: 200,
		Header: headerMap(header.Vary, "Accept-Encoding,Accept-Language",
			header.ContentLength, "9"),
		VaryValue: vary,
		Body:      []byte("market=fr"),
		BodySize:  9,
		StoredAt:  time.Now(),
		TTL:       60 * time.Second,
	}
	require.NoError(t, store.Put(context.Background(), frKey, frObj))

	// Same market must HIT its own variant through the fast path.
	resp, ok := fp.TryHit(marketReq("fr"), time.Now())
	require.True(t, ok, "TryHit should serve the fr variant")
	require.NotNil(t, resp)
	assert.Equal(t, "HIT", resp.CacheResult)
	require.GreaterOrEqual(t, len(resp.Buffers), 3)
	assert.Equal(t, "market=fr", string(resp.Buffers[2]))
	fp.Release(resp)

	// A different market must not hit the fr variant.
	resp2, ok := fp.TryHit(marketReq("us"), time.Now())
	assert.False(t, ok, "a different BM-Market must not hit the fr variant")
	assert.Nil(t, resp2)
}
