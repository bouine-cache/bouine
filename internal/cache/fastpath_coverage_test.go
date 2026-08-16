package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestEvaluateFromRaw_NoStore(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "no-store"}
	req.NHeaders = 1
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Bypass, d.Decision)
}

func TestEvaluateFromRaw_NoCache(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "no-cache"}
	req.NHeaders = 1
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second, ETag: `"v1"`}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Revalidate, d.Decision)
}

func TestEvaluateFromRaw_MustRevalidate(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StoredAt:     time.Now().Add(-2 * time.Second),
		TTL:          time.Second,
		CacheControl: "max-age=1, must-revalidate",
	}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Revalidate, d.Decision)
}

func TestEvaluateFromRaw_MaxStale(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StoredAt: time.Now().Add(-10 * time.Second),
		TTL:      time.Second,
	}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.CacheControl, Value: "max-stale=60"}
	req.NHeaders = 1
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, StaleHit, d.Decision)
}

func TestEvaluateFromRaw_Fresh(t *testing.T) {
	t.Parallel()
	obj := &api.Object{StoredAt: time.Now(), TTL: 60 * time.Second}
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, obj, time.Now())
	assert.Equal(t, Hit, d.Decision)
}

func TestEvaluateFromRaw_NilObj(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	d := evaluateFromRaw(req, nil, time.Now())
	assert.Equal(t, Miss, d.Decision)
}

func TestVariantKeyFromRaw_VaryStar(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	assert.Equal(t, primary, variantKeyFromRaw(primary, "*", req, nil))
}

func TestVariantKeyFromRaw_EmptyVary(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	assert.Equal(t, primary, variantKeyFromRaw(primary, "", req, nil))
}

func TestVariantKeyFromRaw_DifferentHeaders(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req1 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req1.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "gzip"}
	req1.NHeaders = 1
	req2 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req2.Headers[0] = api.RawHeader{Key: "Accept-Encoding", Value: "br"}
	req2.NHeaders = 1
	k1 := variantKeyFromRaw(primary, "Accept-Encoding", req1, nil)
	k2 := variantKeyFromRaw(primary, "Accept-Encoding", req2, nil)
	assert.NotEqual(t, k2, k1)
	assert.NotEqual(t, primary, k1)
}

func TestVariantKeyFromRaw_PolicyExclusion(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	policy := NewKeyPolicy(nil, nil, map[string]bool{"x-request-id": true}, nil, false, false)
	req1 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req1.Headers[0] = api.RawHeader{Key: "X-Request-Id", Value: "abc"}
	req1.NHeaders = 1
	req2 := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req2.Headers[0] = api.RawHeader{Key: "X-Request-Id", Value: "xyz"}
	req2.NHeaders = 1
	k1 := variantKeyFromRaw(primary, "X-Request-Id", req1, policy)
	k2 := variantKeyFromRaw(primary, "X-Request-Id", req2, policy)
	assert.Equal(t, k2, k1)
	assert.Equal(t, primary, k1)
}

func TestVariantKeyFromRaw_TooManyFields(t *testing.T) {
	t.Parallel()
	primary := testkey.Key(100)
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	// >16 Vary fields → falls back to primary.
	vary := ""
	for i := range 20 {
		if i > 0 {
			vary += ", "
		}
		vary += "X-H" + string(rune('0'+i))
	}
	assert.Equal(t, primary, variantKeyFromRaw(primary, vary, req, nil))
}

func TestQualifiesForFastPath_IfRange(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: "If-Range", Value: `"etag"`}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestQualifiesForFastPath_IfMatch(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: "If-Match", Value: `"etag"`}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestQualifiesForFastPath_TransferEncoding(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.TransferEncoding, Value: "chunked"}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestQualifiesForFastPath_PragmaNoCache(t *testing.T) {
	t.Parallel()
	req := &api.RawRequest{Method: "GET", Path: "/", Host: "x.com", Scheme: "http"}
	req.Headers[0] = api.RawHeader{Key: header.Pragma, Value: "no-cache"}
	req.NHeaders = 1
	assert.False(t, qualifiesForFastPath(req))
}

func TestShouldSkipHeader(t *testing.T) {
	t.Parallel()
	noCache := map[string]bool{"Set-Cookie": true}
	assert.True(t, shouldSkipHeader(header.XBouinePath, noCache))
	assert.True(t, shouldSkipHeader(header.Connection, noCache))
	assert.True(t, shouldSkipHeader(header.TE, noCache))
	assert.True(t, shouldSkipHeader(header.Trailer, noCache))
	assert.True(t, shouldSkipHeader(header.Upgrade, noCache))
	assert.True(t, shouldSkipHeader(header.Age, noCache))
	assert.True(t, shouldSkipHeader("Set-Cookie", noCache))
	assert.False(t, shouldSkipHeader("Content-Type", noCache))
}

func TestSkipStaticHeader(t *testing.T) {
	t.Parallel()
	noCache := map[string]bool{}
	assert.True(t, skipStaticHeader(header.XCache, noCache))
	assert.True(t, skipStaticHeader(header.XCacheSource, noCache))
	assert.True(t, skipStaticHeader(header.Warning, noCache))
	assert.False(t, skipStaticHeader("Content-Type", noCache))
}

func TestParseNoCacheFieldNames(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, parseNoCacheFieldNames(""))
	})
	t.Run("no_no_cache", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, parseNoCacheFieldNames("max-age=60"))
	})
	t.Run("with_fields", func(t *testing.T) {
		t.Parallel()
		m := parseNoCacheFieldNames(`no-cache="Set-Cookie, Content-Encoding"`)
		require.NotNil(t, m)
		assert.True(t, m["Set-Cookie"])
		assert.True(t, m["Content-Encoding"])
	})
}

func TestAppendCanonicalPathString_Empty(t *testing.T) {
	t.Parallel()
	var buf [64]byte
	n := appendCanonicalPathString(buf[:], 0, "")
	assert.Equal(t, "/", string(buf[:n]))
}

func TestAppendCanonicalPathString_DuplicateSlashes(t *testing.T) {
	t.Parallel()
	var buf [64]byte
	n := appendCanonicalPathString(buf[:], 0, "/a//b")
	assert.Equal(t, "/a/b", string(buf[:n]))
}

func TestRelease_NilResponse(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	fp := NewFastPathHandlerFromStore(store)
	fp.Release(nil) // must not panic
}

func TestRelease_OversizedBuffer(t *testing.T) {
	t.Parallel()
	store := newTestStore()
	fp := NewFastPathHandlerFromStore(store)
	// Create a response with an oversized buffer.
	resp := &api.FastPathResponse{
		StatusCode: 200,
	}
	resp.HeaderBuf = make([]byte, 2*1024*1024) // 2 MiB > maxFastPathHeaderBytes
	fp.Release(resp)
}

func newTestStore() *storage.HotStore {
	return storage.NewHotStore(storage.HotConfig{MaxBytes: 1 << 20})
}
