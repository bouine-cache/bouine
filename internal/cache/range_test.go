package cache

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

type fastRangeWriter struct {
	ctx *fasthttp.RequestCtx
}

func (w *fastRangeWriter) SetHeader(key, value string) {
	w.ctx.Response.Header.Set(key, value)
}

func (w *fastRangeWriter) WriteHeader(code int) {
	w.ctx.SetStatusCode(code)
}

func (w *fastRangeWriter) Write(b []byte) (int, error) {
	return w.ctx.Write(b)
}

func TestServeRange_SingleRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     headerMap(header.ContentType, "text/plain"),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
	}

	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=0-4")

	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, fasthttp.StatusPartialContent, respCode(r))
	require.Equal(t, "Hello", respBody(r))
	cr := respHeader(r, header.ContentRange)
	require.Equal(t, "bytes 0-4/13", cr)
	xc := respHeader(r, header.XCache)
	require.Equal(t, "HIT", xc)
}

func TestServeRange_SuffixRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcdefghij"),
		BodySize:   10,
	}

	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=-3")

	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, "hij", respBody(r))
}

func TestServeRange_OpenEnded(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}

	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=3-")

	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, "de", respBody(r))
}

func TestServeRange_Unsatisfiable(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("ab"),
		BodySize:   2,
	}

	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=5-10")

	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, fasthttp.StatusRequestedRangeNotSatisfiable, respCode(r))
}

func TestServeRange_NoRangeHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := testCtx("GET", "http://example.com/")

	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.False(t, ok)
}

func TestServeRange_MultiRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("abcde"), BodySize: 5}
	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=0-1, 3-4")

	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, 206, respCode(r))
	ct := respHeader(r, header.ContentType)
	require.True(t, strings.HasPrefix(ct, "multipart/byteranges"))
	xc := respHeader(r, header.XCache)
	require.Equal(t, "HIT", xc)
}

func TestServeRange_StaleWarning(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}
	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=0-1")
	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, true, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, "STALE", respHeader(r, header.XCache))
	w := respHeader(r, header.Warning)
	assert.Contains(t, w, "110")
}

func TestServeRange_NonBytesPrefix(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "items=0-1")
	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.False(t, ok)
}

func TestServeRange_HEAD_SingleRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}
	r := testCtxWithHeader("HEAD", "http://example.com/", header.Range, "bytes=0-1")
	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, fasthttp.StatusPartialContent, respCode(r))
	assert.Empty(t, respBody(r))
}

func TestServeRange_HEAD_MultiRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}
	r := testCtxWithHeader("HEAD", "http://example.com/", header.Range, "bytes=0-1, 3-4")
	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, fasthttp.StatusPartialContent, respCode(r))
	assert.Empty(t, respBody(r))
}

func TestServeRange_MissingContentType(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}
	r := testCtxWithHeader("GET", "http://example.com/", header.Range, "bytes=0-1, 3-4")
	ok := ServeRange(&fastRangeWriter{ctx: r}, requestInfoFromCtx(r), obj, false, api.SourceHot)
	require.True(t, ok)
	ct := respHeader(r, header.ContentType)
	require.Contains(t, ct, "multipart/byteranges")
	assert.Contains(t, respBody(r), "application/octet-stream")
}

func TestParseRange_NoDash(t *testing.T) {
	t.Parallel()
	_, _, ok := parseRange("garbage", 10)
	require.False(t, ok)
}

func TestParseRange_SuffixLeZero(t *testing.T) {
	t.Parallel()
	_, _, ok := parseRange("-0", 10)
	require.False(t, ok)
}

func TestParseRange_SuffixLargerThanSize(t *testing.T) {
	t.Parallel()
	start, end, ok := parseRange("-100", 10)
	require.True(t, ok)
	assert.Equal(t, int64(0), start)
	assert.Equal(t, int64(9), end)
}

func TestParseRange_EndLessThanStart(t *testing.T) {
	t.Parallel()
	_, _, ok := parseRange("5-3", 10)
	require.False(t, ok)
}

func TestParseRange_EndClampedToSize(t *testing.T) {
	t.Parallel()
	start, end, ok := parseRange("3-100", 10)
	require.True(t, ok)
	assert.Equal(t, int64(3), start)
	assert.Equal(t, int64(9), end)
}
