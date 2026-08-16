package cache

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestServeRange_SingleRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.FromHTTP(http.Header{header.ContentType: {"text/plain"}}),
		Body:       []byte("Hello, World!"),
		BodySize:   13,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, http.StatusPartialContent, rr.Code)
	require.Equal(t, "Hello", rr.Body.String())
	cr := rr.Header().Get(header.ContentRange)
	require.Equal(t, "bytes 0-4/13", cr)
	xc := rr.Header().Get(header.XCache)
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

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=-3")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, "hij", rr.Body.String())
}

func TestServeRange_OpenEnded(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=3-")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, "de", rr.Body.String())
}

func TestServeRange_Unsatisfiable(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("ab"),
		BodySize:   2,
	}

	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=5-10")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, http.StatusRequestedRangeNotSatisfiable, rr.Code)
}

func TestServeRange_NoRangeHeader(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := httptest.NewRequest("GET", "/", nil)
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.False(t, ok)
}

func TestServeRange_MultiRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("abcde"), BodySize: 5}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1, 3-4")
	rr := httptest.NewRecorder()

	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	require.Equal(t, 206, rr.Code)
	ct := rr.Header().Get(header.ContentType)
	require.True(t, strings.HasPrefix(ct, "multipart/byteranges"))
	xc := rr.Header().Get(header.XCache)
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
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1")
	rr := httptest.NewRecorder()
	ok := ServeRange(rr, r, obj, true, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, "STALE", rr.Header().Get(header.XCache))
	w := rr.Header().Get(header.Warning)
	assert.Contains(t, w, "110")
}

func TestServeRange_NonBytesPrefix(t *testing.T) {
	t.Parallel()
	obj := &api.Object{Body: []byte("x"), BodySize: 1}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "items=0-1")
	rr := httptest.NewRecorder()
	ok := ServeRange(rr, r, obj, false, api.SourceHot)
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
	r := httptest.NewRequest("HEAD", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1")
	rr := httptest.NewRecorder()
	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, http.StatusPartialContent, rr.Code)
	assert.Empty(t, rr.Body.String())
}

func TestServeRange_HEAD_MultiRange(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}
	r := httptest.NewRequest("HEAD", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1, 3-4")
	rr := httptest.NewRecorder()
	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	assert.Equal(t, http.StatusPartialContent, rr.Code)
	assert.Empty(t, rr.Body.String())
}

func TestServeRange_MissingContentType(t *testing.T) {
	t.Parallel()
	obj := &api.Object{
		StatusCode: 200,
		Header:     header.Map{},
		Body:       []byte("abcde"),
		BodySize:   5,
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set(header.Range, "bytes=0-1, 3-4")
	rr := httptest.NewRecorder()
	ok := ServeRange(rr, r, obj, false, api.SourceHot)
	require.True(t, ok)
	ct := rr.Header().Get(header.ContentType)
	require.Contains(t, ct, "multipart/byteranges")
	// Body should contain application/octet-stream as the part content-type.
	assert.Contains(t, rr.Body.String(), "application/octet-stream")
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
