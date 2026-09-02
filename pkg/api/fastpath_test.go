package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRawRequest_Header(t *testing.T) {
	t.Parallel()
	req := &RawRequest{
		Headers: [MaxRawHeaders]RawHeader{
			{Key: "Content-Type", Value: "text/html"},
			{Key: "X-Custom", Value: "abc"},
		},
		NHeaders: 2,
	}
	assert.Equal(t, "text/html", req.Header("content-type"))
	assert.Equal(t, "abc", req.Header("X-Custom"))
	assert.Equal(t, "", req.Header("missing"))
}

func TestRawRequest_HeaderMultiple(t *testing.T) {
	t.Parallel()
	req := &RawRequest{
		Headers: [MaxRawHeaders]RawHeader{
			{Key: "Accept", Value: "text/html"},
			{Key: "Accept-Encoding", Value: "gzip"},
		},
		NHeaders: 2,
	}
	assert.Equal(t, "text/html", req.Header("accept"))
	assert.Equal(t, "gzip", req.Header("accept-encoding"))
	assert.Equal(t, "", req.Header("missing"))
}

func TestRawRequest_HasHeader(t *testing.T) {
	t.Parallel()
	req := &RawRequest{
		Headers: [MaxRawHeaders]RawHeader{
			{Key: "Content-Type", Value: "text/html"},
		},
		NHeaders: 1,
	}
	assert.True(t, req.HasHeader("content-type"))
	assert.True(t, req.HasHeader("Content-Type"))
	assert.False(t, req.HasHeader("missing"))
}

func TestRawRequest_EmptyHeaders(t *testing.T) {
	t.Parallel()
	req := &RawRequest{NHeaders: 0}
	assert.Equal(t, "", req.Header("anything"))
	assert.False(t, req.HasHeader("anything"))
}

func TestEqualFold(t *testing.T) {
	t.Parallel()
	assert.True(t, EqualFold("Content-Type", "content-type"))
	assert.True(t, EqualFold("CONTENT-TYPE", "content-type"))
	assert.True(t, EqualFold("Host", "HOST"))
	assert.True(t, EqualFold("", ""))
	assert.False(t, EqualFold("Content-Type", "Content-Length"))
	assert.False(t, EqualFold("Host", "X-Host"))
	assert.False(t, EqualFold("ab", "abc"))
	assert.False(t, EqualFold("abc", "ab"))
}

func TestEqualFold_SpecialChars(t *testing.T) {
	t.Parallel()
	assert.True(t, EqualFold("X-Custom-Header-1", "x-custom-header-1"))
	assert.False(t, EqualFold("X-Custom", "x-custom-2"))
}

func TestFastPathResponse_Fields(t *testing.T) {
	t.Parallel()
	resp := &FastPathResponse{
		StatusCode:  200,
		CacheResult: "hit",
		Source:      "hot",
		Pool:        "api",
		BytesOut:    1024,
	}
	assert.Equal(t, 200, resp.StatusCode)
	assert.Equal(t, "hit", resp.CacheResult)
	assert.Equal(t, "hot", resp.Source)
	assert.Equal(t, "api", resp.Pool)
	assert.Equal(t, 1024, resp.BytesOut)
}

// fakeFastPathHandler is a test stub for FastPathHandler.
type fakeFastPathHandler struct {
	released bool
}

func (f *fakeFastPathHandler) TryHit(_ *RawRequest, _ time.Time) (*FastPathResponse, bool) {
	return nil, false
}

func (f *fakeFastPathHandler) Release(_ *FastPathResponse) {
	f.released = true
}

func TestFastPathHandler_Interface(t *testing.T) {
	t.Parallel()
	fh := &fakeFastPathHandler{}
	var h FastPathHandler = fh
	resp, ok := h.TryHit(&RawRequest{}, time.Now())
	require.False(t, ok)
	require.Nil(t, resp)
	h.Release(nil)
	assert.True(t, fh.released)
}

// fakeFastPathMetrics is a test stub for FastPathMetrics.
type fakeFastPathMetrics struct {
	hitCount       int
	smugglingCount int
}

func (f *fakeFastPathMetrics) RecordHit(_, _, _, _ string, _, _ int, _ time.Duration) {
	f.hitCount++
}

func (f *fakeFastPathMetrics) IncrementSmugglingRejected() {
	f.smugglingCount++
}

func TestFastPathMetrics_Interface(t *testing.T) {
	t.Parallel()
	var m FastPathMetrics = &fakeFastPathMetrics{}
	m.RecordHit("GET", "api", "hit", "hot", 200, 1024, 5*time.Millisecond)
	m.IncrementSmugglingRejected()
	assert.Equal(t, 1, m.(*fakeFastPathMetrics).hitCount)
	assert.Equal(t, 1, m.(*fakeFastPathMetrics).smugglingCount)
}
