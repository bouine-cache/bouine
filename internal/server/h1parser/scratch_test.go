package h1parser

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestParseRequest_ScratchReuse verifies the per-connection scratch
// RawRequest is fully reset between requests: fields from a previous
// request (headers, host, method, path, query, version) must not leak
// into a subsequent request parsed on the same connection, and the
// returned request must alias the scratch struct.
func TestParseRequest_ScratchReuse(t *testing.T) {
	parser := New(nil, nil)

	first := "GET /first/path?b=2&a=1 HTTP/1.1\r\nHost: first.example.com\r\nX-First: v1\r\nX-Second: v2\r\n\r\n"
	second := "POST /second HTTP/1.0\r\nHost: second.example.com\r\n\r\n"

	var readBuf [readBufferSize]byte
	var scratch api.RawRequest

	req, fallThrough, _, err := parser.parseRequest(&mockConn{r: bytes.NewReader([]byte(first))}, &readBuf, &scratch)
	require.NoError(t, err)
	require.False(t, fallThrough)
	assert.Equal(t, "GET", req.Method)
	assert.Equal(t, "/first/path", req.Path)
	assert.Equal(t, "b=2&a=1", req.Query)
	assert.Equal(t, "first.example.com", req.Host)
	assert.Equal(t, "HTTP/1.1", req.HTTPVersion)
	assert.Equal(t, 3, req.NHeaders)
	assert.Equal(t, "first.example.com", req.Header("Host"))

	// Second request on the same connection, same buffers: parseRequest
	// reuses the scratch struct. Note: a real keep-alive connection reads
	// the second request into the same readBuf, which is truncated to
	// [:0] before reading, so stale header slices are overwritten.
	req2, fallThrough2, _, err := parser.parseRequest(&mockConn{r: bytes.NewReader([]byte(second))}, &readBuf, &scratch)
	require.NoError(t, err)
	require.False(t, fallThrough2)
	assert.Equal(t, "POST", req2.Method)
	assert.Equal(t, "/second", req2.Path)
	assert.Equal(t, "", req2.Query)
	assert.Equal(t, "second.example.com", req2.Host)
	assert.Equal(t, "HTTP/1.0", req2.HTTPVersion)
	assert.Equal(t, 1, req2.NHeaders)
	// Aliasing: both returned requests point at the scratch struct.
	assert.Same(t, req, req2)
}

// TestParseRequest_MalformedSecondRequest resets scratch correctly when
// the first request populated headers but the second is malformed.
func TestParseRequest_MalformedSecondRequest(t *testing.T) {
	parser := New(nil, nil)

	first := "GET /ok HTTP/1.1\r\nHost: ok.example.com\r\nX-Many: headers\r\n\r\n"
	second := "BADREQUEST\r\n"

	var readBuf [readBufferSize]byte
	var scratch api.RawRequest

	_, _, _, err := parser.parseRequest(&mockConn{r: bytes.NewReader([]byte(first))}, &readBuf, &scratch)
	require.NoError(t, err)

	// Malformed second request returns an error and must not observe
	// stale state from the first.
	_, fallThrough, _, err := parser.parseRequest(&mockConn{r: bytes.NewReader([]byte(second))}, &readBuf, &scratch)
	require.Error(t, err)
	assert.True(t, fallThrough)
	// The scratch was reset: NHeaders is 0 before the parse error
	// short-circuits.
	assert.Zero(t, scratch.NHeaders)
}
