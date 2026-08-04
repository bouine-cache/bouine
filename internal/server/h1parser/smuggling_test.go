package h1parser

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSmugglingDetected_CLPlusTE(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Length", Value: "5"},
			{Key: "Transfer-Encoding", Value: "chunked"},
		},
		NHeaders: 3,
	}
	assert.True(t, smugglingDetected(req))
}

func TestSmugglingDetected_DuplicateCL(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Length", Value: "5"},
			{Key: "Content-Length", Value: "10"},
		},
		NHeaders: 3,
	}
	assert.True(t, smugglingDetected(req))
}

func TestSmugglingDetected_NoSmuggling(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/html"},
		},
		NHeaders: 2,
	}
	assert.False(t, smugglingDetected(req))
}

func TestSmugglingDetected_OnlyTE(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Transfer-Encoding", Value: "chunked"},
		},
		NHeaders: 2,
	}
	assert.False(t, smugglingDetected(req))
}

func TestSmugglingDetected_OnlyCL(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Length", Value: "5"},
		},
		NHeaders: 2,
	}
	assert.False(t, smugglingDetected(req))
}

func TestParseHeaders_ObsFold(t *testing.T) {
	// obs-fold: a continuation line starting with SP (RFC 9110 §5.5).
	// The parser must return ErrFallThrough.
	raw := []byte("GET / HTTP/1.1\r\nX-Custom: value\r\n continuation\r\n\r\n")
	req := &api.RawRequest{}
	err := parseHeaders(raw, req)
	if err != ErrFallThrough {
		t.Errorf("parseHeaders with obs-fold should return ErrFallThrough, got %v", err)
	}
}

func TestParseHeaders_TooManyHeaders(t *testing.T) {
	// Build a request with more than MaxRawHeaders headers.
	raw := []byte("GET / HTTP/1.1\r\n")
	for i := 0; i < api.MaxRawHeaders+5; i++ {
		raw = append(raw, []byte("X-Header: value\r\n")...)
	}
	raw = append(raw, []byte("\r\n")...)
	req := &api.RawRequest{}
	err := parseHeaders(raw, req)
	if err != ErrFallThrough {
		t.Errorf("parseHeaders with too many headers should return ErrFallThrough, got %v", err)
	}
}

func TestSmugglingHookCalled(t *testing.T) {
	called := false
	parser := New(nil, nil,
		WithSmugglingHook(func() { called = true }),
	)

	// Simulate a CL+TE smuggling request.
	raw := []byte("GET /cached HTTP/1.1\r\nHost: example.com\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n")
	conn := &mockConn{r: bytes.NewReader(raw)}

	var readBuf [readBufferSize]byte

	// parseRequest should detect smuggling, call the hook, and fall
	// through with the parsed request so net/http can return 400.
	req, fallThrough, _, err := parser.parseRequest(conn, &readBuf)
	require.NoError(t, err, "parseRequest returned error")
	assert.True(t, fallThrough)
	assert.NotNil(t, req)
	assert.True(t, called)
}
