package h1parser

import (
	"bytes"
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
	if !smugglingDetected(req) {
		t.Error("smugglingDetected should return true for CL+TE conflict")
	}
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
	if !smugglingDetected(req) {
		t.Error("smugglingDetected should return true for duplicate Content-Length")
	}
}

func TestSmugglingDetected_NoSmuggling(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/html"},
		},
		NHeaders: 2,
	}
	if smugglingDetected(req) {
		t.Error("smugglingDetected should return false for normal request")
	}
}

func TestSmugglingDetected_OnlyTE(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Transfer-Encoding", Value: "chunked"},
		},
		NHeaders: 2,
	}
	if smugglingDetected(req) {
		t.Error("smugglingDetected should return false for TE without CL")
	}
}

func TestSmugglingDetected_OnlyCL(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Content-Length", Value: "5"},
		},
		NHeaders: 2,
	}
	if smugglingDetected(req) {
		t.Error("smugglingDetected should return false for single Content-Length")
	}
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
	if err != nil {
		t.Fatalf("parseRequest returned error: %v", err)
	}
	if !fallThrough {
		t.Error("parseRequest should fall through on smuggling")
	}
	if req == nil {
		t.Error("parseRequest should return the parsed request for net/http to reject")
	}
	if !called {
		t.Error("smugglingHook was not called")
	}
}
