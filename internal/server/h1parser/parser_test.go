package h1parser

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestParseRequestLine(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		method  string
		path    string
		query   string
		version string
		host    string
		nHdrs   int
	}{
		{
			name:    "simple GET",
			input:   "GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n",
			method:  "GET",
			path:    "/hello",
			version: "HTTP/1.1",
			host:    "example.com",
			nHdrs:   1,
		},
		{
			name:    "GET with query",
			input:   "GET /api?foo=bar HTTP/1.1\r\nHost: localhost\r\n\r\n",
			method:  "GET",
			path:    "/api",
			query:   "foo=bar",
			version: "HTTP/1.1",
			host:    "localhost",
			nHdrs:   1,
		},
		{
			name:    "HEAD with headers",
			input:   "HEAD / HTTP/1.1\r\nHost: a.com\r\nAccept: text/html\r\nUser-Agent: test\r\n\r\n",
			method:  "HEAD",
			path:    "/",
			version: "HTTP/1.1",
			host:    "a.com",
			nHdrs:   3,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := &api.RawRequest{}
			buf := []byte(tt.input)
			if err := parseRequestLine(buf, req); err != nil {
				t.Fatalf("parseRequestLine: %v", err)
			}
			if req.Method != tt.method {
				t.Errorf("method=%q want %q", req.Method, tt.method)
			}
			if req.Path != tt.path {
				t.Errorf("path=%q want %q", req.Path, tt.path)
			}
			if req.Query != tt.query {
				t.Errorf("query=%q want %q", req.Query, tt.query)
			}
			if req.HTTPVersion != tt.version {
				t.Errorf("version=%q want %q", req.HTTPVersion, tt.version)
			}
			if err := parseHeaders(buf, req); err != nil {
				t.Fatalf("parseHeaders: %v", err)
			}
			if req.NHeaders != tt.nHdrs {
				t.Errorf("nHeaders=%d want %d", req.NHeaders, tt.nHdrs)
			}
			if req.Host != tt.host {
				t.Errorf("host=%q want %q", req.Host, tt.host)
			}
		})
	}
}

func TestRawRequest_Header(t *testing.T) {
	req := &api.RawRequest{
		Headers: [api.MaxRawHeaders]api.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/html"},
		},
		NHeaders: 2,
	}
	v := req.Header("host")
	assert.Equal(t, "example.com", v)
	v = req.Header("ACCEPT")
	assert.Equal(t, "text/html", v)
	v = req.Header("X-Custom")
	assert.Equal(t, "", v)
}

// repeatReader is an io.Reader that yields raw endlessly, simulating a
// keep-alive connection delivering back-to-back requests.
type repeatReader struct {
	buf []byte
	off int
}

func (r *repeatReader) Read(p []byte) (int, error) {
	n := copy(p, r.buf[r.off:])
	r.off += n
	if r.off >= len(r.buf) {
		r.off = 0
	}
	return n, nil
}

// BenchmarkGate_H1Parse_Get measures the full production parse path:
// parseRequest on a keep-alive stream, including the scratch RawRequest
// reset. The gate budget is 0 allocs/op — this benchmark exists
// precisely so that a per-request allocation in parseRequest (e.g. a
// heap-allocated RawRequest, which once dominated hit-path GC pressure
// at ~4 KB/request) fails CI instead of hiding behind a benchmark that
// pre-allocates outside the loop.
func BenchmarkGate_H1Parse_Get(b *testing.B) {
	raw := []byte("GET /api/v1/users/42 HTTP/1.1\r\nHost: example.com\r\nAccept: application/json\r\nUser-Agent: Bouine-Test/1.0\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n")
	parser := New(nil, nil)
	conn := &mockConn{r: &repeatReader{buf: raw}}
	var readBuf [readBufferSize]byte
	var scratch api.RawRequest

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req, fallThrough, _, err := parser.parseRequest(conn, &readBuf, &scratch)
		if err != nil {
			b.Fatalf("parseRequest: %v", err)
		}
		if fallThrough {
			b.Fatal("parseRequest fell through unexpectedly")
		}
		if req.Method != "GET" {
			b.Fatalf("method = %q", req.Method)
		}
	}
}

// BenchmarkH1Parse_Get_Components measures the individual parse helpers
// in isolation (request line, header block, header-end scan) for
// component-level analysis. Not gated on allocs — the production-path
// gate is BenchmarkGate_H1Parse_Get above.
func BenchmarkH1Parse_Get_Components(b *testing.B) {
	raw := []byte("GET /api/v1/users/42 HTTP/1.1\r\nHost: example.com\r\nAccept: application/json\r\nUser-Agent: Bouine-Test/1.0\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req := &api.RawRequest{}
		_ = findHeaderEnd(raw)
		_ = parseRequestLine(raw, req)
		_ = parseHeaders(raw, req)
	}
}
