package h1parser

import (
	"testing"

	"github.com/bouine-cache/bouine/internal/server"
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
			req := &server.RawRequest{}
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
	req := &server.RawRequest{
		Headers: [server.MaxRawHeaders]server.RawHeader{
			{Key: "Host", Value: "example.com"},
			{Key: "Accept", Value: "text/html"},
		},
		NHeaders: 2,
	}
	if v := req.Header("host"); v != "example.com" {
		t.Errorf("Header(host)=%q want example.com", v)
	}
	if v := req.Header("ACCEPT"); v != "text/html" {
		t.Errorf("Header(ACCEPT)=%q want text/html", v)
	}
	if v := req.Header("X-Custom"); v != "" {
		t.Errorf("Header(X-Custom)=%q want empty", v)
	}
}

func BenchmarkH1Parse_Get(b *testing.B) {
	raw := []byte("GET /api/v1/users/42 HTTP/1.1\r\nHost: example.com\r\nAccept: application/json\r\nUser-Agent: Bouine-Test/1.0\r\nX-Forwarded-For: 10.0.0.1\r\n\r\n")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		req := &server.RawRequest{}
		_ = parseRequestLine(raw, req)
		_ = parseHeaders(raw, req)
	}
}
