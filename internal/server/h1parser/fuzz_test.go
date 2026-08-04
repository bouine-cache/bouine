package h1parser

import (
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
)

// FuzzParseRequestLine fuzzes the request line parser with arbitrary
// byte sequences. The parser must never panic on any input.
func FuzzParseRequestLine(f *testing.F) {
	f.Add("GET /hello HTTP/1.1\r\nHost: example.com\r\n\r\n")
	f.Add("POST /api?foo=bar HTTP/1.1\r\nHost: localhost\r\n\r\n")
	f.Add("HEAD / HTTP/1.0\r\n\r\n")
	f.Add("\r\n")
	f.Add("")

	f.Fuzz(func(t *testing.T, data string) {
		req := &api.RawRequest{}
		_ = parseRequestLine([]byte(data), req)
	})
}

// FuzzParseHeaders fuzzes the header parser with arbitrary byte
// sequences. The parser must never panic on any input. It exercises
// obs-fold, duplicate headers, CL+TE smuggling patterns, and malformed
// header lines.
func FuzzParseHeaders(f *testing.F) {
	f.Add("GET /hello HTTP/1.1\r\nHost: example.com\r\nAccept: text/html\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nX-Custom: value\r\n continuation\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nHost: a.com\r\nHost: b.com\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\n\r\n")

	f.Fuzz(func(t *testing.T, data string) {
		req := &api.RawRequest{}
		_ = parseHeaders([]byte(data), req)
	})
}

// FuzzSmugglingDetected fuzzes the smuggling detector with arbitrary
// header combinations. It must never panic and must return a bool.
func FuzzSmugglingDetected(f *testing.F) {
	f.Add("GET / HTTP/1.1\r\nHost: a.com\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nContent-Length: 5\r\nContent-Length: 10\r\n\r\n")
	f.Add("GET / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n")

	f.Fuzz(func(t *testing.T, data string) {
		req := &api.RawRequest{}
		_ = parseHeaders([]byte(data), req)
		_ = smugglingDetected(req)
	})
}
