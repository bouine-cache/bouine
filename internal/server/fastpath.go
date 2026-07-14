package server

import (
	"net"
	"time"
)

// RawRequest is a parsed HTTP/1.1 request. It is populated by the h1parser
// from a pooled read buffer — all string fields are slices of that buffer,
// so they are valid only until the buffer is reused. The FastPathHandler
// must copy any strings it needs to retain beyond the TryHit call.
//
// Unstable.
type RawRequest struct {
	Method      string
	Path        string
	Query       string
	Host        string
	Headers     [MaxRawHeaders]RawHeader
	NHeaders    int
	HTTPVersion string
}

// MaxRawHeaders caps the number of headers the h1parser can store inline.
// Requests exceeding this fall through to net/http.
const MaxRawHeaders = 100

// RawHeader is a single parsed header key-value pair. Both Key and Value
// are slices of the read buffer — zero allocation.
type RawHeader struct {
	Key   string
	Value string
}

// Header returns the value for the given header key (case-insensitive).
// Returns "" if the header is not present.
func (r *RawRequest) Header(key string) string {
	for i := 0; i < r.NHeaders; i++ {
		h := &r.Headers[i]
		if equalFold(h.Key, key) {
			return h.Value
		}
	}
	return ""
}

// HasHeader reports whether the given header key is present.
func (r *RawRequest) HasHeader(key string) bool {
	for i := 0; i < r.NHeaders; i++ {
		if equalFold(r.Headers[i].Key, key) {
			return true
		}
	}
	return false
}

// FastPathHandler is implemented by the cache layer (L3). L1 calls it
// through this interface — no upward import from L1 to L3.
//
// TryHit attempts to serve a cache hit from the parsed request. If the
// request qualifies (GET/HEAD, no conditional headers, cache hit), it
// returns a non-nil FastPathResponse. If the request does not qualify
// (miss, conditional, range, etc.), it returns nil — the caller falls
// through to net/http.
//
// Unstable.
type FastPathHandler interface {
	TryHit(req *RawRequest, now time.Time) (*FastPathResponse, bool)
}

// FastPathResponse is the pre-serialized response for a cache hit.
// L1 writes it directly to net.Conn via net.Buffers — no http.ResponseWriter.
//
// Buffers layout: [0] = status line, [1] = header block, [2] = body.
// Buffers[0] and [1] are slices of HeaderBuf — a pooled buffer. After
// WriteTo returns, the caller returns HeaderBuf to the pool.
// Buffers[2] is obj.Body — owned by the cache, not pooled, not returned.
//
// Unstable.
type FastPathResponse struct {
	Buffers     net.Buffers
	HeaderBuf   []byte
	StatusCode  int
	CacheResult string
	Source      string
	Route       string
	BytesOut    int
}

// equalFold compares two ASCII strings case-insensitively without
// allocating. HTTP header keys are ASCII (RFC 9110 §5.1).
func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 32
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 32
		}
		if ca != cb {
			return false
		}
	}
	return true
}
