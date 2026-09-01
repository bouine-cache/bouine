package api

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
	Headers     [MaxRawHeaders]RawHeader
	Method      string
	Path        string
	Query       string
	Host        string
	Scheme      string // "http" or "https" — set by the listener
	HTTPVersion string
	NHeaders    int
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
		if EqualFold(h.Key, key) {
			return h.Value
		}
	}
	return ""
}

// HasHeader reports whether the given header key is present.
func (r *RawRequest) HasHeader(key string) bool {
	for i := 0; i < r.NHeaders; i++ {
		if EqualFold(r.Headers[i].Key, key) {
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
// Release returns a FastPathResponse (and its pooled header buffer) to
// the pool. The caller MUST call Release after serveHit has finished
// writing, even on error. After Release, the response is invalid and
// must not be used.
//
// Unstable.
type FastPathHandler interface {
	TryHit(req *RawRequest, now time.Time) (*FastPathResponse, bool)
	Release(resp *FastPathResponse)
}

// FastPathResponse is the pre-serialized response for a cache hit.
// L1 writes it directly to net.Conn via net.Buffers — no http.ResponseWriter.
//
// Buffers layout: [0] = status line, [1] = header block, [2] = body.
// Buffers[0] and [1] are slices of HeaderBuf — a pooled buffer. After
// WriteTo returns, the caller calls FastPathHandler.Release to return
// both HeaderBuf (via BufPtr) and this response to their pools.
// Buffers[2] is obj.Body — owned by the cache, not pooled, not returned.
//
// BuffersArr is the fixed-size backing array for Buffers. net.Buffers.WriteTo
// consumes the Buffers slice (advancing past the backing array via *v = (*v)[1:]),
// leaving Buffers with len=0, cap=0. Rebuilding Buffers from buffersArr on
// every TryHit avoids allocating a new [][]byte backing array on pool reuse.
//
// Unstable.
type FastPathResponse struct {
	BufPtr      *[]byte // original pool pointer for HeaderBuf, used by Release
	CacheResult string
	Source      string
	Route       string
	BuffersArr  [3][]byte // fixed-size backing for Buffers; rebuilt every TryHit
	Buffers     net.Buffers
	HeaderBuf   []byte
	StatusCode  int
	BytesOut    int
}

// EqualFold compares two ASCII strings case-insensitively without
// allocating. HTTP header keys are ASCII (RFC 9110 §5.1).
func EqualFold(a, b string) bool {
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

// FastPathMetrics is implemented by the observability layer (L7) to
// record fast-path hits without going through the middleware chain.
// The h1parser calls RecordHit after serving a hit. L1 depends on
// this interface (declared in the leaf package), not on L7 directly.
//
// Unstable.
type FastPathMetrics interface {
	RecordHit(method, route, cacheResult, source string, status, bytesOut int, duration time.Duration)
	// IncrementSmugglingRejected is called when the h1parser detects an
	// HTTP smuggling attempt (CL+TE conflict, duplicate Content-Length,
	// obs-fold). The implementation increments a Prometheus counter.
	IncrementSmugglingRejected()
}
