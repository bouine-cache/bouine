package api

import (
	"net"
	"time"

	"github.com/valyala/fasthttp"
)

// RawRequest is a parsed HTTP/1.1 request. It is populated by the h1parser
// from a pooled read buffer — all string fields are slices of that buffer,
// so they are valid only until the buffer is reused. The FastPathHandler
// must copy any strings it needs to retain beyond the TryHit call.
//
// Unstable.
//
// same allocator size class (3456), and the field order keeps the
// per-request hot string group contiguous ahead of the scalar tail.
//
//nolint:govet // fieldalignment: the reported 8-byte saving is inside the
type RawRequest struct {
	// Headers is the bulk array (readers iterate [0:NHeaders) only); it
	// leads the struct so the scalar tail stays in the final cache
	// lines, matching the h1parser's per-request access pattern.
	Headers     [MaxRawHeaders]RawHeader
	Method      string
	Path        string
	Query       string
	Host        string
	Scheme      string // "http" or "https" — set by the listener
	HTTPVersion string
	// CacheControlRaw is the last-seen raw Cache-Control header value
	// captured during the header scan. The fast path parses it via
	// ParseCacheControl only when non-empty, avoiding a re-scan of the
	// header array. Empty string means the request carried none.
	CacheControlRaw string
	ScanFlags       RequestScanFlags
	NHeaders        int
	// ConnectionClose reports whether the request carried a
	// "Connection: close" token (RFC 9110 §7.6.1). The parser sets it
	// once while scanning headers; the fast path reads it to emit
	// "Connection: close" on the response (§9.6) and its callers to
	// close the connection after the hit instead of re-scanning
	// headers. False for the zero value.
	//
	// ScanFlags is the single-pass header scan result: a bitmask of
	// RequestScanFlag bits (conditional/precondition headers, TE/CL
	// presence, duplicate CL count saturated at 2, Pragma: no-cache).
	// Populated by the h1parser while appending headers; consumers
	// (fast-path qualification, smuggling detection) read flags
	// instead of re-scanning the header array. Zero value = none set.
	// Hand-built RawRequests that set Headers without ScanFlags must
	// call req.RecomputeScanFlags() first (see its doc).
	ConnectionClose bool
}

// RequestScanFlags is the bitmask of single-pass header scan results
// carried on RawRequest.ScanFlags. Bit values must never change
// (consumers persist none of them, but the zero-cost contract of the
// fused scan depends on the layout staying stable across packages).
type RequestScanFlags uint32

// RequestScanFlag bits. DisqualifyFastPath is a derived composite bit:
// the parser sets it directly when any conditional/precondition,
// Transfer-Encoding, or Content-Length header is seen, so the fast path
// can bail with one test in the common case.
const (
	// FlagHasCL: a Content-Length header is present.
	FlagHasCL RequestScanFlags = 1 << iota
	// FlagHasTE: a Transfer-Encoding header is present.
	FlagHasTE
	// FlagDuplicateCL: more than one Content-Length header (saturated;
	// 2+ duplicates are smuggling either way).
	FlagDuplicateCL
	// FlagPragmaNoCache: a "Pragma: no-cache" header is present
	// (case-insensitive value comparison per RFC 9110 §16.2 legacy use).
	FlagPragmaNoCache
	// FlagHostSeen: at least one Host header is present. The first
	// occurrence's value is in Host; duplicates do not overwrite it.
	FlagHostSeen
	// FlagHasConnection: a Connection header is present; its value is
	// re-read from the header array only when token scanning is needed.
	FlagHasConnection

	// DisqualifyFastPath is set when any conditional (If-None-Match,
	// If-Modified-Since, If-Range, If-Unmodified-Since, If-Match),
	// Range, Transfer-Encoding, or Content-Length header is present:
	// the request can never be served by the plain-hit fast path.
	// Qualification checks that would otherwise scan headers become a
	// single bit test.
	DisqualifyFastPath RequestScanFlags = 1 << 24
)

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

// FastPathHandlerCtx is the fasthttp-native version of FastPathHandler.
// It receives a *fasthttp.RequestCtx (populated by the rewritten
// h1parser) instead of *RawRequest. The response is written directly
// to net.Conn via FastPathResponse.Buffers — ctx is only used for
// reading the request, not for writing the response.
//
// This interface will replace FastPathHandler after the h1parser
// rewrite (Phase 1 of the fasthttp migration, issue #521). It exists
// now so that Phase 0 can land the interface change without breaking
// the existing h1parser, which still uses *RawRequest.
//
// Unstable.
type FastPathHandlerCtx interface {
	TryHit(ctx *fasthttp.RequestCtx, now time.Time) (*FastPathResponse, bool)
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
// CloseConn, when true, tells the writer the response header block ends
// with "Connection: close" (RFC 9110 §9.6): the connection must not be
// reused for another request after this response. Set by the fast path
// when the request requested close; the h1parser and the reactor both
// read it to terminate their keep-alive loops after the flush.
//
// Unstable.
type FastPathResponse struct {
	BufPtr      *[]byte // original pool pointer for HeaderBuf, used by Release
	CacheResult string
	Source      string
	// Pool is the upstream pool serving the hit, consumed by the
	// metrics hook as the upstream_pool label. It comes from the
	// route's pool config, never from request input.
	Pool       string
	BuffersArr [3][]byte // fixed-size backing for Buffers; rebuilt every TryHit
	Buffers    net.Buffers
	HeaderBuf  []byte
	StatusCode int
	// StatusEnd splits BuffersArr[0] (status line) from [1] (header
	// block): the offset of the first header byte inside HeaderBuf or
	// the composed head. Stored so the composed-head cache can slice a
	// cached head without rescanning for the \r\n.
	StatusEnd int
	BytesOut  int
	CloseConn bool
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

// RecomputeScanFlags rebuilds ScanFlags and CacheControlRaw from the
// Headers array [0,NHeaders). The parser populates both during its
// single-pass scan, so production callers never need this; it exists
// for hand-built RawRequests (tests, admin purge paths) that set
// Headers directly and then call a consumer that reads the flags. It
// must stay in sync with the parser's scan — a drift here is a
// correctness bug in every flag consumer, so the parser's fused scan
// and this function share the same flag-assignment rules, including
// the duplicate-Content-Length count (FlagDuplicateCL), which is
// per-request state no single-header helper can derive.
func (r *RawRequest) RecomputeScanFlags() {
	var flags RequestScanFlags
	var ccRaw string
	clSeen := false
	for i := 0; i < r.NHeaders; i++ {
		h := &r.Headers[i]
		flags |= ScanFlagForHeader(h.Key, h.Value)
		if EqualFold(h.Key, "Content-Length") {
			if clSeen {
				flags |= FlagDuplicateCL
			}
			clSeen = true
		}
		if EqualFold(h.Key, "Cache-Control") {
			ccRaw = h.Value
		}
	}
	r.ScanFlags = flags
	r.CacheControlRaw = ccRaw
}

// ScanFlagForHeader maps one header key/value to its scan flags. It is
// the single source of truth shared by the parser's fused scan and
// RecomputeScanFlags — both must call it with identical semantics:
// first Host wins (caller checks FlagHostSeen before assigning r.Host),
// last Cache-Control wins (caller overwrites on every occurrence),
// duplicate CL saturates.
func ScanFlagForHeader(key, value string) RequestScanFlags {
	var flags RequestScanFlags
	switch {
	case EqualFold(key, "Host"):
		flags |= FlagHostSeen
	case EqualFold(key, "If-None-Match"),
		EqualFold(key, "If-Modified-Since"),
		EqualFold(key, "If-Range"),
		EqualFold(key, "If-Unmodified-Since"),
		EqualFold(key, "If-Match"),
		EqualFold(key, "Range"):
		flags |= DisqualifyFastPath
	case EqualFold(key, "Transfer-Encoding"):
		flags |= FlagHasTE | DisqualifyFastPath
	case EqualFold(key, "Content-Length"):
		flags |= FlagHasCL | DisqualifyFastPath
	case EqualFold(key, "Pragma"):
		if EqualFold(value, "no-cache") {
			flags |= FlagPragmaNoCache | DisqualifyFastPath
		}
	case EqualFold(key, "Connection"):
		flags |= FlagHasConnection
	}
	return flags
}

// FastPathMetrics is implemented by the observability layer (L7) to
// record fast-path hits without going through the middleware chain.
// The h1parser calls RecordHit after serving a hit. L1 depends on
// this interface (declared in the leaf package), not on L7 directly.
//
// Unstable.
type FastPathMetrics interface {
	RecordHit(pool, cacheResult, source string, status, bytesOut int, duration time.Duration)
	// IncrementSmugglingRejected is called when the h1parser detects an
	// HTTP smuggling attempt (CL+TE conflict, duplicate Content-Length,
	// obs-fold). The implementation increments a Prometheus counter.
	IncrementSmugglingRejected()
}
