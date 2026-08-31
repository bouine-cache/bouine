package cache

import (
	"context"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/xxhash/v3"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// fastPathHeaderPool pools 4 KB header write buffers for FastPathResponse.
// Buffers that grow beyond 64 KB are discarded to avoid pinning memory.
var fastPathHeaderPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 4096)
		return &buf
	},
}

// fastPathRespPool pools FastPathResponse objects. The Buffers slice is
// rebuilt from the fixed-size BuffersArr on every TryHit, so we don't
// pre-allocate it here.
var fastPathRespPool = sync.Pool{
	New: func() any {
		return &api.FastPathResponse{}
	},
}

// maxFastPathHeaderBytes caps the serialized header block. Responses
// exceeding this fall through to the full handler.
const maxFastPathHeaderBytes = 8 * 1024

// FastPathHandler implements api.FastPathHandler for the cache layer.
// It holds a reference to the storage store and cache config, and attempts
// to serve cache hits without constructing a full RequestInfo.
type FastPathHandler struct {
	store          storage.Store
	policy         *KeyPolicy // nil = no query/header policy
	onStale        func(req *api.RawRequest, key api.Key, stale *api.Object)
	cachedDate     atomic.Pointer[string]
	routeName      string
	cachedDateUnix atomic.Int64
}

// NewFastPathHandler creates a FastPathHandler from a Handler's config.
// The Handler must be fully initialized before calling this.
func NewFastPathHandler(h *Handler) *FastPathHandler {
	return &FastPathHandler{
		store:     h.store,
		routeName: h.routeName,
		policy:    h.policy,
	}
}

// WithOnStale sets the callback invoked after a fast-path StaleHit is
// served, so the wired Handler can trigger stale-while-revalidate
// background revalidation (RFC 5861 §3). The callback runs on the
// h1parser goroutine: it must not block and must not retain the
// RawRequest (its fields alias the connection's read buffer) without
// copying.
func (f *FastPathHandler) WithOnStale(fn func(req *api.RawRequest, key api.Key, stale *api.Object)) *FastPathHandler {
	f.onStale = fn
	return f
}

// NewFastPathHandlerFromStore creates a FastPathHandler directly from a
// storage.Store. This is used by the engine when the fast path is enabled
// but no specific route handler is available (e.g. multi-route setups
// where the store is shared across routes).
func NewFastPathHandlerFromStore(store storage.Store) *FastPathHandler {
	return &FastPathHandler{
		store: store,
	}
}

// TryHit attempts to serve a cache hit from the parsed request. See
// api.FastPathHandler for the full contract.
func (f *FastPathHandler) TryHit(req *api.RawRequest, now time.Time) (*api.FastPathResponse, bool) {
	reqCC, ok := qualifiesForFastPath(req)
	if !ok {
		return nil, false
	}

	// Use context.Background() directly — store.Get has its own internal
	// timeout mechanisms. Creating a context.WithTimeout per hit allocates
	// a timer + cancelFunc + deadlineCtx on the heap, which defeats the
	// fast-path's zero-allocation goal.
	ctx := context.Background()

	key := buildKeyFromRaw(req, f.policy)
	obj, src, err := f.store.Get(ctx, key)
	if err != nil || obj == nil {
		return nil, false
	}

	// Handle Vary: if the object has a Vary header, re-fetch the variant.
	lookupKey := key
	if vary := obj.VaryValue; vary != "" {
		vk := variantKeyFromRaw(key, vary, req, f.policy)
		if vk != key {
			vobj, vsrc, verr := f.store.Get(ctx, vk)
			if verr != nil || vobj == nil {
				return nil, false
			}
			obj = vobj
			src = vsrc
			lookupKey = vk
		}
	}

	disp := evaluateFromRaw(req, obj, now, reqCC)
	switch disp.Decision {
	case Hit:
		resp := f.serializeResponse(req, obj, src, now, "HIT")
		if resp == nil {
			return nil, false
		}
		return resp, true
	case StaleHit:
		resp := f.serializeResponse(req, obj, src, now, "STALE")
		if resp == nil {
			return nil, false
		}
		// Stale-while-revalidate: the miss-path handler triggers a
		// background refresh after serving a stale object (RFC 5861
		// §3, handler.go triggerBgRevalidate). Without this the fast
		// path would serve stale objects that never refresh.
		if f.onStale != nil && obj.StaleWhileRevalidate > 0 {
			f.onStale(req, lookupKey, obj)
		}
		return resp, true
	default:
		return nil, false
	}
}

// qualifiesForFastPath checks request-level conditions that must be met
// before attempting a cache lookup. This avoids the store.Get call
// entirely for requests that can never be served from cache.
// Returns the parsed request Cache-Control directives so the caller can
// pass them to evaluateFromRaw without re-parsing.
func qualifiesForFastPath(req *api.RawRequest) (Directives, bool) {
	if req.Method != "GET" && req.Method != "HEAD" {
		return Directives{}, false
	}
	var reqCC Directives
	var ccRaw string
	var pragmaNoCache bool
	for i := 0; i < req.NHeaders; i++ {
		h := &req.Headers[i]
		switch {
		case api.EqualFold(h.Key, header.IfNoneMatch),
			api.EqualFold(h.Key, header.IfModifiedSince),
			api.EqualFold(h.Key, header.Range),
			api.EqualFold(h.Key, "If-Range"),
			api.EqualFold(h.Key, "If-Unmodified-Since"),
			api.EqualFold(h.Key, "If-Match"):
			return Directives{}, false
		}
		if api.EqualFold(h.Key, header.TransferEncoding) || api.EqualFold(h.Key, header.ContentLength) {
			return Directives{}, false
		}
		if api.EqualFold(h.Key, header.CacheControl) {
			ccRaw = h.Value
		}
		if api.EqualFold(h.Key, header.Pragma) && api.EqualFold(h.Value, "no-cache") {
			pragmaNoCache = true
		}
	}
	if ccRaw != "" {
		reqCC = ParseCacheControl(ccRaw)
		if reqCC.NoCache || reqCC.NoStore {
			return Directives{}, false
		}
	}
	if pragmaNoCache {
		return Directives{}, false
	}
	return reqCC, true
}

// getCachedDate returns the HTTP-formatted Date string for the current
// second, using a lock-free cache to avoid the expensive
// time.Time.AppendFormat call on every FastPath hit. The cache is
// per-FastPathHandler and valid for one second; concurrent goroutines
// may compute the same string simultaneously (benign race — the result
// is deterministic for a given unix second).
func (f *FastPathHandler) getCachedDate(now time.Time) string {
	unix := now.Unix()
	if unix == f.cachedDateUnix.Load() {
		if p := f.cachedDate.Load(); p != nil {
			return *p
		}
	}
	ds := now.UTC().AppendFormat(nil, httpTimeFormat)
	s := string(ds)
	f.cachedDateUnix.Store(unix)
	f.cachedDate.Store(&s)
	return s
}

// serializeResponse builds a FastPathResponse from a cached object.
// The composed head — status line + static headers + dynamic headers
// (Age, X-Cache, X-Cache-Source, Warning, Date) for the current second
// and this cacheResult/source/close-conn combination — is cached on
// the object: every input is constant within one wall-clock second,
// so hits inside a cached second reuse the exact bytes and skip
// per-hit header appends entirely.
//
// When the request asked for Connection: close (RFC 9110 §9.6), the
// composed head ends with "Connection: close" and the response
// carries CloseConn so the writer closes the connection after the
// flush instead of reusing it.
func (f *FastPathHandler) serializeResponse(req *api.RawRequest, obj *api.Object, src api.Source, now time.Time, cacheResult string) *api.FastPathResponse {
	closeConn := req.ConnectionClose
	if head, statusEnd := obj.ComposedHeadFor(now, cacheResult, src, closeConn); head != nil {
		return responseFromComposedHead(obj, head, statusEnd, req, cacheResult, src, f.routeName, closeConn)
	}

	resp := f.composeResponse(req, obj, src, now, cacheResult, closeConn)
	if resp == nil {
		return nil
	}

	// Cache the composed bytes for the rest of this second. The bytes
	// are copied out of the pooled buffer: the composed head must
	// outlive the pool round-trip (it stays aliased by the object while
	// the pooled buffer is reused for the next miss compose).
	composed := make([]byte, len(resp.HeaderBuf))
	copy(composed, resp.HeaderBuf)
	obj.StoreComposedHead(now, cacheResult, src, closeConn, resp.StatusEnd, composed)
	return resp
}

// composeResponse serializes the full response head into a pooled
// buffer: status line + static headers (from serializedHead, falling
// back to on-the-fly serialization) + dynamic headers. Returns nil
// when the head exceeds maxFastPathHeaderBytes (the pool buffer is
// already returned then).
func (f *FastPathHandler) composeResponse(req *api.RawRequest, obj *api.Object, src api.Source, now time.Time, cacheResult string, closeConn bool) *api.FastPathResponse {
	bufPtr := fastPathHeaderPool.Get().(*[]byte)
	hbuf := (*bufPtr)[:0]

	hbuf = appendStatusLine(hbuf, obj.StatusCode)

	// Use lazily-computed pre-serialized static headers if available.
	// On the first fast-path hit, getOrComputeSerializedHead computes
	// and caches the header block. Subsequent hits reuse the cached bytes.
	if head := f.getOrComputeSerializedHead(obj); head != nil {
		hbuf = append(hbuf, head...)
	} else {
		// Fallback: serialize headers on-the-fly (serialization failed
		// or headers exceed maxFastPathHeaderBytes).
		hbuf = appendResponseHeaders(hbuf, obj, src, now, cacheResult, f.getCachedDate(now), closeConn)
	}

	// Append dynamic headers (Age, X-Cache, X-Cache-Source, Warning, Date).
	dateStr := f.getCachedDate(now)
	hbuf = appendDynamicHeaders(hbuf, obj, src, now, cacheResult, dateStr, closeConn)

	if cap(hbuf) > maxFastPathHeaderBytes {
		*bufPtr = hbuf
		fastPathHeaderPool.Put(bufPtr)
		return nil
	}

	return buildFastPathResponse(hbuf, bufPtr, obj, req, cacheResult, src, f.routeName, closeConn)
}

// getOrComputeSerializedHead returns the lazily-computed serialized
// header block for the object. On the first call, it computes the
// serialized headers via serializeHead and stores them atomically.
// Subsequent calls return the cached bytes. Returns nil if the
// serialized headers exceed maxFastPathHeaderBytes (the fast-path
// falls back to appendResponseHeaders in that case).
func (f *FastPathHandler) getOrComputeSerializedHead(obj *api.Object) []byte {
	if head := obj.LoadSerializedHead(); head != nil {
		return head
	}
	head := serializeHead(obj)
	if len(head) > maxFastPathHeaderBytes {
		return nil // too large, don't cache — fall back to per-hit serialization
	}
	obj.StoreSerializedHead(head)
	// Re-load to return the atomically-stored value, not the local.
	// Concurrent callers may have stored a different *[]byte with the
	// same content; returning the stored value ensures consistency.
	return obj.LoadSerializedHead()
}

// appendResponseHeaders serializes the stored object's static headers
// into hbuf, then appends dynamic headers via appendDynamicHeaders.
// Used as a fallback when SerializedHead is not available (warm-tier
// objects loaded from disk without pre-serialization).
func appendResponseHeaders(hbuf []byte, obj *api.Object, src api.Source, now time.Time, cacheResult string, dateStr string, closeConn bool) []byte {
	var noCacheFields map[string]bool
	if obj.CacheControl != "" {
		noCacheFields = parseNoCacheFieldNames(obj.CacheControl)
	}
	n := obj.Header.Len()
	for i := 0; i < n; i++ {
		key, value := obj.Header.At(i)
		if skipStaticHeader(key, noCacheFields) {
			continue
		}
		hbuf = append(hbuf, key...)
		hbuf = append(hbuf, ": "...)
		hbuf = append(hbuf, value...)
		hbuf = append(hbuf, '\r', '\n')
	}

	hbuf = appendDynamicHeaders(hbuf, obj, src, now, cacheResult, dateStr, closeConn)
	return hbuf
}

// appendDynamicHeaders writes the per-request dynamic headers (Date, Age,
// X-Cache, X-Cache-Source, Warning, Connection) plus the trailing \r\n
// that terminates the HTTP header block. Called after either the
// pre-serialized static headers or the fallback header iteration.
// The Connection trailer reflects the request's own token (RFC 9110
// §9.6): "close" when the client requested close, keep-alive otherwise.
func appendDynamicHeaders(hbuf []byte, obj *api.Object, src api.Source, now time.Time, cacheResult string, dateStr string, closeConn bool) []byte {
	// Date: preserve the origin's Date header (RFC 9110 §6.6.1 — Date
	// represents when the message was originated, not when the cache served
	// it). Only synthesize a Date when the stored object has none.
	if !obj.HasDate {
		hbuf = append(hbuf, header.Date...)
		hbuf = append(hbuf, ": "...)
		hbuf = append(hbuf, dateStr...)
		hbuf = append(hbuf, '\r', '\n')
	}

	age := ComputeAge(obj, now)
	hbuf = append(hbuf, header.Age...)
	hbuf = append(hbuf, ": "...)
	hbuf = strconv.AppendInt(hbuf, int64(age.Seconds()), 10)
	hbuf = append(hbuf, '\r', '\n')

	hbuf = append(hbuf, header.XCache...)
	hbuf = append(hbuf, ": "...)
	hbuf = append(hbuf, cacheResult...)
	hbuf = append(hbuf, '\r', '\n')

	if src != "" {
		hbuf = append(hbuf, header.XCacheSource...)
		hbuf = append(hbuf, ": "...)
		hbuf = append(hbuf, string(src)...)
		hbuf = append(hbuf, '\r', '\n')
	}

	if cacheResult == "STALE" {
		hbuf = append(hbuf, header.Warning...)
		hbuf = append(hbuf, ": 110 - \"Response is Stale\"\r\n"...)
	}

	if closeConn {
		hbuf = append(hbuf, "Connection: close\r\n"...)
	} else {
		hbuf = append(hbuf, "Connection: keep-alive\r\n"...)
	}
	hbuf = append(hbuf, '\r', '\n')
	return hbuf
}

// buildFastPathResponse splits hbuf into status line + header block and
// creates the FastPathResponse with net.Buffers for writev. The response
// is obtained from fastPathRespPool with a pre-allocated Buffers slice,
// eliminating per-hit allocations. req is used only for the HEAD body
// elision; it may be nil when the caller handles the body itself (the
// composed-head path).
func buildFastPathResponse(hbuf []byte, bufPtr *[]byte, obj *api.Object, req *api.RawRequest, cacheResult string, src api.Source, routeName string, closeConn bool) *api.FastPathResponse {
	statusEnd := 0
	for statusEnd < len(hbuf)-1 && (hbuf[statusEnd] != '\r' || hbuf[statusEnd+1] != '\n') {
		statusEnd++
	}
	statusEnd += 2

	statusLine := hbuf[:statusEnd]
	headerBlock := hbuf[statusEnd:]

	body := obj.Body
	if req != nil && req.Method == "HEAD" {
		body = nil
	}

	resp := fastPathRespPool.Get().(*api.FastPathResponse)
	resp.HeaderBuf = hbuf
	resp.BufPtr = bufPtr
	resp.StatusCode = obj.StatusCode
	resp.CacheResult = cacheResult
	resp.Source = string(src)
	resp.Route = routeName
	resp.BytesOut = len(body)
	resp.StatusEnd = statusEnd
	resp.CloseConn = closeConn

	// Rebuild Buffers from the fixed-size backing array. net.Buffers.WriteTo
	// consumes the slice (advancing past the backing array), so we cannot
	// reuse the Buffers slice across hits — we must reset it from buffersArr.
	resp.BuffersArr[0] = statusLine
	resp.BuffersArr[1] = headerBlock
	resp.BuffersArr[2] = body
	resp.Buffers = resp.BuffersArr[:3]
	return resp
}

// responseFromComposedHead builds a response from a cached composed
// head without any per-hit header serialization: the head bytes are
// sliced into status line + header block, the body aliased, and only
// the pooled FastPathResponse wrapper is allocated. No pool header
// buffer is consumed (the composed head owns its bytes).
func responseFromComposedHead(obj *api.Object, head []byte, statusEnd int, req *api.RawRequest, cacheResult string, src api.Source, routeName string, closeConn bool) *api.FastPathResponse {
	body := obj.Body
	if req.Method == "HEAD" {
		body = nil
	}

	resp := fastPathRespPool.Get().(*api.FastPathResponse)
	resp.HeaderBuf = nil
	resp.BufPtr = nil
	resp.StatusCode = obj.StatusCode
	resp.CacheResult = cacheResult
	resp.Source = string(src)
	resp.Route = routeName
	resp.BytesOut = len(body)
	resp.StatusEnd = statusEnd
	resp.CloseConn = closeConn

	resp.BuffersArr[0] = head[:statusEnd]
	resp.BuffersArr[1] = head[statusEnd:]
	resp.BuffersArr[2] = body
	resp.Buffers = resp.BuffersArr[:3]
	return resp
}

// Release returns a FastPathResponse and its pooled header buffer to their
// sync.Pools. Implements api.FastPathHandler. The caller MUST call Release
// after serveHit has finished writing. After Release, the response is invalid.
func (f *FastPathHandler) Release(resp *api.FastPathResponse) {
	if resp == nil {
		return
	}
	// Return the header buffer using the original pool pointer, avoiding
	// a new *[]byte allocation that would occur with &resp.HeaderBuf[:0].
	if resp.BufPtr != nil {
		if cap(resp.HeaderBuf) <= maxFastPathHeaderBytes {
			*resp.BufPtr = resp.HeaderBuf[:0]
			fastPathHeaderPool.Put(resp.BufPtr)
		}
		// Oversized buffers are discarded (not returned to pool).
		resp.BufPtr = nil
	}
	// Reset and return the response to its pool.
	resp.HeaderBuf = nil
	resp.BuffersArr = [3][]byte{}
	resp.Buffers = nil
	resp.StatusCode = 0
	resp.StatusEnd = 0
	resp.CacheResult = ""
	resp.Source = ""
	resp.Route = ""
	resp.BytesOut = 0
	resp.CloseConn = false
	fastPathRespPool.Put(resp)
}

// appendStatusLine writes the HTTP/1.1 status line into buf.
// Fast paths the most common status codes with pre-formatted constants
// to avoid strconv.AppendInt + statusMessage lookup (~12% of FastPath CPU).
func appendStatusLine(buf []byte, statusCode int) []byte {
	switch statusCode {
	case 200:
		return append(buf, "HTTP/1.1 200 OK\r\n"...)
	case 301:
		return append(buf, "HTTP/1.1 301 Moved Permanently\r\n"...)
	case 302:
		return append(buf, "HTTP/1.1 302 Found\r\n"...)
	case 304:
		return append(buf, "HTTP/1.1 304 Not Modified\r\n"...)
	case 404:
		return append(buf, "HTTP/1.1 404 Not Found\r\n"...)
	case 500:
		return append(buf, "HTTP/1.1 500 Internal Server Error\r\n"...)
	}
	buf = append(buf, "HTTP/1.1 "...)
	buf = strconv.AppendInt(buf, int64(statusCode), 10)
	buf = append(buf, ' ')
	buf = append(buf, statusMessage(statusCode)...)
	buf = append(buf, '\r', '\n')
	return buf
}

// shouldSkipHeader reports whether a header should be excluded from the
// fast-path response. This covers internal bouine headers, hop-by-hop
// headers (RFC 9110 §7.6.1), and no-cache fields from the response
// Cache-Control (RFC 9111 §5.2.2.4).
func shouldSkipHeader(key string, noCacheFields map[string]bool) bool {
	switch key {
	case header.XBouinePath, header.XBouineHost, header.XBouineRoute:
		return true
	case header.Connection, header.KeepAlive, header.TransferEncoding,
		header.TE, header.Trailer, header.Upgrade:
		return true
	case header.Age:
		return true
	}
	return noCacheFields[key]
}

// skipStaticHeader reports whether a header should be excluded from the
// pre-serialized static header block (used by both serializeHead and the
// fallback appendResponseHeaders). It combines shouldSkipHeader (internal,
// hop-by-hop, no-cache fields) with dynamic headers that are set
// per-request by appendDynamicHeaders (X-Cache, X-Cache-Source, Warning).
// Date is NOT excluded — the origin's Date header is preserved per
// RFC 9110 §6.6.1; appendDynamicHeaders adds a Date only when the
// stored object has none.
func skipStaticHeader(key string, noCacheFields map[string]bool) bool {
	if shouldSkipHeader(key, noCacheFields) {
		return true
	}
	switch key {
	case header.XCache, header.XCacheSource, header.Warning:
		return true
	}
	return false
}

// parseNoCacheFieldNames extracts the field names from a Cache-Control
// no-cache="..." directive and returns them as a set. Returns nil if
// there are no no-cache fields.
func parseNoCacheFieldNames(ccHeader string) map[string]bool {
	if ccHeader == "" {
		return nil
	}
	cc := ParseCacheControl(ccHeader)
	if cc.NoCacheFields == "" {
		return nil
	}
	fields := strings.FieldsFunc(cc.NoCacheFields, func(r rune) bool {
		return r == ',' || r == ' '
	})
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]bool, len(fields))
	for _, f := range fields {
		if f != "" {
			m[header.InternKey(f)] = true
		}
	}
	return m
}

// buildKeyFromRaw computes the canonical cache key from a RawRequest.
// It mirrors BuildKey but reads from RawRequest fields instead of
// RawRequest, avoiding the allocation of a full request struct.
//
// Zero-alloc on the hot path: uses a 512-byte stack buffer.
func buildKeyFromRaw(req *api.RawRequest, policy *KeyPolicy) api.Key {
	var buf [512]byte
	n := 0

	// Scheme.
	scheme := req.Scheme
	if scheme == "" {
		scheme = "http"
	}
	n += copyOverflow(buf[:], n, scheme)
	n = appendByte(buf[:], n, '|')

	// Host (canonical).
	n = appendCanonicalHost(buf[:], n, req.Host)
	n = appendByte(buf[:], n, '|')

	// Path (canonical).
	n = appendCanonicalPathString(buf[:], n, req.Path)
	n = appendByte(buf[:], n, '|')

	// Query (canonical sorted, with optional param stripping).
	n = appendCanonicalQueryString(buf[:], n, req.Query, policy)
	n = appendByte(buf[:], n, '|')

	// Method (HEAD→GET).
	method := req.Method
	if method == "HEAD" {
		method = "GET"
	}
	n += copyOverflow(buf[:], n, method)

	if n <= len(buf) {
		return NewKey(buf[:n])
	}

	// Overflow: redo with a heap buffer.
	heap := make([]byte, n)
	n = 0
	n += copyOverflow(heap, n, scheme)
	n = appendByte(heap, n, '|')
	n = appendCanonicalHost(heap, n, req.Host)
	n = appendByte(heap, n, '|')
	n = appendCanonicalPathString(heap, n, req.Path)
	n = appendByte(heap, n, '|')
	n = appendCanonicalQueryString(heap, n, req.Query, policy)
	n = appendByte(heap, n, '|')
	n += copyOverflow(heap, n, method)
	return NewKey(heap[:n])
}

// appendCanonicalPathString canonicalizes a path string (collapse
// duplicate slashes, "" → "/"). Mirrors appendCanonicalPath but reads
// from a string instead of *url.URL.
func appendCanonicalPathString(buf []byte, n int, p string) int {
	if p == "" {
		p = "/"
	}
	prev := byte(0)
	for i := 0; i < len(p); i++ {
		c := p[i]
		if c == '/' && prev == '/' {
			continue
		}
		if n < len(buf) {
			buf[n] = c
		}
		n++
		prev = c
	}
	return n
}

// appendCanonicalQueryString canonicalizes a query string (sorted params,
// optional stripping). Mirrors appendCanonicalQuery but reads from a
// raw query string instead of *url.URL.
func appendCanonicalQueryString(buf []byte, n int, raw string, p *KeyPolicy) int {
	if raw == "" {
		return n
	}

	// No-policy path: identical to existing code, no policy overhead.
	if p == nil {
		return appendCanonicalQueryStringNoPolicy(buf, n, raw)
	}

	var stackPairs [8]kvPair
	var seen stackSeen
	np := 0
	simple := true

	for s := raw; s != ""; {
		var seg string
		if i := strings.IndexByte(s, '&'); i >= 0 {
			seg, s = s[:i], s[i+1:]
		} else {
			seg, s = s, ""
		}
		k, v, _ := strings.Cut(seg, "=")
		if p.shouldStripParam(k, v, &seen) {
			continue
		}
		if strings.IndexByte(k, '%') >= 0 || strings.IndexByte(v, '%') >= 0 {
			simple = false
			break
		}
		if np >= len(stackPairs) {
			simple = false
			break
		}
		p.markSeen(k, &seen)
		stackPairs[np] = kvPair{k, v}
		np++
	}

	if !simple {
		return appendCanonicalQuerySlowString(buf, n, raw, p)
	}

	sortPairs(stackPairs[:np])
	return writeSortedPairs(buf, n, stackPairs[:np])
}

// appendCanonicalQueryStringNoPolicy is the existing fast path with no
// policy checking. Identical to the pre-change code path to ensure
// zero regression for the common case.
func appendCanonicalQueryStringNoPolicy(buf []byte, n int, raw string) int {
	var stackPairs [8]kvPair
	np := 0
	simple := true

	for s := raw; s != ""; {
		var seg string
		if i := strings.IndexByte(s, '&'); i >= 0 {
			seg, s = s[:i], s[i+1:]
		} else {
			seg, s = s, ""
		}
		k, v, _ := strings.Cut(seg, "=")
		if strings.IndexByte(k, '%') >= 0 || strings.IndexByte(v, '%') >= 0 {
			simple = false
			break
		}
		if np >= len(stackPairs) {
			simple = false
			break
		}
		stackPairs[np] = kvPair{k, v}
		np++
	}

	if !simple {
		return appendCanonicalQuerySlowStringNoPolicy(buf, n, raw)
	}

	sortPairs(stackPairs[:np])
	return writeSortedPairs(buf, n, stackPairs[:np])
}

// appendCanonicalQuerySlowStringNoPolicy is the existing slow path with
// no policy checking. Uses url.ParseQuery for percent-decoding to match
// key.go's appendCanonicalQuerySlowNoPolicy canonical encoding.
func appendCanonicalQuerySlowStringNoPolicy(buf []byte, n int, raw string) int {
	params, _ := url.ParseQuery(raw)
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	first := true
	for _, k := range keys {
		vals := params[k]
		sort.Strings(vals)
		for _, v := range vals {
			if !first {
				n = appendByte(buf, n, '&')
			}
			first = false
			n += copyOverflow(buf, n, url.QueryEscape(k))
			n = appendByte(buf, n, '=')
			n += copyOverflow(buf, n, url.QueryEscape(v))
		}
	}
	return n
}

// appendCanonicalQuerySlowString handles complex query strings with policy.
// Uses url.ParseQuery for percent-decoding to match appendCanonicalQuerySlow's
// canonical encoding. Called only with p != nil.
//
//nolint:gocyclo // 23: policy application is inherently branchy
func appendCanonicalQuerySlowString(buf []byte, n int, raw string, p *KeyPolicy) int {
	params, _ := url.ParseQuery(raw)
	keys := make([]string, 0, len(params))
	for k := range params {
		if p.keepParams != nil && !p.keepParams[k] {
			continue
		}
		if p.stripParams != nil && p.stripParams[k] {
			continue
		}
		stripped := false
		for i := range p.stripPrefixes {
			if strings.HasPrefix(k, p.stripPrefixes[i]) {
				stripped = true
				break
			}
		}
		if stripped {
			continue
		}
		if p.stripEmpty && p.keepParams == nil {
			allEmpty := true
			for _, v := range params[k] {
				if v != "" {
					allEmpty = false
					break
				}
			}
			if allEmpty {
				continue
			}
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	first := true
	for _, k := range keys {
		vals := params[k]
		if p.dedup {
			vals = vals[:1]
		} else {
			sort.Strings(vals)
		}
		if p.stripEmpty && p.keepParams == nil {
			filtered := vals[:0]
			for _, v := range vals {
				if v != "" {
					filtered = append(filtered, v)
				}
			}
			vals = filtered
			if len(vals) == 0 {
				continue
			}
		}
		for _, v := range vals {
			if !first {
				n = appendByte(buf, n, '&')
			}
			first = false
			n += copyOverflow(buf, n, url.QueryEscape(k))
			n = appendByte(buf, n, '=')
			n += copyOverflow(buf, n, url.QueryEscape(v))
		}
	}
	return n
}

// evaluateFromRaw runs a simplified RFC 9111 state machine for the fast
// path. It only handles Hit and StaleHit — all other dispositions return
// false so the caller falls through to the full handler. This avoids the full
// Evaluate overhead for requests that can be served from cache.
func evaluateFromRaw(_ *api.RawRequest, obj *api.Object, now time.Time, reqCC Directives) Disposition {
	if obj == nil {
		return Disposition{Decision: Miss}
	}

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}

	// Use pre-computed response CC flags to avoid ParseCacheControl on every hit.
	if obj.RespNoCache || reqCC.NoCache {
		return Disposition{Decision: Revalidate}
	}

	// Fresh check.
	if freshWithRequestCC(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}

	// Stale checks: SWR, SIE, max-stale, heuristic freshness.
	if obj.RespMustRevalidate {
		return Disposition{Decision: Revalidate}
	}
	if reqCC.MaxStaleSet {
		originAge := effectiveOriginAge(obj)
		age := now.Sub(obj.StoredAt) + originAge
		staleAge := age - (obj.TTL + originAge)
		if staleAge <= reqCC.MaxStale {
			return Disposition{Decision: StaleHit, Object: obj}
		}
	}
	if obj.StaleForSWR(now) {
		return Disposition{Decision: StaleHit, Object: obj}
	}

	return Disposition{Decision: Revalidate}
}

// variantKeyFromRaw computes the variant key from a RawRequest.
// It mirrors VariantKey but reads header values from RawRequest
// instead of a header.Map from RequestInfo. Vary:* returns primary (RFC 9111 §4.1;
// isCacheBlocked prevents such objects from being stored).
func variantKeyFromRaw(primary api.Key, vary string, req *api.RawRequest, policy *KeyPolicy) api.Key {
	if vary == "" {
		return primary
	}
	if varyContainsStar(vary) {
		return primary
	}

	// Parse and sort Vary field names.
	var fields [maxVaryFields]string
	n := 0
	for f := range strings.SplitSeq(vary, ",") {
		if n >= maxVaryFields {
			return primary // pathological — fall back
		}
		fields[n] = strings.ToLower(strings.TrimSpace(f))
		n++
	}
	if n == 0 {
		return primary
	}
	// Insertion sort.
	for i := 1; i < n; i++ {
		for j := i; j > 0 && fields[j-1] > fields[j]; j-- {
			fields[j-1], fields[j] = fields[j], fields[j-1]
		}
	}

	// Build hash input.
	var buf [256]byte
	off := 0
	written := false
	for i := 0; i < n; i++ {
		f := fields[i]
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		val := normalizeHeaderValue(req.Header(f))
		needed := len(f) + 1 + len(val) + 1
		if off+needed > len(buf) {
			return primary // overflow — fall back
		}
		off += copy(buf[off:], f)
		buf[off] = '='
		off++
		off += copy(buf[off:], val)
		buf[off] = ';'
		off++
		written = true
	}
	if !written {
		return primary
	}
	return primary.WithVary(xxhash.Sum64(buf[:off]))
}
