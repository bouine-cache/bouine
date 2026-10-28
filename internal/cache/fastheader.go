package cache

import (
	"bytes"
	"reflect"
	"time"
	"unsafe"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// fastheader.go provides zero-allocation fast-path variants of cache
// functions that read request headers directly from *fasthttp.RequestCtx
// via Peek, avoiding the headerFromCtx allocation (which constructs a
// header.Map with string conversions for every header on every request).
//
// These functions are used on the cache-hit path where only a few specific
// headers are needed (Cache-Control, Pragma, If-None-Match,
// If-Modified-Since, Range). Most requests don't carry these headers,
// so Peek returns nil and no string allocation occurs.
//
// The miss/revalidate paths continue to use requestInfoFromCtx which
// builds a full header.Map (needed for Vary matching, cacheability
// checks, storage, etc.).
//
// The response side of the hit path lives here too: getOrComputeFastHeader
// (the cached *fasthttp.ResponseHeader served objects reuse across hits)
// and the noDefaultDate field poke it needs, so fasthttp never
// auto-generates a Date over the stored origin value.

// evaluateFast runs the RFC 9111 state machine via the shared evaluate,
// reading request directives straight from the *fasthttp.RequestCtx via
// Peek (zero-alloc) and deriving the response gate from the pre-parsed
// object flags — no ParseCacheControl on the hit path.
func evaluateFast(ctx *fasthttp.RequestCtx, obj *api.Object, now time.Time) Disposition {
	method := ctx.Method()
	if !bytes.Equal(method, []byte("GET")) && !bytes.Equal(method, []byte("HEAD")) {
		return Disposition{Decision: Bypass}
	}

	var reqCC Directives
	if rawCC := ctx.Request.Header.Peek(header.CacheControl); len(rawCC) > 0 {
		reqCC = ParseCacheControlBytes(rawCC)
	}

	if !reqCC.NoCache && bytes.Equal(ctx.Request.Header.Peek(header.Pragma), []byte("no-cache")) {
		reqCC.NoCache = true
	}

	return evaluate(obj, reqCC, respGateFromObject(obj), now)
}

// tryConditional304 checks if the client's conditional headers match
// the cached object, using direct Peek calls. Returns true if a 304
// response was sent. xCache labels the response with the X-Cache value
// of the serving path ("HIT" for hits, "REVALIDATED" after a successful
// revalidation); result selects the synthesized CDN-Cache-Control cap
// for downstream_freshness: sync routes (ADR-0063 I4) — a StaleHit
// conditional gets cacheStale, so the 304 carries the mirror cap and
// Warning: 110. rw applies the route's response header rewrite
// directives to the 304; nil for routes without directives.
func (h *Handler) tryConditional304(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source, xCache string, result cacheResult, now time.Time, rw func(*fasthttp.ResponseHeader)) bool {
	inm := ctx.Request.Header.Peek(header.IfNoneMatch)
	if len(inm) > 0 {
		if obj.ETag != "" && etagMatch(string(inm), obj.ETag) {
			h.write304(ctx, obj, src, xCache, result, now, rw)
			return true
		}
		return false
	}
	ims := ctx.Request.Header.Peek(header.IfModifiedSince)
	if len(ims) > 0 {
		imsTime := parseHTTPDate(string(ims))
		if imsTime.IsZero() {
			return false
		}
		if !obj.LastModified.IsZero() && !obj.LastModified.After(imsTime) {
			h.write304(ctx, obj, src, xCache, result, now, rw)
			return true
		}
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get(header.Date); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					h.write304(ctx, obj, src, xCache, result, now, rw)
					return true
				}
			}
		}
	}
	return false
}

// write304 sets the headers and status code for a 304 Not Modified
// response using SetCanonical to skip key normalization. Under
// downstream_freshness: sync the 304 carries the same synthesized
// CDN-Cache-Control cap as the corresponding 200 serve (ADR-0063 I4:
// RFC 9111 §4.3.4 — a 304's headers update the downstream stored entry,
// so an uncapped 304 would let a downstream cache re-freshen unboundedly)
// plus Warning: 110 when the 304 is stale-sourced, mirroring the 200
// stale serve. X-Cache keeps today's label ("HIT"/"REVALIDATED").
func (h *Handler) write304(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source, xCache string, result cacheResult, now time.Time, rw func(*fasthttp.ResponseHeader)) {
	if obj.ETag != "" {
		ctx.Response.Header.SetCanonical(header.S2b(header.ETag), header.S2b(obj.ETag))
	}
	if result == cacheStale {
		ctx.Response.Header.SetCanonical(header.S2b(header.Warning), header.S2b(`110 - "Response is Stale"`))
	}
	if h.syncCDC {
		var cdcBuf [80]byte
		ctx.Response.Header.SetCanonical(header.S2b(header.CDNCacheControl),
			appendServeCDCDirectives(cdcBuf[:0], obj, now, result))
	}
	ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b(xCache))
	ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
	ctx.SetStatusCode(fasthttp.StatusNotModified)
	if rw != nil {
		rw(&ctx.Response.Header)
	}
}

// hasRangeHeader returns true if the request has a Range header, using
// a zero-allocation Peek.
func hasRangeHeader(ctx *fasthttp.RequestCtx) bool {
	return len(ctx.Request.Header.Peek(header.Range)) > 0
}

// noDefaultDateOffset is the byte offset of the unexported noDefaultDate
// field within fasthttp.ResponseHeader. Computed once at package init
// via reflect. Used by setNoDefaultDate to set the field without needing
// a public setter on fasthttp.
var noDefaultDateOffset = func() uintptr {
	t := reflect.TypeOf(fasthttp.ResponseHeader{})
	f, ok := t.FieldByName("noDefaultDate")
	if !ok {
		return 0 // field not found; setNoDefaultDate will be a no-op
	}
	return f.Offset
}()

// setNoDefaultDate sets the unexported noDefaultDate field on a
// fasthttp.ResponseHeader to true. This prevents fasthttp from
// auto-generating a Date header in AppendBytes, which would overwrite
// the stored origin Date set via SetDateRaw.
func setNoDefaultDate(hdr *fasthttp.ResponseHeader) {
	if noDefaultDateOffset == 0 {
		return
	}
	ptr := (*bool)(unsafe.Pointer(uintptr(unsafe.Pointer(hdr)) + noDefaultDateOffset)) //nolint:gosec // G103: controlled unsafe for fasthttp interop
	*ptr = true
}

// getOrComputeFastHeader lazily builds a *fasthttp.ResponseHeader from
// the stored object's static headers (see skipStaticHeader for the
// exclusions) on the first cache hit, then reuses it on subsequent hits
// via CopyTo. Cached in obj.FastHeader (atomic.Value) for race-safe
// reuse across goroutines; noDefaultDate prevents fasthttp from
// auto-adding a Date.
func getOrComputeFastHeader(obj *api.Object) *fasthttp.ResponseHeader {
	if v := obj.FastHeader.Load(); v != nil {
		return v.(*fasthttp.ResponseHeader)
	}
	hdr := &fasthttp.ResponseHeader{}
	hdr.DisableNormalizing()
	obj.Header.WriteToFastHTTP(hdr)
	// The map check backstops HasDate for objects whose flag was lost in
	// transit (pre-v6 wire decode, ADR-0053): WriteToFastHTTP skips Date
	// by contract, so a false flag with a stored Date would serve a
	// response with no Date header at all.
	if obj.HasDate || obj.Header.Has(header.Date) {
		dateVal := obj.Header.Get(header.Date)
		header.SetDateRaw(hdr, dateVal)
	}
	if obj.HasConnectionList {
		stripConnectionListedHeaders(hdr, obj.Header)
	}
	if obj.HasNoCacheFields {
		stripNoCacheFields(hdr, obj.CacheControl)
	}
	hdr.SetStatusCode(obj.StatusCode)
	setNoDefaultDate(hdr)
	obj.FastHeader.Store(hdr)
	return hdr
}
