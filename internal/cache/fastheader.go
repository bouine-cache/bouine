package cache

import (
	"bytes"
	"time"

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

// tryConditional304Fast checks if the client's conditional headers match
// the cached object, using direct Peek calls. Returns true if a 304
// response was sent. rw applies the route's response header rewrite
// directives to the 304; nil for routes without directives.
func tryConditional304Fast(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source, rw func(*fasthttp.ResponseHeader)) bool {
	inm := ctx.Request.Header.Peek(header.IfNoneMatch)
	if len(inm) > 0 {
		if obj.ETag != "" && etagMatch(string(inm), obj.ETag) {
			write304Fast(ctx, obj, src, rw)
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
			write304Fast(ctx, obj, src, rw)
			return true
		}
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get(header.Date); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					write304Fast(ctx, obj, src, rw)
					return true
				}
			}
		}
	}
	return false
}

// write304Fast sets the headers and status code for a 304 Not Modified
// response using SetCanonical to skip key normalization.
func write304Fast(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source, rw func(*fasthttp.ResponseHeader)) {
	if obj.ETag != "" {
		ctx.Response.Header.SetCanonical(header.S2b(header.ETag), header.S2b(obj.ETag))
	}
	ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
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
