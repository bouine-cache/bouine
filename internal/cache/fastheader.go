package cache

import (
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

// evaluateFast runs the RFC 9111 state machine using direct Peek calls
// on the request headers, avoiding headerFromCtx allocation.
func evaluateFast(ctx *fasthttp.RequestCtx, obj *api.Object, now time.Time) Disposition {
	method := ctx.Method()
	if string(method) != "GET" && string(method) != "HEAD" {
		return Disposition{Decision: Bypass}
	}

	var reqCC Directives
	if rawCC := ctx.Request.Header.Peek(header.CacheControl); len(rawCC) > 0 {
		reqCC = ParseCacheControl(string(rawCC))
	}

	if !reqCC.NoCache && string(ctx.Request.Header.Peek(header.Pragma)) == "no-cache" {
		reqCC.NoCache = true
	}

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	ccStr := obj.CacheControl
	if ccStr == "" {
		ccStr = obj.Header.Get(header.CacheControl)
	}
	respCC := ParseCacheControl(ccStr)

	if d, ok := evalNoCache(reqCC, respCC, obj); ok {
		return d
	}
	if freshWithRequestCC(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}
	return evalStale(reqCC, respCC, obj, now)
}

// tryConditional304Fast checks if the client's conditional headers match
// the cached object, using direct Peek calls. Returns true if a 304
// response was sent.
func tryConditional304Fast(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source) bool {
	inm := ctx.Request.Header.Peek(header.IfNoneMatch)
	if len(inm) > 0 {
		if obj.ETag != "" && etagMatch(string(inm), obj.ETag) {
			if obj.ETag != "" {
				ctx.Response.Header.Set(header.ETag, obj.ETag)
			}
			ctx.Response.Header.Set(header.XCache, "HIT")
			ctx.Response.Header.Set(header.XCacheSource, string(src))
			ctx.SetStatusCode(fasthttp.StatusNotModified)
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
			if obj.ETag != "" {
				ctx.Response.Header.Set(header.ETag, obj.ETag)
			}
			ctx.Response.Header.Set(header.XCache, "HIT")
			ctx.Response.Header.Set(header.XCacheSource, string(src))
			ctx.SetStatusCode(fasthttp.StatusNotModified)
			return true
		}
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get(header.Date); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					if obj.ETag != "" {
						ctx.Response.Header.Set(header.ETag, obj.ETag)
					}
					ctx.Response.Header.Set(header.XCache, "HIT")
					ctx.Response.Header.Set(header.XCacheSource, string(src))
					ctx.SetStatusCode(fasthttp.StatusNotModified)
					return true
				}
			}
		}
	}
	return false
}

// hasRangeHeader returns true if the request has a Range header, using
// a zero-allocation Peek.
func hasRangeHeader(ctx *fasthttp.RequestCtx) bool {
	return len(ctx.Request.Header.Peek(header.Range)) > 0
}
