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

// evaluateFast runs the RFC 9111 state machine using direct Peek calls
// on the request headers, avoiding headerFromCtx allocation.
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

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	// Use pre-computed response CC flags to avoid ParseCacheControl on every hit.
	if obj.RespNoCache || reqCC.NoCache {
		if obj.ETag != "" || !obj.LastModified.IsZero() {
			return Disposition{Decision: Revalidate, Object: obj}
		}
		return Disposition{Decision: Miss}
	}
	if freshWithRequestCC(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}
	if obj.RespMustRevalidate {
		return revalidateOrMiss(obj)
	}
	// Stale checks (RFC 9111 §4.2).
	return staleDisposition(obj, reqCC, now)
}

// staleDisposition evaluates stale-path directives (SWR, SIE, max-stale,
// heuristic freshness) and returns the resulting Disposition. Extracted
// from evaluateFast to keep cyclomatic complexity under the gocyclo limit.
func staleDisposition(obj *api.Object, reqCC Directives, now time.Time) Disposition {
	originAge := effectiveOriginAge(obj)
	if reqCC.MaxStaleSet {
		age := now.Sub(obj.StoredAt) + originAge
		staleAge := age - (obj.TTL + originAge)
		if staleAge <= reqCC.MaxStale {
			return Disposition{Decision: StaleHit, Object: obj}
		}
	}
	if obj.StaleForSWR(now) {
		return Disposition{Decision: StaleHit, Object: obj}
	}
	if obj.StaleForSIE(now) {
		return revalidateOrMiss(obj)
	}
	// Heuristic freshness (RFC 9111 §4.2.2): only applicable when the
	// response had no explicit freshness directives. Parse response CC
	// lazily here — this is a rare edge case on the stale path.
	ccStr := obj.CacheControl
	if ccStr == "" {
		ccStr = obj.Header.Get(header.CacheControl)
	}
	respCC := ParseCacheControl(ccStr)
	if !respCC.MaxAgeSet && !respCC.SMaxAgeSet && obj.Header.Get(header.Expires) == "" {
		return Disposition{Decision: StaleHit, Object: obj}
	}
	return revalidateOrMiss(obj)
}

// tryConditional304Fast checks if the client's conditional headers match
// the cached object, using direct Peek calls. Returns true if a 304
// response was sent.
func tryConditional304Fast(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source) bool {
	inm := ctx.Request.Header.Peek(header.IfNoneMatch)
	if len(inm) > 0 {
		if obj.ETag != "" && etagMatch(string(inm), obj.ETag) {
			write304Fast(ctx, obj, src)
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
			write304Fast(ctx, obj, src)
			return true
		}
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get(header.Date); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					write304Fast(ctx, obj, src)
					return true
				}
			}
		}
	}
	return false
}

// write304Fast sets the headers and status code for a 304 Not Modified
// response using SetCanonical to skip key normalization.
func write304Fast(ctx *fasthttp.RequestCtx, obj *api.Object, src api.Source) {
	if obj.ETag != "" {
		ctx.Response.Header.SetCanonical(header.S2b(header.ETag), header.S2b(obj.ETag))
	}
	ctx.Response.Header.SetCanonical(header.S2b(header.XCache), header.S2b("HIT"))
	ctx.Response.Header.SetCanonical(header.S2b(header.XCacheSource), header.S2b(string(src)))
	ctx.SetStatusCode(fasthttp.StatusNotModified)
}

// hasRangeHeader returns true if the request has a Range header, using
// a zero-allocation Peek.
func hasRangeHeader(ctx *fasthttp.RequestCtx) bool {
	return len(ctx.Request.Header.Peek(header.Range)) > 0
}
