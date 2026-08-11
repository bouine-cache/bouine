package cache

import (
	"net/http"
	"time"

	"github.com/bouine-cache/bouine/pkg/header"
)

// cacheable.go contains the cache storage eligibility functions.
// IsCacheable is the main entry point; the rest are helpers.

// isCDNCCCharForbidden reports whether b is a character that is not allowed
// in a CDN-Cache-Control value (non-token chars per RFC 9213 §2 / RFC 7230 §3.2.6).
func isCDNCCCharForbidden(b byte) bool {
	return b == '&' || b == '@' || b == '[' || b == ']' || b == '{' || b == '}' || b == '"'
}

// hasMeaningfulCDNCCDirective reports whether d contains at least one directive
// that can influence caching behaviour. Values with no meaningful directives are
// treated as absent (RFC 9211 §4).
func hasMeaningfulCDNCCDirective(d Directives) bool {
	return d.MaxAgeSet || d.SMaxAgeSet || d.NoStore || d.Private || d.NoCache
}

// cdnCacheControl returns the effective Cache-Control directives for a
// shared cache (CDN tier) per RFC 9211. When CDN-Cache-Control is
// present it takes precedence over Cache-Control for all shared-cache
// decisions; otherwise Cache-Control is used.
// If the CDN-CC value contains unknown or invalid token types (per
// RFC 9211 §4 "must be able to parse the CDN-Cache-Control field as a
// list of tokens"), the header is treated as absent.
func cdnCacheControl(respHeader http.Header) (Directives, bool) {
	v := mergeHeaderValues(respHeader, "CDN-Cache-Control")
	if v == "" {
		return Directives{}, false
	}
	// Reject values containing non-token characters (§9213 §4).
	// A CDN-CC value with garbage tokens must be ignored entirely,
	// falling back to Cache-Control.
	for _, b := range []byte(v) {
		// RFC 7230 §3.2.6 token chars: VCHAR except delimiters.
		// We reject &, invalid bytes and other non-token noise.
		if b < 0x21 || b > 0x7e {
			continue // spaces / commas are legal separators
		}
		if isCDNCCCharForbidden(b) {
			// Non-token characters or quoted-string values — treat whole value as invalid.
			// RFC 9213 §2: CDN-Cache-Control must use sf-integer for duration values, not quoted-strings.
			return Directives{}, false
		}
	}
	d := ParseCacheControl(v)
	// If the parsed directives contain no meaningful directives, treat absent.
	if !hasMeaningfulCDNCCDirective(d) {
		return Directives{}, false
	}
	return d, true
}

// IsCacheable determines whether an origin response should be stored.
// negativeTTL enables negative caching for error statuses (404, 405,
// 410, 501) when > 0.
func IsCacheable(status int, reqHeader, respHeader http.Header, negativeTTL ...time.Duration) bool {
	// CDN-Cache-Control overrides Cache-Control for shared-cache
	// decisions (RFC 9211). Use it when present.
	var respCC Directives
	var hasCDN bool
	if cdnCC, ok := cdnCacheControl(respHeader); ok {
		respCC = cdnCC
		hasCDN = true
	} else {
		respCC = ParseCacheControl(mergeHeaderValues(respHeader, header.CacheControl))
	}

	if isCacheBlocked(status, respCC, hasCDN, reqHeader, respHeader) {
		return false
	}

	// Explicit freshness.
	if respCC.MaxAgeSet || respCC.SMaxAgeSet {
		return true
	}

	// When CDN-Cache-Control is absent, also check the plain CC header
	// for max-age/s-maxage directives (captured above in respCC).
	// Explicit freshness:

	// Valid Expires header: only a syntactically correct date counts as
	// explicit freshness. An invalid Expires must not prevent heuristic
	// caching (RFC 9111 §4.2.1 “do not use invalid Expires in calculations”).
	if exp := respHeader.Get(header.Expires); exp != "" {
		if !parseHTTPDate(exp).IsZero() {
			return true
		}
	}

	// Heuristic freshness: only if the response has Last-Modified AND the
	// status code is heuristically cacheable (RFC 9111 §4.2.2). When
	// Cache-Control: public is present, unknown status codes (e.g. 599) are
	// also eligible — the server is explicitly opting in to caching.
	if respHeader.Get(header.LastModified) != "" && (isHeuristicStatus(status) || respCC.Public) {
		// Pragma: no-cache in a response without explicit Cache-Control
		// blocks heuristic caching (RFC 9111 §5.4 / HTTP/1.0 compat).
		if isBlockedByPragma(respCC, respHeader) {
			return false
		}
		return true
	}

	// Negative caching: cache error responses with a configured TTL.
	if len(negativeTTL) > 0 && negativeTTL[0] > 0 && IsNegativeCacheable(status) {
		return true
	}

	// RFC 9111 §4: A POST response MAY be stored if it has explicit
	// freshness (max-age or Expires). Covered above by the explicit
	// freshness checks; nothing extra needed for the method itself.

	return false
}

// IsCacheableWithDefault extends IsCacheable with the operator-configured
// default-TTL fallback. When the strict RFC 9111 decision is "not
// cacheable" purely because the response carries no freshness information
// (no max-age/s-maxage, no valid Expires, no Last-Modified) and defaultTTL
// is > 0, an otherwise-unblocked response with a heuristically-cacheable
// status becomes eligible — the operator has explicitly opted in by
// configuring ttl_default. This mirrors nginx's proxy_cache_valid: a
// cache lifetime supplied for responses the origin left unspecified.
//
// All RFC 9111 blocking directives are still honoured: no-store, private,
// Pragma: no-cache, Vary: *, Set-Cookie (without explicit freshness), and
// Authorization (without public/must-revalidate/s-maxage) all prevent
// storage regardless of defaultTTL.
func IsCacheableWithDefault(status int, reqHeader, respHeader http.Header, negativeTTL, defaultTTL time.Duration) bool {
	if IsCacheable(status, reqHeader, respHeader, negativeTTL) {
		return true
	}
	if defaultTTL <= 0 {
		return false
	}
	var respCC Directives
	var hasCDN bool
	if cdnCC, ok := cdnCacheControl(respHeader); ok {
		respCC = cdnCC
		hasCDN = true
	} else {
		respCC = ParseCacheControl(mergeHeaderValues(respHeader, header.CacheControl))
	}
	if isCacheBlocked(status, respCC, hasCDN, reqHeader, respHeader) {
		return false
	}
	// Only successful / heuristically-cacheable statuses are eligible for
	// the default-TTL fallback; 5xx and other error statuses are excluded
	// so origin errors are never silently cached by an operator default.
	return isHeuristicStatus(status)
}

// parsedResponse holds pre-parsed cache-control directives so callers can
// avoid re-parsing the same headers up to 6 times per miss (IsCacheable
// parses, isCacheBlocked re-parses for hasCDN, IsCacheableWithDefault
// re-parses again).
type parsedResponse struct {
	status     int
	respCC     Directives
	hasCDN     bool
	reqHeader  http.Header
	respHeader http.Header
}

// newParsedResponse constructs a parsedResponse from the response and
// request headers, parsing Cache-Control/CDN-Cache-Control exactly once.
func newParsedResponse(status int, reqHeader, respHeader http.Header) parsedResponse {
	p := parsedResponse{
		status:     status,
		reqHeader:  reqHeader,
		respHeader: respHeader,
	}
	if cdnCC, hasCDN := cdnCacheControl(respHeader); hasCDN {
		p.respCC = cdnCC
		p.hasCDN = hasCDN
	} else {
		p.respCC = ParseCacheControl(mergeHeaderValues(respHeader, header.CacheControl))
	}
	return p
}

// isCacheable checks cacheability using pre-parsed directives.
// This is the zero-reparse path for callers that have already called
// cdnCacheControl or ParseCacheControl (e.g. buildObject).
func (p *parsedResponse) isCacheable(negativeTTL time.Duration) bool {
	if isCacheBlocked(p.status, p.respCC, p.hasCDN, p.reqHeader, p.respHeader) {
		return false
	}
	if p.respCC.MaxAgeSet || p.respCC.SMaxAgeSet {
		return true
	}
	if exp := p.respHeader.Get(header.Expires); exp != "" {
		if !parseHTTPDate(exp).IsZero() {
			return true
		}
	}
	if p.respHeader.Get(header.LastModified) != "" && (isHeuristicStatus(p.status) || p.respCC.Public) {
		if !p.hasCDN && isBlockedByPragma(p.respCC, p.respHeader) {
			return false
		}
		return true
	}
	if negativeTTL > 0 && IsNegativeCacheable(p.status) {
		return true
	}
	return false
}

// isCacheableWithDefault extends isCacheable with the
// operator-configured default-TTL fallback, using pre-parsed directives.
func (p *parsedResponse) isCacheableWithDefault(negativeTTL, defaultTTL time.Duration) bool {
	if p.isCacheable(negativeTTL) {
		return true
	}
	if defaultTTL <= 0 {
		return false
	}
	if isCacheBlocked(p.status, p.respCC, p.hasCDN, p.reqHeader, p.respHeader) {
		return false
	}
	return isHeuristicStatus(p.status)
}

func isCacheBlocked(status int, respCC Directives, hasCDN bool, reqHeader, respHeader http.Header) bool {
	// RFC 9111 §5.2.2.3: must-understand means the cache MAY store even when
	// no-store is present, but ONLY if the cache understands the status code.
	// Unknown status codes (e.g. 599) do NOT satisfy must-understand.
	if respCC.NoStore {
		if !respCC.MustUnderstand || !isUnderstoodStatus(status) {
			return true
		}
	}
	if respCC.Private {
		return true
	}
	// Only check Pragma in the response when using the plain CC path;
	// CDN-CC completely replaces the CC semantics so Pragma doesn't apply.
	if !hasCDN {
		if isBlockedByPragma(respCC, respHeader) {
			return true
		}
	}
	// RFC 9111 §4.1: a stored response with Vary:* "always fails to
	// match." RFC 9111 permits storing such responses but forbids
	// serving without revalidation; bouine refuses to store them at
	// all. This is the sole gate — VariantKey/variantKeyFromRaw return
	// primary for Vary:* (a no-op), relying on this gate to prevent
	// storage.
	if hasVaryStar(respHeader) {
		return true
	}
	if isBlockedBySetCookie(respCC, respHeader) {
		return true
	}
	if reqHeader.Get(header.Authorization) != "" {
		if !respCC.Public && !respCC.MustRevalidate && !respCC.SMaxAgeSet {
			return true
		}
	}
	return false
}

func isBlockedByPragma(respCC Directives, h http.Header) bool {
	// RFC 9111 §5.4: Pragma: no-cache in a response blocks caching only when
	// there is no explicit freshness information. If Last-Modified or Expires
	// is present, heuristic or explicit freshness overrides Pragma.
	return h.Get(header.Pragma) == "no-cache" &&
		!respCC.MaxAgeSet && !respCC.SMaxAgeSet &&
		h.Get(header.Expires) == "" &&
		h.Get(header.LastModified) == ""
}

func hasVaryStar(h http.Header) bool {
	for _, v := range h.Values(header.Vary) {
		if varyContainsStar(v) {
			return true
		}
	}
	return false
}

func isBlockedBySetCookie(respCC Directives, h http.Header) bool {
	if h.Get(header.SetCookie) == "" {
		return false
	}
	// A shared cache MAY store responses with Set-Cookie if the
	// response has explicit freshness (max-age or s-maxage).
	return !respCC.MaxAgeSet && !respCC.SMaxAgeSet
}

// isUnderstoodStatus reports whether this cache understands the given
// status code well enough to satisfy RFC 9111 §5.2.2.3 must-understand.
// Only explicitly enumerated status codes count as "understood".
func isUnderstoodStatus(status int) bool {
	switch status {
	case 200, 203, 204, 206,
		300, 301, 302, 303, 304, 307, 308,
		400, 401, 403, 404, 405, 406, 408, 409, 410, 411, 412, 413, 414, 415, 416,
		500, 501, 502, 503, 504:
		return true
	}
	return false
}

// isHeuristicStatus reports whether the status code permits heuristic
// freshness per RFC 9110 §15. The list is intentionally conservative;
// 5xx codes are excluded (origin errors should not be silently cached).
func isHeuristicStatus(status int) bool {
	switch status {
	case 200, 203, 204, 206,
		300, 301, 308,
		404, 405, 410, 414, 501:
		return true
	}
	return false
}
