package cache

import (
	"net/http"
	"time"
)

// cacheable.go contains the cache storage eligibility functions.
// IsCacheable is the main entry point; the rest are helpers.

// IsCacheable determines whether an origin response should be stored.
// negativeTTL enables negative caching for error statuses (404, 405,
// 410, 501) when > 0.
func IsCacheable(status int, reqHeader, respHeader http.Header, negativeTTL ...time.Duration) bool {
	respCC := ParseCacheControl(mergeHeaderValues(respHeader, "Cache-Control"))

	if isCacheBlocked(respCC, reqHeader, respHeader) {
		return false
	}

	// Explicit freshness.
	if respCC.MaxAgeSet || respCC.SMaxAgeSet {
		return true
	}
	if respHeader.Get("Expires") != "" {
		return true
	}

	// Heuristic freshness: only if the response has Last-Modified
	// AND the status code is heuristically cacheable (RFC 9111 §4.2.2).
	if respHeader.Get("Last-Modified") != "" && isHeuristicStatus(status) {
		return true
	}

	// Negative caching: cache error responses with a configured TTL.
	if len(negativeTTL) > 0 && negativeTTL[0] > 0 && IsNegativeCacheable(status) {
		return true
	}

	return false
}

func isCacheBlocked(respCC Directives, reqHeader, respHeader http.Header) bool {
	if respCC.NoStore || respCC.Private {
		return true
	}
	if isBlockedByPragma(respCC, respHeader) {
		return true
	}
	if hasVaryStar(respHeader) {
		return true
	}
	if isBlockedBySetCookie(respCC, respHeader) {
		return true
	}
	if reqHeader.Get("Authorization") != "" {
		if !respCC.Public && !respCC.MustRevalidate && !respCC.SMaxAgeSet {
			return true
		}
	}
	return false
}

func isBlockedByPragma(respCC Directives, h http.Header) bool {
	return h.Get("Pragma") == "no-cache" &&
		!respCC.MaxAgeSet && !respCC.SMaxAgeSet &&
		h.Get("Expires") == ""
}

func hasVaryStar(h http.Header) bool {
	for _, v := range h.Values("Vary") {
		if varyContainsStar(v) {
			return true
		}
	}
	return false
}

func isBlockedBySetCookie(respCC Directives, h http.Header) bool {
	if h.Get("Set-Cookie") == "" {
		return false
	}
	// A shared cache MAY store responses with Set-Cookie if the
	// response has explicit freshness (max-age or s-maxage).
	return !respCC.MaxAgeSet && !respCC.SMaxAgeSet
}

func isHeuristicStatus(status int) bool {
	switch status {
	case 200, 203, 204, 206, 300, 301, 308, 404, 405, 410, 414, 501:
		return true
	}
	return false
}
