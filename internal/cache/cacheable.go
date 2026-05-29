package cache

import (
	"net/http"
	"time"
)

// cacheable.go contains the cache storage eligibility functions.
// IsCacheable is the main entry point; the rest are helpers.

// cdnCacheControl returns the effective Cache-Control directives for a
// shared cache (CDN tier) per RFC 9211. When CDN-Cache-Control is
// present it takes precedence over Cache-Control for all shared-cache
// decisions; otherwise Cache-Control is used.
func cdnCacheControl(respHeader http.Header) (Directives, bool) {
	if v := mergeHeaderValues(respHeader, "CDN-Cache-Control"); v != "" {
		return ParseCacheControl(v), true
	}
	return Directives{}, false
}

// IsCacheable determines whether an origin response should be stored.
// negativeTTL enables negative caching for error statuses (404, 405,
// 410, 501) when > 0.
func IsCacheable(status int, reqHeader, respHeader http.Header, negativeTTL ...time.Duration) bool {
	// CDN-Cache-Control overrides Cache-Control for shared-cache
	// decisions (RFC 9211). Use it when present.
	var respCC Directives
	if cdnCC, hasCDN := cdnCacheControl(respHeader); hasCDN {
		respCC = cdnCC
	} else {
		respCC = ParseCacheControl(mergeHeaderValues(respHeader, "Cache-Control"))
	}

	if isCacheBlocked(respCC, reqHeader, respHeader) {
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
	if exp := respHeader.Get("Expires"); exp != "" {
		if !parseHTTPDate(exp).IsZero() {
			return true
		}
	}

	// Heuristic freshness: only if the response has Last-Modified AND the
	// status code is heuristically cacheable (RFC 9111 §4.2.2).
	if respHeader.Get("Last-Modified") != "" && isHeuristicStatus(status) {
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

func isCacheBlocked(respCC Directives, reqHeader, respHeader http.Header) bool {
	if respCC.NoStore || respCC.Private {
		return true
	}
	// Only check Pragma in the response when using the plain CC path;
	// CDN-CC completely replaces the CC semantics so Pragma doesn't apply.
	if _, hasCDN := cdnCacheControl(respHeader); !hasCDN {
		if isBlockedByPragma(respCC, respHeader) {
			return true
		}
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
