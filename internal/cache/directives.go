package cache

import (
	"time"

	"github.com/bouine-cache/bouine/pkg/header"
)

// The RFC 9111 §5.2 Cache-Control tokenizer moved to pkg/header
// (header.ParseCacheControl) so internal/storage can re-derive an
// Object's transient gate flags at decode time without importing this
// package — L2 may not import L3 (see ADR-0053). The aliases below keep
// this package's public API unchanged.

// Directives holds the parsed Cache-Control directives from either a
// request or a response. Zero values mean the directive was absent.
type Directives = header.Directives

// ParseCacheControl parses a Cache-Control header value into
// Directives. Zero-alloc: scans the header bytes in place without
// allocating slices or substrings.
func ParseCacheControl(headerValue string) Directives {
	return header.ParseCacheControl(headerValue)
}

// ParseCacheControlBytes parses a Cache-Control header value from a
// []byte without converting to string first. Used on the bypass path
// where the header value comes from fasthttp's Peek (zero-copy []byte).
func ParseCacheControlBytes(cc []byte) Directives {
	return header.ParseCacheControlBytes(cc)
}

// parseIntNoAlloc parses a non-negative decimal integer without
// allocating. Implementation shared with the tokenizer in pkg/header;
// used here by parseOriginAge and other header-value parsing sites.
var parseIntNoAlloc = header.ParseIntNoAlloc

// FreshnessLifetime computes the freshness lifetime of a response
// per RFC 9111 §4.2.1. When CDN-Cache-Control is present it takes
// precedence over Cache-Control for shared-cache TTL decisions
// (RFC 9213).
func FreshnessLifetime(respCC Directives, getHdr func(string) string) (time.Duration, bool) {
	// CDN-Cache-Control takes precedence when present.
	if cdnCC := getHdr(header.CDNCacheControl); cdnCC != "" {
		cdnD := ParseCacheControl(cdnCC)
		if cdnD.MaxAgeSet {
			return cdnD.MaxAge, true
		}
		if cdnD.NoStore || cdnD.Private {
			return 0, true // blocked by CDN directive
		}
		// CDN-CC present but no TTL directive — treat as expired.
		return 0, true
	}
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge, true
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge, true
	}
	if exp := getHdr(header.Expires); exp != "" {
		expTime := parseHTTPDate(exp)
		if expTime.IsZero() {
			// Invalid Expires → treat as no freshness information,
			// not as explicitly expired, so heuristic can still apply.
			return 0, false
		}
		dateStr := getHdr(header.Date)
		dateTime := parseHTTPDate(dateStr)
		if dateTime.IsZero() {
			return 0, false
		}
		return expTime.Sub(dateTime), true
	}
	return 0, false
}

// FreshnessLifetimeH is like FreshnessLifetime but takes header.Map
// directly so it can detect multiple Expires headers (which are
// invalid per RFC 9110 §5.3) and read CDN-Cache-Control.
func FreshnessLifetimeH(respCC Directives, h header.Map) (time.Duration, bool) {
	// CDN-Cache-Control takes precedence when present (RFC 9213).
	if cdnCC := h.GetAll(header.CDNCacheControl); cdnCC != "" {
		cdnD := ParseCacheControl(cdnCC)
		if cdnD.MaxAgeSet {
			return cdnD.MaxAge, true
		}
		if cdnD.NoStore || cdnD.Private {
			return 0, true
		}
		return 0, true
	}
	if respCC.SMaxAgeSet {
		return respCC.SMaxAge, true
	}
	if respCC.MaxAgeSet {
		return respCC.MaxAge, true
	}
	expiresVal := h.Get(header.Expires)
	if expiresVal == "" {
		return 0, false
	}
	expTime := parseHTTPDate(expiresVal)
	if expTime.IsZero() {
		// Syntactically invalid Expires → treat as no freshness info
		// (RFC 9111 §4.2.1: don't use invalid Expires in calculations).
		return 0, false
	}
	dateStr := h.Get(header.Date)
	dateTime := parseHTTPDate(dateStr)
	if dateTime.IsZero() {
		// RFC 9111 §4.2.1: if Date is absent or invalid, use the current
		// time as a proxy for the response date so Expires-based freshness
		// can still be computed.
		dateTime = time.Now()
	}
	return expTime.Sub(dateTime), true
}
