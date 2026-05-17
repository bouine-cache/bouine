package cache

import (
	"net/http"
	"strings"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// Decision is the outcome of the cache state machine.
type Decision int

const (
	// Hit means serve from cache, no origin contact.
	Hit Decision = iota
	// Miss means not in cache; fetch from origin.
	Miss
	// Revalidate means stale; conditional fetch possible.
	Revalidate
	// StaleHit means serve stale (SWR/SIE window).
	StaleHit
	// Bypass means directives forbid caching.
	Bypass
)

// Disposition describes what the caller should do after the Decision.
type Disposition struct {
	Decision Decision
	Object   *api.Object
}

// Evaluate runs the RFC 9111 state machine.
func Evaluate(r *http.Request, obj *api.Object, now time.Time) Disposition {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return Disposition{Decision: Bypass}
	}

	reqCC := ParseCacheControl(mergeHeaderValues(r.Header, "Cache-Control"))

	// Pragma: no-cache is equivalent to Cache-Control: no-cache
	// for HTTP/1.0 compatibility (RFC 9111 §5.4).
	if !reqCC.NoCache && r.Header.Get("Pragma") == "no-cache" {
		reqCC.NoCache = true
	}

	if reqCC.NoStore {
		return Disposition{Decision: Bypass}
	}
	if obj == nil {
		return evalMiss(reqCC)
	}

	respCC := ParseCacheControl(mergeHeaderValues(obj.Header, "Cache-Control"))

	if d, ok := evalNoCache(reqCC, respCC, obj); ok {
		return d
	}
	if isFresh(obj, reqCC, now) {
		return Disposition{Decision: Hit, Object: obj}
	}
	return evalStale(reqCC, respCC, obj, now)
}

func evalMiss(reqCC Directives) Disposition {
	if reqCC.OnlyIfCached {
		// RFC 9111 §5.2.1.7: return 504 Gateway Timeout.
		return Disposition{Decision: Bypass}
	}
	return Disposition{Decision: Miss}
}

// ClientConditionalMatch checks if a cached object satisfies the
// client's conditional headers (If-None-Match / If-Modified-Since).
// If it matches, the handler should return 304 instead of 200.
func ClientConditionalMatch(r *http.Request, obj *api.Object) bool {
	// If-None-Match takes precedence (RFC 9110 §13.1.2).
	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if obj.ETag != "" && etagMatch(inm, obj.ETag) {
			return true
		}
		return false
	}
	// If-Modified-Since (RFC 9110 §13.1.3).
	if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		imsTime := parseHTTPDate(ims)
		if imsTime.IsZero() {
			return false
		}
		if !obj.LastModified.IsZero() && !obj.LastModified.After(imsTime) {
			return true
		}
		// Fall back to Date header then StoredAt if no Last-Modified.
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get("Date"); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					return true
				}
			}
		}
	}
	return false
}

// etagMatch checks if needle matches any ETag in the comma-separated
// list (which may contain "*" or quoted tags). Weak comparison used
// per RFC 9110 §8.8.3.2.
func etagMatch(list, needle string) bool {
	if list == "*" {
		return true
	}
	// Normalize: strip W/ prefix for weak comparison.
	norm := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) >= 2 && (s[0] == 'W' || s[0] == 'w') && s[1] == '/' {
			s = s[2:]
		}
		return strings.Trim(s, "\"")
	}
	needleNorm := norm(needle)
	for _, tag := range strings.Split(list, ",") {
		if norm(tag) == needleNorm {
			return true
		}
	}
	return false
}

func evalNoCache(reqCC, respCC Directives, obj *api.Object) (Disposition, bool) {
	if respCC.NoCache || reqCC.NoCache {
		if obj.ETag != "" || !obj.LastModified.IsZero() {
			return Disposition{Decision: Revalidate, Object: obj}, true
		}
		return Disposition{Decision: Miss}, true
	}
	return Disposition{}, false
}

func isFresh(obj *api.Object, reqCC Directives, now time.Time) bool {
	// Compute the response age including any origin Age header.
	age := now.Sub(obj.StoredAt) + parseOriginAge(obj.Header)
	fresh := age < obj.TTL

	if reqCC.MaxAgeSet && age > reqCC.MaxAge {
		fresh = false
	}
	if reqCC.MinFreshSet && (obj.TTL-age) < reqCC.MinFresh {
		fresh = false
	}
	return fresh
}

func evalStale(reqCC, respCC Directives, obj *api.Object, now time.Time) Disposition {
	if respCC.MustRevalidate || respCC.ProxyRevalidate {
		return revalidateOrMiss(obj)
	}
	if reqCC.MaxStaleSet {
		age := now.Sub(obj.StoredAt) + parseOriginAge(obj.Header)
		staleAge := age - obj.TTL
		if staleAge <= reqCC.MaxStale {
			return Disposition{Decision: StaleHit, Object: obj}
		}
	}
	if obj.StaleButServable(now) {
		return Disposition{Decision: StaleHit, Object: obj}
	}
	return revalidateOrMiss(obj)
}

func revalidateOrMiss(obj *api.Object) Disposition {
	if obj.ETag != "" || !obj.LastModified.IsZero() {
		return Disposition{Decision: Revalidate, Object: obj}
	}
	return Disposition{Decision: Miss}
}

// IsCacheable determines whether an origin response should be stored.
func IsCacheable(status int, reqHeader, respHeader http.Header) bool {
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

// HeuristicTTL computes a heuristic freshness lifetime from
// Last-Modified per RFC 9111 §4.2.2. The heuristic is 10% of
// the time since Last-Modified. Returns 0 if not applicable.
func HeuristicTTL(header http.Header, now time.Time) time.Duration {
	lm := header.Get("Last-Modified")
	if lm == "" {
		return 0
	}
	lmTime := parseHTTPDate(lm)
	if lmTime.IsZero() {
		return 0
	}
	age := now.Sub(lmTime)
	if age <= 0 {
		return 0
	}
	return age / 10
}

// ComputeAge calculates the Age header value per RFC 9111 §4.2.3.
func ComputeAge(obj *api.Object, now time.Time) time.Duration {
	age := now.Sub(obj.StoredAt)
	// Also account for any Age header from the origin (upstream cache).
	age += parseOriginAge(obj.Header)
	return age
}

// parseOriginAge parses the Age header from the response, handling
// malformed values per RFC 9110 §5.6.1. Invalid or negative values
// return 0. Values > 2^31 are treated as stale (RFC 9111 §5.1).
func parseOriginAge(header http.Header) time.Duration {
	ageStr := header.Get("Age")
	if ageStr == "" {
		return 0
	}
	secs, ok := parseIntNoAlloc(ageStr)
	if !ok || secs < 0 {
		return 0
	}
	// RFC 9111 §5.1: if Age > 2^31, treat as stale (very large).
	if secs > 2147483648 {
		return time.Duration(2147483648) * time.Second
	}
	return time.Duration(secs) * time.Second
}

// mergeHeaderValues joins all values of a header name into a single
// comma-separated string. HTTP allows multiple headers with the same
// name; Cache-Control especially may appear as multiple lines.
func mergeHeaderValues(header http.Header, name string) string { //nolint:unparam // intentionally general
	vals := header.Values(name)
	if len(vals) <= 1 {
		return header.Get(name)
	}
	return strings.Join(vals, ", ")
}

// MergeHeaders304 merges headers from a 304 response into the stored
// object per RFC 9111 §3.2. The 304 response's headers update the
// stored response, except for content-specific headers.
func MergeHeaders304(stored *api.Object, resp304Header http.Header) {
	// Headers that MUST NOT be updated from a 304 (content-specific).
	skip := map[string]bool{
		"Content-Length":    true,
		"Content-Encoding":  true,
		"Transfer-Encoding": true,
	}
	for k, vals := range resp304Header {
		if skip[k] {
			continue
		}
		stored.Header[k] = vals
	}
}

// ConditionalHeaders sets If-None-Match and If-Modified-Since on a
// revalidation request from the stored object's validators.
func ConditionalHeaders(req *http.Request, obj *api.Object) {
	if obj.ETag != "" {
		// Ensure the ETag is properly quoted (RFC 9110 §8.8.3).
		etag := quoteETag(obj.ETag)
		req.Header.Set("If-None-Match", etag)
	}
	if !obj.LastModified.IsZero() {
		req.Header.Set("If-Modified-Since",
			obj.LastModified.UTC().Format(http.TimeFormat))
	}
}

// quoteETag ensures an ETag value is properly quoted. Unquoted ETags
// like "abcdef" become "\"abcdef\"". Weak ETags like W/"abcdef" are
// left as-is. Already-quoted ETags are returned unchanged.
func quoteETag(etag string) string {
	if etag == "" {
		return etag
	}
	// Already quoted (starts with " or W/).
	if etag[0] == '"' || (len(etag) >= 2 && (etag[0] == 'W' || etag[0] == 'w') && etag[1] == '/') {
		return etag
	}
	return "\"" + etag + "\""
}

// parseHTTPDate tries multiple date formats used in HTTP headers
// (RFC 1123, RFC 850, ANSI C asctime). Also handles case-insensitive
// timezone (e.g., "gmt" → "GMT"). Returns zero time on failure.
// Rejects dates with non-standard time fields (e.g., 1-digit hour)
// or multiple consecutive spaces (malformed).
func parseHTTPDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	if !validHTTPTimeField(s) {
		return time.Time{}
	}
	// Reject RFC 1123/850-style dates with multiple consecutive spaces
	// (malformed). ANSI C asctime has a legitimate "  " before
	// single-digit days so only reject when a comma is present (RFC
	// 1123/850 indicator).
	if strings.Contains(s, ",") && strings.Contains(s, "  ") {
		return time.Time{}
	}
	s = normalizeTZ(s)

	formats := []string{
		http.TimeFormat,                 // RFC 1123
		time.RFC850,                     // "Monday, 02-Jan-06 15:04:05 MST"
		"Mon Jan  2 15:04:05 2006",      // ANSI C asctime
		"Mon, 2 Jan 2006 15:04:05 GMT",  // single-digit day
		"Mon,  2 Jan 2006 15:04:05 GMT", // double-space before day
	}
	for _, fmt := range formats {
		if t, err := time.Parse(fmt, s); err == nil {
			return t
		}
	}
	return time.Time{}
}

// validHTTPTimeField checks that the time portion (HH:MM:SS) in an
// HTTP date has 2-digit fields. Rejects "0:00:00" but accepts
// "00:00:00".
func validHTTPTimeField(s string) bool {
	// Find the time portion by looking for the pattern NN:NN:NN.
	// In RFC 1123 format, the time is after the year and a space.
	idx := strings.Index(s, ":")
	if idx < 2 {
		return true // no colon or too early — let time.Parse decide
	}
	// Check that the char before the first colon is a digit (2-digit hour).
	if s[idx-2] < '0' || s[idx-2] > '9' {
		return false // 1-digit hour: the char 2 positions before ':' is not a digit
	}
	return true
}

// normalizeTZ uppercases common timezone abbreviations at the end of
// a date string so "gmt" and "Gmt" become "GMT".
func normalizeTZ(s string) string {
	if len(s) >= 3 {
		tail := s[len(s)-3:]
		if strings.EqualFold(tail, "gmt") {
			s = s[:len(s)-3] + "GMT"
		}
	}
	return s
}
