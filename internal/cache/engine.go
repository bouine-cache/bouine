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

	reqCC := ParseCacheControl(r.Header.Get("Cache-Control"))

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

	respCC := ParseCacheControl(obj.Header.Get("Cache-Control"))

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
		// Fall back to StoredAt if no Last-Modified.
		if obj.LastModified.IsZero() && !obj.StoredAt.After(imsTime) {
			return true
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
	age := now.Sub(obj.StoredAt)
	fresh := obj.Fresh(now)

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
		staleAge := now.Sub(obj.StoredAt) - obj.TTL
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
	respCC := ParseCacheControl(respHeader.Get("Cache-Control"))

	if respCC.NoStore {
		return false
	}
	if respCC.Private {
		return false
	}
	// Vary: * means every request is unique — never store.
	// Check for * anywhere in the Vary field list, not just exact match.
	if varyContainsStar(respHeader.Get("Vary")) {
		return false
	}
	if respHeader.Get("Set-Cookie") != "" {
		return false
	}
	if reqHeader.Get("Authorization") != "" {
		if !respCC.Public && !respCC.MustRevalidate && !respCC.SMaxAgeSet {
			return false
		}
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

func isHeuristicStatus(status int) bool {
	switch status {
	case 200, 203, 204, 206, 300, 301, 308, 404, 405, 410, 414, 501:
		return true
	}
	return false
}

// HeuristicTTL computes a heuristic freshness lifetime from
// Last-Modified per RFC 9111 §4.2.2. Returns 0 if not applicable.
// The standard heuristic is 10% of the age of the response since
// Last-Modified.
func HeuristicTTL(header http.Header, now time.Time) time.Duration {
	lm := header.Get("Last-Modified")
	if lm == "" {
		return 0
	}
	lmTime, err := time.Parse(http.TimeFormat, lm)
	if err != nil {
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
	if existingAge := obj.Header.Get("Age"); existingAge != "" {
		if secs, ok := parseIntNoAlloc(existingAge); ok {
			age += time.Duration(secs) * time.Second
		}
	}
	return age
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
		req.Header.Set("If-None-Match", obj.ETag)
	}
	if !obj.LastModified.IsZero() {
		req.Header.Set("If-Modified-Since",
			obj.LastModified.UTC().Format(http.TimeFormat))
	}
}

// parseHTTPDate tries multiple date formats used in HTTP headers
// (RFC 1123, RFC 850, ANSI C asctime). Also handles case-insensitive
// timezone (e.g., "gmt" → "GMT"). Returns zero time on failure.
func parseHTTPDate(s string) time.Time {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}
	}
	// Normalize common timezone case variants.
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
