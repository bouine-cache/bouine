package cache

import (
	"net/http"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// httpdate.go provides HTTP date parsing, age computation, heuristic
// TTL calculation, and header-value normalization utilities.

// HeuristicTTL computes a heuristic freshness lifetime from
// Last-Modified per RFC 9111 §4.2.2. The heuristic is 10% of
// the interval between the Date header (or now as fallback) and
// Last-Modified. Returns 0 if not applicable.
func HeuristicTTL(h http.Header, now time.Time) time.Duration {
	lm := h.Get(header.LastModified)
	if lm == "" {
		return 0
	}
	lmTime := parseHTTPDate(lm)
	if lmTime.IsZero() {
		return 0
	}
	// RFC 9111 §4.2.2: use the Date header as the reference time so the
	// freshness lifetime is stable across proxy hops and does not grow
	// when the response is served from a cache that has held it for a while.
	refTime := now
	if dateStr := h.Get(header.Date); dateStr != "" {
		if dt := parseHTTPDate(dateStr); !dt.IsZero() {
			refTime = dt
		}
	}
	age := refTime.Sub(lmTime)
	if age <= 0 {
		return 0
	}
	return age / 10
}

// ComputeAge calculates the Age header value per RFC 9111 §4.2.3.
func ComputeAge(obj *api.Object, now time.Time) time.Duration {
	age := now.Sub(obj.StoredAt)
	// Also account for any Age the object had at the origin. Prefer the
	// pre-stored OriginAge field (set once at cache-fill) over re-parsing the
	// header on every hit; effectiveOriginAge falls back to the header for
	// warm-tier / legacy objects whose field is zero.
	age += effectiveOriginAge(obj)
	return age
}

// parseOriginAge parses the Age header from the response, handling
// malformed values per RFC 9110 §5.6.1. Invalid, negative, or
// non-integer values (e.g. floats like "7200.0") return 0.
// Values > 2^31 are treated as stale (RFC 9111 §5.1).
func parseOriginAge[H headerGetter](h H) time.Duration {
	ageStr := strings.TrimSpace(h.Get(header.Age))
	if ageStr == "" {
		return 0
	}
	// RFC 9111 §5.1: Age = delta-seconds = 1*DIGIT. Reject float values
	// (e.g. "7200.0") because a decimal point makes the value non-conformant.
	// Other non-digit suffixes (e.g. "7200;foo", "7200, 0") are tolerated
	// via the normal parseIntNoAlloc stop-at-non-digit behaviour.
	for i := 0; i < len(ageStr); i++ {
		if ageStr[i] == '.' {
			return 0
		}
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
//
// This is for http.Header only — header.Map joins multi-values at store
// time, so callers with a header.Map should use Get directly.
func mergeHeaderValues(h http.Header, name string) string {
	vals := h.Values(name)
	if len(vals) <= 1 {
		return h.Get(name)
	}
	return strings.Join(vals, ", ")
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
