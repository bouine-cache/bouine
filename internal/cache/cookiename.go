package cache

import (
	"strings"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// cookieNameScanner matches cookie names in a Cookie header value
// without allocating. It is the shared engine behind
// cache.bypass_on_cookie_names (issue #768) on both the slow path
// (fasthttp Peek) and the H1 fast path (RawRequest headers).
//
// The Cookie header grammar is RFC 6265 §4.2: a semicolon-separated list
// of "name=value" pairs (a value-less "name" is also legal). Cookie
// names are case-insensitive (RFC 6265 §4.1.1 cookie-name is a token;
// §5.3 case-sensitivity is defined over the case-insensitive form).
// A name matches when a cookie-pair's name token equals a listed name
// case-insensitively — never as a substring and never inside a value.
//
// All matching state is precomputed at handler build time (normalized
// names, length buckets) so the per-request scan is byte comparisons
// only. The list is sorted by length so the scan can stop early: a
// cookie name longer than every listed name cannot match.
type cookieNameScanner struct {
	// names holds the normalized (trimmed, lowercased) cookie names,
	// sorted longest-first so hasCookieName can cut the scan short.
	names []string
	// maxLen is the length of the longest listed name; a cookie name
	// longer than this cannot match any entry.
	maxLen int
	// minLen is the length of the shortest listed name; a cookie name
	// shorter than this cannot match any entry.
	minLen int
}

// newCookieNameScanner validates and normalizes the bypass list.
// Names are trimmed and lowercased once; entries are validated
// case-insensitively for uniqueness by the config layer
// (validateCookieNames), the scanner itself tolerates duplicates
// (matching is idempotent). An empty list returns a scanner whose
// HasCookie is always false.
func newCookieNameScanner(names []string) cookieNameScanner {
	s := cookieNameScanner{}
	if len(names) == 0 {
		return s
	}
	s.names = make([]string, 0, len(names))
	for _, raw := range names {
		n := strings.ToLower(strings.TrimSpace(raw))
		if n != "" {
			s.names = append(s.names, n)
		}
	}
	if len(s.names) == 0 {
		s.names = nil
		return s
	}
	// Sort longest-first: the per-cookie scan stops at the first
	// listed name shorter than the candidate, so longer candidates
	// never compare against irrelevant entries. Within equal lengths
	// order is irrelevant (equality is exact).
	for i := 1; i < len(s.names); i++ {
		for j := i; j > 0 && len(s.names[j-1]) < len(s.names[j]); j-- {
			s.names[j-1], s.names[j] = s.names[j], s.names[j-1]
		}
	}
	s.maxLen = len(s.names[0])
	s.minLen = len(s.names[len(s.names)-1])
	return s
}

// empty reports whether the scanner can never match. Handlers resolve
// this at build time: an empty scanner costs nothing on the request
// path.
func (s cookieNameScanner) empty() bool {
	return len(s.names) == 0
}

// hasCookieName reports whether the Cookie header value contains a
// cookie whose name equals one of the listed names
// (case-insensitively). Zero-alloc: the value is scanned in place.
//
// Grammar notes: RFC 6265 §5.4's strict parsing (leading/trailing
// spaces, quoted values, the "extends" heuristic) is deliberately
// NOT implemented — a shared cache matching a bypass list needs
// name-token equality, and cookie-pair splitting on ";" never splits
// inside a quoted value because a cookie-value containing ';' is
// forbidden by the grammar (RFC 6265 §4.1.1 cookie-value excludes
// DQUOTE except in the quoted-string form, and ';' is not
// cookie-octet). A malicious "name" containing '=' (e.g.
// "a=b" as the pair name) just never matches a listed name that
// config validation guarantees to be a token.
func (s cookieNameScanner) hasCookieName(cookieValue string) bool {
	if s.empty() || cookieValue == "" {
		return false
	}
	for pair := range strings.SplitSeq(cookieValue, ";") {
		name, _, _ := strings.Cut(pair, "=")
		if s.nameListed(name) {
			return true
		}
	}
	return false
}

// nameListed reports whether one cookie-pair's name token is in the
// list. The name is trimmed of surrounding whitespace (RFC 6265 §5.2
// strips OWS around the pair before the "=" split; the grammar has no
// in-token spaces) and lowercased in place — ASCII-only, so bytes.EqualFold
// on the original bytes is avoided by comparing against the precomputed
// lowercase list with an inline fold.
func (s cookieNameScanner) nameListed(rawName string) bool {
	// Trim OWS around the name token FIRST (RFC 6265 §5.2 strips it
	// around the pair before the "=" split, and RFC 9110 §5.6.3 permits
	// spaces around the ";" separators — clients send "a=1; b=2", so
	// every pair after the first arrives with a leading space). The
	// length gate must run on the trimmed token: gating on the raw
	// length rejected " session_id" (11 bytes) against a 10-byte listed
	// name before the trim could see it — the listed cookie was missed
	// in every position except the first.
	n := len(rawName)
	for n > 0 && (rawName[n-1] == ' ' || rawName[n-1] == '\t') {
		n--
	}
	off := 0
	for off < n && (rawName[off] == ' ' || rawName[off] == '\t') {
		off++
	}
	name := rawName[off:n]
	if name == "" || len(name) < s.minLen || len(name) > s.maxLen {
		return false
	}
	for _, listed := range s.names {
		if len(listed) < len(name) {
			// Sorted longest-first: every later entry is shorter
			// still — the candidate cannot match.
			break
		}
		if len(listed) == len(name) && asciiEqualFoldStrings(name, listed) {
			return true
		}
	}
	return false
}

// cookieBypassTriggered reports whether this request must take the
// cookie-bypass branch: either the route's presence trigger is armed
// (cache.bypass_on_cookie) and the request carries any non-empty
// Cookie header, or the route lists names
// (cache.bypass_on_cookie_names) and the request carries one of them.
//
// Multi-line Cookie headers (RFC 9110 §5.2 permits repeated field
// lines): fasthttp's Peek(Cookie) does NOT join them — before
// collectCookies runs it returns only the FIRST line (peekArgBytes,
// first argsKV match), so a listed cookie on the second line would
// be invisible to a single Peek. PeekAll returns every line, so the
// scan walks each one's pairs; the flag-on presence trigger fires on
// the first non-empty line. PeekAll populates h.mulHeader — safe
// here because the bytes are consumed before any other Peek call
// (fasthttp's own documented contract).
//
// Cost: flag-off routes (no names, no presence trigger) return before
// any header read; presence-trigger routes pay one PeekAll of a
// header fasthttp already parsed; named routes pay one PeekAll plus
// the per-line scan of the bypass list.
func (h *Handler) cookieBypassTriggered(hdr *fasthttp.RequestHeader) bool {
	if !h.bypassOnCookie && h.bypassCookieNames.empty() {
		return false
	}
	for _, line := range hdr.PeekAll(header.Cookie) {
		if len(line) == 0 {
			continue
		}
		if h.bypassOnCookie || h.bypassCookieNames.hasCookieName(header.BytesToString(line)) {
			return true
		}
	}
	return false
}

// cookieBypassTriggeredRaw reports the same trigger for a RawRequest
// (the H1 fast path): TryHit gates on it before the store Get. The
// h1parser keeps every Cookie header line separately (unlike fasthttp,
// which merges), so the scan walks each line's pairs individually — a
// listed cookie on the second line must bypass exactly as it would on
// the first. Flag and scanner are passed in so both Handler and
// FastPathHandler share the one implementation (the fast path holds
// its own build-time copies of the owner's fields).
func cookieBypassTriggeredRaw(bypassOnCookie bool, scanner cookieNameScanner, req *api.RawRequest) bool {
	if !bypassOnCookie && scanner.empty() {
		return false
	}
	for i := 0; i < req.NHeaders; i++ {
		hdr := &req.Headers[i]
		if !asciiEqualFoldStrings(hdr.Key, "Cookie") {
			continue
		}
		if len(hdr.Value) == 0 {
			continue
		}
		if bypassOnCookie {
			return true
		}
		if scanner.hasCookieName(hdr.Value) {
			return true
		}
	}
	return false
}

// asciiEqualFoldStrings compares two strings case-insensitively for
// ASCII bytes (cookie names are tokens: ASCII-only by RFC 6265
// §4.1.1). Equivalent to strings.EqualFold for ASCII input without the
// Unicode table walk. Distinct from uabypass's asciiEqualFold, which
// folds only its first argument against an already-lowered compiled
// segment.
func asciiEqualFoldStrings(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
