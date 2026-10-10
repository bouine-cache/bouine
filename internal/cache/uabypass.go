package cache

import "strings"

// uaBypass holds a route's compiled cache.bypass_on_user_agent
// patterns (issue #771, ADR-0055). nil (or empty globs) means the route
// has no patterns: both request-path gates are then a single nil
// check, and pattern-less routes are bit-for-bit unchanged.
type uaBypass struct {
	globs []uaGlob
}

// uaGlob is one compiled pattern: the literal runs between `*`
// wildcards, ASCII-lowercased at compile time (handler build, cold
// path). Matching is ASCII-case-insensitive against the full
// User-Agent string: `*` matches any run of bytes (including `/` —
// deliberately unlike path.Match, whose `*` stops at `/` and would
// break `Bot/1.0`-shaped patterns). All other characters are literal.
type uaGlob struct {
	segs [][]byte
}

// compileUABypass compiles the validated pattern list into a matcher.
// The config layer (validateBypassOnUserAgent) rejects malformed
// entries at load time; defensively, compile skips empty patterns and
// any pattern made only of `*` wildcards (lone `*`, `**`, `***` — all
// segments empty) so a hand-built HandlerConfig can never turn into
// "bypass everything".
func compileUABypass(patterns []string) *uaBypass {
	if len(patterns) == 0 {
		return nil
	}
	globs := make([]uaGlob, 0, len(patterns))
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		rawSegs := strings.Split(p, "*")
		onlyWildcards := true
		for _, seg := range rawSegs {
			if seg != "" {
				onlyWildcards = false
				break
			}
		}
		if onlyWildcards {
			continue
		}
		segs := make([][]byte, len(rawSegs))
		for i, seg := range rawSegs {
			// ASCII-fold, not strings.ToLower: the matcher folds the
			// subject with the same byte-wise rule, so non-ASCII bytes
			// compare byte-exact on both sides (a Unicode-aware lower
			// here would over-match subjects the matcher cannot fold).
			b := []byte(seg)
			for j := range b {
				b[j] = foldASCII(b[j])
			}
			segs[i] = b
		}
		globs = append(globs, uaGlob{segs: segs})
	}
	if len(globs) == 0 {
		return nil
	}
	return &uaBypass{globs: globs}
}

// matchBytes reports whether the User-Agent (slow path: fasthttp Peek
// bytes) matches any compiled pattern. Zero allocations: the loop
// reads the caller's slice directly.
func (b *uaBypass) matchBytes(ua []byte) bool {
	for i := range b.globs {
		if uaGlobMatch(b.globs[i], ua) {
			return true
		}
	}
	return false
}

// matchString reports whether the User-Agent (fast path:
// RawRequest.Header string, an alias of the connection read buffer)
// matches any compiled pattern. Zero allocations: no []byte
// conversion.
func (b *uaBypass) matchString(ua string) bool {
	for i := range b.globs {
		if uaGlobMatch(b.globs[i], ua) {
			return true
		}
	}
	return false
}

// uaGlobMatch reports whether ua matches g. Generic over the two
// representations the request path holds the User-Agent in: []byte
// (slow path Peek) and string (fast path RawRequest.Header) — two
// static instantiations of the same loop, no boxing, no allocation.
// An exact pattern (no `*`) must match the whole string, not a
// substring; operators wanting substring semantics write
// `*Pattern*`.
func uaGlobMatch[S []byte | ~string](g uaGlob, ua S) bool {
	n := len(g.segs)
	if n == 1 {
		return asciiEqualFold(ua, g.segs[0])
	}
	first, last := g.segs[0], g.segs[n-1]
	if len(ua) < len(first)+len(last) {
		return false
	}
	if !asciiEqualFold(ua[:len(first)], first) {
		return false
	}
	if !asciiEqualFold(ua[len(ua)-len(last):], last) {
		return false
	}
	rest := ua[len(first) : len(ua)-len(last)]
	for _, seg := range g.segs[1 : n-1] {
		idx := asciiIndex(rest, seg)
		if idx < 0 {
			return false
		}
		rest = rest[idx+len(seg):]
	}
	return true
}

// asciiEqualFold compares a with the compiled (lowercased) segment b,
// ignoring ASCII case. Bytes >= 0x80 compare exactly: User-Agent
// products are ASCII in practice, and a byte-exact fallback never
// over-matches.
func asciiEqualFold[S []byte | ~string](a S, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range b {
		if foldASCII(a[i]) != b[i] {
			return false
		}
	}
	return true
}

// asciiIndex returns the first index at or after which the lowercased
// sub appears in s (ASCII-case-insensitive), or -1. Naive scan: this
// runs only on bypass-configured routes' requests, bounded by 16
// patterns and the 8 KiB per-header budget (threat-model T37).
func asciiIndex[S []byte | ~string](s S, sub []byte) int {
outer:
	for i := 0; i+len(sub) <= len(s); i++ {
		for j := 0; j < len(sub); j++ {
			if foldASCII(s[i+j]) != sub[j] {
				continue outer
			}
		}
		return i
	}
	return -1
}
