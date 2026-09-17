package cache

import (
	"bytes"
	"regexp"

	"github.com/valyala/fasthttp"
)

// maxRewriteOutputBytes caps the rewritten URI. A replacement template
// like `$1$1$1$1` against a long path doubles the path every match, so
// the output of even a single RE2 substitution can be quadratic in the
// input. Origins reject multi-KiB paths anyway; past this bound the
// rewrite is skipped and the origin sees the original path. The cap is
// a guard, never a normal-case cost: paths this long fail the server's
// 8 KiB URL limit long before reaching here.
const maxRewriteOutputBytes = 16 * 1024

// PathRewrite rewrites the path component of origin-bound request URIs
// with a pre-compiled RE2 pattern and replacement template
// (config.RouteRequest.PathRewrite). It is the regex sibling of
// StripRequestURI: the same boundary contract — cache keys, ban
// matching, purges, and client-facing surfaces keep the original path;
// only the origin-bound copy is rewritten.
//
// Hardening decisions, each pinned by a test:
//   - The query string is split off before matching and re-appended
//     unchanged, so the pattern can never swallow or reorder query
//     parameters (a rewrite that eats ?signature=... silently breaks
//     auth).
//   - Only the first match is replaced (FindSubmatchIndex + Expand),
//     matching nginx `rewrite ... break` semantics.
//   - A result that does not start with "/" is discarded and the
//     original path passed through: a relative path would corrupt the
//     origin request line (fasthttp requires a request-target).
//   - Output is capped at maxRewriteOutputBytes, blocking $1$1$1
//     amplification.
//
// Unstable: the config surface may change.
type PathRewrite struct {
	pattern string // raw pattern, for String()
	re      *regexp.Regexp
	replace []byte // replacement template ([]byte for Expand)
}

// NewPathRewrite compiles the pattern. The pattern was already
// validated by config.validatePathRewrite; a compile error here is a
// programming error, so it panics — it cannot happen through validated
// config.
func NewPathRewrite(pattern, replace string) *PathRewrite {
	return &PathRewrite{
		re:      regexp.MustCompile(pattern),
		replace: []byte(replace),
		pattern: pattern,
	}
}

// RewriteURI rewrites the path of uri (path[?query]) and returns the
// result. When the pattern does not match, the rewrite is invalid
// (relative result, oversized output), or the URI has no '?' split
// anomaly, the original uri is returned unchanged. The returned slice
// may alias uri; callers must not mutate it in place.
func (p *PathRewrite) RewriteURI(uri []byte) []byte {
	q := bytes.IndexByte(uri, '?')
	var path, query []byte
	if q >= 0 {
		path, query = uri[:q], uri[q:]
	} else {
		path = uri
	}
	loc := p.re.FindSubmatchIndex(path)
	if loc == nil {
		return uri
	}
	// Expand substitutes only the template against the matched span —
	// it does not copy surrounding context. Rebuild the full path as
	// prefix + expansion + suffix: nginx `rewrite ... break` first-
	// match semantics keep the unmatched head and tail.
	dst := make([]byte, 0, len(path)+len(p.replace))
	dst = append(dst, path[:loc[0]]...)
	dst = p.re.Expand(dst, p.replace, path, loc)
	dst = append(dst, path[loc[1]:]...)
	if len(dst) == 0 || dst[0] != '/' {
		return uri
	}
	if len(dst)+len(query) > maxRewriteOutputBytes {
		return uri
	}
	if len(query) > 0 {
		dst = append(dst, query...)
	}
	return dst
}

// Apply rewrites the request URI in place on the origin-bound request.
// It is the regex analogue of StripRequestURI for builder-lowered
// static routes: no-op when nothing applies.
func (p *PathRewrite) Apply(req *fasthttp.Request) {
	rewritten := p.RewriteURI(req.RequestURI())
	if !bytes.Equal(rewritten, req.RequestURI()) {
		req.SetRequestURIBytes(rewritten)
	}
}

// String renders the rewrite for logs and the dashboard
// ("pattern -> template").
func (p *PathRewrite) String() string {
	return p.pattern + " -> " + string(p.replace)
}
