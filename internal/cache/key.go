package cache

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/pkg/api"
)

// BuildKeyFromURL computes the canonical cache key from a raw URL
// string. Used by admin purge/refresh endpoints where no
// *http.Request is available.
func BuildKeyFromURL(rawURL string) api.Key {
	if rawURL == "" {
		return 0
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return 0
	}
	r := &http.Request{
		Method: http.MethodGet,
		URL:    u,
		Host:   u.Host,
	}
	return BuildKey(r)
}

// BuildKey constructs the canonical primary cache key from a request.
// The key is deterministic and stable across nodes.
//
// Zero-alloc on the hot path: uses a 4 KB stack buffer. If the
// canonical key exceeds 4 KB (rare — the project caps URLs at 8 KiB),
// it falls back to a heap buffer.
func BuildKey(r *http.Request, skip ...map[string]bool) api.Key {
	var skipSet map[string]bool
	if len(skip) > 0 {
		skipSet = skip[0]
	}

	var stack [4096]byte
	n := buildKeyInto(stack[:], r, skipSet)
	if n <= len(stack) {
		return api.Key(xxhash.Sum64(stack[:n]))
	}

	// Overflow: redo with a heap buffer sized to fit.
	heap := make([]byte, n)
	buildKeyInto(heap, r, skipSet)
	return api.Key(xxhash.Sum64(heap))
}

// buildKeyInto writes the canonical key bytes into dst and returns the
// total number of bytes that the canonical key occupies. If the key
// exceeds len(dst), the content is truncated but the returned length
// reflects the full canonical key size, allowing the caller to detect
// overflow and reallocate.
func buildKeyInto(dst []byte, r *http.Request, skipSet map[string]bool) int {
	n := 0

	// Scheme.
	if r.TLS != nil {
		n += copyOverflow(dst, n, "https|")
	} else {
		n += copyOverflow(dst, n, "http|")
	}

	// Host (canonical).
	n = appendCanonicalHost(dst, n, r.Host)
	n = appendByte(dst, n, '|')

	// Path (canonical).
	n = appendCanonicalPath(dst, n, r.URL)
	n = appendByte(dst, n, '|')

	// Query (canonical sorted, with optional param stripping).
	n = appendCanonicalQuery(dst, n, r.URL, skipSet)
	n = appendByte(dst, n, '|')

	// Method (HEAD→GET).
	if r.Method == http.MethodHead {
		n += copyOverflow(dst, n, http.MethodGet)
	} else {
		n += copyOverflow(dst, n, r.Method)
	}

	return n
}

// appendByte writes a single byte at offset n into dst. If n is past
// the end of dst the write is skipped but n is still incremented so the
// caller can detect overflow by comparing n > len(dst).
func appendByte(dst []byte, n int, b byte) int {
	if n < len(dst) {
		dst[n] = b
	}
	return n + 1
}

// copyOverflow copies src into dst starting at offset n. If n is past
// the end of dst, no bytes are copied but the full length of src is
// returned, allowing callers to track the true canonical size for
// overflow detection.
func copyOverflow(dst []byte, n int, src string) int {
	if n >= len(dst) {
		return len(src)
	}
	return copy(dst[n:], src)
}

func appendCanonicalHost(buf []byte, n int, host string) int {
	// Lowercase + strip default port.
	for i := range len(host) {
		c := host[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if n < len(buf) {
			buf[n] = c
		}
		n++
	}
	// Strip :80 or :443 suffix.
	if hasSuffix(buf, n, ":80") {
		n -= 3
	} else if hasSuffix(buf, n, ":443") {
		n -= 4
	}
	return n
}

// hasSuffix reports whether buf[:n] ends with s. n must not exceed len(buf).
func hasSuffix(buf []byte, n int, s string) bool {
	if n < len(s) || n > len(buf) {
		return false
	}
	return string(buf[n-len(s):n]) == s
}

func appendCanonicalPath(buf []byte, n int, u *url.URL) int {
	p := u.Path
	if p == "" {
		p = "/"
	}
	prev := byte(0)
	for i := range len(p) {
		c := p[i]
		if c == '/' && prev == '/' {
			continue
		}
		if n < len(buf) {
			buf[n] = c
		}
		n++
		prev = c
	}
	return n
}

func appendCanonicalQuery(buf []byte, n int, u *url.URL, skip map[string]bool) int {
	raw := u.RawQuery
	if raw == "" {
		return n
	}

	// Fast path: for ≤8 simple ASCII params (no percent-encoding) use a
	// stack-allocated pair array and an insertion sort to avoid the
	// url.Values map + keys slice allocations from the slow path.
	var stackPairs [8]kvPair
	np := 0
	simple := true

	for s := raw; s != ""; {
		var seg string
		if i := strings.IndexByte(s, '&'); i >= 0 {
			seg, s = s[:i], s[i+1:]
		} else {
			seg, s = s, ""
		}
		k, v, _ := strings.Cut(seg, "=")
		if skip != nil && skip[k] {
			continue
		}
		if strings.IndexByte(k, '%') >= 0 || strings.IndexByte(v, '%') >= 0 {
			simple = false
			break
		}
		if np >= len(stackPairs) {
			simple = false
			break
		}
		stackPairs[np] = kvPair{k, v}
		np++
	}

	if !simple {
		return appendCanonicalQuerySlow(buf, n, u, skip)
	}

	// Insertion sort by key (fast for ≤8 elements, zero alloc).
	pairs := stackPairs[:np]
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0 && pairs[j].k < pairs[j-1].k; j-- {
			pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
		}
	}

	return writeSortedPairs(buf, n, pairs)
}

type kvPair struct{ k, v string }

func writeSortedPairs(buf []byte, n int, pairs []kvPair) int {
	first := true
	for _, p := range pairs {
		if !first {
			n = appendByte(buf, n, '&')
		}
		first = false
		n += copyOverflow(buf, n, p.k)
		n = appendByte(buf, n, '=')
		n += copyOverflow(buf, n, p.v)
	}
	return n
}

// appendCanonicalQuerySlow handles query strings with percent-encoded
// characters or more than 8 parameters. Allocates via url.Values.
func appendCanonicalQuerySlow(buf []byte, n int, u *url.URL, skip map[string]bool) int {
	// Parse + sort. Allocates url.Values map and a keys slice, but only
	// for complex or long query strings (≥9 params or percent-encoded).
	params := u.Query()
	keys := make([]string, 0, len(params))
	for k := range params {
		if skip != nil && skip[k] {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	first := true
	for _, k := range keys {
		vals := params[k]
		sort.Strings(vals)
		for _, v := range vals {
			if !first {
				n = appendByte(buf, n, '&')
			}
			first = false
			n += copyOverflow(buf, n, url.QueryEscape(k))
			n = appendByte(buf, n, '=')
			n += copyOverflow(buf, n, url.QueryEscape(v))
		}
	}
	return n
}

// BuildVaryKey constructs the secondary key from the Vary header
// values in the response and the corresponding request headers.
// List-valued headers (Accept-Language, Accept-Encoding, Accept) are
// normalised by sorting their comma-separated tokens so that
// "en, fr" and "fr, en" produce the same cache key.
func BuildVaryKey(vary string, reqHeader http.Header, exclude ...map[string]bool) string {
	if vary == "" || vary == "*" {
		return vary
	}

	var excludeSet map[string]bool
	if len(exclude) > 0 {
		excludeSet = exclude[0]
	}

	fields := strings.Split(vary, ",")
	sort.Strings(fields)

	var stack [256]byte
	n := buildVaryKeyInto(stack[:], fields, reqHeader, excludeSet)
	if n <= len(stack) {
		return strconv.FormatUint(xxhash.Sum64(stack[:n]), 16)
	}
	heap := make([]byte, n)
	buildVaryKeyInto(heap, fields, reqHeader, excludeSet)
	return strconv.FormatUint(xxhash.Sum64(heap), 16)
}

// buildVaryKeyInto writes the canonical Vary key bytes into dst and
// returns the total canonical length. If the key exceeds len(dst), the
// content is truncated but the returned length reflects the full size.
func buildVaryKeyInto(dst []byte, fields []string, reqHeader http.Header, exclude map[string]bool) int {
	n := 0
	for _, f := range fields {
		f = strings.TrimSpace(strings.ToLower(f))
		if exclude != nil && exclude[f] {
			continue
		}
		n += copyOverflow(dst, n, f)
		n = appendByte(dst, n, '=')
		val := reqHeader.Get(f)
		if isListValuedVaryField(f) {
			val = normaliseListHeader(val)
		}
		n += copyOverflow(dst, n, val)
		n = appendByte(dst, n, ';')
	}
	return n
}

// isListValuedVaryField reports whether a Vary field name contains a
// list of tokens whose order should be normalised for cache key purposes.
func isListValuedVaryField(f string) bool {
	switch f {
	case "accept-language", "accept-encoding", "accept", "accept-charset":
		return true
	}
	return false
}

// normaliseListHeader sorts the comma-separated tokens in a list header
// value so that equivalent but differently-ordered values produce the
// same string. Quality factors (q=...) are kept with their token.
func normaliseListHeader(v string) string {
	if v == "" || !strings.Contains(v, ",") {
		return strings.TrimSpace(v)
	}
	parts := strings.Split(v, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(p)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
