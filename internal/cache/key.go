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
// Zero-alloc: uses a stack buffer for the canonical string.
func BuildKey(r *http.Request) api.Key {
	// Stack buffer large enough for most URLs. If exceeded, falls back
	// to heap (rare — URLs > 512 bytes are unusual).
	var buf [512]byte
	n := 0

	// Scheme.
	if r.TLS != nil {
		n += copy(buf[n:], "https|")
	} else {
		n += copy(buf[n:], "http|")
	}

	// Host (canonical).
	n = appendCanonicalHost(buf[:], n, r.Host)
	buf[n] = '|'
	n++

	// Path (canonical).
	n = appendCanonicalPath(buf[:], n, r.URL)
	buf[n] = '|'
	n++

	// Query (canonical sorted).
	n = appendCanonicalQuery(buf[:], n, r.URL)
	buf[n] = '|'
	n++

	// Method (HEAD→GET).
	if r.Method == http.MethodHead {
		n += copy(buf[n:], http.MethodGet)
	} else {
		n += copy(buf[n:], r.Method)
	}

	return api.Key(xxhash.Sum64(buf[:n]))
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
			n++
		}
	}
	// Strip :80 or :443 suffix.
	written := buf[:n]
	if len(written) >= 3 && written[n-3] == ':' && written[n-2] == '8' && written[n-1] == '0' {
		n -= 3
	} else if len(written) >= 4 && written[n-4] == ':' && written[n-3] == '4' && written[n-2] == '4' && written[n-1] == '3' {
		n -= 4
	}
	return n
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
			n++
		}
		prev = c
	}
	return n
}

func appendCanonicalQuery(buf []byte, n int, u *url.URL) int {
	raw := u.RawQuery
	if raw == "" {
		return n
	}

	// Fast path: for ≤8 simple ASCII params (no percent-encoding) use a
	// stack-allocated pair array and an insertion sort to avoid the
	// url.Values map + keys slice allocations from the slow path.
	type kvPair struct{ k, v string }
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
		return appendCanonicalQuerySlow(buf, n, u)
	}

	// Insertion sort by key (fast for ≤8 elements, zero alloc).
	pairs := stackPairs[:np]
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0 && pairs[j].k < pairs[j-1].k; j-- {
			pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
		}
	}

	first := true
	for _, p := range pairs {
		if !first {
			if n < len(buf) {
				buf[n] = '&'
				n++
			}
		}
		first = false
		n += copy(buf[n:], p.k)
		if n < len(buf) {
			buf[n] = '='
			n++
		}
		n += copy(buf[n:], p.v)
	}
	return n
}

// appendCanonicalQuerySlow handles query strings with percent-encoded
// characters or more than 8 parameters. Allocates via url.Values.
func appendCanonicalQuerySlow(buf []byte, n int, u *url.URL) int {
	// Parse + sort. Allocates url.Values map and a keys slice, but only
	// for complex or long query strings (≥9 params or percent-encoded).
	params := u.Query()
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	first := true
	for _, k := range keys {
		vals := params[k]
		sort.Strings(vals)
		for _, v := range vals {
			if !first {
				if n < len(buf) {
					buf[n] = '&'
					n++
				}
			}
			first = false
			n += copy(buf[n:], url.QueryEscape(k))
			if n < len(buf) {
				buf[n] = '='
				n++
			}
			n += copy(buf[n:], url.QueryEscape(v))
		}
	}
	return n
}

// BuildVaryKey constructs the secondary key from the Vary header
// values in the response and the corresponding request headers.
// List-valued headers (Accept-Language, Accept-Encoding, Accept) are
// normalised by sorting their comma-separated tokens so that
// "en, fr" and "fr, en" produce the same cache key.
func BuildVaryKey(vary string, reqHeader http.Header) string {
	if vary == "" || vary == "*" {
		return vary
	}

	fields := strings.Split(vary, ",")
	sort.Strings(fields)

	var buf [256]byte
	n := 0
	for _, f := range fields {
		f = strings.TrimSpace(strings.ToLower(f))
		n += copy(buf[n:], f)
		if n < len(buf) {
			buf[n] = '='
			n++
		}
		val := reqHeader.Get(f)
		// Normalise list-valued headers: sort their comma-separated tokens
		// so "en, fr" and "fr, en" hash identically.
		if isListValuedVaryField(f) {
			val = normaliseListHeader(val)
		}
		n += copy(buf[n:], val)
		if n < len(buf) {
			buf[n] = ';'
			n++
		}
	}
	// Return the hash as a compact string.
	h := xxhash.Sum64(buf[:n])
	return strconv.FormatUint(h, 16)
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
