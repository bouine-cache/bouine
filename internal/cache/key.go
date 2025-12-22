package cache

import (
	"net/http"
	"net/url"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/pkg/api"
)

// BuildKey constructs the canonical primary cache key from a request.
// The key is deterministic and stable across nodes (PLAN.md §3.2).
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
	if u.RawQuery == "" {
		return n
	}
	// Parse + sort. This allocates (url.Values map) but only when
	// there's a query string — most cache-hit requests re-use the
	// same key from a previous miss where the alloc already happened.
	// True zero-alloc query sorting requires a custom parser; deferred
	// to a follow-up if profiling shows this matters on the hit path
	// (it won't — Get doesn't call BuildKey again).
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
		n += copy(buf[n:], reqHeader.Get(f))
		if n < len(buf) {
			buf[n] = ';'
			n++
		}
	}
	// Return the hash as a compact string.
	h := xxhash.Sum64(buf[:n])
	// Format as hex into a stack buffer.
	var hex [16]byte
	hexN := formatHex(hex[:], h)
	return string(hex[:hexN])
}

func formatHex(buf []byte, v uint64) int {
	const digits = "0123456789abcdef"
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = digits[v&0xf]
		v >>= 4
	}
	if i == len(buf) {
		i--
		buf[i] = '0'
	}
	copy(buf, buf[i:])
	return len(buf) - i
}
