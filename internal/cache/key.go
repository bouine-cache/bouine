package cache

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/bouine-cache/bouine/pkg/api"
)

// BuildKeyFromURL computes the canonical cache key from a raw URL
// string. Used by admin purge/refresh endpoints where no
// *http.Request is available.
func BuildKeyFromURL(rawURL string, policy *KeyPolicy) api.Key {
	if rawURL == "" {
		return api.Key{}
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return api.Key{}
	}
	r := &http.Request{
		Method: http.MethodGet,
		URL:    u,
		Host:   u.Host,
	}
	return BuildKey(r, policy)
}

// key2Seed is the seed for the second independent xxhash64 used to
// detect collisions on the primary key. A different seed produces a
// statistically independent hash, giving 128-bit collision resistance
// (birthday bound ~2^64 objects) without the cost of SHA-256.
const key2Seed = 0x626f75696e6532 // "bouine2" in ASCII

// BuildKey constructs the canonical primary cache key from a request.
// The key is deterministic and stable across nodes.
//
// Returns (primary, secondary): two independent xxhash64 digests of
// the same canonical buffer. The primary is the map index; the
// secondary is stored on the entry and verified on Get to detect
// collisions (issue #51). Same design as Varnish/Nginx (wide hash,
// no key string) but using two fast xxhash64 calls instead of one
// slow SHA-256.
//
// Zero-alloc on the hot path: uses a 512-byte stack buffer. If the
// canonical key exceeds 512 bytes (rare — the project caps URLs at 8 KiB),
// it falls back to a heap buffer via buildKeyHeap.
func BuildKey(r *http.Request, policy *KeyPolicy) api.Key {
	var buf [512]byte
	n := 0

	// Scheme.
	if r.TLS != nil {
		n += copyOverflow(buf[:], n, "https|")
	} else {
		n += copyOverflow(buf[:], n, "http|")
	}

	// Host (canonical).
	n = appendCanonicalHost(buf[:], n, r.Host)
	n = appendByte(buf[:], n, '|')

	// Path (canonical).
	n = appendCanonicalPath(buf[:], n, r.URL)
	n = appendByte(buf[:], n, '|')

	// Query (canonical sorted, with optional param stripping).
	n = appendCanonicalQuery(buf[:], n, r.URL, policy)
	n = appendByte(buf[:], n, '|')

	// Method (HEAD→GET).
	if r.Method == http.MethodHead {
		n += copyOverflow(buf[:], n, http.MethodGet)
	} else {
		n += copyOverflow(buf[:], n, r.Method)
	}

	if n <= len(buf) {
		h := xxhash.NewWithSeed(key2Seed)
		_, _ = h.Write(buf[:n])
		secondary := h.Sum64()
		return api.Key{Hash: xxhash.Sum64(buf[:n]), Hash2: secondary}
	}

	// Overflow: redo with a heap buffer sized to fit.
	return buildKeyHeap(r, policy, n)
}

// buildKeyHeap handles the rare case where the canonical key exceeds the
// 512-byte stack buffer. It allocates a heap buffer and rebuilds the key.
func buildKeyHeap(r *http.Request, policy *KeyPolicy, n int) api.Key {
	heap := make([]byte, n)
	n = 0

	if r.TLS != nil {
		n += copyOverflow(heap, n, "https|")
	} else {
		n += copyOverflow(heap, n, "http|")
	}

	n = appendCanonicalHost(heap, n, r.Host)
	n = appendByte(heap, n, '|')

	n = appendCanonicalPath(heap, n, r.URL)
	n = appendByte(heap, n, '|')

	n = appendCanonicalQuery(heap, n, r.URL, policy)
	n = appendByte(heap, n, '|')

	if r.Method == http.MethodHead {
		n += copyOverflow(heap, n, http.MethodGet)
	} else {
		n += copyOverflow(heap, n, r.Method)
	}
	h := xxhash.NewWithSeed(key2Seed)
	_, _ = h.Write(heap[:n])
	secondary := h.Sum64()
	return api.Key{Hash: xxhash.Sum64(heap[:n]), Hash2: secondary}
}

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

func appendCanonicalQuery(buf []byte, n int, u *url.URL, p *KeyPolicy) int {
	raw := u.RawQuery
	if raw == "" {
		return n
	}

	// No-policy path: identical to existing code, no policy overhead.
	if p == nil {
		return appendCanonicalQueryNoPolicy(buf, n, u)
	}

	var stackPairs [8]kvPair
	var seen stackSeen
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
		if p.shouldStripParam(k, v, &seen) {
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
		p.markSeen(k, &seen)
		stackPairs[np] = kvPair{k, v}
		np++
	}

	if !simple {
		return appendCanonicalQuerySlow(buf, n, u, p)
	}

	sortPairs(stackPairs[:np])
	return writeSortedPairs(buf, n, stackPairs[:np])
}

// appendCanonicalQueryNoPolicy is the existing fast path with no policy
// checking. Identical to the pre-change code path to ensure zero
// regression for the common case (most routes have no query policy).
// Takes u *url.URL to avoid allocating a new url.URL on the slow path.
func appendCanonicalQueryNoPolicy(buf []byte, n int, u *url.URL) int {
	raw := u.RawQuery
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
		return appendCanonicalQuerySlowNoPolicy(buf, n, u)
	}

	sortPairs(stackPairs[:np])
	return writeSortedPairs(buf, n, stackPairs[:np])
}

// appendCanonicalQuerySlowNoPolicy is the existing slow path with no
// policy checking. Identical to the pre-change code to ensure zero
// regression. No nil checks, no policy branches.
func appendCanonicalQuerySlowNoPolicy(buf []byte, n int, u *url.URL) int {
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

// sortPairs insertion-sorts kvPair slices by key, then by value.
// Used by both the policy and no-policy fast paths to ensure
// identical ordering. Zero allocations.
func sortPairs(pairs []kvPair) {
	for i := 1; i < len(pairs); i++ {
		for j := i; j > 0; j-- {
			if pairs[j].k < pairs[j-1].k ||
				(pairs[j].k == pairs[j-1].k && pairs[j].v < pairs[j-1].v) {
				pairs[j], pairs[j-1] = pairs[j-1], pairs[j]
			} else {
				break
			}
		}
	}
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
// Called only with p != nil. The no-policy slow path uses
// appendCanonicalQuerySlowNoPolicy.
//
//nolint:gocyclo // 23: policy application is inherently branchy
func appendCanonicalQuerySlow(buf []byte, n int, u *url.URL, p *KeyPolicy) int {
	params := u.Query()
	keys := make([]string, 0, len(params))
	for k := range params {
		// keepParams allowlist: if set, skip anything not in it.
		// Allowlisted params survive stripEmpty (they keep all values).
		if p.keepParams != nil && !p.keepParams[k] {
			continue
		}
		// stripParams blocklist.
		if p.stripParams != nil && p.stripParams[k] {
			continue
		}
		// stripPrefixes: linear scan.
		stripped := false
		for i := range p.stripPrefixes {
			if strings.HasPrefix(k, p.stripPrefixes[i]) {
				stripped = true
				break
			}
		}
		if stripped {
			continue
		}
		// stripEmpty (when no allowlist): skip key if ALL values are empty.
		// Allowlisted params are exempt (they passed the allowlist check
		// above). The keepParams == nil guard ensures this.
		// Do not add stripEmpty checks without this guard.
		if p.stripEmpty && p.keepParams == nil {
			allEmpty := true
			for _, v := range params[k] {
				if v != "" {
					allEmpty = false
					break
				}
			}
			if allEmpty {
				continue
			}
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	first := true
	for _, k := range keys {
		vals := params[k]
		// dedup: take only the first value (first in request order).
		// Do NOT sort values when dedup is enabled — dedup eliminates
		// the multi-value case, sorting is pointless.
		if p.dedup {
			vals = vals[:1]
		} else {
			sort.Strings(vals)
		}
		// stripEmpty (when no allowlist): filter individual empty values.
		// Allowlisted params are exempt (keepParams == nil guard).
		if p.stripEmpty && p.keepParams == nil {
			filtered := vals[:0]
			for _, v := range vals {
				if v != "" {
					filtered = append(filtered, v)
				}
			}
			vals = filtered
			if len(vals) == 0 {
				continue
			}
		}
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
func BuildVaryKey(vary string, reqHeader http.Header, policy *KeyPolicy) string {
	if vary == "" || vary == "*" {
		return vary
	}

	fields := strings.Split(vary, ",")
	sort.Strings(fields)

	var stack [256]byte
	n := buildVaryKeyInto(stack[:], fields, reqHeader, policy)
	if n <= len(stack) {
		return strconv.FormatUint(xxhash.Sum64(stack[:n]), 16)
	}
	heap := make([]byte, n)
	buildVaryKeyInto(heap, fields, reqHeader, policy)
	return strconv.FormatUint(xxhash.Sum64(heap), 16)
}

// buildVaryKeyInto writes the canonical Vary key bytes into dst and
// returns the total canonical length. If the key exceeds len(dst), the
// content is truncated but the returned length reflects the full size.
func buildVaryKeyInto(dst []byte, fields []string, reqHeader http.Header, policy *KeyPolicy) int {
	n := 0
	for _, f := range fields {
		f = strings.TrimSpace(strings.ToLower(f))
		if policy != nil && policy.ShouldExcludeHeader(f) {
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
