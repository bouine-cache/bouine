package cache

import (
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/bouine-cache/xxhash/v3"

	"github.com/bouine-cache/bouine/pkg/api"
)

// NewKey computes the 128-bit cache key from canonical bytes via a single
// XXH128 hash. The result is stored in the canonical big-endian layout
// from xxhash.Uint128.Bytes() (high 64 bits first, then low 64 bits).
// The full [16]byte is a 128-bit collision check when used as a map key.
// Zero allocations: Sum128 is a one-shot function with no heap state.
func NewKey(canonical []byte) api.Key {
	h := xxhash.Sum128(canonical)
	return api.NewKeyFromBytes(h.Bytes())
}

// BuildKeyFromURL computes the canonical cache key from a raw URL
// string. Used by admin purge/refresh endpoints where no
// request context is available.
func BuildKeyFromURL(rawURL string, policy *KeyPolicy) api.Key {
	if rawURL == "" {
		return api.Key{}
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return api.Key{}
	}
	_ = u // URL parsed for BuildKeyFromURL
	ri := RequestInfo{Method: "GET", URI: rawURL, Host: u.Host, Path: u.Path, TLS: u.Scheme == "https"}
	return BuildKey(ri, policy)
}

// BuildKey constructs the canonical primary cache key from a request.
// The key is deterministic and stable across nodes.
//
// Zero-alloc on the hot path: uses a 512-byte stack buffer. If the
// canonical key exceeds 512 bytes (rare — the project caps URLs at 8 KiB),
// it falls back to a heap buffer via buildKeyHeap.
func BuildKey(ri RequestInfo, policy *KeyPolicy) api.Key {
	var buf [512]byte
	n := 0

	// Scheme.
	if ri.TLS {
		n += copyOverflow(buf[:], n, "https|")
	} else {
		n += copyOverflow(buf[:], n, "http|")
	}

	// Host (canonical). include_host: false emits the empty segment
	// ("http||/") instead of removing the delimiter so the canonical
	// form stays unambiguous against paths carrying '|' and the
	// overflow-length arithmetic is unchanged.
	if includeHostKey(policy) {
		n = appendCanonicalHost(buf[:], n, ri.GetHost())
	}
	n = appendByte(buf[:], n, '|')

	// Path (canonical).
	n = appendCanonicalPathString(buf[:], n, ri.GetPath())
	n = appendByte(buf[:], n, '|')

	// Query (canonical sorted, with optional param stripping).
	n = appendCanonicalQueryString(buf[:], n, extractRawQuery(ri.GetURI()), policy)
	n = appendByte(buf[:], n, '|')

	// Method (HEAD→GET).
	if ri.GetMethod() == "HEAD" {
		n += copyOverflow(buf[:], n, "GET")
	} else {
		n += copyOverflow(buf[:], n, ri.GetMethod())
	}

	if n <= len(buf) {
		return NewKey(buf[:n])
	}

	// Overflow: redo with a heap buffer sized to fit.
	return buildKeyHeap(ri, policy, n)
}

// buildKeyHeap handles the rare case where the canonical key exceeds the
// 512-byte stack buffer. It allocates a heap buffer and rebuilds the key.
func buildKeyHeap(ri RequestInfo, policy *KeyPolicy, n int) api.Key {
	heap := make([]byte, n)
	n = 0

	if ri.TLS {
		n += copyOverflow(heap, n, "https|")
	} else {
		n += copyOverflow(heap, n, "http|")
	}

	if includeHostKey(policy) {
		n = appendCanonicalHost(heap, n, ri.GetHost())
	}
	n = appendByte(heap, n, '|')

	n = appendCanonicalPathString(heap, n, ri.GetPath())
	n = appendByte(heap, n, '|')

	n = appendCanonicalQueryString(heap, n, extractRawQuery(ri.GetURI()), policy)
	n = appendByte(heap, n, '|')

	if ri.GetMethod() == "HEAD" {
		n += copyOverflow(heap, n, "GET")
	} else {
		n += copyOverflow(heap, n, ri.GetMethod())
	}

	return NewKey(heap[:n])
}

// BuildKeyFast constructs the canonical primary cache key directly from
// *fasthttp.RequestCtx byte slices, avoiding the 4 string() conversions
// that BuildKey requires (Method, URI, Host, Path). Only the query string
// portion of the URI is converted to string (for strings.* parsing), which
// is typically short. The method is compared as []byte to avoid allocation.
//
// Zero-alloc on the hot path when the URL has no query string.
func BuildKeyFast(method, uri, host, path []byte, tls bool, policy *KeyPolicy) api.Key {
	var buf [512]byte
	n := 0

	// Scheme.
	if tls {
		n += copyOverflowBytes(buf[:], n, sHTTPS)
	} else {
		n += copyOverflowBytes(buf[:], n, sHTTP)
	}

	// Host (canonical), unless the route opts out (include_host: false
	// emits the empty segment — see BuildKey).
	if includeHostKey(policy) {
		n = appendCanonicalHostBytes(buf[:], n, host)
	}
	n = appendByte(buf[:], n, '|')

	// Path (canonical).
	n = appendCanonicalPathBytes(buf[:], n, path)
	n = appendByte(buf[:], n, '|')

	// Query (canonical sorted, with optional param stripping).
	rawQuery := extractRawQueryBytes(uri)
	if len(rawQuery) > 0 {
		n = appendCanonicalQueryString(buf[:], n, string(rawQuery), policy)
	}
	n = appendByte(buf[:], n, '|')

	// Method (HEAD→GET).
	if bytesEqual(method, headBytes) {
		n += copyOverflowBytes(buf[:], n, sGET)
	} else {
		n += copyOverflowBytesFromBytes(buf[:], n, method)
	}

	if n <= len(buf) {
		return NewKey(buf[:n])
	}

	// Overflow: redo with a heap buffer sized to fit.
	heap := make([]byte, n)
	n = 0
	if tls {
		n += copyOverflowBytes(heap, n, sHTTPS)
	} else {
		n += copyOverflowBytes(heap, n, sHTTP)
	}
	if includeHostKey(policy) {
		n = appendCanonicalHostBytes(heap, n, host)
	}
	n = appendByte(heap, n, '|')
	n = appendCanonicalPathBytes(heap, n, path)
	n = appendByte(heap, n, '|')
	if len(rawQuery) > 0 {
		n = appendCanonicalQueryString(heap, n, string(rawQuery), policy)
	}
	n = appendByte(heap, n, '|')
	if bytesEqual(method, headBytes) {
		n += copyOverflowBytes(heap, n, sGET)
	} else {
		n += copyOverflowBytesFromBytes(heap, n, method)
	}
	return NewKey(heap[:n])
}

// Pre-allocated string constants for scheme/method to avoid repeated
// string literal allocations in copyOverflowBytes.
var (
	sHTTPS    = "https|"
	sHTTP     = "http|"
	sGET      = "GET"
	headBytes = []byte("HEAD")
)

// bytesEqual compares two byte slices without allocation.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

// copyOverflowBytesFromBytes is the []byte variant of copyOverflowBytes.
func copyOverflowBytesFromBytes(dst []byte, n int, src []byte) int {
	if n < len(dst) {
		copy(dst[n:], src)
	}
	return len(src)
}

// copyOverflowBytes is the []byte variant of copyOverflow.
func copyOverflowBytes(dst []byte, n int, src string) int {
	if n < len(dst) {
		copy(dst[n:], src)
	}
	return len(src)
}

// appendCanonicalHostBytes is the []byte variant of appendCanonicalHost.
func appendCanonicalHostBytes(buf []byte, n int, host []byte) int {
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
	if hasSuffixBytes(buf, n, ":80") {
		n -= 3
	} else if hasSuffixBytes(buf, n, ":443") {
		n -= 4
	}
	return n
}

// hasSuffixBytes is the []byte variant of hasSuffix.
func hasSuffixBytes(buf []byte, n int, s string) bool {
	if n < len(s) || n > len(buf) {
		return false
	}
	return string(buf[n-len(s):n]) == s
}

// appendCanonicalPathBytes is the []byte variant of appendCanonicalPathString.
func appendCanonicalPathBytes(buf []byte, n int, p []byte) int {
	if len(p) == 0 {
		p = []byte("/")
	}
	prev := byte(0)
	for i := 0; i < len(p); i++ {
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

// extractRawQueryBytes extracts the raw query string from a URI as []byte.
// Returns nil if there is no query component.
func extractRawQueryBytes(uri []byte) []byte {
	if i := bytesIndexByte(uri, '?'); i >= 0 {
		return uri[i+1:]
	}
	return nil
}

// bytesIndexByte is a thin wrapper around bytes.IndexByte to avoid
// importing the bytes package (already imported via sort/strconv/strings).
func bytesIndexByte(b []byte, c byte) int {
	for i, v := range b {
		if v == c {
			return i
		}
	}
	return -1
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

// copyOverflow copies src into dst starting at offset n. The copy is
// best-effort: only bytes that fit within dst are written. The return
// value is always len(src), allowing callers to track the true canonical
// size for overflow detection regardless of whether the copy was partial.
func copyOverflow(dst []byte, n int, src string) int {
	if n < len(dst) {
		copy(dst[n:], src)
	}
	return len(src)
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

// appendCanonicalQueryNoPolicy is the existing fast path with no policy
// checking. Identical to the pre-change code path to ensure zero
// regression for the common case (most routes have no query policy).
// Takes u *url.URL to avoid allocating a new url.URL on the slow path.

// appendCanonicalQuerySlowNoPolicy is the existing slow path with no
// policy checking. Identical to the pre-change code to ensure zero
// regression. No nil checks, no policy branches.

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

// BuildVaryKey constructs the secondary key from the Vary header
// values in the response and the corresponding request headers.
// List-valued headers (Accept-Language, Accept-Encoding, Accept) are
// normalised by sorting their comma-separated tokens so that
// "en, fr" and "fr, en" produce the same cache key.
func BuildVaryKey(vary string, reqHeader header.Map, policy *KeyPolicy) string {
	if vary == "" || vary == "*" {
		return vary
	}
	if h, ok := varyAssertionHash(vary, varyAssertSrc{m: reqHeader}, policy); ok {
		return strconv.FormatUint(h, 16)
	}
	return buildVaryKeySlow(vary, reqHeader, policy)
}

// varyKeyMatches reports whether BuildVaryKey(vary, src's header map,
// policy) == want without materializing the map or the hex string: the
// zero-alloc form of the peer variant gates (fast and slow path). Falls
// back to the allocating BuildVaryKey on the inputs varyAssertionHash
// declines (pathological field count, oversized hash input, non-ASCII
// field names), so the answer is identical on every input (pinned by
// FuzzVaryKeyMatchesParity).
func varyKeyMatches(vary string, src varyAssertSrc, policy *KeyPolicy, want string) bool {
	if vary == "" || vary == "*" {
		return vary == want
	}
	h, ok := varyAssertionHash(vary, src, policy)
	if !ok {
		return buildVaryKeySlow(vary, src.toMap(), policy) == want
	}
	var hex [16]byte
	return string(strconv.AppendUint(hex[:0], h, 16)) == want
}

// varyAssertSrc is the request-header view BuildVaryKey's assertion
// hash reads: a header.Map (the slow path's RequestInfo view) when r
// is nil, otherwise the H1 fast path's *api.RawRequest read as the map
// reqHeaderMapFromRaw would build, without building it. A concrete
// struct rather than a generic constraint: a generic method call via
// the shape dictionary makes escape analysis assume its string
// argument escapes, which would heap-move the stack hash buffer the
// lowercased field name lives in.
//
// RawRequest/map equivalences: a lowercase ASCII field name matches
// the first line (request order; SortEntries is stable) whose
// canonical key equals the field's canonical form, the same line
// api.EqualFold selects, since MIME canonicalization only changes
// ASCII letter case; values carry the same right-trim of OWS
// reqHeaderMapFromRaw applies.
type varyAssertSrc struct {
	r *api.RawRequest
	m header.Map
}

// getValue returns exactly what header.Map.Get returns for a
// lowercase Vary field name over the (materialized) map.
func (s varyAssertSrc) getValue(field string) string {
	if s.r == nil {
		return s.m.Get(field)
	}
	return strings.TrimRight(s.r.Header(field), " \t")
}

// toMap returns the map view for the allocating fallback.
func (s varyAssertSrc) toMap() header.Map {
	if s.r == nil {
		return s.m
	}
	return reqHeaderMapFromRaw(s.r)
}

// cookieAll returns exactly what header.Map.CookieAll returns over the
// map view: for a RawRequest, right-trimmed Cookie line values, empty
// lines skipped, one line returned as-is (zero-alloc), several joined
// with "; ".
func (s varyAssertSrc) cookieAll() string {
	if s.r == nil {
		return s.m.CookieAll()
	}
	single := ""
	count := 0
	for i := 0; i < s.r.NHeaders; i++ {
		if !api.EqualFold(s.r.Headers[i].Key, header.Cookie) {
			continue
		}
		if v := strings.TrimRight(s.r.Headers[i].Value, " \t"); v != "" {
			if count == 0 {
				single = v
			}
			count++
		}
	}
	if count <= 1 {
		return single
	}
	var b []byte
	for i := 0; i < s.r.NHeaders; i++ {
		if !api.EqualFold(s.r.Headers[i].Key, header.Cookie) {
			continue
		}
		v := strings.TrimRight(s.r.Headers[i].Value, " \t")
		if v == "" {
			continue
		}
		if len(b) > 0 {
			b = append(b, ';', ' ')
		}
		b = append(b, v...)
	}
	return string(b)
}

// varyAssertionHash computes the xxhash64 BuildVaryKey hex-encodes,
// with zero allocations of its own: Vary fields are split into a stack
// array, the hash input into a stack buffer. ok is false when the
// input needs the allocating path (more than maxVaryFields fields, a
// hash input over 256 bytes, or a non-ASCII field name whose Unicode
// ToLower/TrimSpace semantics this ASCII mirror does not reproduce);
// callers then use buildVaryKeySlow, which produces the same digest.
//
// Ordering is load-bearing: buildVaryKeySlow sorts the RAW split
// fields (untrimmed, original case) and only then trims and lowercases
// each one. Sorting the normalized names instead (variantKeyCore's
// order) would reorder fields like " b" vs "a" or "B" vs "a" and
// change every stored assertion hash.
//
//nolint:gocyclo // 16: mirrors buildVaryKeySlow's per-field branches plus the overflow guards
func varyAssertionHash(vary string, src varyAssertSrc, policy *KeyPolicy) (uint64, bool) {
	var fields [maxVaryFields]string
	n := 0
	for f := range strings.SplitSeq(vary, ",") {
		if n >= maxVaryFields {
			return 0, false
		}
		fields[n] = f
		n++
	}
	for i := 1; i < n; i++ {
		for j := i; j > 0 && fields[j-1] > fields[j]; j-- {
			fields[j-1], fields[j] = fields[j], fields[j-1]
		}
	}

	var buf [256]byte
	off := 0
	for i := range n {
		raw, ok := trimASCIISpace(fields[i])
		if !ok {
			return 0, false
		}
		if off+len(raw)+2 > len(buf) {
			return 0, false
		}
		start := off
		for j := range len(raw) {
			c := raw[j]
			if 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			buf[start+j] = c
		}
		f := header.BytesToString(buf[start : start+len(raw)])
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		var val string
		if f == cookiePresenceField && policy.hasCookiePresence() {
			val = policy.cookiePresenceValue(src.cookieAll())
		} else {
			val = varyAssertionValue(f, src.getValue(f), policy)
		}
		end := start + len(raw) + 1 + len(val) + 1
		if end > len(buf) {
			return 0, false
		}
		off = start + len(raw)
		buf[off] = '='
		off++
		off += copy(buf[off:], val)
		buf[off] = ';'
		off++
	}
	return xxhash.Sum64(buf[:off]), true
}

// trimASCIISpace is strings.TrimSpace(strings.ToLower(s))'s trimming
// half for an all-ASCII s (the ASCII whitespace set TrimSpace uses).
// ok is false when s carries a non-ASCII byte: Unicode case mapping
// and Unicode whitespace are left to the allocating path.
func trimASCIISpace(s string) (string, bool) {
	for i := range len(s) {
		if s[i] >= 0x80 {
			return "", false
		}
	}
	start, end := 0, len(s)
	for start < end && isASCIISpaceByte(s[start]) {
		start++
	}
	for end > start && isASCIISpaceByte(s[end-1]) {
		end--
	}
	return s[start:end], true
}

func isASCIISpaceByte(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\v' || c == '\f' || c == '\r'
}

// buildVaryKeySlow is the reference (allocating) BuildVaryKey
// implementation: the fallback for inputs varyAssertionHash declines,
// and the oracle its parity tests compare against.
func buildVaryKeySlow(vary string, reqHeader header.Map, policy *KeyPolicy) string {
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
func buildVaryKeyInto(dst []byte, fields []string, reqHeader header.Map, policy *KeyPolicy) int {
	n := 0
	for _, f := range fields {
		f = strings.TrimSpace(strings.ToLower(f))
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		n += copyOverflow(dst, n, f)
		n = appendByte(dst, n, '=')
		var val string
		if f == cookiePresenceField && policy.hasCookiePresence() {
			// Cookie-presence assertion (issue #768): the same
			// presence-bit string variantKeyCore hashes, or peers
			// reject every presence-keyed exchange. CookieAll joins
			// multi-line Cookie entries with the RFC 6265 §4.2
			// separator — GetAll's ", " join would fold the second
			// line's first pair into the first line's last value, where
			// the ";"-split never sees it and the bits hash wrong
			// (the RawRequest-derived map keeps lines separate).
			val = policy.cookiePresenceValue(reqHeader.CookieAll())
		} else {
			val = varyAssertionValue(f, reqHeader.Get(f), policy)
		}
		n += copyOverflow(dst, n, val)
		n = appendByte(dst, n, ';')
	}
	return n
}

// varyAssertionValue normalizes one Vary-nominated request header value
// for the stored-assertion hash (BuildVaryKey — the hex stored on
// objects and compared by the peer gates). Accept-Encoding is bucketed
// by encodingBucket unless the policy pins verbatim, and
// Accept-Language by langBucket (docs/plans/
// accept-encoding-bucketing.md §10; an unbucketable value falls back
// to the legacy normalization) — both rules MUST match varyHeaderValue
// (vary.go) or the peer gates reject every bucketed-variant exchange
// (ADR-0051; pinned by
// TestVaryKeyEncodingBucket_ParityAcrossPaths). Every other field keeps
// this path's legacy semantics: the four list-valued headers are
// lowercase+sorted, all other field values pass through verbatim —
// NOT the normalize-everything rule of varyHeaderValue. The two paths
// already disagreed before bucketing; unifying them would silently
// rekey every stored variant on routes with custom Vary headers.
func varyAssertionValue(field, value string, policy *KeyPolicy) string {
	switch field {
	case "accept-encoding":
		if !policy.verbatimEncoding() {
			return encodingBucket(value)
		}
	case "accept-language":
		if tag, ok := langBucket(value); ok {
			return strings.ToLower(tag)
		}
	}
	if isListValuedVaryField(field) {
		return normaliseListHeader(value)
	}
	return value
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
