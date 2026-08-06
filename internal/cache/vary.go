package cache

import (
	"net/http"
	"sort"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/bouine-cache/bouine/pkg/api"
)

// MaxVariants is the default cap on stored variants per primary key.
// Enforced by the handler: a Put is skipped when the variant count for
// a primary key exceeds this value, preventing Vary blow-up attacks.
const MaxVariants = 64

// maxVaryFields caps the number of Vary header fields we process.
// RFC 9110 does not limit Vary fields, but >16 is pathological and
// almost certainly an attack. The stack buffer for field names is sized
// accordingly.
const maxVaryFields = 16

// varyContainsStar reports whether the Vary header value contains "*"
// as one of its field names. "Vary: *, foo" and "Vary: foo, *" both
// mean "every request is unique" (RFC 9110 §12.5.5).
func varyContainsStar(vary string) bool {
	for f := range strings.SplitSeq(vary, ",") {
		if strings.TrimSpace(f) == "*" {
			return true
		}
	}
	return false
}

// VariantKey computes a composite storage key from the primary key and
// the Vary header. If the response has no Vary (or Vary contains *),
// only the primary key is used. Header names listed in exclude are
// skipped — the variant key is computed as if those headers were absent
// from the Vary list. When exclusion empties the Vary list entirely,
// the variant key collapses to the primary key.
//
// Zero-alloc fast path: when the Vary header has ≤ maxVaryFields fields
// and the total hash input fits in 256 bytes, the function uses a
// stack-allocated buffer and xxhash.Sum64 instead of allocating a
// *xxhash.Digest on the heap. Falls back to the allocation path for
// pathological inputs.
//
// The variant's collision guard is derived by XORing the vary hash into
// both of the primary's hashes (see api.Key.WithVary), so the variant's
// guard is independent from the primary's.
//
//nolint:gocyclo // 17: Vary header parsing is inherently branchy
func VariantKey(primary api.Key, vary string, reqHeader http.Header, policy *KeyPolicy) api.Key {
	if vary == "" {
		return primary
	}
	if varyContainsStar(vary) {
		h := xxhash.New()
		_, _ = h.WriteString("*")
		for k, vals := range reqHeader {
			_, _ = h.WriteString(k)
			for _, v := range vals {
				_, _ = h.WriteString(v)
			}
		}
		vHash := h.Sum64()
		return primary.WithVary(vHash)
	}

	// Parse and sort Vary field names using a stack-allocated array.
	// Avoids strings.Split []string allocation and sort.Strings slice.
	var fields [maxVaryFields]string
	n := 0
	for f := range strings.SplitSeq(vary, ",") {
		if n >= maxVaryFields {
			// Pathological Vary — fall back to alloc path.
			return variantKeySlow(primary, vary, reqHeader, policy)
		}
		fields[n] = strings.ToLower(strings.TrimSpace(f))
		n++
	}
	if n == 0 {
		return primary
	}
	// Inline insertion sort (n is typically 1-3, max 16).
	for i := 1; i < n; i++ {
		for j := i; j > 0 && fields[j-1] > fields[j]; j-- {
			fields[j-1], fields[j] = fields[j], fields[j-1]
		}
	}

	// Build hash input into a stack buffer and use xxhash.Sum64
	// (no heap allocation) instead of xxhash.New() (allocates *Digest).
	var buf [256]byte
	off := 0
	written := false
	for i := 0; i < n; i++ {
		f := fields[i]
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		val := normalizeHeaderValue(reqHeader.Get(f))
		needed := len(f) + 1 + len(val) + 1 // f=val;
		if off+needed > len(buf) {
			// Buffer overflow — fall back to alloc path.
			return variantKeySlow(primary, vary, reqHeader, policy)
		}
		off += copy(buf[off:], f)
		buf[off] = '='
		off++
		off += copy(buf[off:], val)
		buf[off] = ';'
		off++
		written = true
	}
	if !written {
		return primary
	}
	vHash := xxhash.Sum64(buf[:off])
	return primary.WithVary(vHash)
}

// variantKeySlow is the fallback allocation path for Vary headers that
// exceed the stack buffer limits (too many fields or too much data).
func variantKeySlow(primary api.Key, vary string, reqHeader http.Header, policy *KeyPolicy) api.Key {
	fields := strings.Split(strings.ToLower(vary), ",")
	for i, f := range fields {
		fields[i] = strings.TrimSpace(f)
	}
	sort.Strings(fields)
	h := xxhash.New()
	written := false
	for _, f := range fields {
		if policy != nil && policy.ShouldExcludeHeader(f) {
			continue
		}
		_, _ = h.WriteString(f)
		_, _ = h.WriteString("=")
		val := normalizeHeaderValue(reqHeader.Get(f))
		_, _ = h.WriteString(val)
		_, _ = h.WriteString(";")
		written = true
	}
	if !written {
		return primary
	}
	vHash := h.Sum64()
	return primary.WithVary(vHash)
}

// normalizeHeaderValue lowercases and sorts comma-separated tokens in
// a header value so "en, FR" and "fr, en" produce the same key.
func normalizeHeaderValue(v string) string {
	v = strings.TrimSpace(v)
	if !strings.Contains(v, ",") {
		return strings.ToLower(v)
	}
	parts := strings.Split(v, ",")
	for i, p := range parts {
		parts[i] = strings.TrimSpace(strings.ToLower(p))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}
