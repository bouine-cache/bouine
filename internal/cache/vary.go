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

// varyContainsStar reports whether the Vary header value contains "*"
// as one of its field names. "Vary: *, foo" and "Vary: foo, *" both
// mean "every request is unique" (RFC 9110 §12.5.5).
func varyContainsStar(vary string) bool {
	for _, f := range strings.Split(vary, ",") {
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
func VariantKey(primary api.Key, vary string, reqHeader http.Header, exclude ...map[string]bool) api.Key {
	if vary == "" {
		return primary
	}
	var excludeSet map[string]bool
	if len(exclude) > 0 {
		excludeSet = exclude[0]
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
		return api.Key(uint64(primary) ^ h.Sum64())
	}

	fields := strings.Split(strings.ToLower(vary), ",")
	for i, f := range fields {
		fields[i] = strings.TrimSpace(f)
	}
	sort.Strings(fields)
	h := xxhash.New()
	written := false
	for _, f := range fields {
		if excludeSet != nil && excludeSet[f] {
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
	return api.Key(uint64(primary) ^ h.Sum64())
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
