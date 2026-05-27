package cache

import (
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/pkg/api"
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
// only the primary key is used.
func VariantKey(primary api.Key, vary string, reqHeader http.Header) api.Key {
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
		return api.Key(uint64(primary) ^ h.Sum64())
	}

	h := xxhash.New()
	fields := strings.Split(strings.ToLower(vary), ",")
	for i, f := range fields {
		fields[i] = strings.TrimSpace(f)
	}
	sort.Strings(fields)
	for _, f := range fields {
		_, _ = h.WriteString(f)
		_, _ = h.WriteString("=")
		// Normalize header value: trim, lowercase, sort comma-separated
		// tokens. This handles Accept-Language: "en, fr" vs "fr, en".
		val := normalizeHeaderValue(reqHeader.Get(f))
		_, _ = h.WriteString(val)
		_, _ = h.WriteString(";")
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

// ServeRange handles a Range request against a fully cached body. It
// parses the Range header, validates it, and writes a 206 Partial
// Content response with the appropriate Content-Range header.
//
// Only single-range requests are supported; multi-range
// (multipart/byteranges) is deferred.
//
// Returns true if the range was served, false if the caller should
// serve the full body instead (malformed range, unsatisfiable, etc).
func ServeRange(w http.ResponseWriter, r *http.Request, obj *api.Object) bool {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return false
	}

	// Only support "bytes=start-end" form.
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return false
	}
	spec := strings.TrimPrefix(rangeHeader, "bytes=")

	// Reject multi-range.
	if strings.Contains(spec, ",") {
		return false
	}

	start, end, ok := parseRange(spec, obj.BodySize)
	if !ok {
		// 416 Range Not Satisfiable.
		w.Header().Set("Content-Range", "bytes */"+strconv.FormatInt(obj.BodySize, 10))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return true
	}

	// Write response headers from stored object.
	for k, vals := range obj.Header {
		for _, v := range vals {
			w.Header().Add(k, v)
		}
	}
	length := end - start + 1
	w.Header().Set("Content-Range",
		"bytes "+strconv.FormatInt(start, 10)+"-"+
			strconv.FormatInt(end, 10)+"/"+
			strconv.FormatInt(obj.BodySize, 10))
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.WriteHeader(http.StatusPartialContent)

	if r.Method != http.MethodHead {
		_, _ = w.Write(obj.Body[start : end+1])
	}
	return true
}

// parseRange parses a "start-end" byte range spec. Returns the
// inclusive start and end offsets and whether the range is valid.
func parseRange(spec string, size int64) (int64, int64, bool) {
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

	// Suffix range: "-500" means last 500 bytes.
	if startStr == "" {
		suffix, err := strconv.ParseInt(endStr, 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		start := size - suffix
		if start < 0 {
			start = 0
		}
		return start, size - 1, true
	}

	start, err := strconv.ParseInt(startStr, 10, 64)
	if err != nil || start < 0 || start >= size {
		return 0, 0, false
	}

	// Open-ended range: "500-" means from 500 to end.
	if endStr == "" {
		return start, size - 1, true
	}

	end, err := strconv.ParseInt(endStr, 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}
	if end >= size {
		end = size - 1
	}
	return start, end, true
}
