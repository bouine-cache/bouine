package cache

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/pkg/api"
	"github.com/thylong/bouine/pkg/header"
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

// ServeRange handles a Range request against a fully cached body. It
// parses the Range header, validates it, and writes a 206 Partial Content
// response. Both single-range and multi-range (multipart/byteranges) are
// supported. Returns true if a range response was written.
//
// stale controls the X-Cache label: true for StaleHit (STALE + Warning 110),
// false for a fresh Hit (HIT).
func ServeRange(w http.ResponseWriter, r *http.Request, obj *api.Object, stale bool) bool {
	rangeHeader := r.Header.Get(header.Range)
	if rangeHeader == "" {
		return false
	}
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return false
	}
	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	specs := strings.Split(spec, ",")

	// Normalise: validate and resolve all ranges first.
	type resolvedRange struct{ start, end int64 }
	ranges := make([]resolvedRange, 0, len(specs))
	for _, s := range specs {
		start, end, ok := parseRange(strings.TrimSpace(s), obj.BodySize)
		if !ok {
			w.Header().Set(header.ContentRange, "bytes */"+strconv.FormatInt(obj.BodySize, 10))
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return true
		}
		ranges = append(ranges, resolvedRange{start, end})
	}

	// Copy stored response headers (skip Content-Length; we replace it).
	obj.Header.Range(func(k, v string) bool {
		if strings.EqualFold(k, header.ContentLength) {
			return true
		}
		w.Header().Add(k, v)
		return true
	})

	if stale {
		w.Header()[header.XCache] = headerSTALE
		w.Header()[header.Warning] = []string{`110 - "Response is Stale"`}
	} else {
		w.Header()[header.XCache] = headerHIT
	}

	if len(ranges) == 1 {
		// Single-range: standard 206.
		ra := ranges[0]
		length := ra.end - ra.start + 1
		w.Header().Set(header.ContentRange,
			"bytes "+strconv.FormatInt(ra.start, 10)+"-"+
				strconv.FormatInt(ra.end, 10)+"/"+
				strconv.FormatInt(obj.BodySize, 10))
		w.Header().Set(header.ContentLength, strconv.FormatInt(length, 10))
		w.WriteHeader(http.StatusPartialContent)
		if r.Method != http.MethodHead {
			_, _ = w.Write(obj.Body[ra.start : ra.end+1])
		}
		return true
	}

	// Multi-range: multipart/byteranges (RFC 7233 §4.1).
	boundary := "bouine-range-" + strconv.FormatUint(uint64(obj.Key), 16)
	contentType := "multipart/byteranges; boundary=" + boundary
	w.Header().Set(header.ContentType, contentType)
	w.WriteHeader(http.StatusPartialContent)
	if r.Method == http.MethodHead {
		return true
	}
	ct := obj.Header.Get(header.ContentType)
	if ct == "" {
		ct = "application/octet-stream"
	}
	for _, ra := range ranges {
		_, _ = fmt.Fprintf(w, "\r\n--%s\r\nContent-Type: %s\r\nContent-Range: bytes %d-%d/%d\r\n\r\n",
			boundary, ct, ra.start, ra.end, obj.BodySize)
		_, _ = w.Write(obj.Body[ra.start : ra.end+1])
	}
	_, _ = fmt.Fprintf(w, "\r\n--%s--\r\n", boundary)
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
