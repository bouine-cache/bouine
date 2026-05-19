package cache

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/pkg/api"
)

// VaryConfig controls per-route Vary behavior.
type VaryConfig struct {
	// MaxVariants caps the number of stored variants per primary key.
	// Zero means the package default (64).
	MaxVariants int
}

// MaxVariants is the default cap on stored variants per primary key.
// Used by the handler to enforce the Vary blow-up limit (PLAN.md §3).
const MaxVariants = 64

// VariantKey computes a composite storage key from the primary key and
// the Vary header. If the response has no Vary (or Vary: *), only the
// primary key is used.
func VariantKey(primary api.Key, vary string, reqHeader http.Header) api.Key {
	if vary == "" {
		return primary
	}
	if vary == "*" {
		// Vary: * means every request is unique — use a different key
		// each time by hashing the full request headers. In practice
		// this makes the object uncacheable by the Evaluate logic, but
		// we handle it here defensively.
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
	for _, f := range fields {
		f = strings.TrimSpace(f)
		_, _ = h.WriteString(f)
		_, _ = h.WriteString("=")
		_, _ = h.WriteString(reqHeader.Get(f))
		_, _ = h.WriteString(";")
	}
	return api.Key(uint64(primary) ^ h.Sum64())
}

// ServeRange handles a Range request against a fully cached body. It
// parses the Range header, validates it, and writes a 206 Partial
// Content response with the appropriate Content-Range header.
//
// Only single-range requests are supported in phase 3; multi-range
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
