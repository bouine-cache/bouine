package cache

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// ServeRange handles a Range request against a fully cached body. It
// parses the Range header, validates it, and writes a 206 Partial Content
// response. Both single-range and multi-range (multipart/byteranges) are
// supported. Returns true if a range response was written.
//
// stale controls the X-Cache label: true for StaleHit (STALE + Warning 110),
// false for a fresh Hit (HIT). src is the storage-tier source (hot/warm/peer),
// set as X-Cache-Source via direct map assignment (zero alloc).
func ServeRange(w http.ResponseWriter, r *http.Request, obj *api.Object, stale bool, src api.Source) bool {
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
		w.Header()[header.XCacheSource] = sourceSlice(src)
		w.Header()[header.Warning] = []string{`110 - "Response is Stale"`}
	} else {
		w.Header()[header.XCache] = headerHIT
		w.Header()[header.XCacheSource] = sourceSlice(src)
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
	boundary := "bouine-range-" + strconv.FormatUint(obj.Key.Primary(), 16)
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
