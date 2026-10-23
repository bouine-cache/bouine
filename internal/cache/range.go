package cache

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// rangeWriter is the interface ServeRange needs from the response writer.
type rangeWriter interface {
	SetHeader(key, value string)
	WriteHeader(int)
	Write([]byte) (int, error)
}

// serveRange handles a Range request against a fully cached body.
// Under downstream_freshness: sync the 206 serves carry the same
// synthesized CDN-Cache-Control cap as the corresponding 200 serve
// (ADR-0063 behavior matrix), and the origin's CDC-family lines are
// skipped in the stored-header copy (invariant I3).
func (h *Handler) serveRange(w rangeWriter, ri RequestInfo, obj *api.Object, stale bool, now time.Time, src api.Source) bool {
	rangeHeader := ri.Header.Get(header.Range)
	if rangeHeader == "" {
		return false
	}
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return false
	}
	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	specs := strings.Split(spec, ",")

	type resolvedRange struct{ start, end int64 }
	ranges := make([]resolvedRange, 0, len(specs))
	for _, s := range specs {
		start, end, ok := parseRange(strings.TrimSpace(s), obj.BodySize)
		if !ok {
			w.SetHeader(header.ContentRange, "bytes */"+strconv.FormatInt(obj.BodySize, 10))
			w.WriteHeader(416)
			return true
		}
		ranges = append(ranges, resolvedRange{start, end})
	}

	copyStoredHeaders(w, obj, h.syncCDC)
	xCache := "HIT"
	if stale {
		xCache = "STALE"
		w.SetHeader(header.Warning, `110 - "Response is Stale"`)
	}
	w.SetHeader(header.XCache, xCache)
	w.SetHeader(header.XCacheSource, string(src))
	h.setRangeCDC(w, obj, stale, now)

	if len(ranges) == 1 {
		ra := ranges[0]
		length := ra.end - ra.start + 1
		w.SetHeader(header.ContentRange,
			"bytes "+strconv.FormatInt(ra.start, 10)+"-"+
				strconv.FormatInt(ra.end, 10)+"/"+
				strconv.FormatInt(obj.BodySize, 10))
		w.SetHeader(header.ContentLength, strconv.FormatInt(length, 10))
		w.WriteHeader(206)
		if ri.GetMethod() != "HEAD" {
			_, _ = w.Write(obj.Body[ra.start : ra.end+1])
		}
		return true
	}

	boundary := "bouine-range-" + obj.Key.Hex()
	w.SetHeader(header.ContentType, "multipart/byteranges; boundary="+boundary)
	w.WriteHeader(206)
	if ri.GetMethod() == "HEAD" {
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

// copyStoredHeaders copies the stored object's headers to w, skipping
// Content-Length (recomputed for the served range) and, under
// downstream_freshness: sync (ADR-0063 I3), the origin's CDC-family
// lines: the synthesized cap below is authoritative.
func copyStoredHeaders(w rangeWriter, obj *api.Object, syncCDC bool) {
	obj.Header.Range(func(k, v string) bool {
		if strings.EqualFold(k, header.ContentLength) {
			return true
		}
		if syncCDC && isCDCFamilyHeader(k) {
			return true
		}
		w.SetHeader(k, v)
		return true
	})
}

// setRangeCDC writes the synthesized CDN-Cache-Control on a 206 serve
// (ADR-0063 behavior matrix: the cap rides along, like Warning: 110).
// No-op under passthrough.
func (h *Handler) setRangeCDC(w rangeWriter, obj *api.Object, stale bool, now time.Time) {
	if !h.syncCDC {
		return
	}
	var cdcBuf [80]byte
	result := cacheHit
	if stale {
		result = cacheStale
	}
	w.SetHeader(header.CDNCacheControl,
		string(appendServeCDCDirectives(cdcBuf[:0], obj, now, result)))
}

func parseRange(spec string, size int64) (int64, int64, bool) {
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}

	startStr := strings.TrimSpace(parts[0])
	endStr := strings.TrimSpace(parts[1])

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
