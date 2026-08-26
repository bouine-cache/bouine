package cache

import (
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// headerLookup provides a uniform read interface over response headers
// from either a header.Map or a *fasthttp.ResponseHeader.
type headerLookup struct {
	fastHdr *fasthttp.ResponseHeader
	hdr     header.Map
}

func fromHeaderMap(h header.Map) headerLookup {
	return headerLookup{hdr: h}
}

// fromFastHTTPHeader creates a headerLookup backed directly by a
// *fasthttp.ResponseHeader. This enables CopyToFastHTTP to use
// ResponseHeader.CopyTo (bulk struct copy) instead of per-header
// SetCanonical, and defers the FromFastHTTP conversion to ToMap()
// until buildObject actually needs the header.Map.
func fromFastHTTPHeader(h *fasthttp.ResponseHeader) headerLookup {
	return headerLookup{fastHdr: h}
}

func (h headerLookup) Get(key string) string {
	if h.fastHdr != nil {
		v := h.fastHdr.Peek(key)
		if len(v) == 0 {
			return ""
		}
		return string(v)
	}
	return h.hdr.Get(key)
}

func (h headerLookup) GetAll(key string) string {
	if h.fastHdr != nil {
		values := h.fastHdr.PeekAll(key)
		if len(values) == 0 {
			return ""
		}
		if len(values) == 1 {
			return string(values[0])
		}
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = string(v)
		}
		return strings.Join(parts, ", ")
	}
	return h.hdr.GetAll(key)
}

func (h headerLookup) Has(key string) bool {
	if h.fastHdr != nil {
		return h.fastHdr.Peek(key) != nil
	}
	return h.hdr.Has(key)
}

func (h headerLookup) VisitAll(fn func(key, value string)) {
	if h.fastHdr != nil {
		for k, v := range h.fastHdr.All() {
			fn(string(k), string(v))
		}
		return
	}
	h.hdr.Range(func(key, value string) bool {
		fn(key, value)
		return true
	})
}

func (h headerLookup) ToMap() header.Map {
	if h.fastHdr != nil {
		return header.FromFastHTTP(h.fastHdr)
	}
	return h.hdr
}

// CopyToFastHTTP copies all headers to dst using the fastest available
// method. When the underlying source is a *fasthttp.ResponseHeader, it
// uses CopyTo (a bulk struct copy without per-header normalization).
// Otherwise it falls back to WriteToFastHTTP (per-header SetCanonical).
func (h headerLookup) CopyToFastHTTP(dst *fasthttp.ResponseHeader) {
	if h.fastHdr != nil {
		h.fastHdr.CopyTo(dst)
		return
	}
	h.hdr.WriteToFastHTTP(dst)
}
