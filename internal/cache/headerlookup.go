package cache

import (
	"strings"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/header"
)

// headerLookup provides a uniform read interface over response headers
// from either a header.Map or a *fasthttp.ResponseHeader.
type headerLookup struct {
	hdr     header.Map
	fastHdr *fasthttp.ResponseHeader
}

func fromHeaderMap(h header.Map) headerLookup {
	return headerLookup{hdr: h}
}

func (h headerLookup) Get(key string) string {
	if h.fastHdr != nil {
		return string(h.fastHdr.Peek(key))
	}
	return h.hdr.Get(key)
}

func (h headerLookup) GetAll(key string) string {
	if h.fastHdr != nil {
		values := h.fastHdr.PeekAll(key)
		if len(values) == 0 {
			return ""
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
