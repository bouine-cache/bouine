package cache

import (
	"net/http"

	"github.com/valyala/fasthttp"
)

// headerLookup provides a uniform read interface over response headers
// from either net/http (http.Header) or fasthttp (*fasthttp.ResponseHeader).
// This allows fetchResult to carry fasthttp response headers without
// changing every consumer function.
type headerLookup struct {
	httpHdr http.Header
	fastHdr *fasthttp.ResponseHeader
}

// fromHTTPHeader wraps an http.Header in a headerLookup.
func fromHTTPHeader(h http.Header) headerLookup {
	return headerLookup{httpHdr: h}
}

// fromFastHeader wraps a *fasthttp.ResponseHeader in a headerLookup.
// The caller must ensure the ResponseHeader is not reset/reused while
// the headerLookup is alive.
func fromFastHeader(h *fasthttp.ResponseHeader) headerLookup {
	return headerLookup{fastHdr: h}
}

// Get returns the first value for the given header key (case-insensitive).
// Returns "" if the key is not present.
func (h headerLookup) Get(key string) string {
	if h.fastHdr != nil {
		return string(h.fastHdr.Peek(key))
	}
	return h.httpHdr.Get(key)
}

// Has reports whether the given header key is present.
func (h headerLookup) Has(key string) bool {
	if h.fastHdr != nil {
		return h.fastHdr.Peek(key) != nil
	}
	return h.httpHdr.Get(key) != ""
}

// VisitAll calls fn for each header key-value pair.
func (h headerLookup) VisitAll(fn func(key, value string)) {
	if h.fastHdr != nil {
		for k, v := range h.fastHdr.All() {
			fn(string(k), string(v))
		}
		return
	}
	for k, vals := range h.httpHdr {
		for _, v := range vals {
			fn(k, v)
		}
	}
}

// ToHTTP converts the headerLookup to an http.Header. Used by
// buildObject which needs http.Header for header.FromHTTP and
// other functions that still expect the map type.
func (h headerLookup) ToHTTP() http.Header {
	if h.fastHdr != nil {
		out := make(http.Header, 8)
		for k, v := range h.fastHdr.All() {
			out.Add(string(k), string(v))
		}
		return out
	}
	return h.httpHdr
}
