package cache

import (
	"net/url"
	"strings"

	"github.com/bouine-cache/bouine/pkg/header"
)

// RequestInfo is a lightweight snapshot of request fields needed by
// cache functions. It keeps the cache package's leaf files free
// of HTTP server dependencies.
//
// To avoid string([]byte) allocations on the miss path, fields can be
// populated either as strings (from test helpers / URL parsing) or as
// []byte slices (from *fasthttp.RequestCtx). The GetMethod/GetURI/
// GetHost/GetPath methods return the string form, converting lazily
// from the []byte form only when called. On the hot miss path, only
// GetPath and GetHost are called (by buildObject); Method and URI
// are only needed on cold paths (background revalidation).
type RequestInfo struct {
	Method     string
	URI        string
	Host       string
	Path       string
	RemoteAddr string
	TLS        bool
	Header     header.Map

	// []byte forms populated by requestInfoFromCtx. These reference the
	// *fasthttp.RequestCtx's internal buffers directly (zero-copy).
	// When non-nil, the corresponding string field is empty and is
	// materialized on demand by the Get* methods.
	methodBytes []byte
	uriBytes    []byte
	hostBytes   []byte
	pathBytes   []byte
}

// GetMethod returns the request method as a string, converting from
// the []byte form if necessary.
func (ri RequestInfo) GetMethod() string {
	if ri.Method != "" {
		return ri.Method
	}
	return string(ri.methodBytes)
}

// GetURI returns the request URI as a string, converting from the
// []byte form if necessary.
func (ri RequestInfo) GetURI() string {
	if ri.URI != "" {
		return ri.URI
	}
	return string(ri.uriBytes)
}

// GetHost returns the request host as a string, converting from the
// []byte form if necessary.
func (ri RequestInfo) GetHost() string {
	if ri.Host != "" {
		return ri.Host
	}
	return string(ri.hostBytes)
}

// GetPath returns the request path as a string, converting from the
// []byte form if necessary.
func (ri RequestInfo) GetPath() string {
	if ri.Path != "" {
		return ri.Path
	}
	return string(ri.pathBytes)
}

// requestInfoFromHTTP builds a RequestInfo from individual request
// fields. Used by test helpers that work with net/http request fixtures.
func requestInfoFromHTTP(method, uri, host, path string, tls bool, hdr header.Map) RequestInfo {
	return RequestInfo{
		Method: method,
		URI:    uri,
		Host:   host,
		Path:   path,
		TLS:    tls,
		Header: hdr,
	}
}

// extractRawQuery extracts the raw query string from a URI.
// Returns "" if there is no query component.
func extractRawQuery(uri string) string {
	if i := strings.IndexByte(uri, '?'); i >= 0 {
		return uri[i+1:]
	}
	return ""
}

// requestInfoFromURL creates a RequestInfo from a method and URL string.
// Used by tests that construct requests from URL strings.
func requestInfoFromURL(method, rawURL string) RequestInfo { //nolint:unparam
	u, err := url.Parse(rawURL)
	if err != nil {
		return RequestInfo{Method: method, URI: rawURL}
	}
	scheme := u.Scheme == "https"
	return RequestInfo{
		Method: method,
		URI:    rawURL,
		Host:   u.Host,
		Path:   u.Path,
		TLS:    scheme,
		Header: header.Map{},
	}
}
