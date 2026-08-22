package cache

import (
	"net/url"
	"strings"

	"github.com/bouine-cache/bouine/pkg/header"
)

// RequestInfo is a lightweight snapshot of request fields needed by
// cache functions that previously took *http.Request. It avoids
// importing net/http in the cache package's leaf files.
type RequestInfo struct {
	Method     string
	URI        string
	Host       string
	Path       string
	RemoteAddr string
	TLS        bool
	Header     header.Map
}

// requestInfoFromHTTP builds a RequestInfo from an *http.Request.
// Used by handler.go (which still imports net/http for the shim).
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
