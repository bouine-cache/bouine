package cache

import (
	"net/http"
	"strings"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// conditional.go handles conditional request matching (304 / If-None-Match
// / If-Modified-Since), ETag manipulation, and header merging for
// revalidated responses.

// ClientConditionalMatch checks if a cached object satisfies the
// client's conditional headers (If-None-Match / If-Modified-Since).
// If it matches, the handler should return 304 instead of 200.
func ClientConditionalMatch(r *http.Request, obj *api.Object) bool {
	// If-None-Match takes precedence (RFC 9110 §13.1.2).
	if inm := r.Header.Get(header.IfNoneMatch); inm != "" {
		if obj.ETag != "" && etagMatch(inm, obj.ETag) {
			return true
		}
		return false
	}
	// If-Modified-Since (RFC 9110 §13.1.3).
	if ims := r.Header.Get(header.IfModifiedSince); ims != "" {
		imsTime := parseHTTPDate(ims)
		if imsTime.IsZero() {
			return false
		}
		if !obj.LastModified.IsZero() && !obj.LastModified.After(imsTime) {
			return true
		}
		// Fall back to Date header then StoredAt if no Last-Modified.
		if obj.LastModified.IsZero() {
			if d := obj.Header.Get(header.Date); d != "" {
				if dt := parseHTTPDate(d); !dt.IsZero() && !dt.After(imsTime) {
					return true
				}
			}
		}
	}
	return false
}

// etagMatch checks if needle matches any ETag in the comma-separated
// list (which may contain "*" or quoted tags). Weak comparison used
// per RFC 9110 §8.8.3.2.
func etagMatch(list, needle string) bool {
	if list == "*" {
		return true
	}
	// Normalize: strip W/ prefix for weak comparison.
	norm := func(s string) string {
		s = strings.TrimSpace(s)
		if len(s) >= 2 && (s[0] == 'W' || s[0] == 'w') && s[1] == '/' {
			s = s[2:]
		}
		return strings.Trim(s, "\"")
	}
	needleNorm := norm(needle)
	for _, tag := range strings.Split(list, ",") {
		if norm(tag) == needleNorm {
			return true
		}
	}
	return false
}

// MergeHeaders304 merges headers from a 304 response into the stored
// object per RFC 9111 §3.2. The 304 response's headers update the
// stored response, except for content-specific headers.
func MergeHeaders304(stored *api.Object, resp304Header http.Header) {
	// Headers that MUST NOT be updated from a 304 (content-specific).
	// Set-Cookie is excluded because SetValues joins multi-values with
	// ", " which is non-conformant per RFC 9110 §5.2.
	skip := map[string]bool{
		header.ContentLength:    true,
		header.ContentEncoding:  true,
		header.TransferEncoding: true,
		header.SetCookie:        true,
	}
	for k, vals := range resp304Header {
		if skip[k] {
			continue
		}
		stored.Header.SetValues(k, vals)
	}
}

// ConditionalHeaders sets If-None-Match and If-Modified-Since on a
// revalidation request from the stored object's validators.
func ConditionalHeaders(req *http.Request, obj *api.Object) {
	if obj.ETag != "" {
		// Ensure the ETag is properly quoted (RFC 9110 §8.8.3).
		etag := quoteETag(obj.ETag)
		req.Header.Set(header.IfNoneMatch, etag)
	}
	if !obj.LastModified.IsZero() {
		req.Header.Set(header.IfModifiedSince,
			obj.LastModified.UTC().Format(http.TimeFormat))
	}
}

// quoteETag ensures an ETag value is properly quoted. Unquoted ETags
// like "abcdef" become "\"abcdef\"". Weak ETags like W/"abcdef" are
// left as-is. Already-quoted ETags are returned unchanged.
func quoteETag(etag string) string {
	if etag == "" {
		return etag
	}
	// Already quoted (starts with " or W/).
	if etag[0] == '"' || (len(etag) >= 2 && (etag[0] == 'W' || etag[0] == 'w') && etag[1] == '/') {
		return etag
	}
	return "\"" + etag + "\""
}
