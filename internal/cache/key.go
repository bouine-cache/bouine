// Package cache is the L4 cache engine. It implements the RFC 9111
// cache state machine described in PLAN.md §3.
//
// The engine is deterministic: its only inputs are an http.Request,
// the matching *api.Object (if any), and the current time. Its output
// is a Decision (HIT, MISS, REVALIDATE, STALE_HIT, BYPASS).
//
// The engine does NOT do I/O. It evaluates headers and metadata and
// returns a decision; the caller (the pipeline) acts on it.
package cache

import (
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/thylong/bouine/pkg/api"
)

// BuildKey constructs the canonical primary cache key from a request.
// The key is deterministic and stable across nodes (PLAN.md §3.2).
func BuildKey(r *http.Request) api.Key {
	h := xxhash.New()

	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	_, _ = h.WriteString(scheme)
	_, _ = h.WriteString("|")

	host := canonicalHost(r.Host)
	_, _ = h.WriteString(host)
	_, _ = h.WriteString("|")

	path := canonicalPath(r.URL)
	_, _ = h.WriteString(path)
	_, _ = h.WriteString("|")

	query := canonicalQuery(r.URL)
	_, _ = h.WriteString(query)
	_, _ = h.WriteString("|")

	method := r.Method
	if method == http.MethodHead {
		method = http.MethodGet
	}
	_, _ = h.WriteString(method)

	return api.Key(h.Sum64())
}

// BuildVaryKey constructs the secondary key from the Vary header
// values in the response and the corresponding request headers.
func BuildVaryKey(vary string, reqHeader http.Header) string {
	if vary == "" || vary == "*" {
		return vary
	}

	fields := strings.Split(vary, ",")
	sort.Strings(fields)

	h := xxhash.New()
	for _, f := range fields {
		f = strings.TrimSpace(strings.ToLower(f))
		_, _ = h.WriteString(f)
		_, _ = h.WriteString("=")
		_, _ = h.WriteString(reqHeader.Get(f))
		_, _ = h.WriteString(";")
	}
	return strconv.FormatUint(h.Sum64(), 36)
}

func canonicalHost(host string) string {
	h := strings.ToLower(host)
	if idx := strings.LastIndex(h, ":"); idx > 0 {
		port := h[idx+1:]
		h = h[:idx]
		if port == "80" || port == "443" {
			return h
		}
		return h + ":" + port
	}
	return h
}

func canonicalPath(u *url.URL) string {
	p := u.Path
	if p == "" {
		p = "/"
	}
	// Collapse duplicate slashes.
	for strings.Contains(p, "//") {
		p = strings.ReplaceAll(p, "//", "/")
	}
	return p
}

func canonicalQuery(u *url.URL) string {
	if u.RawQuery == "" {
		return ""
	}
	params := u.Query()
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteByte('&')
		}
		vals := params[k]
		sort.Strings(vals)
		for j, v := range vals {
			if j > 0 {
				sb.WriteByte('&')
			}
			sb.WriteString(url.QueryEscape(k))
			sb.WriteByte('=')
			sb.WriteString(url.QueryEscape(v))
		}
	}
	return sb.String()
}
