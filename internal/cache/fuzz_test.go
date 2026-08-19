package cache

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// FuzzBuildKey fuzzes the cache key builder with arbitrary method, host,
// path, and query strings. BuildKey must never panic and must be
// deterministic: identical inputs produce identical keys.
func FuzzBuildKey(f *testing.F) {
	f.Add("GET", "example.com", "/api/v1/products", "sort=price&order=asc")
	f.Add("HEAD", "Example.COM", "/", "")
	f.Add("POST", "localhost:8080", "/upload", "")
	f.Add("GET", "example.com:80", "/products", "")
	f.Add("GET", "example.com:443", "/products", "")
	f.Add("GET", "EXAMPLE.COM", "/products", "")
	f.Add("GET", "example.com", "//double///slash", "")
	f.Add("GET", "example.com", "", "")
	f.Add("GET", "example.com", "/", "a=1&b=2&b=3&a=1")
	f.Add("", "", "", "")
	f.Add("GET", "example.com", "/path with spaces", "")
	f.Add("GET", "example.com", "/path?already=has=query", "extra=true")

	f.Fuzz(func(t *testing.T, method, host, path, query string) {
		// Limit input sizes to prevent pathological cases from stalling
		// the fuzzer (e.g. extremely long query strings that cause
		// url.Query() to allocate gigabytes on the slow path).
		if len(method) > 64 || len(host) > 512 || len(path) > 4096 || len(query) > 4096 {
			t.Skip()
		}

		// Build the URL manually instead of httptest.NewRequest, which
		// panics on methods containing spaces or other invalid tokens.
		rawURL := "http://" + host + path
		if query != "" {
			rawURL += "?" + query
		}
		u, err := url.Parse(rawURL)
		if err != nil {
			t.Skip()
		}
		r := &http.Request{
			Method: method,
			URL:    u,
			Host:   host,
		}

		k1 := BuildKey(r, nil)
		k2 := BuildKey(r, nil)
		if k1 != k2 {
			t.Fatalf("BuildKey not deterministic: %v != %v", k1, k2)
		}
	})
}

// FuzzParseCacheControl fuzzes the Cache-Control header parser with
// arbitrary strings. It must never panic and must be deterministic:
// parsing the same header twice yields identical Directives.
func FuzzParseCacheControl(f *testing.F) {
	f.Add("no-cache")
	f.Add("no-store")
	f.Add("max-age=3600")
	f.Add("public, max-age=600")
	f.Add("s-maxage=600")
	f.Add("stale-while-revalidate=60")
	f.Add("stale-if-error=300")
	f.Add(`no-cache="Set-Cookie"`)
	f.Add("public, max-age=600, stale-while-revalidate=30, stale-if-error=120")
	f.Add("")
	f.Add(",")
	f.Add(",,,")
	f.Add("garbage with spaces and = signs but no valid tokens")
	f.Add("max-age=99999999999999999999")
	f.Add("max-age=-1")
	f.Add("no-cache, no-store, must-revalidate, proxy-revalidate")
	f.Add(`private="field1, field2"`)
	f.Add("immutable")
	f.Add("only-if-cached")
	f.Add("no-transform")

	f.Fuzz(func(t *testing.T, cc string) {
		d1 := ParseCacheControl(cc)
		d2 := ParseCacheControl(cc)
		if d1 != d2 {
			t.Fatalf("ParseCacheControl not deterministic for %q", cc)
		}
	})
}

// FuzzVariantKey fuzzes the Vary-based variant key computation. It must
// never panic, be deterministic, and produce the same key when the Vary
// header fields are reversed — the core security property that prevents
// semantically identical responses from getting different cache entries.
func FuzzVariantKey(f *testing.F) {
	f.Add("Accept", `{"Accept": ["text/html"]}`)
	f.Add("Accept-Encoding", `{"Accept-Encoding": ["gzip, br"]}`)
	f.Add("Accept, Accept-Language", `{"Accept": ["text/html"], "Accept-Language": ["en-US"]}`)
	f.Add("Accept-Language, Accept", `{"Accept": ["text/html"], "Accept-Language": ["en-US"]}`)
	f.Add("*", `{"Accept": ["text/html"]}`)
	f.Add("", `{"Accept": ["text/html"]}`)
	f.Add("Accept-Encoding", `{"Accept-Encoding": ["gzip,br"]}`)
	f.Add("Accept-Encoding", `{"Accept-Encoding": ["br, gzip"]}`)
	f.Add("X-Custom", `{"X-Custom": ["value with spaces"]}`)
	f.Add("Accept, Accept, Accept", `{"Accept": ["text/html"]}`)

	f.Fuzz(func(t *testing.T, vary, headerJSON string) {
		var hdrs http.Header
		if err := json.Unmarshal([]byte(headerJSON), &hdrs); err != nil {
			t.Skip()
		}

		primary := api.Key{}

		k1 := VariantKey(primary, vary, hdrs, nil)
		k2 := VariantKey(primary, vary, hdrs, nil)
		if k1 != k2 {
			t.Fatalf("VariantKey not deterministic for vary=%q headers=%q", vary, headerJSON)
		}

		// Commutativity: reordering Vary fields must produce the same
		// key. VariantKey sorts fields internally, so "A, B" and "B, A"
		// must yield identical keys.
		if vary != "" && !strings.Contains(vary, "*") {
			fields := strings.Split(vary, ",")
			if len(fields) > 1 {
				for i, j := 0, len(fields)-1; i < j; i, j = i+1, j-1 {
					fields[i], fields[j] = fields[j], fields[i]
				}
				reversed := strings.Join(fields, ",")
				kRev := VariantKey(primary, reversed, hdrs, nil)
				if k1 != kRev {
					t.Fatalf("VariantKey not commutative: %q vs %q -> %v != %v",
						vary, reversed, k1, kRev)
				}
			}
		}
	})
}

// FuzzEvaluate fuzzes the RFC 9111 cache state machine with arbitrary
// request methods, Cache-Control headers, response Cache-Control headers,
// TTL values, origin age, and elapsed time offsets. Evaluate must never
// panic and must be deterministic for the same inputs. It exercises the
// hit, miss (nil obj), stale, revalidate, and bypass paths, including
// the no-validator path where stale/no-cache objects cannot revalidate.
func FuzzEvaluate(f *testing.F) {
	f.Add("GET", "no-cache", "public, max-age=600", 600, 0, 100, false, true)
	f.Add("GET", "no-store", "public, max-age=600", 600, 0, 0, false, true)
	f.Add("GET", "", "public, max-age=600", 600, 0, 300, false, true)
	f.Add("GET", "", "public, max-age=600", 600, 0, 700, false, true)
	f.Add("GET", "", "no-cache", 600, 0, 0, false, true)
	f.Add("GET", "", "public, max-age=600, stale-while-revalidate=60", 600, 0, 650, false, true)
	f.Add("GET", "", "public, max-age=600, stale-if-error=300", 600, 0, 800, false, true)
	f.Add("GET", "max-age=300", "public, max-age=600", 600, 0, 400, false, true)
	f.Add("GET", "max-stale=100", "public, max-age=600", 600, 120, 700, false, true)
	f.Add("GET", "min-fresh=60", "public, max-age=600", 600, 0, 580, false, true)
	f.Add("GET", "", "must-revalidate", 600, 0, 700, false, true)
	f.Add("GET", "only-if-cached", "public, max-age=600", 600, 0, 0, false, true)
	f.Add("GET", "", "immutable", 600, 0, 999999, false, true)
	f.Add("GET", "", "", 0, 0, 0, false, true)
	f.Add("HEAD", "", "public, max-age=600", 600, 0, 300, false, true)
	f.Add("POST", "", "public, max-age=600", 600, 0, 0, false, true)
	f.Add("GET", "", "public, max-age=600", 600, 0, 100, true, true)
	f.Add("GET", "no-cache", "", 0, 0, 0, true, true)
	f.Add("GET", "only-if-cached", "", 0, 0, 0, true, true)
	f.Add("GET", "pragma:no-cache", "public, max-age=600", 600, 0, 100, false, true)
	// No-validator seeds: stale/no-cache objects without ETag or
	// LastModified must fall back to Miss, not Revalidate.
	f.Add("GET", "", "public, max-age=600", 600, 0, 700, false, false)
	f.Add("GET", "no-cache", "public, max-age=600", 600, 0, 100, false, false)
	f.Add("GET", "", "no-cache", 600, 0, 0, false, false)
	f.Add("GET", "", "must-revalidate", 600, 0, 700, false, false)

	f.Fuzz(func(t *testing.T, method, reqCC, respCC string, ttlSec, ageSec, elapsedSec int, nilObj, hasValidator bool) {
		u, err := url.Parse("http://example.com/")
		if err != nil {
			t.Skip()
		}
		req := &http.Request{
			Method: method,
			URL:    u,
			Host:   "example.com",
			Header: http.Header{},
		}
		if pragma, ok := strings.CutPrefix(reqCC, "pragma:"); ok {
			req.Header.Set(header.Pragma, pragma)
		} else if reqCC != "" {
			req.Header.Set(header.CacheControl, reqCC)
		}

		storedAt := fuzzFixedTime
		ttl := durationFromSec(ttlSec)
		now := storedAt.Add(durationFromSec(elapsedSec))

		var obj *api.Object
		if !nilObj {
			respHeaders := http.Header{
				header.CacheControl: {respCC},
			}
			if ageSec > 0 {
				respHeaders.Set(header.Age, strconv.Itoa(ageSec))
			}
			obj = &api.Object{
				StatusCode:   http.StatusOK,
				Header:       header.FromHTTP(respHeaders),
				CacheControl: respCC,
				StoredAt:     storedAt,
				TTL:          ttl,
				OriginAge:    durationFromSec(ageSec),
			}
			if hasValidator {
				obj.ETag = `"etag-fuzz"`
			}
		}

		d1 := Evaluate(req, obj, now)
		d2 := Evaluate(req, obj, now)
		if d1.Decision != d2.Decision {
			t.Fatalf("Evaluate not deterministic: %d != %d for method=%q reqCC=%q respCC=%q ttl=%d age=%d elapsed=%d nilObj=%v hasValidator=%v",
				d1.Decision, d2.Decision, method, reqCC, respCC, ttlSec, ageSec, elapsedSec, nilObj, hasValidator)
		}
	})
}

// fuzzFixedTime is a deterministic time for fuzz tests. Using a fixed time
// instead of time.Now() ensures reproducibility and complies with
// AGENTS.md §8: "no time.Now() in tests."
var fuzzFixedTime = time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

// durationFromSec converts an int to time.Duration, clamping negatives to
// 0. Used for TTL, origin age, and elapsed time — all non-negative in
// practice, and negative values from the fuzzer would produce negative
// durations that don't make semantic sense for freshness calculations.
func durationFromSec(s int) time.Duration {
	if s < 0 {
		return 0
	}
	return time.Duration(s) * time.Second
}
