package cache

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestRefreshRegistryRegisterLookup(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/foo"},
		Header: http.Header{
			"Accept-Encoding": {"gzip"},
			"X-Test":          {"val1"},
		},
	}

	key := api.Key(42)
	r.Register(key, req, "", 0)

	entry := r.Lookup(key)
	if entry == nil {
		t.Fatal("Lookup returned nil after Register")
	}
	if entry.method != http.MethodGet {
		t.Fatalf("method = %q, want GET", entry.method)
	}
	if entry.url != "https://example.com/foo" {
		t.Fatalf("url = %q, want https://example.com/foo", entry.url)
	}
	// Accept-Encoding should be stored (always stored).
	if ae := entry.header.Get("Accept-Encoding"); ae != "gzip" {
		t.Fatalf("Accept-Encoding = %q, want gzip", ae)
	}
	// X-Test should NOT be stored (not a Vary header).
	if x := entry.header.Get("X-Test"); x != "" {
		t.Fatalf("X-Test = %q, should not be stored", x)
	}
}

func TestRefreshRegistryVaryHeaders(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/bar"},
		Header: http.Header{
			"Accept":          {"application/json"},
			"Accept-Language": {"en-US"},
			"X-Trace-Id":      {"abc123"},
		},
	}

	key := api.Key(99)
	r.Register(key, req, "Accept, Accept-Language", 0)

	entry := r.Lookup(key)
	if entry == nil {
		t.Fatal("Lookup returned nil")
	}
	// Accept and Accept-Language should be stored (in Vary).
	if v := entry.header.Get("Accept"); v != "application/json" {
		t.Fatalf("Accept = %q, want application/json", v)
	}
	if v := entry.header.Get("Accept-Language"); v != "en-US" {
		t.Fatalf("Accept-Language = %q, want en-US", v)
	}
	// X-Trace-Id should NOT be stored (not in Vary).
	if v := entry.header.Get("X-Trace-Id"); v != "" {
		t.Fatalf("X-Trace-Id = %q, should not be stored", v)
	}
}

func TestRefreshRegistryUnregister(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/baz"},
		Header: http.Header{},
	}

	key := api.Key(1)
	r.Register(key, req, "", 0)
	if r.Len() != 1 {
		t.Fatalf("Len = %d, want 1", r.Len())
	}

	r.Unregister(key)
	if r.Len() != 0 {
		t.Fatalf("Len after Unregister = %d, want 0", r.Len())
	}

	if entry := r.Lookup(key); entry != nil {
		t.Fatal("Lookup returned non-nil after Unregister")
	}
}

func TestRefreshRegistryHeaderIsSnapshot(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/snap"},
		Header: http.Header{
			"Accept-Encoding": {"gzip"},
		},
	}

	key := api.Key(77)
	r.Register(key, req, "", 0)

	// Mutate the original request header after registration.
	req.Header.Set("Accept-Encoding", "br")

	// The registry should still have the original value.
	entry := r.Lookup(key)
	if entry == nil {
		t.Fatal("Lookup returned nil")
	}
	if v := entry.header.Get("Accept-Encoding"); v != "gzip" {
		t.Fatalf("Accept-Encoding = %q, want gzip (snapshot)", v)
	}
}

func TestRefreshRegistryLen(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/len"},
		Header: http.Header{},
	}

	for i := range 5 {
		r.Register(api.Key(i), req, "", 0)
	}
	if r.Len() != 5 {
		t.Fatalf("Len = %d, want 5", r.Len())
	}
}
