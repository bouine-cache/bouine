package cache

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"

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

	key := api.Key{Hash: 42}
	r.Register(key, req, "", 0)

	entry := r.Lookup(key)
	require.NotNil(t, entry)
	require.Equal(t, http.MethodGet, entry.method)
	require.Equal(t, "https://example.com/foo", entry.url)
	// Accept-Encoding should be stored (always stored).
	ae := entry.header.Get("Accept-Encoding")
	require.Equal(t, "gzip", ae)
	// X-Test should NOT be stored (not a Vary header).
	x := entry.header.Get("X-Test")
	require.Equal(t, "", x)
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

	key := api.Key{Hash: 99}
	r.Register(key, req, "Accept, Accept-Language", 0)

	entry := r.Lookup(key)
	require.NotNil(t, entry)
	// Accept and Accept-Language should be stored (in Vary).
	v := entry.header.Get("Accept")
	require.Equal(t, "application/json", v)
	v = entry.header.Get("Accept-Language")
	require.Equal(t, "en-US", v)
	// X-Trace-Id should NOT be stored (not in Vary).
	v = entry.header.Get("X-Trace-Id")
	require.Equal(t, "", v)
}

func TestRefreshRegistryUnregister(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/baz"},
		Header: http.Header{},
	}

	key := api.Key{Hash: 1}
	r.Register(key, req, "", 0)
	require.Equal(t, 1, r.Len())

	r.Unregister(key)
	require.Equal(t, 0, r.Len())

	entry := r.Lookup(key)
	require.Nil(t, entry)
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

	key := api.Key{Hash: 77}
	r.Register(key, req, "", 0)

	// Mutate the original request header after registration.
	req.Header.Set("Accept-Encoding", "br")

	// The registry should still have the original value.
	entry := r.Lookup(key)
	require.NotNil(t, entry)
	v := entry.header.Get("Accept-Encoding")
	require.Equal(t, "gzip", v)
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
		r.Register(api.Key{Hash: uint64(i)}, req, "", 0)
	}
	require.Equal(t, 5, r.Len())
}
