package cache

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

func TestRefreshRegistryRegisterLookup(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := testCtx("GET", "https://example.com/foo")
	req.Request.Header.Set("Accept-Encoding", "gzip")
	req.Request.Header.Set("X-Test", "val1")

	key := testkey.Key(42)
	r.Register(key, requestInfoFromCtx(req), "", 0)

	entry := r.Lookup(key)
	require.NotNil(t, entry)
	require.Equal(t, "GET", entry.method)
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

	req := testCtx("GET", "https://example.com/bar")
	req.Request.Header.Set("Accept", "application/json")
	req.Request.Header.Set("Accept-Language", "en-US")
	req.Request.Header.Set("X-Trace-Id", "abc123")

	key := testkey.Key(99)
	r.Register(key, requestInfoFromCtx(req), "Accept, Accept-Language", 0)

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

	req := testCtx("GET", "https://example.com/baz")

	key := testkey.Key(1)
	r.Register(key, requestInfoFromCtx(req), "", 0)
	require.Equal(t, 1, r.Len())

	r.Unregister(key)
	require.Equal(t, 0, r.Len())

	entry := r.Lookup(key)
	require.Nil(t, entry)
}

func TestRefreshRegistryHeaderIsSnapshot(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := testCtx("GET", "https://example.com/snap")
	req.Request.Header.Set("Accept-Encoding", "gzip")

	key := testkey.Key(77)
	r.Register(key, requestInfoFromCtx(req), "", 0)

	// Mutate the original request header after registration.
	req.Request.Header.Set("Accept-Encoding", "br")

	// The registry should still have the original value.
	entry := r.Lookup(key)
	require.NotNil(t, entry)
	v := entry.header.Get("Accept-Encoding")
	require.Equal(t, "gzip", v)
}

func TestRefreshRegistryLen(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()

	req := testCtx("GET", "https://example.com/len")

	for i := range 5 {
		r.Register(testkey.Key(uint64(i)), requestInfoFromCtx(req), "", 0)
	}
	require.Equal(t, 5, r.Len())
}

func TestDecrementPersist(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	key := testkey.Key(1)

	t.Run("key_not_found", func(t *testing.T) {
		t.Parallel()
		require.False(t, r.DecrementPersist(key))
	})

	req := testCtx("GET", "https://example.com/")
	info := requestInfoFromCtx(req)

	t.Run("persist_zero", func(t *testing.T) {
		t.Parallel()
		r2 := newRefreshRegistry()
		r2.Register(key, info, "", 0)
		require.False(t, r2.DecrementPersist(key))
	})

	t.Run("persist_positive", func(t *testing.T) {
		t.Parallel()
		r3 := newRefreshRegistry()
		r3.Register(key, info, "", 3)
		require.True(t, r3.DecrementPersist(key))
		entry := r3.Lookup(key)
		require.NotNil(t, entry)
		assert.Equal(t, 2, entry.persistCycles)
		require.True(t, r3.DecrementPersist(key))
		require.True(t, r3.DecrementPersist(key))
		require.False(t, r3.DecrementPersist(key))
	})
}

func TestRefreshRegistry_VaryStar(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	req := testCtx("GET", "https://example.com/")
	req.Request.Header.Set("Accept-Encoding", "gzip")
	req.Request.Header.Set("X-Custom", "val")
	key := testkey.Key(55)
	r.Register(key, requestInfoFromCtx(req), "*", 0)
	entry := r.Lookup(key)
	require.NotNil(t, entry)
	// Vary:* clones all headers.
	assert.Equal(t, "gzip", entry.header.Get("Accept-Encoding"))
	assert.Equal(t, "val", entry.header.Get("X-Custom"))
}

func TestRefreshRegistry_Concurrent(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	req := testCtx("GET", "https://example.com/")
	info := requestInfoFromCtx(req)

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n uint64) {
			defer wg.Done()
			k := testkey.Key(n)
			r.Register(k, info, "", 0)
			_ = r.Lookup(k)
			if n%2 == 0 {
				r.Unregister(k)
			}
		}(uint64(i))
	}
	wg.Wait()
	// After all goroutines: even keys unregistered, odd keys remain.
	// Len should be 50 (odd keys 1,3,5,...,99).
	assert.Equal(t, 50, r.Len())
}
