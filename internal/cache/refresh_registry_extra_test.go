package cache

import (
	"net/http"
	"net/url"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

func TestDecrementPersist(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	key := testkey.Key(1)

	t.Run("key_not_found", func(t *testing.T) {
		t.Parallel()
		require.False(t, r.DecrementPersist(key))
	})

	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/"},
		Header: http.Header{},
	}

	t.Run("persist_zero", func(t *testing.T) {
		t.Parallel()
		r2 := newRefreshRegistry()
		r2.Register(key, req, "", 0)
		require.False(t, r2.DecrementPersist(key))
	})

	t.Run("persist_positive", func(t *testing.T) {
		t.Parallel()
		r3 := newRefreshRegistry()
		r3.Register(key, req, "", 3)
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
	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/"},
		Header: http.Header{
			"Accept-Encoding": {"gzip"},
			"X-Custom":        {"val"},
		},
	}
	key := testkey.Key(55)
	r.Register(key, req, "*", 0)
	entry := r.Lookup(key)
	require.NotNil(t, entry)
	// Vary:* clones all headers.
	assert.Equal(t, "gzip", entry.header.Get("Accept-Encoding"))
	assert.Equal(t, "val", entry.header.Get("X-Custom"))
}

func TestRefreshRegistry_Concurrent(t *testing.T) {
	t.Parallel()
	r := newRefreshRegistry()
	req := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Scheme: "https", Host: "example.com", Path: "/"},
		Header: http.Header{},
	}
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n uint64) {
			defer wg.Done()
			k := testkey.Key(n)
			r.Register(k, req, "", 0)
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
