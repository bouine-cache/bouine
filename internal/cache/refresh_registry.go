package cache

import (
	"strings"
	"sync"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// refreshEntry stores the minimal request information needed to
// reconstruct a conditional revalidation request for a cached object.
// Only Vary-relevant request headers are stored (the headers listed
// in the response's Vary header, plus Accept-Encoding for content
// negotiation). This reduces per-entry memory from ~1.3 KB (all
// headers) to ~200–450 B (0–3 headers).
type refreshEntry struct {
	url           string     // compact: "https://host/path?query"
	method        string     // GET or HEAD
	header        header.Map // snapshot of Vary-relevant request headers only
	persistCycles int        // remaining grace refresh cycles when the popularity gate would block
}

// refreshRegistry maps cache keys to the request info needed to
// reconstruct a background refresh fetch. It is populated on
// storeObject when refresh-before-expiry is enabled, and
// cleaned up on explicit Delete, invalidateAndProxy, and when the
// scheduler detects the object is gone from the store.
type refreshRegistry struct {
	entries map[api.Key]*refreshEntry
	mu      sync.Mutex
}

func newRefreshRegistry() *refreshRegistry {
	return &refreshRegistry{
		entries: make(map[api.Key]*refreshEntry),
	}
}

// Register stores request info for key. Only Vary-relevant headers
// (from the response's Vary header, plus Accept-Encoding) are kept.
// The header map is cloned — storing a reference to r.Header would
// race with the HTTP server's request pooling.
func (r *refreshRegistry) Register(key api.Key, ri RequestInfo, varyHeader string, persistCycles int) {
	saved := header.Map{}

	// Always store Accept-Encoding for content negotiation.
	if ae := ri.Header.Get(header.AcceptEncoding); ae != "" {
		saved.Set(header.AcceptEncoding, ae)
	}

	// Store each Vary-relevant header.
	if varyHeader != "" {
		for _, h := range strings.Split(varyHeader, ",") {
			h = strings.TrimSpace(h)
			if h == "" || h == "*" {
				// Vary: * means the response varies on the full
				// request — store all headers. This is rare but
				// correct.
				if h == "*" {
					saved = ri.Header.Clone()
				}
				continue
			}
			if v := ri.Header.Get(h); v != "" {
				saved.Set(h, v)
			}
		}
	}

	r.mu.Lock()
	r.entries[key] = &refreshEntry{
		url:           ri.GetURI(),
		method:        ri.GetMethod(),
		header:        saved,
		persistCycles: persistCycles,
	}
	r.mu.Unlock()
}

// Unregister removes a key from the registry.
func (r *refreshRegistry) Unregister(key api.Key) {
	r.mu.Lock()
	delete(r.entries, key)
	r.mu.Unlock()
}

// Lookup returns the request info for key, or nil if not found.
func (r *refreshRegistry) Lookup(key api.Key) *refreshEntry {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.entries[key]
}

// Len returns the number of entries in the registry.
func (r *refreshRegistry) Len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.entries)
}

// DecrementPersist decrements the persist counter for key and returns
// true if the counter was positive before decrementing (i.e. the object
// had remaining persist budget and one cycle was consumed). Returns false
// if the key is not registered or its persist counter is already zero.
// Used by the refresh popularity gate to keep objects alive for N
// additional TTL cycles after the last access.
func (r *refreshRegistry) DecrementPersist(key api.Key) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry, ok := r.entries[key]
	if !ok || entry.persistCycles <= 0 {
		return false
	}
	entry.persistCycles--
	return true
}
