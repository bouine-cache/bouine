package storage

import (
	"context"

	"github.com/bouine-cache/bouine/pkg/api"
)

// Store is the cache storage interface consumed by the cache
// engine.
//
// Unstable. The Get signature changed in #174 to return api.Source
// alongside the object; the interface may change again as the storage
// tier gains capabilities. Callers depend on this at their own risk.
type Store interface {
	Get(ctx context.Context, key api.Key) (*api.Object, api.Source, error)
	Put(ctx context.Context, key api.Key, obj *api.Object) error
	Delete(ctx context.Context, key api.Key) error
	Ban(ctx context.Context, predicate api.BanExpr) (int, error)
	Stats() api.Stats
	Close(ctx context.Context) error
	// WindowHits returns the per-window hit count for key, or 0 if the
	// key is not in the hot tier. Used by the cache layer's refresh
	// popularity gate to evaluate per-TTL-window access frequency.
	WindowHits(key api.Key) int64
	// Has reports whether key is present in any tier, without side
	// effects. Unlike Get, it does not update access statistics or
	// eviction state.
	Has(key api.Key) bool
}

// KeyLister returns all cache keys in the store. Implemented by HotStore
// and TieredStore. Retained for future key-diff features.
//
// Unstable.
type KeyLister interface {
	Keys() []api.Key
}
