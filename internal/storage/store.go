// Package storage is the L2 layer. It implements the embedded,
// multi-tier cache store (sharded hot tier in RAM + mmap warm tier).
//
// The Store interface is consumed by the L4 cache engine; this package
// provides the concrete implementation backed by a sharded hot tier
// (RAM) and an optional warm tier (mmap segments).
//
// The hot tier uses SIEVE eviction with a lock-free ban check on the Get
// path and a background sweeper for eviction. The warm tier persists
// objects to mmap-backed segments with a write-ahead index log and
// background tombstone compaction (see internal/storage/warm and
// internal/storage/wal). Warm is optional: omit it for ephemeral mode.
package storage

import (
	"context"

	"github.com/thylong/bouine/pkg/api"
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
	// OverBudget reports whether the hot tier exceeds its configured byte
	// budget. Used by anti-entropy to skip backfill under memory pressure
	// and avoid fighting the eviction policy (#175).
	OverBudget() bool
	Close(ctx context.Context) error
}

// KeyLister returns all cache keys in the store. Implemented by HotStore
// and consumed by the anti-entropy reconciler in full cluster mode.
//
// Unstable.
type KeyLister interface {
	Keys() []api.Key
}
