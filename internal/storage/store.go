// Package storage is the L2 layer. It implements the embedded,
// multi-tier cache store (sharded hot tier in RAM + mmap warm tier).
//
// The Store interface is consumed by the L4 cache engine; this package
// provides the concrete implementation backed by a sharded hot tier
// (RAM) and an optional warm tier (mmap segments).
//
// The hot tier uses SIEVE eviction. The warm tier (mmap
// segments, WAL, compaction) is stubbed.
package storage

import (
	"context"

	"github.com/thylong/bouine/pkg/api"
)

// Store is the cache storage interface consumed by the cache
// engine.
//
// Stable.
type Store interface {
	Get(ctx context.Context, key api.Key) (*api.Object, error)
	Put(ctx context.Context, key api.Key, obj *api.Object) error
	Delete(ctx context.Context, key api.Key) error
	Ban(ctx context.Context, predicate api.BanExpr) (int, error)
	Stats() api.Stats
	Close(ctx context.Context) error
}
