// Package storage is the L3 layer. It implements the embedded,
// multi-tier cache store described in PLAN.md §4.
//
// The Store interface is consumed by the L4 cache engine; this package
// provides the concrete implementation backed by a sharded hot tier
// (RAM) and an optional warm tier (mmap, phase 2 follow-up).
//
// Phase 2 ships the hot tier + SIEVE eviction. The warm tier (mmap
// segments, WAL, compaction) is stubbed.
package storage

import (
	"context"

	"github.com/thylong/bouine/pkg/api"
)

// Store is the cache storage interface consumed by the L4 cache
// engine. See PLAN.md §4.4.
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
