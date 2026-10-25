package storage

import (
	"context"
	"time"

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
	// Probe reports the storage state of key — tier, size, TTL,
	// variant identity, protection and ban status — without side
	// effects: no hit accounting, no SIEVE visit, no promotion, no
	// lazy ban eviction. ok=false when the key is in no tier. Has
	// remains the zero-cost boolean form for the data plane; Probe is
	// the metadata-rich form for the admin keys-query surface
	// (bouine keys find, issue #816) and the explain plane (issue
	// #814).
	Probe(key api.Key) (api.KeyState, bool)
	// MatchingBans returns the active lazy bans obj is subject to, in
	// registration (oldest-first) order. Read-only: no pruning, no
	// eviction, no ban-list mutation. A nil obj never matches. Probe
	// reports the boolean; the explain plane asks for the
	// descriptors (issue #814).
	MatchingBans(obj *api.Object) []BanDescriptor
}

// BanDescriptor describes one active lazy ban, as returned by
// Store.MatchingBans. Identity is the pattern triple plus the
// registration time — the lazy ban list carries no separate id.
// ExpiresAt is the registration instant plus the store's configured
// ban TTL, the same deadline the reaper prunes by.
//
// Unstable.
type BanDescriptor struct {
	CreatedAt    time.Time `json:"created_at"`
	ExpiresAt    time.Time `json:"expires_at,omitempty"`
	HostRegex    string    `json:"host_regex,omitempty"`
	PathRegex    string    `json:"path_regex,omitempty"`
	SurrogateKey string    `json:"surrogate_key,omitempty"`
}

// KeyLister returns all cache keys in the store. Implemented by HotStore
// and TieredStore. Retained for future key-diff features.
//
// Unstable.
type KeyLister interface {
	Keys() []api.Key
}
