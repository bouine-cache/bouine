package cache

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
)

// nearcache.go implements the experimental peer near-cache (ADR-0071,
// audit finding F2): a small, bounded, node-local store of
// peer-fetched objects on NON-OWNER nodes, so a hot remote-owned key
// stops paying a peer RPC — the full serialize + transfer + decode +
// copy round on the owner and this node — for every request. Measured
// on the topology harness, 64% of hit-heavy requests are HIT/peer; the
// near-cache serves the Zipf head of those from local memory.
//
// Safety model (the feature is fail-closed everywhere a doubt exists):
//
//   - Admission requires an object that is non-variant (blank VaryKey
//     AND blank VaryValue — a variant body or the ADR-0068 metadata
//     resolver is never admitted), bodyful, and fresh at admission
//     time; the entry's TTL is bounded by nearMaxTTL AND the object's
//     remaining freshness, and its grace windows are zeroed: a near
//     entry never serves stale and never triggers revalidation
//     (revalidation stays the owner's job, issue #509).
//   - Serving requires the SAME freshness evaluation a local hit runs
//     (evaluate / Evaluate); an expired entry is dropped, not served.
//   - Invalidation rides the existing cluster broadcast: purges,
//     refreshes and bans already fan out to every node and apply
//     through the local surfaces (Handler.Purge / SoftPurge and the
//     engine-level store.Ban); the near-cache hooks exactly those
//     surfaces, so a near copy dies with the owner's copy, and bans
//     evaluate inside the near store's Get exactly as in the main
//     store.
//   - Popularity gate: a key must be peer-fetched twice within
//     nearAdmitWindow before it is admitted, so one-pass traffic never
//     churns the bounded store.
//
// The bytes a near hit serves are exactly the bytes the peer RPC would
// have returned, to the same request population, within the same
// freshness window — the near cache changes WHERE the answer is read
// from, not WHAT is answerable. Anything the peer path would refuse
// (no-store origins, variants, identity-bearing routes' bypass gates,
// request no-cache) is refused at the same place it is refused today:
// upstream of the near lookup.

// nearAdmitWindow bounds the popularity gate: a key peer-fetched twice
// within this window is admitted. 30s — long enough that a Zipf head
// key admits on its second request under any realistic arrival rate,
// short enough that a burst of unique keys does not arm a wave of
// admissions for traffic that will never come back. A fixed
// engineering default, not an operator knob; promote with demand (the
// same policy as the unstorable memo's window).
const nearAdmitWindow = 30 * time.Second

// nearMaxTTLDefault bounds a near entry's freshness lifetime
// independently of the origin's TTL: even without invalidation
// propagation, a near copy cannot outlive it. Used when the config
// leaves max_ttl at zero.
const nearMaxTTLDefault = 30 * time.Second

// NearCacheCounters carries the near-cache metrics. Every counter is
// optional (nil-safe): a test-constructed NearCache counts nothing.
// The engine injects prometheus counters (internal/observability);
// the cache layer never imports the metrics library.
//
// Unstable — the experimental feature owns its own surface.
type NearCacheCounters struct {
	// Hits counts near-cache serves (fresh entries only).
	Hits interface{ Inc() }
	// Admissions counts objects actually stored into the near cache
	// (after the popularity gate and all admission guards).
	Admissions interface{ Inc() }
	// Invalidations counts purge/refresh/ban-driven removals observed
	// by the near cache. Sustained non-zero rates mean the fleet's
	// invalidation traffic is fighting near-cache reuse.
	Invalidations interface{ Inc() }
}

// NearCacheConfig parameterizes NewNearCache.
//
// Unstable — the experimental feature owns its own surface.
type NearCacheConfig struct {
	Counters NearCacheCounters
	MaxBytes int64
	MaxTTL   time.Duration
}

// nearDefaultMaxBytes is the byte budget used when the config leaves
// max_bytes at zero: 16 MiB of peer-fetched objects per node — enough
// for ~100 SSR pages at the audited body sizes, small enough to be
// irrelevant next to the hot tier.
const nearDefaultMaxBytes = 16 << 20

// NearCache is the node-local store of peer-fetched objects. A nil
// *NearCache is valid and disabled: every method is nil-safe and
// degrades to "no near cache", so call sites wire the pointer
// unconditionally and the config decides existence.
//
// Unstable — the experimental feature owns its own surface.
type NearCache struct {
	counters NearCacheCounters
	store    storage.Store
	admit    ttlKeyMemo
	maxTTL   time.Duration
	nearHits atomic.Int64
}

// NewNearCache builds a near cache over a dedicated hot-store instance
// (SIEVE eviction, byte-budgeted, ban-aware on Get — the same store
// implementation the main tier uses, at a smaller budget).
func NewNearCache(cfg NearCacheConfig) *NearCache {
	maxBytes := cfg.MaxBytes
	if maxBytes <= 0 {
		maxBytes = nearDefaultMaxBytes
	}
	maxTTL := cfg.MaxTTL
	if maxTTL <= 0 {
		maxTTL = nearMaxTTLDefault
	}
	return &NearCache{
		store:    storage.NewHotStore(storage.HotConfig{MaxBytes: maxBytes}),
		maxTTL:   maxTTL,
		counters: cfg.Counters,
	}
}

// Enabled reports whether a near cache exists. A nil receiver is
// disabled; the config default is off (ADR-0071).
func (n *NearCache) Enabled() bool {
	return n != nil
}

// Lookup returns the near entry for key when it is still fresh at now.
// Expired entries are removed (best effort) and reported as misses:
// a near entry never serves past its freshness lifetime. The store's
// Get evaluates bans exactly like the main store's Get, so a banned
// key reports a miss here the moment it would miss on the owner's
// side of the same broadcast.
func (n *NearCache) Lookup(ctx context.Context, key api.Key, now time.Time) (*api.Object, bool) {
	if n == nil {
		return nil, false
	}
	obj, _, err := n.store.Get(ctx, key)
	if err != nil || obj == nil {
		return nil, false
	}
	if !now.Before(obj.StoredAt.Add(obj.TTL)) {
		// Expired: reclaim the bytes so the bound stays honest.
		_ = n.store.Delete(ctx, key)
		return nil, false
	}
	n.nearHits.Add(1)
	if n.counters.Hits != nil {
		n.counters.Hits.Inc()
	}
	return obj, true
}

// Hits returns the number of near serves observed by this node. Used
// by tests and harness readouts; the prometheus counters are the
// operator surface.
func (n *NearCache) Hits() int64 {
	if n == nil {
		return 0
	}
	return n.nearHits.Load()
}

// Admit stores a peer-fetched object under key when it is safe and
// worth it. Guards, in order: the object must be non-variant (blank
// VaryKey and blank VaryValue — variant bodies and the ADR-0068
// metadata resolver are refused outright), bodyful, and still fresh;
// and the key must have been peer-fetched once before within
// nearAdmitWindow (the popularity gate — first sight only arms the
// gate). The stored copy carries a TTL bounded by both the object's
// remaining freshness and maxTTL, with grace windows zeroed: no stale
// serving, no revalidation from a non-owner.
//
// The stored entry is a store-owned clone: HotStore.Put copies body
// and headers under the shard lock, so obj's buffers are never
// retained and the caller keeps full ownership.
func (n *NearCache) Admit(ctx context.Context, key api.Key, obj *api.Object, now time.Time) {
	if n == nil || obj == nil {
		return
	}
	if obj.VaryKey != "" || obj.VaryValue != "" || len(obj.Body) == 0 {
		return
	}
	remaining := obj.StoredAt.Add(obj.TTL).Sub(now)
	if remaining <= 0 {
		return
	}
	if !n.admit.bypass(key) {
		n.admit.note(key, nearAdmitWindow)
		return
	}
	// CloneForStorage is the ownership boundary (Object carries an
	// atomic — value copies are illegal), and it lets this function
	// bound the copy's freshness without touching the caller's object,
	// which the serve path still reads. Put clones once more under its
	// own boundary: admission pays two body copies per admitted object,
	// once per key per freshness window.
	stored := obj.CloneForStorage(nil)
	stored.StoredAt = now
	stored.TTL = min(remaining, n.maxTTL)
	stored.StaleWhileRevalidate = 0
	stored.StaleIfError = 0
	if err := n.store.Put(ctx, key, stored); err != nil {
		return
	}
	if n.counters.Admissions != nil {
		n.counters.Admissions.Inc()
	}
}

// Purge removes key's near entry. Wired into Handler.Purge so the
// existing purge broadcast (which reaches every node) invalidates the
// near copy with the owner's copy.
func (n *NearCache) Purge(ctx context.Context, key api.Key) {
	if n == nil {
		return
	}
	_ = n.store.Delete(ctx, key)
}

// Refresh removes key's near entry. Near entries carry no grace
// windows, so a soft purge has nothing to mark stale: deleting is the
// conservative equivalent, and the next request re-fetches fresh from
// the owner. Wired into Handler.SoftPurge for the refresh broadcast.
func (n *NearCache) Refresh(ctx context.Context, key api.Key) {
	if n == nil {
		return
	}
	_ = n.store.Delete(ctx, key)
	if n.counters.Invalidations != nil {
		n.counters.Invalidations.Inc()
	}
}

// Ban applies a ban expression to the near store's lazy ban list, the
// same expression the engine applies to the main store when a ban
// broadcasts. Wired next to the engine's store.Ban call so both
// stores evaluate identical ban state on Get.
func (n *NearCache) Ban(ctx context.Context, expr api.BanExpr) {
	if n == nil {
		return
	}
	_, _ = n.store.Ban(ctx, expr)
	if n.counters.Invalidations != nil {
		n.counters.Invalidations.Inc()
	}
}
