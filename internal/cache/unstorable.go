package cache

import (
	"sync"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// unstorable.go implements the unstorable-fill memo (ADR-0066 amendment
// #2): a bounded, node-local registry of keys whose most recent fill
// settled unstorable by explicit origin directive (Cache-Control
// no-store or private), the two directives that forbid shared-cache
// storage outright (RFC 9111 §5.2.2.3, §5.2.2.7 — mirrored from
// isCacheBlocked/isBlockedByDirectives' first two gates, including the
// must-understand escape).
//
// While a key is memoized, an anonymous cold include-free miss for it
// never parks on a fill flight: it fetches its own immediately, the
// exact behavior of a request that may not share a flight (collapseDenied)
// and of the pre-ADR-0066 world. Parking it again could only cost every
// follower one leader-fill wait plus the released-wave stagger (PR #838)
// before the same own-fetch, because the origin just certified that the
// fill stores nothing shareable. No content-sharing surface changes: a
// memoized request is a sequential request, so the memo can only reduce
// cross-request exposure, never add it — the property ADR-0066's
// sequential-equivalence suite pins.

// unstorableMemoWindow bounds how long a directive-unstorable settle
// suppresses fill flights for its key. Five seconds — the same
// engineering scale as passive5xxCountWindow and the default negative
// TTL — is long enough that a hot no-store route (the /miss load-test
// shape: one key, thousands of requests per second) pays the
// wait-plus-stagger once per window instead of per wave, and short
// enough that an origin flipping no-store to storable loses at most one
// window of collapse on the flip (self-healing: memoized requests that
// receive a storable response store it, so later requests hit the store
// and never consult the memo at all). A fixed engineering default, not
// an operator knob; promote with operator demand.
const unstorableMemoWindow = 5 * time.Second

// unstorableMemoShardCount and unstorableMemoPerShard bound the memo:
// 16 shards x 512 keys = 8192 keys, ~40 bytes each. Fixed shards (not
// GOMAXPROCS-derived like inflightShardCount): the memo is consulted
// once per fill-eligible miss and written once per unstorable settle,
// orders of magnitude below the inflight table's traffic.
const (
	unstorableMemoShardCount = 16
	unstorableMemoPerShard   = 512
)

// unstorableShardMask is unstorableMemoShardCount-1; the count is a
// power of two, so Hash64 & mask spreads over every shard.
const unstorableShardMask = unstorableMemoShardCount - 1

// unstorableShard is one shard of the memo. The map holds expiry
// timestamps in Unix nanoseconds; entries are swept lazily (no reaper
// goroutine — AGENTS.md: no bare goroutines).
type unstorableShard struct {
	m  map[api.Key]int64
	mu sync.RWMutex
}

// unstorableMemo is the Handler-owned unstorable-key registry. The
// zero value is usable: bypass on a nil map misses, and note allocates
// the shard map on first write, so hand-constructed test Handlers need
// no initialization.
type unstorableMemo struct {
	shards [unstorableMemoShardCount]unstorableShard
}

// shard picks the memo shard by the key's high 64 bits — the same
// well-distributed XXH128 output the inflight table shards on.
func (m *unstorableMemo) shard(key api.Key) *unstorableShard {
	return &m.shards[key.Hash64()&unstorableShardMask] //nolint:gosec // G115: mask of a power-of-two count 16
}

// note records that key's most recent fill settled unstorable by
// explicit directive, suppressing fill flights for it until
// unstorableMemoWindow elapses. Bounded: when the shard is full the
// sweep below reclaims expired entries first, and a still-full shard
// refuses the write — fail-open back to ADR-0066 collapse, the safe
// default (a missing memo entry only means one more collapsed wave).
func (m *unstorableMemo) note(key api.Key) {
	s := m.shard(key)
	expiry := time.Now().Add(unstorableMemoWindow).UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[api.Key]int64, unstorableMemoPerShard)
	} else if len(s.m) >= unstorableMemoPerShard {
		now := time.Now().UnixNano()
		for k, exp := range s.m {
			if exp <= now {
				delete(s.m, k)
			}
		}
		if len(s.m) >= unstorableMemoPerShard {
			return
		}
	}
	s.m[key] = expiry
}

// bypass reports whether key's fill flights are currently suppressed:
// a fill for it settled unstorable by explicit directive within
// unstorableMemoWindow.
func (m *unstorableMemo) bypass(key api.Key) bool {
	s := m.shard(key)
	s.mu.RLock()
	exp, ok := s.m[key]
	s.mu.RUnlock()
	return ok && exp > time.Now().UnixNano()
}

// refusesSharedStorage reports whether the response's own directives
// forbid shared-cache storage outright: no-store (without the RFC 9111
// §5.2.2.3 must-understand escape for an understood status) or private.
// Mirrors the first two gates of isCacheBlocked/isBlockedByDirectives
// exactly, and deliberately nothing else: Vary:* (a variant-matching
// refusal, not a storage-policy directive), client-state headers
// (ADR-0067's surface — minted per caller, excluded here so the memo
// never encodes an assumption about session-minting origins), error
// statuses (transient — a struggling origin needs collapse's dedup on
// recovery, and negative caching already covers the operator's
// policy), and size caps (capacity, not origin policy) all keep the
// ADR-0066 settle-then-stagger flow.
func refusesSharedStorage(respCC Directives, status int) bool {
	if respCC.NoStore {
		if !respCC.MustUnderstand || !isUnderstoodStatus(status) {
			return true
		}
	}
	return respCC.Private
}

// responseRefusesSharedStorage reports whether sf's completed response
// carries a directive that forbids shared-cache storage outright
// (no-store/private). Runs once per non-cacheable buffered settle —
// miss path only — and mirrors isResponseCacheableBytes' shape: the
// raw fasthttp headers are parsed in a stack buffer (zero alloc), with
// CDN-Cache-Control present (RFC 9213 precedence, rare) falling back
// to the Map path, and the upstream-fallback result (sf.resp == nil,
// issue #598) evaluated over its header.Lookup. The verdict comes from
// the response's own headers, never from the local store outcome: in
// strong cluster mode a non-owner leader stores nothing locally even
// for storable fills, so memoizing on "nothing was stored" would
// suppress fill flights on keys the owner holds — the directive is the
// node-independent signal.
func (h *Handler) responseRefusesSharedStorage(sf *streamFetchResult) bool {
	if sf.resp == nil {
		// Upstream fallback: the buffered headerLookup already owns
		// the response headers.
		m := sf.Header.ToMap()
		p := newParsedResponse(sf.StatusCode, header.Map{}, m)
		return refusesSharedStorage(p.respCC, sf.StatusCode)
	}
	hdr := &sf.resp.Header
	if cdn := hdr.Peek(header.CDNCacheControl); len(cdn) > 0 {
		p := newParsedResponse(sf.StatusCode, header.Map{}, sf.Header.ToMap())
		return refusesSharedStorage(p.respCC, sf.StatusCode)
	}
	var ccBuf [512]byte
	respCC := ParseCacheControlBytes(joinedCacheControlBytes(hdr, &ccBuf))
	return refusesSharedStorage(respCC, sf.StatusCode)
}
