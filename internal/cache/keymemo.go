package cache

import (
	"sync"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// keymemo.go implements ttlKeyMemo: a bounded, node-local set of keys
// remembered for a caller-chosen window, sharded exactly like the
// unstorable-fill memo (unstorable.go) — fixed 16 shards x 512 keys,
// lazily swept, no reaper goroutine. The near-cache uses it as its
// popularity gate (ADR-0071): a peer-fetched key must be seen twice
// within nearAdmitWindow before its object is admitted, so one-pass
// traffic (unique keys) never pollutes the bounded near store while
// Zipf head keys admit on their second request.
//
// The mechanism is deliberately a near-copy of unstorableMemo rather
// than a shared abstraction: unifying the two is a follow-up once the
// near-cache's window semantics stabilize (the unstorable memo's
// window is a fixed constant; this one is caller-supplied per note).

// keyMemoShardCount and keyMemoPerShard bound the memo: 16 shards x
// 512 keys = 8192 remembered keys, ~40 bytes each — the same bound the
// unstorable memo carries.
const (
	keyMemoShardCount = 16
	keyMemoPerShard   = 512
)

// keyMemoShardMask is keyMemoShardCount-1; the count is a power of
// two, so Hash64 & mask spreads over every shard.
const keyMemoShardMask = keyMemoShardCount - 1

// keyMemoShard is one shard. The map holds expiry timestamps in Unix
// nanoseconds; entries are swept lazily (no reaper goroutine).
type keyMemoShard struct {
	m  map[api.Key]int64
	mu sync.RWMutex
}

// ttlKeyMemo is a bounded remember-key-for-a-window registry. The zero
// value is usable: bypass on a nil map misses, and note allocates the
// shard map on first write.
type ttlKeyMemo struct {
	shards [keyMemoShardCount]keyMemoShard
}

// memoShard picks the shard by the key's high 64 bits — the same
// well-distributed XXH128 output the inflight table and the unstorable
// memo shard on.
func (m *ttlKeyMemo) memoShard(key api.Key) *keyMemoShard {
	return &m.shards[key.Hash64()&keyMemoShardMask] //nolint:gosec // G115: mask of a power-of-two count 16
}

// note records key until now+window. Bounded: when the shard is full
// the sweep reclaims expired entries first, and a still-full shard
// refuses the write — fail-open for every consumer by construction (a
// missing entry only means the caller treats the key as first-seen).
func (m *ttlKeyMemo) note(key api.Key, window time.Duration) {
	s := m.memoShard(key)
	expiry := time.Now().Add(window).UnixNano()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[api.Key]int64, keyMemoPerShard)
	} else if len(s.m) >= keyMemoPerShard {
		now := time.Now().UnixNano()
		for k, exp := range s.m {
			if exp <= now {
				delete(s.m, k)
			}
		}
		if len(s.m) >= keyMemoPerShard {
			return
		}
	}
	s.m[key] = expiry
}

// bypass reports whether key was noted within its window.
func (m *ttlKeyMemo) bypass(key api.Key) bool {
	s := m.memoShard(key)
	s.mu.RLock()
	exp, ok := s.m[key]
	s.mu.RUnlock()
	return ok && exp > time.Now().UnixNano()
}
