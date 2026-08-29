package cache

import (
	"runtime"
	"sync"

	"github.com/bouine-cache/bouine/pkg/api"
)

// inflightShardCount is the number of shards in an inflightTable.
// Sized like the hot store's default (GOMAXPROCS, capped at 64) so
// shard locks rarely contend; rounded to a power of two for masking.
var inflightShardCount = func() int {
	n := runtime.GOMAXPROCS(0)
	if n > 64 {
		n = 64
	}
	if n < 1 {
		n = 1
	}
	// Round up to power of two.
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}()

// inflightShard is one shard of the leader/follower table.
type inflightShard struct {
	m  map[api.Key]*inflightStream
	mu sync.Mutex
}

// inflightTable is a sharded map of in-progress streaming fetches,
// keyed by cache key. It replaces the previous sync.Map: under miss
// storms (unique keys), sync.Map's LoadOrStore allocates a new entry
// node per miss and serializes on internal locks; direct map inserts
// under a per-shard mutex allocate nothing and contend only within a
// shard. The shard is chosen by the key's high 64 bits (Hash64), which
// are already well-distributed XXH128 output.
type inflightTable struct {
	shards []inflightShard
	mask   uint64
}

// newInflightTable creates a table with inflightShardCount shards.
func newInflightTable() inflightTable {
	t := inflightTable{
		shards: make([]inflightShard, inflightShardCount),
		mask:   uint64(inflightShardCount) - 1, //nolint:gosec // G115: inflightShardCount is a positive power of two, 1..64
	}
	for i := range t.shards {
		t.shards[i].m = make(map[api.Key]*inflightStream)
	}
	return t
}

func (t *inflightTable) shard(key api.Key) *inflightShard {
	return &t.shards[key.Hash64()&t.mask]
}

// loadOrStore returns the existing entry for key, or stores and
// returns the provided entry if none exists. loaded reports whether
// an existing entry was found.
func (t *inflightTable) loadOrStore(key api.Key, v *inflightStream) (actual *inflightStream, loaded bool) {
	s := t.shard(key)
	s.mu.Lock()
	existing, ok := s.m[key]
	if !ok {
		s.m[key] = v
		s.mu.Unlock()
		return v, false
	}
	s.mu.Unlock()
	return existing, true
}

// delete removes the entry for key.
func (t *inflightTable) delete(key api.Key) {
	s := t.shard(key)
	s.mu.Lock()
	delete(s.m, key)
	s.mu.Unlock()
}

// inflightPool is deliberately NOT used: an inflightStream cannot be
// safely returned to a pool because the leader cannot know when every
// follower that loaded the pointer has finished reading res after
// close(done). The struct is left to the GC; the win over the previous
// sync.Map is the sharded direct-map insert (0 allocs vs an entry node
// per miss), not struct reuse.
