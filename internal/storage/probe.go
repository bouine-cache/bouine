package storage

import (
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// keyStateFromObject fills the object-derived fields of st from obj.
// It reads metadata only — never Body: the probe surface must not
// copy payload bytes, and slab-backed bodies are unsafe to read
// outside the shard lock. The caller fills the tier-specific fields
// (Tier, WindowHits, BackedToWarm, Protected) and evaluates ban
// status. Hits is read with an atomic load: the warm-tier encoder
// reads it outside the shard lock (issue #218), so plain reads are
// racy even under RLock.
func keyStateFromObject(st *api.KeyState, obj *api.Object) {
	st.SizeBytes = obj.BodySize
	if !obj.StoredAt.IsZero() {
		st.StoredAt = obj.StoredAt.UTC().Format(time.RFC3339)
	}
	st.TTLSeconds = int64(obj.TTL / time.Second)
	st.StatusCode = obj.StatusCode
	st.Hits = atomic.LoadUint64(&obj.Hits)
	st.Vary = obj.VaryValue
	st.VaryKey = obj.VaryKey
	st.Pool = obj.Pool
	st.SurrogateKeys = obj.SurrogateKeys
	st.KeepGrace = obj.KeepGrace
}

// Probe reports the state of key in the hot tier without side
// effects: no hit accounting, no SIEVE visited bit, no windowHits
// bump, no lazy ban eviction. Metadata is read under the shard read
// lock; the Body is never touched. Ban status is evaluated against
// the current ban snapshot after the lock is released — the fields
// the matcher reads (stamped host/path headers, surrogate keys,
// stored-at) are immutable after store and GC-pinned by the local
// reference, so the unlock-then-match is race-free. A banned object
// stays Present with Banned=true: the next data-plane Get reclaims it
// and counts a miss.
func (h *HotStore) Probe(key api.Key) (api.KeyState, bool) {
	s := h.shard(key)
	s.mu.RLock()
	e := s.entries[key]
	var st api.KeyState
	var obj *api.Object
	if e != nil {
		obj = e.obj
		st.Present = true
		st.Tier = "hot"
		keyStateFromObject(&st, obj)
		st.WindowHits = e.windowHits.Load()
		st.BackedToWarm = e.hasBackup
	}
	s.mu.RUnlock()
	if obj == nil {
		return api.KeyState{}, false
	}
	st.Banned = h.matchesActiveBan(obj)
	return st, true
}

// Probe reports the state of key across the tiered store: the hot
// tier first (full metadata), then the warm tier via a side-effect-
// free record read (no SIEVE visit, no promotion). It mirrors Get's
// tier order and ban semantics — a warm hit is checked against the
// hot tier's ban list, exactly as Get does — minus every side effect.
// An undecodable warm blob reports absent: a real lookup durably
// evicts it (issue #171), so the probe answers what the next Get
// would serve.
func (t *TieredStore) Probe(key api.Key) (api.KeyState, bool) {
	if st, ok := t.hot.Probe(key); ok {
		return st, true
	}
	if t.warm == nil {
		return api.KeyState{}, false
	}
	body, err := t.warm.Peek(key)
	if err != nil || body == nil {
		return api.KeyState{}, false
	}
	loaded, err := decodeObject(body)
	if err != nil {
		return api.KeyState{}, false
	}
	var st api.KeyState
	st.Present = true
	st.Tier = "warm"
	keyStateFromObject(&st, loaded)
	st.Protected = t.warm.Protected(key)
	st.Banned = t.hot.MatchesActiveBan(loaded)
	return st, true
}
