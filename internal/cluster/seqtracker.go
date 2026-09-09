package cluster

import (
	"sync"
	"time"
)

// seqTrackerTTL bounds how long an issuer's high-watermark stays
// remembered without being refreshed. A node reusing an issuer name
// (in practice a restarted pod with the same name) starts a new Seq
// sequence at 1; after this TTL its old entries expire and the new
// sequence is accepted instead of dropped.
const seqTrackerTTL = time.Hour

// seqTrackers bounds the number of tracked issuers. More issuers than
// this is a misconfiguration (issuers are cluster node names).
const seqTrackers = 256

// seqTracker dedups invalidation events per issuer using their
// monotonic Seq (ADR-0044). Strong mode delivers every event twice —
// once via HTTP fan-out, once via gossip — so the receive path drops
// any event whose Seq was already seen. First-seen sequences must be
// strictly increasing to also collapse re-sent batches after a
// partition heals.
//
// The zero value is ready to use.
type seqTracker struct {
	high     map[string]uint64
	lastSeen map[string]time.Time
	now      func() time.Time
	mu       sync.Mutex
}

func newSeqTracker() *seqTracker {
	return &seqTracker{
		high:     make(map[string]uint64),
		lastSeen: make(map[string]time.Time),
		now:      time.Now,
	}
}

// seen reports whether (issuer, seq) was already applied and records
// it otherwise. Returns true when the event is a duplicate.
func (t *seqTracker) seen(issuer string, seq uint64) bool {
	if issuer == "" {
		// Events without an issuer cannot be deduped; never drop them.
		return false
	}
	now := t.now()
	t.mu.Lock()
	defer t.mu.Unlock()
	if high, ok := t.high[issuer]; ok {
		if last := t.lastSeen[issuer]; now.Sub(last) > seqTrackerTTL {
			// High-watermark expired: this issuer restarted with a
			// fresh sequence. Drop the stale state and accept.
			delete(t.high, issuer)
			delete(t.lastSeen, issuer)
		} else if seq <= high {
			return true
		}
	}
	t.high[issuer] = seq
	t.lastSeen[issuer] = now
	if len(t.high) > seqTrackers {
		t.pruneLocked(now)
	}
	return false
}

// pruneLocked drops the oldest half of the tracked issuers. Called
// with t.mu held only on the rare overflow path; steady state is
// bounded by cluster size (≪ seqTrackers).
func (t *seqTracker) pruneLocked(now time.Time) {
	expired := 0
	for issuer, last := range t.lastSeen {
		if now.Sub(last) > seqTrackerTTL {
			delete(t.high, issuer)
			delete(t.lastSeen, issuer)
			expired++
		}
	}
	if expired == 0 {
		// No expired entries: drop the least recently seen issuer to
		// bound memory even under adversarial issuer cardinality.
		var oldest string
		var oldestTime time.Time
		first := true
		for issuer, last := range t.lastSeen {
			if first || last.Before(oldestTime) {
				oldest, oldestTime, first = issuer, last, false
			}
		}
		if !first {
			delete(t.high, oldest)
			delete(t.lastSeen, oldest)
		}
	}
}
