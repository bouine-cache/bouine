package cluster

import (
	"sync"
	"time"
)

// seqTrackerTTL bounds how long an issuer's dedup state stays
// remembered without being refreshed. A node reusing an issuer name
// (in practice a restarted pod with the same name) starts a new Seq
// sequence at 1; after this TTL its old state expires and the new
// sequence is accepted instead of dropped.
const seqTrackerTTL = time.Hour

// seqTrackers bounds the number of tracked issuers. More issuers than
// this is a misconfiguration (issuers are cluster node names).
const seqTrackers = 256

// seqDedupWindow is the number of recent Seq values per issuer for
// which exact duplicates are detected. Split batch frames travel as
// separate UDP datagrams and memberlist processes its message handoff
// queue LIFO, so events legitimately arrive out of order: a Seq below
// the issuer's high-watermark may be a not-yet-seen event rather than
// a replay. Seqs further below the watermark than this window are
// treated as stale replays and dropped (issue #754).
const seqDedupWindow = 4096

// seqWindow is the per-issuer dedup state: the high-watermark plus a
// bitmap of the Seq values seen in [high-seqDedupWindow+1, high].
// Slot collisions are impossible within the window because any two
// seqs in it differ by less than seqDedupWindow.
type seqWindow struct {
	high uint64
	bits [seqDedupWindow / 8]byte
}

func (w *seqWindow) has(seq uint64) bool {
	i := seq % seqDedupWindow
	return w.bits[i/8]&(1<<(i%8)) != 0
}

func (w *seqWindow) mark(seq uint64) {
	i := seq % seqDedupWindow
	w.bits[i/8] |= 1 << (i % 8)
}

func (w *seqWindow) unmark(seq uint64) {
	i := seq % seqDedupWindow
	w.bits[i/8] &^= 1 << (i % 8)
}

// slideTo advances the high-watermark to seq, clearing the slots the
// newly covered range maps to: they may still hold bits for seqs that
// just slid out of the window. Amortized O(1): each slot is cleared at
// most once per window traversal.
func (w *seqWindow) slideTo(seq uint64) {
	if seq-w.high >= seqDedupWindow {
		clear(w.bits[:])
	} else {
		for s := w.high + 1; s <= seq; s++ {
			w.unmark(s)
		}
	}
	w.high = seq
}

// seqTracker dedups invalidation events per issuer using their
// monotonic Seq (ADR-0044). Strong mode delivers every event twice —
// once via HTTP fan-out, once via gossip — so the receive path drops
// any event whose Seq was already seen. Exact duplicates are detected
// across the last seqDedupWindow Seqs; older Seqs below the
// high-watermark are dropped as stale replays.
//
// The zero value is ready to use.
type seqTracker struct {
	wins     map[string]*seqWindow
	lastSeen map[string]time.Time
	now      func() time.Time
	mu       sync.Mutex
}

func newSeqTracker() *seqTracker {
	return &seqTracker{
		wins:     make(map[string]*seqWindow),
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
	w, ok := t.wins[issuer]
	if ok && now.Sub(t.lastSeen[issuer]) > seqTrackerTTL {
		// Dedup state expired: this issuer restarted with a fresh
		// sequence. Drop the stale state and accept.
		delete(t.wins, issuer)
		delete(t.lastSeen, issuer)
		ok = false
	}
	dup := t.recordLocked(w, ok, issuer, seq, now)
	if len(t.wins) > seqTrackers {
		t.pruneLocked(now)
	}
	return dup
}

// recordLocked applies the dedup decision for one event. w is nil when
// the issuer has no state yet; ok mirrors its presence.
func (t *seqTracker) recordLocked(w *seqWindow, ok bool, issuer string, seq uint64, now time.Time) bool {
	if !ok {
		w = &seqWindow{high: seq}
		w.mark(seq)
		t.wins[issuer] = w
		t.lastSeen[issuer] = now
		return false
	}
	if seq > w.high {
		w.slideTo(seq)
		w.mark(seq)
		t.lastSeen[issuer] = now
		return false
	}
	if w.high-seq >= seqDedupWindow {
		// Older than the dedup window: a stale replay, not an
		// out-of-order delivery. Drop it.
		return true
	}
	if w.has(seq) {
		return true
	}
	// Below the high-watermark but not yet seen: an out-of-order
	// delivery (split batch frames, LIFO handoff). Accept it.
	w.mark(seq)
	t.lastSeen[issuer] = now
	return false
}

// pruneLocked drops the oldest half of the tracked issuers. Called
// with t.mu held only on the rare overflow path; steady state is
// bounded by cluster size (≪ seqTrackers).
func (t *seqTracker) pruneLocked(now time.Time) {
	expired := 0
	for issuer, last := range t.lastSeen {
		if now.Sub(last) > seqTrackerTTL {
			delete(t.wins, issuer)
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
			delete(t.wins, oldest)
			delete(t.lastSeen, oldest)
		}
	}
}
