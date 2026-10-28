package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// This file pins ttlKeyMemo — the near-cache popularity gate's
// remembering mechanism (ADR-0071) — mirroring the unstorable memo's
// own bounds suite: window semantics, the full-shard sweep, and the
// fail-open refusal.

func TestKeyMemo_WindowAndExpiry(t *testing.T) {
	t.Parallel()
	var m ttlKeyMemo
	key := BuildKey(mustRequestInfo(t, "http://example.com/memo-window"), nil)

	m.note(key, 20*time.Millisecond)
	require.True(t, m.bypass(key), "a noted key bypasses within its window")

	// The window lapses: the entry is expired even though the map still
	// holds it (sweep is lazy).
	time.Sleep(30 * time.Millisecond)
	require.False(t, m.bypass(key), "an expired note must not bypass")

	// Re-noting after expiry works (the sweep or overwrite path).
	m.note(key, time.Minute)
	require.True(t, m.bypass(key))
}

func TestKeyMemo_BoundedFailOpen(t *testing.T) {
	t.Parallel()
	var m ttlKeyMemo
	// Noting 16*512 distinct keys guarantees every one of the 16
	// shards has been offered more than its 512-slot cap; live entries
	// cannot be swept yet, so the overflow writes are refused.
	for i := range 16 * keyMemoPerShard {
		m.note(BuildKey(mustRequestInfo(t, fmt.Sprintf("http://example.com/k%d", i)), nil), time.Minute)
	}
	for i := range m.shards {
		m.shards[i].mu.RLock()
		require.LessOrEqual(t, len(m.shards[i].m), keyMemoPerShard,
			"every shard stays within its cap")
		m.shards[i].mu.RUnlock()
	}

	// The refused writes (shards already full of live entries) must not
	// panic and must fail open: an un-noted key is first-seen traffic.
	fresh := BuildKey(mustRequestInfo(t, "http://example.com/fresh"), nil)
	require.False(t, m.bypass(fresh))
}

func TestKeyMemo_SweepReclaimsExpired(t *testing.T) {
	t.Parallel()
	var m ttlKeyMemo
	// Fill every shard with short-lived notes.
	for i := range 16 * keyMemoPerShard {
		m.note(BuildKey(mustRequestInfo(t, fmt.Sprintf("http://example.com/old%d", i)), nil), time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	// A full-but-expired shard must sweep and accept new notes — the
	// bound is a cap on LIVE memory, not a denial of service after one
	// busy window. Hash spread is uniform, not exact: a shard may see
	// a few more than 512 refill keys and legitimately refuse the
	// overflow, so the pinned invariant is that the sweep reclaimed
	// capacity (near-total acceptance), not per-key exactness.
	accepted := 0
	for i := range 16 * keyMemoPerShard {
		key := BuildKey(mustRequestInfo(t, fmt.Sprintf("http://example.com/new%d", i)), nil)
		m.note(key, time.Minute)
		if m.bypass(key) {
			accepted++
		}
	}
	require.Greater(t, accepted, 16*400,
		"a swept shard must accept new notes (a broken sweep accepts ~0)")
	for i := range m.shards {
		m.shards[i].mu.RLock()
		require.LessOrEqual(t, len(m.shards[i].m), keyMemoPerShard)
		m.shards[i].mu.RUnlock()
	}
}
