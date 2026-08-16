package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRefreshRateLimiter_Allows(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(5)
	now := time.Now()
	for range 5 {
		require.True(t, rl.Allow(now))
	}
	require.False(t, rl.Allow(now))
}

func TestRefreshRateLimiter_Refill(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(3)
	now := time.Now()
	for range 3 {
		rl.Allow(now)
	}
	require.False(t, rl.Allow(now))
	later := now.Add(time.Second)
	require.True(t, rl.Allow(later))
}

func TestRefreshRateLimiter_NoLeak(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(10)
	now := time.Now()
	for range 10 {
		rl.Allow(now)
	}
	later := now.Add(2 * time.Second)
	count := 0
	for range 20 {
		if rl.Allow(later) {
			count++
		}
	}
	if count > 10 {
		t.Fatalf("refill leaked: got %d tokens, max should be 10", count)
	}
	if count < 1 {
		t.Fatalf("expected at least 1 token after refill, got %d", count)
	}
}

func TestRefreshRateLimiter_CappedAtMax(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(5)
	later := time.Now().Add(10 * time.Second)
	count := 0
	for range 20 {
		if rl.Allow(later) {
			count++
		}
	}
	require.Equal(t, 5, count)
}

func TestRefreshRateLimiter_ElapsedLeZero(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(5)
	now := time.Now()
	// First call consumes a token.
	require.True(t, rl.Allow(now))
	// Second call with same time: elapsed = 0, no refill.
	require.True(t, rl.Allow(now))
	// Exhaust all tokens.
	for range 3 {
		require.True(t, rl.Allow(now))
	}
	// Now empty, same time.
	require.False(t, rl.Allow(now))
}

func TestRefreshRateLimiter_RefillZero(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(1)
	now := time.Now()
	require.True(t, rl.Allow(now))
	// Sub-second elapsed → refill = 0 (elapsed * max / 1e9 < 1).
	later := now.Add(500 * time.Millisecond)
	require.False(t, rl.Allow(later))
}

func TestRefreshRateLimiter_Concurrent(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(100)
	now := time.Now()
	var allowed, denied int64
	var mu sync.Mutex
	var wg sync.WaitGroup
	for range 200 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if rl.Allow(now) {
				mu.Lock()
				allowed++
				mu.Unlock()
			} else {
				mu.Lock()
				denied++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	// Invariant: allowed tokens never exceed max (100).
	assert.LessOrEqual(t, allowed, int64(100))
	// All 200 calls must be accounted for.
	assert.Equal(t, int64(200), allowed+denied)
}
