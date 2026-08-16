package cache

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
