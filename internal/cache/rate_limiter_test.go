package cache

import (
	"testing"
	"time"

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
