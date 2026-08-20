package cloudflare_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cf "github.com/bouine-cache/bouine/internal/cloudflare"
)

func TestTokenPool_SingleToken(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1"}, 0)
	token, idx := pool.Next()
	require.Equal(t, "token1", token)
	require.Equal(t, 0, idx)
	require.Equal(t, 1, pool.Len())
	require.Equal(t, 1, pool.AvailableCount())
}

func TestTokenPool_RoundRobin(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2", "token3"}, 0)

	seen := make(map[string]int)
	for range 9 {
		token, _ := pool.Next()
		seen[token]++
	}

	// Each token should be used 3 times.
	require.Equal(t, 3, seen["token1"])
	require.Equal(t, 3, seen["token2"])
	require.Equal(t, 3, seen["token3"])
}

func TestTokenPool_MarkRateLimited(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2"}, 100*time.Millisecond)

	// Mark token1 as rate-limited.
	pool.MarkRateLimited(0, 0)

	// Next should skip token1 and return token2.
	token, _ := pool.Next()
	require.Equal(t, "token2", token)

	// AvailableCount should be 1 (only token2).
	require.Equal(t, 1, pool.AvailableCount())
}

func TestTokenPool_CooldownExpires(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2"}, 50*time.Millisecond)

	pool.MarkRateLimited(0, 0)

	// token1 should be skipped.
	token, _ := pool.Next()
	require.Equal(t, "token2", token)

	// Wait for cooldown to expire.
	time.Sleep(100 * time.Millisecond)

	// token1 should be available again.
	avail := pool.AvailableCount()
	require.Equal(t, 2, avail)
}

func TestTokenPool_AllRateLimited(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2"}, 100*time.Millisecond)

	// Mark both tokens as rate-limited.
	pool.MarkRateLimited(0, 0)
	pool.MarkRateLimited(1, 0)

	require.Equal(t, 0, pool.AvailableCount())

	// Next should still return a token (the one with earliest cooldown expiry).
	token, _ := pool.Next()
	require.NotEmpty(t, token)
}

func TestTokenPool_RetryAfterOverridesCooldown(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2"}, 10*time.Second)

	// Mark with a short retryAfter.
	pool.MarkRateLimited(0, 50*time.Millisecond)

	// Should be back available quickly.
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 2, pool.AvailableCount())
}

func TestTokenPool_OnRotateCallback(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2"}, 0)

	rotated := make(chan int, 1)
	pool.OnRotate(func(idx int) {
		rotated <- idx
	})

	pool.MarkRateLimited(0, 0)

	select {
	case idx := <-rotated:
		require.Equal(t, 0, idx)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("OnRotate callback not called")
	}
}

func TestTokenPool_EmptyPool(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool(nil, 0)

	token, _ := pool.Next()
	require.Equal(t, "", token)
	require.Equal(t, 0, pool.Len())
	require.Equal(t, 0, pool.AvailableCount())
}

func TestTokenPool_MarkRateLimited_InvalidIndex(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1"}, 0)

	// Should not panic on invalid index.
	pool.MarkRateLimited(-1, 0)
	pool.MarkRateLimited(99, 0)
}

func TestTokenPool_RetryAfterCapped(t *testing.T) {
	t.Parallel()
	pool := cf.NewTokenPool([]string{"token1", "token2"}, 10*time.Second)

	// A retryAfter of 10 minutes should be capped to 5 minutes (but still
	// longer than the default cooldown). We can't easily test the exact
	// duration, but we can verify the token is still in cooldown.
	pool.MarkRateLimited(0, 10*time.Minute)

	// token1 should be in cooldown.
	token, _ := pool.Next()
	require.Equal(t, "token2", token)
}

func TestNew_MultipleTokens(t *testing.T) {
	t.Parallel()
	c, err := cf.New(cf.Config{
		ZoneID:    "zone1",
		APIToken:  "token1",
		APITokens: []string{"token2", "token3"},
	})
	require.NoError(t, err)
	require.NotNil(t, c)
	require.Equal(t, "zone1", c.ZoneID())
}

func TestNew_OnlyAdditionalTokens(t *testing.T) {
	t.Parallel()
	c, err := cf.New(cf.Config{
		ZoneID:    "zone1",
		APIToken:  "", // primary empty
		APITokens: []string{"token1", "token2"},
	})
	require.NoError(t, err)
	require.NotNil(t, c)
}

func TestNew_AllTokensEmpty(t *testing.T) {
	t.Parallel()
	_, err := cf.New(cf.Config{
		ZoneID:    "zone1",
		APIToken:  "",
		APITokens: []string{"", ""}, // all empty
	})
	require.Error(t, err)
}
