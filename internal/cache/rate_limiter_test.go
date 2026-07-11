package cache

import (
	"testing"
	"time"
)

func TestRefreshRateLimiter_Allows(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(5)
	now := time.Now()
	for i := range 5 {
		if !rl.Allow(now) {
			t.Fatalf("call %d: expected Allow=true, got false", i)
		}
	}
	if rl.Allow(now) {
		t.Fatalf("6th call: expected Allow=false (bucket empty), got true")
	}
}

func TestRefreshRateLimiter_Refill(t *testing.T) {
	t.Parallel()
	rl := newRefreshRateLimiter(3)
	now := time.Now()
	for range 3 {
		rl.Allow(now)
	}
	if rl.Allow(now) {
		t.Fatalf("bucket should be empty")
	}
	later := now.Add(time.Second)
	if !rl.Allow(later) {
		t.Fatalf("after 1s: expected Allow=true (refilled), got false")
	}
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
	if count != 5 {
		t.Fatalf("expected 5 tokens (capped at max), got %d", count)
	}
}
