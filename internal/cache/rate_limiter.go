package cache

import (
	"sync"
	"time"
)

// refreshRateLimiter is a token-bucket limiter with per-second refill.
// It is checked before spawning a refresh goroutine. When no token is
// available, the caller defers the refresh by re-scheduling with jitter.
//
// Uses a mutex rather than atomics because this is a background-path
// limiter (at most refresh_max_rps calls/s per route), not a hot-path
// function. The admin rate limiter (admin/server.go) uses a
// channel+goroutine pattern; we use a mutex here to avoid spawning a
// background goroutine per route.
type refreshRateLimiter struct {
	mu     sync.Mutex
	tokens int64
	max    int64
	lastNs int64
}

func newRefreshRateLimiter(rps int) *refreshRateLimiter {
	return &refreshRateLimiter{
		tokens: int64(rps),
		max:    int64(rps),
		lastNs: time.Now().UnixNano(),
	}
}

// Allow returns true if a token is available. It refills the bucket
// based on elapsed time since the last call.
func (r *refreshRateLimiter) Allow(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	elapsed := now.UnixNano() - r.lastNs
	if elapsed > 0 {
		refill := r.max * elapsed / int64(time.Second)
		if refill > 0 {
			r.tokens = min(r.tokens+refill, r.max)
			r.lastNs = now.UnixNano()
		}
	}
	if r.tokens <= 0 {
		return false
	}
	r.tokens--
	return true
}
