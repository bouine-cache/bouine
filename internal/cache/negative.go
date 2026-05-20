package cache

import (
	"math/rand/v2"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// negativeStatuses are HTTP status codes eligible for negative caching
// when a negative_ttl is configured (RFC 9111 §4.2.2 heuristic list).
var negativeStatuses = map[int]bool{
	404: true,
	405: true,
	410: true,
	501: true,
}

// IsNegativeCacheable reports whether the status code is eligible for
// negative caching.
func IsNegativeCacheable(status int) bool {
	return negativeStatuses[status]
}

// JitterTTL applies a random ±pct% jitter to a TTL. pct is clamped to
// 0–50. Returns the original TTL when pct <= 0.
func JitterTTL(ttl time.Duration, pct int) time.Duration {
	if pct <= 0 || ttl <= 0 {
		return ttl
	}
	if pct > 50 {
		pct = 50
	}
	// Random factor in [-pct, +pct] percent.
	factor := 1.0 + float64(rand.IntN(2*pct+1)-pct)/100.0 //nolint:gosec // jitter, not crypto
	jittered := time.Duration(float64(ttl) * factor)
	if jittered < 0 {
		return 0
	}
	return jittered
}

// SoftPurge marks an object as stale by setting its TTL to zero
// relative to now. The object remains in storage so the next request
// triggers a conditional revalidation instead of a full miss.
func SoftPurge(obj *api.Object, now time.Time) {
	if obj == nil {
		return
	}
	// Set TTL so the object is exactly expired at now, preserving
	// SWR/SIE windows for graceful revalidation.
	obj.TTL = now.Sub(obj.StoredAt)
	if obj.TTL < 0 {
		obj.TTL = 0
	}
}
