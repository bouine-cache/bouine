package cache

import (
	"math/rand/v2"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// StatusTTL resolves per-status negative-caching TTLs. The policy type
// and its parser live in pkg/api so config validation and cache lookup
// share one construction path and one resolution order (exact > class >
// not cacheable). A nil *StatusTTL disables negative caching; the
// policy is built once by config.Validate and handed to the handler —
// never re-validated or re-built here.
type StatusTTL = api.StatusTTLPolicy

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
