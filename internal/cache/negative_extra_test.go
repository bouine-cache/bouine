package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestJitterTTL_NegativeGuard(t *testing.T) {
	t.Parallel()
	// With 50% jitter on a very large TTL, the minimum is 50% of the TTL.
	// The negative guard (jittered < 0 → 0) is unreachable for positive TTLs
	// since factor is in [-50%, +50%]. But a zero TTL with positive pct
	// returns 0 (ttl <= 0 guard).
	assert.Equal(t, time.Duration(0), JitterTTL(0, 50))
}

func TestSoftPurge_NegativeTTL(t *testing.T) {
	t.Parallel()
	// Clock skew: now is BEFORE StoredAt.
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &api.Object{
		StoredAt: now.Add(30 * time.Second), // future store time
		TTL:      60 * time.Second,
	}
	SoftPurge(obj, now)
	// TTL = now - StoredAt = -30s → clamped to 0.
	assert.Equal(t, time.Duration(0), obj.TTL)
}
