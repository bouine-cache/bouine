package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestIsNegativeCacheable(t *testing.T) {
	t.Parallel()
	tests := []struct {
		status int
		want   bool
	}{
		{200, false},
		{301, false},
		{404, true},
		{405, true},
		{410, true},
		{500, false},
		{501, true},
		{502, false},
	}
	for _, tt := range tests {
		got := IsNegativeCacheable(tt.status)
		assert.Equal(t, tt.want, got)
	}
}

func TestJitterTTL(t *testing.T) {
	t.Parallel()
	ttl := 100 * time.Second

	t.Run("zero_pct_returns_original", func(t *testing.T) {
		if got := JitterTTL(ttl, 0); got != ttl {
			t.Errorf("JitterTTL(100s, 0) = %v, want %v", got, ttl)
		}
	})

	t.Run("negative_pct_returns_original", func(t *testing.T) {
		if got := JitterTTL(ttl, -5); got != ttl {
			t.Errorf("JitterTTL(100s, -5) = %v, want %v", got, ttl)
		}
	})

	t.Run("zero_ttl_returns_zero", func(t *testing.T) {
		if got := JitterTTL(0, 10); got != 0 {
			t.Errorf("JitterTTL(0, 10) = %v, want 0", got)
		}
	})

	t.Run("pct_clamped_to_50", func(t *testing.T) {
		for range 100 {
			got := JitterTTL(ttl, 100)
			if got < 50*time.Second || got > 150*time.Second {
				t.Errorf("JitterTTL(100s, 100) = %v, outside [50s, 150s]", got)
			}
		}
	})

	t.Run("10pct_range", func(t *testing.T) {
		for range 200 {
			got := JitterTTL(ttl, 10)
			if got < 90*time.Second || got > 110*time.Second {
				t.Errorf("JitterTTL(100s, 10) = %v, outside [90s, 110s]", got)
			}
		}
	})
}

func TestSoftPurge(t *testing.T) {
	t.Parallel()
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &api.Object{
		StatusCode: 200,
		Header:     headerMap(header.ContentType, "text/html"),
		Body:       []byte("hello"),
		StoredAt:   now.Add(-30 * time.Second),
		TTL:        60 * time.Second,
		ETag:       `"abc"`,
	}

	SoftPurge(obj, now)

	assert.False(t, obj.Fresh(now))
	assert.Equal(t, 30*time.Second, obj.TTL)
	assert.Equal(t, `"abc"`, obj.ETag)

	t.Run("nil_safe", func(t *testing.T) {
		SoftPurge(nil, now)
	})
}

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
