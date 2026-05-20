package cache

import (
	"net/http"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

func TestIsNegativeCacheable(t *testing.T) {
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
		if got := IsNegativeCacheable(tt.status); got != tt.want {
			t.Errorf("IsNegativeCacheable(%d) = %v, want %v", tt.status, got, tt.want)
		}
	}
}

func TestJitterTTL(t *testing.T) {
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
	now := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	obj := &api.Object{
		StatusCode: 200,
		Header:     http.Header{"Content-Type": {"text/html"}},
		Body:       []byte("hello"),
		StoredAt:   now.Add(-30 * time.Second),
		TTL:        60 * time.Second,
		ETag:       `"abc"`,
	}

	SoftPurge(obj, now)

	if obj.Fresh(now) {
		t.Error("object should be stale after soft-purge")
	}
	if obj.TTL != 30*time.Second {
		t.Errorf("TTL = %v, want 30s (now - StoredAt)", obj.TTL)
	}
	if obj.ETag != `"abc"` {
		t.Error("soft-purge should preserve ETag for revalidation")
	}

	t.Run("nil_safe", func(t *testing.T) {
		SoftPurge(nil, now)
	})
}
