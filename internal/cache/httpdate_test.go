package cache

import (
	"net/http"
	"testing"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// TestComputeAge_PrefersStoredOriginAge proves ComputeAge uses the pre-parsed
// obj.OriginAge field rather than re-parsing the Age header on every hit. The
// stored field and the header are set to DIFFERENT values so the stored field
// must win; if ComputeAge fell back to parsing the header it would return 5s.
func TestComputeAge_PrefersStoredOriginAge(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &api.Object{
		StoredAt:  now, // elapsed-since-store contributes 0
		OriginAge: 30 * time.Second,
		Header:    http.Header{"Age": {"5"}},
	}
	if got := ComputeAge(obj, now); got != 30*time.Second {
		t.Fatalf("ComputeAge = %v, want 30s (stored OriginAge must win over Age header)", got)
	}
}

// TestComputeAge_FallsBackToHeader covers the warm-tier / legacy path where the
// transient OriginAge field is zero: ComputeAge must still recover the origin
// age by parsing the header, keeping the value identical to the old behaviour.
func TestComputeAge_FallsBackToHeader(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &api.Object{
		StoredAt: now,
		Header:   http.Header{"Age": {"42"}},
	}
	if got := ComputeAge(obj, now); got != 42*time.Second {
		t.Fatalf("ComputeAge = %v, want 42s (fallback to Age header when OriginAge==0)", got)
	}
}
