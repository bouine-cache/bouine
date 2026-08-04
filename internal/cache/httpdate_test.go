package cache

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
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
		Header:    header.FromHTTP(http.Header{header.Age: {"5"}}),
	}
	got := ComputeAge(obj, now)
	require.Equal(t, 30*time.Second, got)
}

// TestComputeAge_FallsBackToHeader covers the warm-tier / legacy path where the
// transient OriginAge field is zero: ComputeAge must still recover the origin
// age by parsing the header, keeping the value identical to the old behaviour.
func TestComputeAge_FallsBackToHeader(t *testing.T) {
	t.Parallel()
	now := time.Now()
	obj := &api.Object{
		StoredAt: now,
		Header:   header.FromHTTP(http.Header{header.Age: {"42"}}),
	}
	got := ComputeAge(obj, now)
	require.Equal(t, 42*time.Second, got)
}
