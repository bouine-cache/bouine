package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestHotStore_BanTTLRoundTrip pins the hot-config setter surface
// (`bouine config set cluster.ban_ttl`): the live value reads back
// exactly, and 0 restores the documented built-in default on the
// prune paths.
func TestHotStore_BanTTLRoundTrip(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	defer func() { _ = s.Close(context.Background()) }()

	assert.Equal(t, DefaultBanTTL, s.BanTTL(), "fresh store runs the built-in default")

	s.SetBanTTL(90 * time.Minute)
	assert.Equal(t, 90*time.Minute, s.BanTTL())

	s.SetBanTTL(0)
	assert.Equal(t, time.Duration(0), s.BanTTL(), "0 reads back raw; prune paths apply the default")
}

// TestHotStore_ShortenedBanTTLPrunesOlderBans verifies a runtime
// ban_ttl change is honored by existing prune paths: a ban older than
// the new (shorter) TTL is dropped on the next reap.
func TestHotStore_ShortenedBanTTLPrunesOlderBans(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	_, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "old.example.com"})
	require.NoError(t, err)
	_, err = s.Ban(context.Background(), api.BanExpr{HostRegex: "new.example.com"})
	require.NoError(t, err)

	// Age one ban 2h; the default TTL (24h) keeps it alive.
	s.bans.mu.Lock()
	for i := range s.bans.list {
		if s.bans.list[i].pattern.hostRegex == "old.example.com" {
			s.bans.list[i].created = time.Now().Add(-2 * time.Hour)
		}
	}
	s.bans.mu.Unlock()
	s.reapExpired(time.Now())
	require.Equal(t, 2, s.bans.len(), "under the default TTL both bans survive")

	// Shorten the TTL to 1h at runtime: the aged ban must go.
	s.SetBanTTL(time.Hour)
	s.reapExpired(time.Now())
	assert.Equal(t, 1, s.bans.len(), "ban older than the runtime TTL is pruned")
}

// TestTieredStore_BanTTLDelegation pins the TieredStore facade: the
// hot knob reaches the hot tier unchanged.
func TestTieredStore_BanTTLDelegation(t *testing.T) {
	t.Parallel()
	ts, err := NewTieredStore(TieredConfig{
		Hot:                    HotConfig{MaxBytes: 1 << 20, NumShards: 4},
		TombstoneDrainInterval: -1,
	})
	require.NoError(t, err, "NewTieredStore")
	defer func() { _ = ts.Close(context.Background()) }()

	assert.Equal(t, DefaultBanTTL, ts.BanTTL())
	ts.SetBanTTL(10 * time.Minute)
	assert.Equal(t, 10*time.Minute, ts.BanTTL())
}
