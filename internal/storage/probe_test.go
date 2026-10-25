package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TestHotStore_Probe_Metadata pins the metadata contract of the probe
// surface (bouine keys find, issue #816): every reported field comes
// from the stored object, the tier is hot, and the probe never bumps
// window hits.
func TestHotStore_Probe_Metadata(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	key := testkey.Key(1)
	o := obj(key, 128)
	o.TTL = 2 * time.Minute
	o.VaryValue = "accept-encoding"
	o.Pool = "echo"
	o.SurrogateKeys = []string{"g1"}
	o.Header.Set(header.XBouineHost, "example.com")
	require.NoError(t, s.Put(context.Background(), key, o))

	// A real lookup first, so hits/window counters are non-zero and
	// the probe's no-touch property is observable.
	got, src, err := s.Get(context.Background(), key)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.Equal(t, api.SourceHot, src)

	st, ok := s.Probe(key)
	require.True(t, ok, "probe must find the key")
	assert.True(t, st.Present)
	assert.Equal(t, "hot", st.Tier)
	assert.Equal(t, int64(128), st.SizeBytes)
	assert.Equal(t, int64(120), st.TTLSeconds)
	assert.Equal(t, 200, st.StatusCode)
	assert.Equal(t, "accept-encoding", st.Vary)
	assert.Equal(t, "echo", st.Pool)
	assert.Equal(t, []string{"g1"}, st.SurrogateKeys)
	assert.False(t, st.Banned)
	assert.False(t, st.KeepGrace)
	assert.False(t, st.BackedToWarm)
	assert.GreaterOrEqual(t, st.WindowHits, int64(1))
	assert.NotEmpty(t, st.StoredAt)
	_, err = time.Parse(time.RFC3339, st.StoredAt)
	assert.NoError(t, err, "StoredAt must be RFC 3339")

	// The probe is side-effect-free: repeated probes neither move
	// the window counter nor change any reported value.
	before := s.WindowHits(key)
	st2, ok := s.Probe(key)
	require.True(t, ok)
	assert.Equal(t, st, st2)
	assert.Equal(t, before, s.WindowHits(key), "Probe must not bump window hits")

	// Absent keys report ok=false with a zero state.
	st3, ok := s.Probe(testkey.Key(999))
	assert.False(t, ok)
	assert.False(t, st3.Present)
}

// TestHotStore_Probe_AfterBan pins the post-ban truth on the hot
// tier: Ban's eager scan removes matching hot entries synchronously,
// so the probe reports absence. The banned-but-still-present state is
// observable on the warm arm instead (TestTieredStore_Probe_Warm),
// where the lazy lookup check is the only reaper.
func TestHotStore_Probe_AfterBan(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4, ReaperInterval: -1})
	defer func() { _ = s.Close(context.Background()) }()

	key := testkey.Key(2)
	o := obj(key, 64)
	o.Header.Set(header.XBouineHost, "banned.example.com")
	require.NoError(t, s.Put(context.Background(), key, o))

	st, ok := s.Probe(key)
	require.True(t, ok)
	assert.False(t, st.Banned, "no ban is active yet")

	n, err := s.Ban(context.Background(), api.BanExpr{HostRegex: "banned\\.example\\.com"})
	require.NoError(t, err)
	require.Equal(t, 1, n, "Ban eagerly evicts matching hot entries")

	st, ok = s.Probe(key)
	assert.False(t, ok, "the eager scan removed the entry; the probe reports the post-ban truth")
	assert.False(t, st.Present)
}

// TestTieredStore_Probe_Warm pins the warm-tier arm: after the hot
// copy is gone the probe still finds the object in warm with its
// decoded metadata, without promoting it back to hot.
func TestTieredStore_Probe_Warm(t *testing.T) {
	t.Parallel()
	ts := newTieredStoreWithDir(t, t.TempDir())
	defer func() { _ = ts.Close(context.Background()) }()

	key := testkey.Key(3)
	body := make([]byte, 4096) // over BodyThreshold (1024): hot + warm copy
	o := &api.Object{
		Key:        key,
		StatusCode: 200,
		Header:     headerMap(header.ContentType, "application/octet-stream"),
		Body:       body,
		BodySize:   int64(len(body)),
		StoredAt:   time.Now(),
		TTL:        time.Minute,
	}
	require.NoError(t, ts.Put(context.Background(), key, o))

	st, ok := ts.Probe(key)
	require.True(t, ok)
	assert.Equal(t, "hot", st.Tier, "hot copy first")
	assert.True(t, st.BackedToWarm, "hot copy is warm-backed")

	// Simulate hot eviction: the warm copy must remain probeable with
	// full metadata and no promotion back to hot.
	require.NoError(t, ts.hot.Delete(context.Background(), key))
	st, ok = ts.Probe(key)
	require.True(t, ok)
	assert.True(t, st.Present)
	assert.Equal(t, "warm", st.Tier)
	assert.Equal(t, int64(len(body)), st.SizeBytes)
	assert.Equal(t, int64(60), st.TTLSeconds)
	assert.Equal(t, 200, st.StatusCode)
	assert.False(t, st.Banned)
	assert.False(t, ts.hot.Has(key), "Probe must not promote warm objects to hot")

	// A ban after demotion must be visible on the warm arm too — the
	// probe mirrors Get's ban semantics without Get's side effects.
	_, err := ts.Ban(context.Background(), api.BanExpr{HostRegex: ".*"})
	require.NoError(t, err)
	st, ok = ts.Probe(key)
	require.True(t, ok)
	assert.True(t, st.Banned, "warm probe must evaluate the active ban list")
}

// TestTieredStore_Probe_Absent covers the miss path end to end.
func TestTieredStore_Probe_Absent(t *testing.T) {
	t.Parallel()
	ts := newTieredStoreWithDir(t, t.TempDir())
	defer func() { _ = ts.Close(context.Background()) }()

	st, ok := ts.Probe(testkey.Key(42))
	assert.False(t, ok)
	assert.False(t, st.Present)
}
