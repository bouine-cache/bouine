package config

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestResolve_EmptyConfigMaterializesDefaults pins the effective values
// for an empty (validated) config: /v1/config must show what the
// process will actually run, not the raw tree.
func TestResolve_EmptyConfigMaterializesDefaults(t *testing.T) {
	t.Parallel()
	cfg := Defaults()
	require.NoError(t, cfg.Validate())
	r := cfg.Resolve()

	assert.Equal(t, TLSVersion1_2, r.TLS.MinVersion)
	assert.Equal(t, ClusterModeStrong, r.Cluster.Mode)
	assert.Equal(t, 2, r.Cluster.HopLimit)
	assert.Equal(t, DefaultAdminIdleTimeout, r.Admin.IdleTimeout)
	assert.Equal(t, DefaultJoinTimeout, r.Cluster.JoinTimeout)
	assert.Equal(t, DefaultHandoffQueueDepth, r.Cluster.HandoffQueueDepth)

	assert.Equal(t, EvictionSieve, r.Storage.HotEvictionAlgorithm)
	assert.Equal(t, EvictionSieve, r.Storage.WarmEvictionAlgorithm)
	assert.False(t, r.Storage.WarmSyncDisabled)
	assert.False(t, r.Storage.WALSyncPerEntry)

	// Pool-less config: no pools, no routes.
	assert.Empty(t, r.Pools)
	assert.Empty(t, r.Routes)
}

// TestResolve_ConcreteValuesPassThrough pins that explicit values are
// never overwritten by Resolve.
func TestResolve_ConcreteValuesPassThrough(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen: Listen{Admin: ":9000", Cluster: ":8443"},
		TLS:    TLS{MinVersion: TLSVersion1_3},
		Admin:  AdminConfig{IdleTimeout: 42 * time.Second},
		Cluster: Cluster{
			Mode:        ClusterModeEventual,
			JoinTimeout: 30 * time.Second,
		},
		Storage: Storage{
			EvictionAlgorithm: EvictionCachaner,
		},
	}
	require.NoError(t, cfg.Validate())
	r := cfg.Resolve()

	assert.Equal(t, TLSVersion1_3, r.TLS.MinVersion)
	assert.Equal(t, 42*time.Second, r.Admin.IdleTimeout)
	assert.Equal(t, ClusterModeEventual, r.Cluster.Mode)
	assert.Equal(t, 30*time.Second, r.Cluster.JoinTimeout)
	assert.Equal(t, EvictionCachaner, r.Storage.HotEvictionAlgorithm)
	assert.Equal(t, EvictionCachaner, r.Storage.WarmEvictionAlgorithm)
}

// TestResolve_PerTierEvictionOverride pins the override precedence:
// per-tier field wins over the shared field, shared field over the
// sieve default.
func TestResolve_PerTierEvictionOverride(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen: Listen{Admin: ":9000"},
		Storage: Storage{
			EvictionAlgorithm:    EvictionCachaner,
			HotEvictionAlgorithm: EvictionSieve,
		},
	}
	require.NoError(t, cfg.Validate())
	r := cfg.Resolve()
	assert.Equal(t, EvictionSieve, r.Storage.HotEvictionAlgorithm)
	assert.Equal(t, EvictionCachaner, r.Storage.WarmEvictionAlgorithm)
}

// TestResolve_DisableSentinelsCollapse pins that the -1 disable
// sentinels become explicit bools and never leak negative durations
// into the resolved tree.
func TestResolve_DisableSentinelsCollapse(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen: Listen{Admin: ":9000"},
		Storage: Storage{
			WarmSyncInterval:    -1,
			WALSyncInterval:     -1,
			CompactInterval:     -1,
			CompactStartupDelay: -1,
			CheckpointInterval:  -1,
		},
	}
	require.NoError(t, cfg.Validate())
	r := cfg.Resolve()

	assert.True(t, r.Storage.WarmSyncDisabled)
	assert.True(t, r.Storage.WALSyncPerEntry)
	assert.True(t, r.Storage.CompactionDisabled)
	assert.True(t, r.Storage.CompactStartImmediate)
	assert.True(t, r.Storage.CheckpointingDisabled)
	for _, d := range []time.Duration{
		r.Storage.WarmSyncInterval,
		r.Storage.WALSyncInterval,
		r.Storage.CompactInterval,
		r.Storage.CompactStartupDelay,
		r.Storage.CheckpointInterval,
	} {
		assert.GreaterOrEqual(t, d, time.Duration(0)) // -1 mapped to 0, never negative
	}
}

// TestResolve_RouteCacheDefaults pins the per-route materialization:
// zero-means-default knobs come out concrete and the fetch timeout
// inherits the pool's resolved response-header timeout.
func TestResolve_RouteCacheDefaults(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "app", Targets: []string{"10.0.0.1:80"}},
		},
		Routes: []Route{
			{Name: "r1", Pool: "app"},
		},
	}
	require.NoError(t, cfg.Validate())
	r := cfg.Resolve()
	rc := r.Routes[0].Cache

	assert.Equal(t, DefaultMaxResponseBytes, rc.MaxResponseBytes)
	assert.Equal(t, DefaultFetchConcurrency, rc.MaxFetchConcurrency)
	assert.Equal(t, DefaultFetchWaitTimeout, rc.FetchWaitTimeout)
	assert.Equal(t, DefaultStreamingBufferBytes, rc.MaxStreamingBufferBytes)
	assert.Equal(t, DefaultResponseHeaderTimeout, rc.FetchTimeout)
	assert.False(t, rc.AllowSetCookie)
}

// TestResolve_RouteCacheExplicit pins that explicit route values pass
// through untouched.
func TestResolve_RouteCacheExplicit(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "app", Targets: []string{"10.0.0.1:80"}, Connect: ConnectPolicy{ResponseHeaderTimeout: 45 * time.Second}},
		},
		Routes: []Route{
			{
				Name: "r1", Pool: "app",
				Cache: RouteCache{
					FetchTimeout:        90 * time.Second,
					FetchWaitTimeout:    250 * time.Millisecond,
					MaxResponseBytes:    ByteSize(8 << 20),
					MaxFetchConcurrency: 7,
				},
			},
		},
	}
	require.NoError(t, cfg.Validate())
	rc := cfg.Resolve().Routes[0].Cache
	assert.Equal(t, 90*time.Second, rc.FetchTimeout)
	assert.Equal(t, 250*time.Millisecond, rc.FetchWaitTimeout)
	assert.Equal(t, ByteSize(8<<20), rc.MaxResponseBytes)
	assert.Equal(t, 7, rc.MaxFetchConcurrency)
}

// TestResolve_PoolConnectDefaults pins the origin connect policy
// materialization.
func TestResolve_PoolConnectDefaults(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen: Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{
			{Name: "app", Targets: []string{"10.0.0.1:80"}},
		},
		Routes: []Route{{Name: "r1", Pool: "app"}},
	}
	require.NoError(t, cfg.Validate())
	rp := cfg.Resolve().Pools[0]
	assert.Equal(t, DefaultDialTimeout, rp.DialTimeout)
	assert.Equal(t, DefaultKeepAlive, rp.KeepAlive)
	assert.Equal(t, DefaultMaxConnsPerHost, rp.MaxConnsPerHost)
	assert.Equal(t, DefaultMaxIdleConnDuration, rp.MaxIdleConnDuration)
	assert.Equal(t, DefaultResponseHeaderTimeout, rp.ResponseHeaderTimeout)
}

// TestResolvedCompleteness pins that every field the cmd builders read
// from the resolved tree is materialized (non-zero where a default
// exists) for a fully-populated config. When a builder grows a new
// read, extend the table here: a resolved field that comes out zero
// for a configured input means the builder silently runs on a missing
// default.
func TestResolvedCompleteness(t *testing.T) {
	t.Parallel()
	enabled := true
	cfg := Config{
		Listen: Listen{HTTP: ":8080", HTTPS: ":8443", Admin: ":9000", Cluster: ":8444"},
		Admin: AdminConfig{
			MaxBatchSize: 100, MaxBodyBytes: 1 << 20,
			RateLimitPerSecond: 10, PprofEnabled: true,
			DrainDuration: 5 * time.Second,
		},
		Cloudflare: CloudflareConfig{ZoneID: "z1", Async: &enabled},
		Storage: Storage{
			WarmDir: "/data/warm", WarmMaxBytes: ByteSize(1 << 30),
			SegmentCacheSize: 8, WarmSyncBatchSize: 100,
			CheckpointWALThreshold: 99, TombstoneQueueSize: 10,
			HotMmapSlab: true,
		},
		UpstreamPools: []UpstreamPool{
			{Name: "app", Targets: []string{"10.0.0.1:80"}, Connect: ConnectPolicy{HedgeTimeout: 1 * time.Second}},
		},
		Routes: []Route{
			{Name: "r1", Pool: "app", Cache: RouteCache{TTLDefault: time.Minute, RefreshBeforeExpiry: true}},
			{Name: "s1", Static: StaticConfig{Root: "/srv", Index: []string{"index.html"}, MaxFileSize: ByteSize(1 << 20)}},
		},
	}
	require.NoError(t, cfg.Validate())
	r := cfg.Resolve()

	// Admin surface.
	assert.Equal(t, DefaultAdminIdleTimeout, r.Admin.IdleTimeout)
	assert.Equal(t, 5*time.Second, r.Admin.DrainDuration)
	assert.Equal(t, 100, r.Admin.MaxBatchSize)
	assert.Equal(t, 1<<20, r.Admin.MaxBodyBytes)
	assert.Equal(t, 10, r.Admin.RateLimitPerSecond)
	assert.True(t, r.Admin.PprofEnabled)
	assert.Equal(t, "z1", r.Cloudflare.ZoneID)
	require.NotNil(t, r.Cloudflare.Async)
	assert.True(t, *r.Cloudflare.Async)

	// Storage surface: every TieredConfig input the builder reads.
	rs := r.Storage
	assert.Equal(t, "/data/warm", rs.WarmDir)
	assert.Equal(t, ByteSize(1<<30), rs.WarmMaxBytes)
	assert.Equal(t, 8, rs.SegmentCacheSize)
	assert.Equal(t, 100, rs.WarmSyncBatchSize)
	assert.Equal(t, int64(99), rs.CheckpointWALThreshold)
	assert.Equal(t, 10, rs.TombstoneQueueSize)
	assert.True(t, rs.HotMmapSlab)
	// Background-loop durations stay as configured (zero passes through;
	// the storage layer owns the zero-default) — the sentinel bools
	// carry the disable decision.
	assert.GreaterOrEqual(t, rs.WarmSyncInterval, time.Duration(0))
	assert.GreaterOrEqual(t, rs.WALSyncInterval, time.Duration(0))
	assert.GreaterOrEqual(t, rs.CompactInterval, time.Duration(0))
	assert.GreaterOrEqual(t, rs.CheckpointInterval, time.Duration(0))
	assert.GreaterOrEqual(t, rs.TombstoneDrainInterval, time.Duration(0))
	assert.False(t, rs.WarmSyncDisabled)
	assert.False(t, rs.WALSyncPerEntry)
	assert.False(t, rs.CompactionDisabled)
	assert.False(t, rs.CompactStartImmediate)
	assert.False(t, rs.CheckpointingDisabled)

	// Pool surface.
	rp := r.Pools[0]
	assert.Equal(t, "app", rp.Name)
	assert.Equal(t, []string{"10.0.0.1:80"}, rp.Targets)
	assert.Equal(t, 1*time.Second, rp.HedgeTimeout)

	// Proxied route surface: the cache knobs the HandlerConfig reads.
	rc := r.Routes[0].Cache
	assert.Equal(t, "r1", r.Routes[0].Name)
	assert.Equal(t, "app", r.Routes[0].Pool)
	assert.Equal(t, time.Minute, rc.TTLDefault)
	assert.True(t, rc.RefreshBeforeExpiry)
	assert.Equal(t, 6*time.Second, rc.RefreshMargin) // 10% of 60s
	assert.Positive(t, rc.FetchTimeout)
	assert.Positive(t, rc.MaxResponseBytes.Bytes())
	assert.Positive(t, rc.MaxFetchConcurrency)
	assert.Positive(t, rc.FetchWaitTimeout)
	assert.Positive(t, rc.MaxStreamingBufferBytes.Bytes())

	// Static route surface.
	sr := r.Routes[1]
	assert.Equal(t, "/srv", sr.Static.Root)
	assert.Equal(t, []string{"index.html"}, sr.Static.Index)
	assert.Equal(t, ByteSize(1<<20), sr.Static.MaxFileSize)
}

// TestResolve_NoSecretsInJSON pins the /v1/config security contract at
// the config layer: marshaling the resolved tree never emits a token
// or a cert/key path.
func TestResolve_NoSecretsInJSON(t *testing.T) {
	t.Parallel()
	cfg := Config{
		Listen:     Listen{Admin: ":9000"},
		Admin:      AdminConfig{Token: "secret-token"},
		Cloudflare: CloudflareConfig{APIToken: "cf-secret", APITokens: []string{"cf-secret-2"}},
		TLS:        TLS{Certs: []TLSCert{{CertFile: "/cert.pem", KeyFile: "/key.pem"}}},
		Cluster:    Cluster{TLS: ClusterTLS{CertFile: "/cluster.pem", KeyFile: "/cluster.key", CABundle: "/ca.pem"}},
	}
	require.NoError(t, cfg.Validate())
	out, err := json.Marshal(cfg.Resolve())
	require.NoError(t, err)
	for _, secret := range []string{
		"secret-token", "cf-secret", "/cert.pem", "/key.pem",
		"/cluster.pem", "/cluster.key", "/ca.pem", "token",
	} {
		assert.NotContains(t, string(out), secret)
	}
}
