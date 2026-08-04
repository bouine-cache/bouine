package insights

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func baseConfig() *config.Config {
	return &config.Config{
		Routes: []config.Route{
			{Name: "api", Match: config.RouteMatch{PathPrefix: "/api"}, Pool: "api-pool"},
		},
		UpstreamPools: []config.UpstreamPool{
			{Name: "api-pool"},
		},
	}
}

func routeStats(route string, reqs, hits, errs int64, hitPct float64) observability.RouteStat {
	return observability.RouteStat{
		Route:    route,
		Requests: reqs,
		Hits:     hits,
		Misses:   reqs - hits,
		Errors:   errs,
		HitPct:   hitPct,
	}
}

func targetStatus(addr string, healthy bool) origin.TargetStatus {
	return origin.TargetStatus{Addr: addr, Healthy: healthy}
}

func TestEvaluateEmptyDataNoPanic(t *testing.T) {
	t.Parallel()
	e := New()
	results := e.Evaluate(t.Context(), InsightData{Config: baseConfig()})
	// With empty data, some rules may still fire (e.g. CDN not configured).
	for _, r := range results {
		assert.NotEqual(t, "", r.ID)
	}
}

func TestRuleCacheLowHitRate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		routes   []observability.RouteStat
		wantSev  Severity
		wantFire bool
	}{
		{
			name:     "no data",
			routes:   nil,
			wantFire: false,
		},
		{
			name:     "low traffic ignored",
			routes:   []observability.RouteStat{routeStats("/api", 50, 10, 0, 20)},
			wantFire: false,
		},
		{
			name:     "high hit rate ignored",
			routes:   []observability.RouteStat{routeStats("/api", 200, 180, 0, 90)},
			wantFire: false,
		},
		{
			name:     "very low hit rate is HIGH",
			routes:   []observability.RouteStat{routeStats("/api", 200, 47, 0, 23.5)},
			wantSev:  SeverityHigh,
			wantFire: true,
		},
		{
			name:     "medium hit rate is MED",
			routes:   []observability.RouteStat{routeStats("/api", 200, 90, 0, 45)},
			wantSev:  SeverityMed,
			wantFire: true,
		},
		{
			name:     "moderate hit rate is LOW",
			routes:   []observability.RouteStat{routeStats("/api", 200, 130, 0, 65)},
			wantSev:  SeverityLow,
			wantFire: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := InsightData{Config: baseConfig(), RouteStats: tc.routes}
			ins := ruleCacheLowHitRate(data)
			if tc.wantFire {
				if ins == nil {
					t.Fatal("expected insight to fire, got nil")
				}
				if ins.Severity != tc.wantSev {
					t.Errorf("severity: want %s, got %s", tc.wantSev, ins.Severity)
				}
			} else if ins != nil {
				t.Fatalf("expected no insight, got %+v", ins)
			}
		})
	}
}

func TestRuleCacheLowHitRateEvidenceUsesWorstRoute(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config: baseConfig(),
		RouteStats: []observability.RouteStat{
			routeStats("/api", 500, 400, 0, 80),
			routeStats("/slow", 200, 50, 0, 25),
		},
	}
	ins := ruleCacheLowHitRate(data)
	require.NotNil(t, ins)
	// Evidence should reference /slow's request count (200), not /api's (500).
	assert.Equal(t, "hit%: 25.0, requests: 200", ins.Evidence)
}

func TestRuleCacheNoNegTTL(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Cache.NegativeTTL = 0
	data := InsightData{
		Config: cfg,
		RouteStats: []observability.RouteStat{
			routeStats("api", 100, 80, 10, 80),
		},
	}
	ins := ruleCacheNoNegTTL(data)
	require.NotNil(t, ins)

	// No errors → should not fire.
	data.RouteStats[0].Errors = 0
	ins = ruleCacheNoNegTTL(data)
	require.Nil(t, ins)
}

func TestRuleUpstreamUnhealthyTarget(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		health   map[string][]origin.TargetStatus
		wantSev  Severity
		wantFire bool
	}{
		{name: "no health data", wantFire: false},
		{
			name: "all healthy",
			health: map[string][]origin.TargetStatus{
				"p": {targetStatus("a:80", true), targetStatus("b:80", true)},
			},
			wantFire: false,
		},
		{
			name: "some unhealthy is MED",
			health: map[string][]origin.TargetStatus{
				"p": {targetStatus("a:80", true), targetStatus("b:80", false)},
			},
			wantSev:  SeverityMed,
			wantFire: true,
		},
		{
			name: "all unhealthy is HIGH",
			health: map[string][]origin.TargetStatus{
				"p": {targetStatus("a:80", false), targetStatus("b:80", false)},
			},
			wantSev:  SeverityHigh,
			wantFire: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			data := InsightData{Config: baseConfig(), PoolHealth: tc.health}
			ins := ruleUpstreamUnhealthyTarget(data)
			if tc.wantFire {
				if ins == nil {
					t.Fatal("expected insight")
				}
				if ins.Severity != tc.wantSev {
					t.Errorf("severity: want %s, got %s", tc.wantSev, ins.Severity)
				}
			} else if ins != nil {
				t.Fatalf("expected no insight, got %+v", ins)
			}
		})
	}
}

func TestRuleUpstreamHigh5xx(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Pool = "api-pool"
	data := InsightData{
		Config: cfg,
		RouteStats: []observability.RouteStat{
			routeStats("api", 200, 100, 25, 50),
		},
	}
	ins := ruleUpstreamHigh5xx(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityHigh, ins.Severity)
}

func TestRuleCDNNotConfigured(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), CFStatus: CFStatus{Enabled: false}}
	ins := ruleCDNNotConfigured(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityLow, ins.Severity)

	data.CFStatus.Enabled = true
	ins = ruleCDNNotConfigured(data)
	require.Nil(t, ins)
}

func TestRuleClusterPeerStale(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config:      baseConfig(),
		PeerResults: []PeerInfo{{Name: "node-1", Stale: false}, {Name: "node-2", Stale: true}},
	}
	ins := ruleClusterPeerStale(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	data.PeerResults[1].Stale = false
	ins = ruleClusterPeerStale(data)
	require.Nil(t, ins)
}

func TestRuleCacheWarmNearFull(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Storage.WarmMaxBytes = 1_000_000
	data := InsightData{
		Config:     cfg,
		StoreStats: api.Stats{WarmBytes: 960_000},
	}
	ins := ruleCacheWarmNearFull(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityHigh, ins.Severity)

	data.StoreStats.WarmBytes = 800_000
	ins = ruleCacheWarmNearFull(data)
	require.Nil(t, ins)
}

func TestRuleConfigJitterZero(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Cache.TTLDefault = 60
	cfg.Routes[0].Cache.JitterPercent = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigJitterZero(data)
	require.NotNil(t, ins)

	cfg.Routes[0].Cache.JitterPercent = 10
	ins = ruleConfigJitterZero(data)
	require.Nil(t, ins)
}

func TestEvaluateSortsBySeverity(t *testing.T) {
	t.Parallel()
	e := New()
	cfg := baseConfig()
	cfg.Storage.WarmMaxBytes = 1_000_000
	data := InsightData{
		Config:     cfg,
		StoreStats: api.Stats{WarmBytes: 960_000},
		RouteStats: []observability.RouteStat{
			routeStats("api", 200, 50, 0, 25),
		},
		CFStatus: CFStatus{Enabled: false},
	}
	results := e.Evaluate(t.Context(), data)
	if len(results) < 2 {
		t.Fatalf("expected at least 2 insights, got %d", len(results))
	}
	for i := 1; i < len(results); i++ {
		if severityRank(results[i-1].Severity) > severityRank(results[i].Severity) {
			t.Errorf("results not sorted by severity at index %d: %s before %s",
				i, results[i-1].Severity, results[i].Severity)
		}
	}
}

func TestRuleCacheNoCacheControl(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config: baseConfig(),
		HeaderAudit: map[string]observability.HeaderAuditSummary{
			"api-pool": {SampleCount: 100, HasCacheControlPct: 30},
		},
	}
	ins := ruleUpstreamNoCacheControl(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	// Below sample threshold.
	data.HeaderAudit["api-pool"] = observability.HeaderAuditSummary{SampleCount: 10}
	ins = ruleUpstreamNoCacheControl(data)
	require.Nil(t, ins)
}

func TestRuleCacheVaryExplosion(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), VaryCapHits: 5}
	ins := ruleCacheVaryExplosion(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	data.VaryCapHits = 0
	ins = ruleCacheVaryExplosion(data)
	require.Nil(t, ins)
}

func TestRuleAnomalyBypassFlood(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config: baseConfig(),
		RequestBuckets: []observability.RequestBucket{
			{Requests: 100, Bypasses: 60},
		},
	}
	ins := ruleAnomalyBypassFlood(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	data.RequestBuckets[0].Bypasses = 10
	ins = ruleAnomalyBypassFlood(data)
	require.Nil(t, ins)
}

// ── Tier 1 tests ─────────────────────────────────────────────────────

func TestRuleConfigTLSBelow12(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Listen.HTTPS = ":443"
	cfg.TLS.MinVersion = "1.0"
	data := InsightData{Config: cfg}
	ins := ruleConfigTLSBelow12(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	cfg.TLS.MinVersion = "1.2"
	ins = ruleConfigTLSBelow12(data)
	require.Nil(t, ins)

	cfg.TLS.MinVersion = ""
	ins = ruleConfigTLSBelow12(data)
	require.Nil(t, ins)
}

func TestRuleConfigTracingSamplingZero(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Tracing.Endpoint = "otel:4317"
	cfg.Tracing.SamplingRate = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigTracingSamplingZero(data)
	require.NotNil(t, ins)

	cfg.Tracing.SamplingRate = 0.5
	ins = ruleConfigTracingSamplingZero(data)
	require.Nil(t, ins)

	cfg.Tracing.Endpoint = ""
	cfg.Tracing.SamplingRate = 0
	ins = ruleConfigTracingSamplingZero(data)
	require.Nil(t, ins)
}

func TestRuleConfigClusterJoinNoListener(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Cluster.Join = []string{"node-1:7946"}
	data := InsightData{Config: cfg}
	ins := ruleConfigClusterJoinNoListener(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	cfg.Listen.Cluster = ":8443"
	ins = ruleConfigClusterJoinNoListener(data)
	require.Nil(t, ins)

	cfg.Listen.Cluster = ""
	cfg.Cluster.Join = nil
	ins = ruleConfigClusterJoinNoListener(data)
	require.Nil(t, ins)
}

func TestRuleConfigPoolNoTimeout(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools[0].Connect.Timeout = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigPoolNoTimeout(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	cfg.UpstreamPools[0].Connect.Timeout = 5 * time.Second
	ins = ruleConfigPoolNoTimeout(data)
	require.Nil(t, ins)
}

func TestRuleConfigPoolNoMaxConnections(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools[0].Connect.MaxConnections = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigPoolNoMaxConnections(data)
	require.NotNil(t, ins)

	cfg.UpstreamPools[0].Connect.MaxConnections = 100
	ins = ruleConfigPoolNoMaxConnections(data)
	require.Nil(t, ins)
}

func TestRuleConfigMaxObjectSizeUnset(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Cache.MaxObjectSize = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigMaxObjectSizeUnset(data)
	require.NotNil(t, ins)

	cfg.Routes[0].Cache.MaxObjectSize = 1048576
	ins = ruleConfigMaxObjectSizeUnset(data)
	require.Nil(t, ins)
}

func TestRuleConfigRouteStripsCacheHeaders(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Response.HeaderRemove = []string{header.CacheControl, "X-Custom"}
	data := InsightData{Config: cfg}
	ins := ruleConfigRouteStripsCacheHeaders(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	cfg.Routes[0].Response.HeaderRemove = []string{"X-Custom"}
	ins = ruleConfigRouteStripsCacheHeaders(data)
	require.Nil(t, ins)
}

// ── Tier 2 tests ─────────────────────────────────────────────────────

func TestRuleCacheHighEvictionRate(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	data := InsightData{
		Config:         cfg,
		StoreStats:     api.Stats{HotEntries: 200, Evictions: 31},
		PrevStoreStats: api.Stats{HotEntries: 200, Evictions: 10},
	}
	ins := ruleCacheHighEvictionRate(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityHigh, ins.Severity)

	data.StoreStats.Evictions = 16 // 3% ratio
	ins = ruleCacheHighEvictionRate(data)
	require.Nil(t, ins)

	data.StoreStats.HotEntries = 50
	ins = ruleCacheHighEvictionRate(data)
	require.Nil(t, ins)
}

func TestRuleCacheStaleHitRatioHigh(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config: baseConfig(),
		RequestBuckets: []observability.RequestBucket{
			{Requests: 200, StaleHits: 50},
		},
	}
	ins := ruleCacheStaleHitRatioHigh(data)
	require.NotNil(t, ins)

	data.RequestBuckets[0].StaleHits = 10 // 5%
	ins = ruleCacheStaleHitRatioHigh(data)
	require.Nil(t, ins)
}

func TestRuleCacheSWRConfiguredButUnused(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Cache.StaleWhileRevalidate = 30 * time.Second
	data := InsightData{
		Config: cfg,
		RequestBuckets: []observability.RequestBucket{
			{Misses: 200, StaleHits: 0},
		},
	}
	ins := ruleCacheSWRConfiguredButUnused(data)
	require.NotNil(t, ins)

	data.RequestBuckets[0].StaleHits = 5
	ins = ruleCacheSWRConfiguredButUnused(data)
	require.Nil(t, ins)

	data.RequestBuckets[0].StaleHits = 0
	data.RequestBuckets[0].Misses = 50
	ins = ruleCacheSWRConfiguredButUnused(data)
	require.Nil(t, ins)
}

func TestRuleAnomalyLatencyP99Spike(t *testing.T) {
	t.Parallel()
	var lowHist observability.LatencyHistogram
	lowHist[3] = 100 // p99 ≈ 10ms (LatencyBoundsMs[3] = 10)

	var highHist observability.LatencyHistogram
	highHist[7] = 100 // p99 ≈ 250ms (LatencyBoundsMs[7] = 250)

	buckets := make([]observability.RequestBucket, 7)
	for i := range 6 {
		buckets[i] = observability.RequestBucket{LatHist: lowHist}
	}
	buckets[6] = observability.RequestBucket{LatHist: highHist}
	data := InsightData{Config: baseConfig(), RequestBuckets: buckets}
	ins := ruleAnomalyLatencyP99Spike(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityHigh, ins.Severity)

	// No spike — all same latency.
	for i := range 7 {
		buckets[i] = observability.RequestBucket{LatHist: lowHist}
	}
	ins = ruleAnomalyLatencyP99Spike(data)
	require.Nil(t, ins)
}

func TestRuleAnomalyRevalidationStorm(t *testing.T) {
	t.Parallel()
	buckets := make([]observability.RequestBucket, 7)
	for i := range 6 {
		buckets[i] = observability.RequestBucket{Revalidated: 10}
	}
	buckets[6] = observability.RequestBucket{Revalidated: 50}
	data := InsightData{Config: baseConfig(), RequestBuckets: buckets}
	ins := ruleAnomalyRevalidationStorm(data)
	require.NotNil(t, ins)

	buckets[6].Revalidated = 15 // 1.5x
	ins = ruleAnomalyRevalidationStorm(data)
	require.Nil(t, ins)
}

func TestRuleUpstreamTargetErrorStreak(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config: baseConfig(),
		PoolHealth: map[string][]origin.TargetStatus{
			"p": {targetStatus("a:80", true)},
		},
	}
	data.PoolHealth["p"][0].ConsecutiveErrors = 5
	ins := ruleUpstreamTargetErrorStreak(data)
	require.NotNil(t, ins)

	data.PoolHealth["p"][0].ConsecutiveErrors = 2
	ins = ruleUpstreamTargetErrorStreak(data)
	require.Nil(t, ins)

	data.PoolHealth["p"][0].ConsecutiveErrors = 5
	data.PoolHealth["p"][0].Healthy = false
	ins = ruleUpstreamTargetErrorStreak(data)
	require.Nil(t, ins)
}

func TestRuleCacheHotTierCritical(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Storage.HotMaxBytes = 1_000_000
	data := InsightData{
		Config:     cfg,
		StoreStats: api.Stats{HotBytes: 960_000},
	}
	ins := ruleCacheHotTierCritical(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityHigh, ins.Severity)

	data.StoreStats.HotBytes = 800_000
	ins = ruleCacheHotTierCritical(data)
	require.Nil(t, ins)
}

func TestRuleCacheWarmEntriesZero(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Storage.WarmMaxBytes = 1_000_000
	data := InsightData{
		Config:     cfg,
		StoreStats: api.Stats{WarmEntries: 0},
	}
	ins := ruleCacheWarmEntriesZero(data)
	require.NotNil(t, ins)

	data.StoreStats.WarmEntries = 10
	ins = ruleCacheWarmEntriesZero(data)
	require.Nil(t, ins)

	cfg.Storage.WarmMaxBytes = 0
	data.StoreStats.WarmEntries = 0
	ins = ruleCacheWarmEntriesZero(data)
	require.Nil(t, ins)
}

// ── Tier 3 tests ─────────────────────────────────────────────────────

func TestRuleClusterBroadcastFailures(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), BroadcastFailures: 5}
	ins := ruleClusterBroadcastFailures(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	data.BroadcastFailures = 0
	ins = ruleClusterBroadcastFailures(data)
	require.Nil(t, ins)
}

func TestRuleCDNLastError(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config:   baseConfig(),
		CFStatus: CFStatus{Enabled: true, LastError: "rate limited"},
	}
	ins := ruleCDNLastError(data)
	require.NotNil(t, ins)

	data.CFStatus.LastError = ""
	ins = ruleCDNLastError(data)
	require.Nil(t, ins)

	data.CFStatus.LastError = "rate limited"
	data.CFStatus.Enabled = false
	ins = ruleCDNLastError(data)
	require.Nil(t, ins)
}

func TestRuleCDNPurgeSkipped(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), CFPurgeSkipped: 3}
	ins := ruleCDNPurgeSkipped(data)
	require.NotNil(t, ins)

	data.CFPurgeSkipped = 0
	ins = ruleCDNPurgeSkipped(data)
	require.Nil(t, ins)
}

func TestRuleConfigPoolPassiveEjectForever(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools[0].Health.Passive.EjectFor = 0
	cfg.UpstreamPools[0].Health.Active.Path = ""
	data := InsightData{Config: cfg}
	ins := ruleConfigPoolPassiveEjectForever(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityMed, ins.Severity)

	cfg.UpstreamPools[0].Health.Passive.EjectFor = 30 * time.Second
	ins = ruleConfigPoolPassiveEjectForever(data)
	require.Nil(t, ins)

	cfg.UpstreamPools[0].Health.Passive.EjectFor = 0
	cfg.UpstreamPools[0].Health.Active.Path = "/health"
	ins = ruleConfigPoolPassiveEjectForever(data)
	require.Nil(t, ins)
}

func TestRuleClusterPeerHealthDegraded(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config: baseConfig(),
		PeerHealth: map[string]float64{
			"node-1": 85.0,
		},
	}
	ins := ruleClusterPeerHealthDegraded(data)
	require.NotNil(t, ins)

	data.PeerHealth["node-1"] = 95.0
	ins = ruleClusterPeerHealthDegraded(data)
	require.Nil(t, ins)
}

func TestRuleUpstreamPoolNoTraffic(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools = append(cfg.UpstreamPools, config.UpstreamPool{Name: "idle-pool"})
	data := InsightData{
		Config: cfg,
		RouteStats: []observability.RouteStat{
			routeStats("api", 200, 100, 0, 50),
		},
	}
	ins := ruleUpstreamPoolNoTraffic(data)
	require.NotNil(t, ins)
	assert.Equal(t, SeverityLow, ins.Severity)

	// All pools have traffic → no insight.
	cfg.UpstreamPools = cfg.UpstreamPools[:1]
	data.Config = cfg
	ins = ruleUpstreamPoolNoTraffic(data)
	require.Nil(t, ins)
}
