package insights

import (
	"testing"
	"time"

	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/pkg/api"
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
		if r.ID == "" {
			t.Error("insight with empty ID returned")
		}
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
	if ins == nil {
		t.Fatal("expected insight")
	}
	// Evidence should reference /slow's request count (200), not /api's (500).
	if ins.Evidence != "hit%: 25.0, requests: 200" {
		t.Errorf("evidence: want 'hit%%: 25.0, requests: 200', got %q", ins.Evidence)
	}
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
	if ins == nil {
		t.Fatal("expected insight when errors present and NegativeTTL=0")
	}

	// No errors → should not fire.
	data.RouteStats[0].Errors = 0
	ins = ruleCacheNoNegTTL(data)
	if ins != nil {
		t.Fatal("expected no insight when no errors")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 10% 5xx")
	}
	if ins.Severity != SeverityHigh {
		t.Errorf("severity: want HIGH, got %s", ins.Severity)
	}
}

func TestRuleCDNNotConfigured(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), CFStatus: CFStatus{Enabled: false}}
	ins := ruleCDNNotConfigured(data)
	if ins == nil {
		t.Fatal("expected insight when CDN not configured")
	}
	if ins.Severity != SeverityLow {
		t.Errorf("severity: want LOW, got %s", ins.Severity)
	}

	data.CFStatus.Enabled = true
	ins = ruleCDNNotConfigured(data)
	if ins != nil {
		t.Fatal("expected no insight when CDN configured")
	}
}

func TestRuleClusterPeerStale(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config:      baseConfig(),
		PeerResults: []PeerInfo{{Name: "node-1", Stale: false}, {Name: "node-2", Stale: true}},
	}
	ins := ruleClusterPeerStale(data)
	if ins == nil {
		t.Fatal("expected insight for stale peer")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	data.PeerResults[1].Stale = false
	ins = ruleClusterPeerStale(data)
	if ins != nil {
		t.Fatal("expected no insight when all peers healthy")
	}
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
	if ins == nil {
		t.Fatal("expected insight at 95% fill")
	}
	if ins.Severity != SeverityHigh {
		t.Errorf("severity at 95%%: want HIGH, got %s", ins.Severity)
	}

	data.StoreStats.WarmBytes = 800_000
	ins = ruleCacheWarmNearFull(data)
	if ins != nil {
		t.Fatal("expected no insight at 80% fill")
	}
}

func TestRuleConfigJitterZero(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Cache.TTLDefault = 60
	cfg.Routes[0].Cache.JitterPercent = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigJitterZero(data)
	if ins == nil {
		t.Fatal("expected insight for zero jitter with TTL")
	}

	cfg.Routes[0].Cache.JitterPercent = 10
	ins = ruleConfigJitterZero(data)
	if ins != nil {
		t.Fatal("expected no insight when jitter set")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 70% missing Cache-Control")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	// Below sample threshold.
	data.HeaderAudit["api-pool"] = observability.HeaderAuditSummary{SampleCount: 10}
	ins = ruleUpstreamNoCacheControl(data)
	if ins != nil {
		t.Fatal("expected no insight below 50 samples")
	}
}

func TestRuleCacheVaryExplosion(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), VaryCapHits: 5}
	ins := ruleCacheVaryExplosion(data)
	if ins == nil {
		t.Fatal("expected insight for Vary cap hits")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	data.VaryCapHits = 0
	ins = ruleCacheVaryExplosion(data)
	if ins != nil {
		t.Fatal("expected no insight when no Vary cap hits")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 60% bypass rate")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	data.RequestBuckets[0].Bypasses = 10
	ins = ruleAnomalyBypassFlood(data)
	if ins != nil {
		t.Fatal("expected no insight at 10% bypass")
	}
}

// ── Tier 1 tests ─────────────────────────────────────────────────────

func TestRuleConfigTLSBelow12(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Listen.HTTPS = ":443"
	cfg.TLS.MinVersion = "1.0"
	data := InsightData{Config: cfg}
	ins := ruleConfigTLSBelow12(data)
	if ins == nil {
		t.Fatal("expected insight for TLS 1.0")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	cfg.TLS.MinVersion = "1.2"
	ins = ruleConfigTLSBelow12(data)
	if ins != nil {
		t.Fatal("expected no insight for TLS 1.2")
	}

	cfg.TLS.MinVersion = ""
	ins = ruleConfigTLSBelow12(data)
	if ins != nil {
		t.Fatal("expected no insight when min version unset")
	}
}

func TestRuleConfigNoOCSPStapling(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Listen.HTTPS = ":443"
	data := InsightData{Config: cfg}
	ins := ruleConfigNoOCSPStapling(data)
	if ins == nil {
		t.Fatal("expected insight when OCSP not configured")
	}

	cfg.TLS.OCSPStapling = "on"
	ins = ruleConfigNoOCSPStapling(data)
	if ins != nil {
		t.Fatal("expected no insight when OCSP stapling on")
	}

	cfg.Listen.HTTPS = ""
	cfg.TLS.OCSPStapling = ""
	ins = ruleConfigNoOCSPStapling(data)
	if ins != nil {
		t.Fatal("expected no insight when HTTPS not configured")
	}
}

func TestRuleConfigTracingSamplingZero(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Tracing.Endpoint = "otel:4317"
	cfg.Tracing.SamplingRate = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigTracingSamplingZero(data)
	if ins == nil {
		t.Fatal("expected insight for zero sampling with endpoint")
	}

	cfg.Tracing.SamplingRate = 0.5
	ins = ruleConfigTracingSamplingZero(data)
	if ins != nil {
		t.Fatal("expected no insight when sampling > 0")
	}

	cfg.Tracing.Endpoint = ""
	cfg.Tracing.SamplingRate = 0
	ins = ruleConfigTracingSamplingZero(data)
	if ins != nil {
		t.Fatal("expected no insight when no endpoint")
	}
}

func TestRuleConfigClusterDisabledWithPeers(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Cluster.Enabled = false
	cfg.Cluster.Join = []string{"node-1:7946"}
	data := InsightData{Config: cfg}
	ins := ruleConfigClusterDisabledWithPeers(data)
	if ins == nil {
		t.Fatal("expected insight when cluster disabled with join addresses")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	cfg.Cluster.Enabled = true
	ins = ruleConfigClusterDisabledWithPeers(data)
	if ins != nil {
		t.Fatal("expected no insight when cluster enabled")
	}

	cfg.Cluster.Enabled = false
	cfg.Cluster.Join = nil
	ins = ruleConfigClusterDisabledWithPeers(data)
	if ins != nil {
		t.Fatal("expected no insight when no join addresses")
	}
}

func TestRuleConfigAntiEntropyDisabled(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Cluster.Enabled = true
	cfg.Cluster.Mode = config.ClusterModeFull
	cfg.Cluster.AntiEntropyInterval = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigAntiEntropyDisabled(data)
	if ins == nil {
		t.Fatal("expected insight for anti-entropy disabled in full mode")
	}

	cfg.Cluster.AntiEntropyInterval = 30 * time.Second
	ins = ruleConfigAntiEntropyDisabled(data)
	if ins != nil {
		t.Fatal("expected no insight when anti-entropy enabled")
	}

	cfg.Cluster.Mode = config.ClusterModeEventual
	cfg.Cluster.AntiEntropyInterval = 0
	ins = ruleConfigAntiEntropyDisabled(data)
	if ins != nil {
		t.Fatal("expected no insight in non-full mode")
	}
}

func TestRuleConfigPoolNoTimeout(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools[0].Connect.Timeout = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigPoolNoTimeout(data)
	if ins == nil {
		t.Fatal("expected insight for no connect timeout")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	cfg.UpstreamPools[0].Connect.Timeout = 5 * time.Second
	ins = ruleConfigPoolNoTimeout(data)
	if ins != nil {
		t.Fatal("expected no insight when timeout set")
	}
}

func TestRuleConfigPoolNoMaxConnections(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools[0].Connect.MaxConnections = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigPoolNoMaxConnections(data)
	if ins == nil {
		t.Fatal("expected insight for no max connections")
	}

	cfg.UpstreamPools[0].Connect.MaxConnections = 100
	ins = ruleConfigPoolNoMaxConnections(data)
	if ins != nil {
		t.Fatal("expected no insight when max connections set")
	}
}

func TestRuleConfigMaxObjectSizeUnset(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Cache.MaxObjectSize = 0
	data := InsightData{Config: cfg}
	ins := ruleConfigMaxObjectSizeUnset(data)
	if ins == nil {
		t.Fatal("expected insight for no max object size")
	}

	cfg.Routes[0].Cache.MaxObjectSize = 1048576
	ins = ruleConfigMaxObjectSizeUnset(data)
	if ins != nil {
		t.Fatal("expected no insight when max object size set")
	}
}

func TestRuleConfigRouteStripsCacheHeaders(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Routes[0].Response.HeaderRemove = []string{"Cache-Control", "X-Custom"}
	data := InsightData{Config: cfg}
	ins := ruleConfigRouteStripsCacheHeaders(data)
	if ins == nil {
		t.Fatal("expected insight for stripping Cache-Control")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	cfg.Routes[0].Response.HeaderRemove = []string{"X-Custom"}
	ins = ruleConfigRouteStripsCacheHeaders(data)
	if ins != nil {
		t.Fatal("expected no insight when only non-cache headers removed")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 10% eviction rate")
	}
	if ins.Severity != SeverityHigh {
		t.Errorf("severity: want HIGH, got %s", ins.Severity)
	}

	data.StoreStats.Evictions = 16 // 3% ratio
	ins = ruleCacheHighEvictionRate(data)
	if ins != nil {
		t.Fatal("expected no insight below 5% eviction")
	}

	data.StoreStats.HotEntries = 50
	ins = ruleCacheHighEvictionRate(data)
	if ins != nil {
		t.Fatal("expected no insight below 100 hot entries")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 25% stale ratio")
	}

	data.RequestBuckets[0].StaleHits = 10 // 5%
	ins = ruleCacheStaleHitRatioHigh(data)
	if ins != nil {
		t.Fatal("expected no insight below 20% stale ratio")
	}
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
	if ins == nil {
		t.Fatal("expected insight for SWR configured but 0 stale hits")
	}

	data.RequestBuckets[0].StaleHits = 5
	ins = ruleCacheSWRConfiguredButUnused(data)
	if ins != nil {
		t.Fatal("expected no insight when stale hits > 0")
	}

	data.RequestBuckets[0].StaleHits = 0
	data.RequestBuckets[0].Misses = 50
	ins = ruleCacheSWRConfiguredButUnused(data)
	if ins != nil {
		t.Fatal("expected no insight below 100 misses")
	}
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
	if ins == nil {
		t.Fatal("expected insight for p99 spike")
	}
	if ins.Severity != SeverityHigh {
		t.Errorf("severity: want HIGH for >3x spike, got %s", ins.Severity)
	}

	// No spike — all same latency.
	for i := range 7 {
		buckets[i] = observability.RequestBucket{LatHist: lowHist}
	}
	ins = ruleAnomalyLatencyP99Spike(data)
	if ins != nil {
		t.Fatal("expected no insight when no spike")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 5x revalidation spike")
	}

	buckets[6].Revalidated = 15 // 1.5x
	ins = ruleAnomalyRevalidationStorm(data)
	if ins != nil {
		t.Fatal("expected no insight below 3x spike")
	}
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
	if ins == nil {
		t.Fatal("expected insight for 5 consecutive errors while healthy")
	}

	data.PoolHealth["p"][0].ConsecutiveErrors = 2
	ins = ruleUpstreamTargetErrorStreak(data)
	if ins != nil {
		t.Fatal("expected no insight below 5 errors")
	}

	data.PoolHealth["p"][0].ConsecutiveErrors = 5
	data.PoolHealth["p"][0].Healthy = false
	ins = ruleUpstreamTargetErrorStreak(data)
	if ins != nil {
		t.Fatal("expected no insight when target already unhealthy")
	}
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
	if ins == nil {
		t.Fatal("expected insight at 96% hot tier fill")
	}
	if ins.Severity != SeverityHigh {
		t.Errorf("severity: want HIGH, got %s", ins.Severity)
	}

	data.StoreStats.HotBytes = 800_000
	ins = ruleCacheHotTierCritical(data)
	if ins != nil {
		t.Fatal("expected no insight below 95%")
	}
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
	if ins == nil {
		t.Fatal("expected insight for warm tier configured but empty")
	}

	data.StoreStats.WarmEntries = 10
	ins = ruleCacheWarmEntriesZero(data)
	if ins != nil {
		t.Fatal("expected no insight when warm entries > 0")
	}

	cfg.Storage.WarmMaxBytes = 0
	data.StoreStats.WarmEntries = 0
	ins = ruleCacheWarmEntriesZero(data)
	if ins != nil {
		t.Fatal("expected no insight when warm tier not configured")
	}
}

// ── Tier 3 tests ─────────────────────────────────────────────────────

func TestRuleClusterReplicationStalled(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Cluster.Mode = config.ClusterModeFull
	data := InsightData{
		Config: cfg,
		// 10 minutes ago — well beyond the 5-minute threshold.
		ReplicationLastRecv: time.Now().Add(-10 * time.Minute).Unix(),
	}
	ins := ruleClusterReplicationStalled(data)
	if ins == nil {
		t.Fatal("expected insight for stalled replication")
	}

	data.ReplicationLastRecv = time.Now().Unix()
	ins = ruleClusterReplicationStalled(data)
	if ins != nil {
		t.Fatal("expected no insight when replication recent")
	}

	data.ReplicationLastRecv = 0
	ins = ruleClusterReplicationStalled(data)
	if ins != nil {
		t.Fatal("expected no insight when never received (0)")
	}

	cfg.Cluster.Mode = config.ClusterModeEventual
	data.ReplicationLastRecv = time.Now().Add(-10 * time.Minute).Unix()
	ins = ruleClusterReplicationStalled(data)
	if ins != nil {
		t.Fatal("expected no insight in non-full mode")
	}
}

func TestRuleClusterNoReplicationTraffic(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.Cluster.Mode = config.ClusterModeFull
	data := InsightData{Config: cfg, ReplicationBytes: 0}
	ins := ruleClusterNoReplicationTraffic(data)
	if ins == nil {
		t.Fatal("expected insight for zero replication traffic in full mode")
	}

	data.ReplicationBytes = 1024
	ins = ruleClusterNoReplicationTraffic(data)
	if ins != nil {
		t.Fatal("expected no insight when replication bytes > 0")
	}

	cfg.Cluster.Mode = config.ClusterModeEventual
	data.ReplicationBytes = 0
	ins = ruleClusterNoReplicationTraffic(data)
	if ins != nil {
		t.Fatal("expected no insight in non-full mode")
	}
}

func TestRuleClusterBroadcastFailures(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), BroadcastFailures: 5}
	ins := ruleClusterBroadcastFailures(data)
	if ins == nil {
		t.Fatal("expected insight for broadcast failures")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	data.BroadcastFailures = 0
	ins = ruleClusterBroadcastFailures(data)
	if ins != nil {
		t.Fatal("expected no insight when no failures")
	}
}

func TestRuleCDNLastError(t *testing.T) {
	t.Parallel()
	data := InsightData{
		Config:   baseConfig(),
		CFStatus: CFStatus{Enabled: true, LastError: "rate limited"},
	}
	ins := ruleCDNLastError(data)
	if ins == nil {
		t.Fatal("expected insight for CF last error")
	}

	data.CFStatus.LastError = ""
	ins = ruleCDNLastError(data)
	if ins != nil {
		t.Fatal("expected no insight when no error")
	}

	data.CFStatus.LastError = "rate limited"
	data.CFStatus.Enabled = false
	ins = ruleCDNLastError(data)
	if ins != nil {
		t.Fatal("expected no insight when CF disabled")
	}
}

func TestRuleCDNPurgeSkipped(t *testing.T) {
	t.Parallel()
	data := InsightData{Config: baseConfig(), CFPurgeSkipped: 3}
	ins := ruleCDNPurgeSkipped(data)
	if ins == nil {
		t.Fatal("expected insight for skipped purges")
	}

	data.CFPurgeSkipped = 0
	ins = ruleCDNPurgeSkipped(data)
	if ins != nil {
		t.Fatal("expected no insight when no purges skipped")
	}
}

func TestRuleConfigPoolPassiveEjectForever(t *testing.T) {
	t.Parallel()
	cfg := baseConfig()
	cfg.UpstreamPools[0].Health.Passive.EjectFor = 0
	cfg.UpstreamPools[0].Health.Active.Path = ""
	data := InsightData{Config: cfg}
	ins := ruleConfigPoolPassiveEjectForever(data)
	if ins == nil {
		t.Fatal("expected insight for passive eject forever")
	}
	if ins.Severity != SeverityMed {
		t.Errorf("severity: want MED, got %s", ins.Severity)
	}

	cfg.UpstreamPools[0].Health.Passive.EjectFor = 30 * time.Second
	ins = ruleConfigPoolPassiveEjectForever(data)
	if ins != nil {
		t.Fatal("expected no insight when EjectFor set")
	}

	cfg.UpstreamPools[0].Health.Passive.EjectFor = 0
	cfg.UpstreamPools[0].Health.Active.Path = "/health"
	ins = ruleConfigPoolPassiveEjectForever(data)
	if ins != nil {
		t.Fatal("expected no insight when active health check configured")
	}
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
	if ins == nil {
		t.Fatal("expected insight for degraded peer uptime")
	}

	data.PeerHealth["node-1"] = 95.0
	ins = ruleClusterPeerHealthDegraded(data)
	if ins != nil {
		t.Fatal("expected no insight when uptime >= 90%")
	}
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
	if ins == nil {
		t.Fatal("expected insight for pool with no traffic")
	}
	if ins.Severity != SeverityLow {
		t.Errorf("severity: want LOW, got %s", ins.Severity)
	}

	// All pools have traffic → no insight.
	cfg.UpstreamPools = cfg.UpstreamPools[:1]
	data.Config = cfg
	ins = ruleUpstreamPoolNoTraffic(data)
	if ins != nil {
		t.Fatal("expected no insight when all pools have traffic")
	}
}
