package insights

import (
	"testing"

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
