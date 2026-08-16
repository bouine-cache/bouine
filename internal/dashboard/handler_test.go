package dashboard

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/dashboard/insights"
	"github.com/bouine-cache/bouine/internal/dashboard/templates"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestSortRouteStats(t *testing.T) {
	t.Parallel()
	stats := []observability.RouteStat{
		{Route: "b", Requests: 10},
		{Route: "a", Requests: 50},
		{Route: "c", Requests: 30},
	}
	sorted := sortRouteStats(stats)
	require.Equal(t, 3, len(sorted))
	assert.Equal(t, "a", sorted[0].Route)
	assert.Equal(t, "c", sorted[1].Route)
	assert.Equal(t, "b", sorted[2].Route)
}

func TestSortRouteStats_Empty(t *testing.T) {
	t.Parallel()
	sorted := sortRouteStats(nil)
	assert.Empty(t, sorted)
}

func TestSortURLStats(t *testing.T) {
	t.Parallel()
	stats := []observability.URLStat{
		{URL: "/b", Requests: 5},
		{URL: "/a", Requests: 20},
	}
	sorted := sortURLStats(stats)
	require.Equal(t, 2, len(sorted))
	assert.Equal(t, "/a", sorted[0].URL)
	assert.Equal(t, "/b", sorted[1].URL)
}

func TestSortURLStats_Empty(t *testing.T) {
	t.Parallel()
	sorted := sortURLStats(nil)
	assert.Empty(t, sorted)
}

func TestApdexScore(t *testing.T) {
	t.Parallel()
	t.Run("zero_total", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, 0.0, apdexScore(observability.LatencyHistogram{}, 0))
	})
	t.Run("all_satisfied", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		score := apdexScore(h, 100)
		assert.Equal(t, 1.0, score)
	})
	t.Run("all_tolerating", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{0, 0, 0, 0, 0, 0, 0, 100, 0, 0, 0}
		score := apdexScore(h, 100)
		assert.Equal(t, 0.5, score)
	})
	t.Run("mixed", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{50, 0, 0, 0, 0, 0, 0, 50, 0, 0, 0}
		score := apdexScore(h, 100)
		assert.Equal(t, 0.75, score)
	})
}

func TestSLOBuckets(t *testing.T) {
	t.Parallel()
	t.Run("zero_total", func(t *testing.T) {
		t.Parallel()
		buckets := sloBuckets(observability.LatencyHistogram{}, 0)
		require.Equal(t, 3, len(buckets))
		for _, b := range buckets {
			assert.Equal(t, 0.0, b.Pct)
		}
	})
	t.Run("all_under_10ms", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
		buckets := sloBuckets(h, 100)
		require.Equal(t, 3, len(buckets))
		assert.Equal(t, 100.0, buckets[0].Pct) // 10ms
		assert.Equal(t, 100.0, buckets[1].Pct) // 100ms
		assert.Equal(t, 100.0, buckets[2].Pct) // 1s
	})
	t.Run("some_over_10ms", func(t *testing.T) {
		t.Parallel()
		h := observability.LatencyHistogram{30, 0, 0, 0, 0, 0, 0, 70, 0, 0, 0}
		buckets := sloBuckets(h, 100)
		assert.Equal(t, 30.0, buckets[0].Pct)  // 10ms
		assert.Equal(t, 30.0, buckets[1].Pct)  // 100ms
		assert.Equal(t, 100.0, buckets[2].Pct) // 1s
	})
}

func TestValidateCacheURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		url  string
		want string
	}{
		{"empty", "", "URL is required"},
		{"invalid", "ht\ttp://invalid", "invalid URL"},
		{"wrong_scheme", "ftp://example.com", "URL must begin with http:// or https://"},
		{"no_host", "http:///path", "URL must include a host"},
		{"valid_http", "http://example.com/path", ""},
		{"valid_https", "https://example.com/path", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := validateCacheURL(tt.url)
			if tt.want == "" {
				assert.Empty(t, got)
			} else {
				assert.Contains(t, got, tt.want)
			}
		})
	}
}

func TestValidateRegex(t *testing.T) {
	t.Parallel()
	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", validateRegex("field", ""))
	})
	t.Run("valid", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "", validateRegex("field", "^/api/.*$"))
	})
	t.Run("invalid", func(t *testing.T) {
		t.Parallel()
		result := validateRegex("field", "[invalid")
		assert.Contains(t, result, "field")
		assert.Contains(t, result, "regex")
	})
}

func TestEncodeJSON(t *testing.T) {
	t.Parallel()
	data := map[string]int{"a": 1, "b": 2}
	b, err := EncodeJSON(data)
	require.NoError(t, err)
	assert.NotEmpty(t, b)
}

func TestEncodeJSON_Nil(t *testing.T) {
	t.Parallel()
	b, err := EncodeJSON(nil)
	require.NoError(t, err)
	assert.Equal(t, "null\n", string(b))
}

func TestParseTimeRange(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input   string
		buckets int
		label   string
	}{
		{"1h", 360, "1h"},
		{"24h", 2160, "24h"},
		{"", 2160, "6h"},
		{"invalid", 2160, "6h"},
	}
	for _, tt := range tests {
		buckets, label := parseTimeRange(tt.input)
		assert.Equal(t, tt.buckets, buckets)
		assert.Equal(t, tt.label, label)
	}
}

func TestLatHistToInts(t *testing.T) {
	t.Parallel()
	h := observability.LatencyHistogram{1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0}
	ints := latHistToInts(h)
	assert.Equal(t, []int64{1, 2, 3, 0, 0, 0, 0, 0, 0, 0, 0}, ints)
}

func TestBuildOverviewStats(t *testing.T) {
	t.Parallel()
	snap := []observability.RequestBucket{
		{Timestamp: 1, Requests: 10, Hits: 8, Misses: 1, Errors: 1, P99MS: 50},
		{Timestamp: 2, Requests: 20, Hits: 15, Misses: 3, Errors: 2, P99MS: 80},
		{Timestamp: 3, Requests: 15, Hits: 12, Misses: 2, Errors: 1, P99MS: 60},
		{Timestamp: 4, Requests: 25, Hits: 20, Misses: 4, Errors: 1, P99MS: 90},
		{Timestamp: 5, Requests: 30, Hits: 25, Misses: 4, Errors: 1, P99MS: 70},
		{Timestamp: 6, Requests: 20, Hits: 18, Misses: 1, Errors: 1, P99MS: 55},
	}
	stats := buildOverviewStats(snap, len(snap))
	// Recent window is last 6 buckets: 10+20+15+25+30+20 = 120.
	assert.Equal(t, int64(120), stats.totalReq)
	assert.Greater(t, stats.hitPct, 0.0)
}

func TestBuildOverviewStats_Empty(t *testing.T) {
	t.Parallel()
	stats := buildOverviewStats(nil, 0)
	assert.Equal(t, int64(0), stats.totalReq)
}

func TestToPeerResultsEnriched(t *testing.T) {
	t.Parallel()
	in := []PeerResult{
		{NodeName: "node1", Summary: observability.MetricsSummary{NodeName: "node1"}, Stale: false},
		{NodeName: "node2", Summary: observability.MetricsSummary{NodeName: "node2"}, Stale: true},
	}
	peersFn := func() []api.PeerInfo {
		return []api.PeerInfo{
			{Name: "node1", DataAddr: "1.1.1.1:8080", AdminAddr: "1.1.1.1:9090", Weight: 1},
			{Name: "node2", DataAddr: "2.2.2.2:8080", AdminAddr: "2.2.2.2:9090", Weight: 2},
		}
	}
	out := toPeerResultsEnriched(in, peersFn)
	assert.Equal(t, 2, len(out))
	assert.Equal(t, "1.1.1.1:8080", out[0].DataAddr)
	assert.Equal(t, "2.2.2.2:8080", out[1].DataAddr)
	assert.True(t, out[1].Stale)
}

func TestToPeerResultsEnriched_NilPeersFn(t *testing.T) {
	t.Parallel()
	in := []PeerResult{{NodeName: "node1"}}
	out := toPeerResultsEnriched(in, nil)
	assert.Equal(t, 1, len(out))
	assert.Equal(t, "node1", out[0].NodeName)
	assert.Empty(t, out[0].DataAddr)
}

func TestNew_CreatesHandler(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	h := New(Config{
		Token: "test-token",
	}, mux)
	require.NotNil(t, h)
	require.NotNil(t, h.auth)
}

func TestHandler_NodeName(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	assert.Equal(t, "unknown", h.nodeName())
}

func TestHandler_NodeName_WithRings(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{Rings: observability.NewRings("my-node")}}
	assert.Equal(t, "my-node", h.nodeName())
}

func TestHandler_SidebarProps_NoRings(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	reqs, hitPct, peerCount, live := h.sidebarProps("6h")
	assert.Equal(t, 0.0, reqs)
	assert.Equal(t, 0.0, hitPct)
	assert.Equal(t, 1, peerCount)
	assert.Equal(t, 1, live)
}

func TestHandler_SidebarProps_WithPeersFn(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{
		PeersFn: func() []api.PeerInfo {
			return []api.PeerInfo{{Name: "a"}, {Name: "b"}, {Name: "c"}}
		},
	}}
	_, _, peerCount, live := h.sidebarProps("6h")
	assert.Equal(t, 3, peerCount)
	assert.Equal(t, 3, live)
}

func TestHandler_CFStatusCard_Nil(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	assert.Nil(t, h.cfStatusCard())
}

func TestHandler_StoreStats_NoStoreFn(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	hotBytes, _, _, _, _, _, _ := h.storeStats()
	assert.Equal(t, int64(0), hotBytes)
}

func TestHandler_BuildRouteToPool_NoConfig(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	m := h.buildRouteToPool()
	assert.Empty(t, m)
}

func TestValidateRegex_Empty(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", validateRegex("field", ""))
}

func TestValidateRegex_Valid(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "", validateRegex("field", "^/api/.*$"))
}

func TestValidateRegex_Invalid(t *testing.T) {
	t.Parallel()
	result := validateRegex("field", "[invalid")
	assert.Contains(t, result, "field")
	assert.Contains(t, result, "regex")
}

// TestConvertInsightCards verifies the insight card mapping and severity counting.
func TestConvertInsightCards(t *testing.T) {
	t.Parallel()
	raw := []insights.Insight{
		{ID: "1", Severity: insights.SeverityHigh, Category: insights.CategoryCDN, Title: "High", Detail: "d", Evidence: "e", Routes: []string{"r1"}, Action: "fix"},
		{ID: "2", Severity: insights.SeverityMed, Category: insights.CategoryCluster, Title: "Med", Detail: "d2"},
		{ID: "3", Severity: insights.SeverityLow, Category: insights.CategoryConfig, Title: "Low", Detail: "d3"},
	}
	cards, high, med, low := convertInsightCards(raw, map[string]string{"r1": "pool1"})
	assert.Equal(t, 3, len(cards))
	assert.Equal(t, 1, high)
	assert.Equal(t, 1, med)
	assert.Equal(t, 1, low)
	assert.Equal(t, "1", cards[0].ID)
	assert.Equal(t, "HIGH", cards[0].Severity)
	assert.Contains(t, cards[0].NodeIDs, "cdn")
}

// TestInsightNodeIDs verifies the node ID mapping for different categories.
func TestInsightNodeIDs(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		category insights.Category
		routes   []string
		wantIDs  []string
	}{
		{"cdn", insights.CategoryCDN, nil, []string{"cdn"}},
		{"cluster", insights.CategoryCluster, nil, []string{"bouine"}},
		{"config", insights.CategoryConfig, nil, []string{"bouine"}},
		{"route_with_pool", insights.CategoryCache, []string{"api"}, []string{"pool:origin1"}},
		{"route_no_pool", insights.CategoryCache, []string{"unknown"}, []string{"bouine"}},
	}
	routeToPool := map[string]string{"api": "origin1"}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ins := insights.Insight{Category: tt.category, Routes: tt.routes}
			ids := insightNodeIDs(ins, routeToPool)
			assert.Equal(t, tt.wantIDs, ids)
		})
	}
}

// TestBuildRouteToPool_WithConfig verifies route-to-pool mapping from config.
func TestBuildRouteToPool_WithConfig(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{
		Config: &config.Config{
			Routes: []config.Route{
				{Name: "api", Pool: "origin1"},
				{Name: "static", Pool: "origin2"},
				{Match: config.RouteMatch{PathPrefix: "/fallback"}, Pool: "origin3"},
			},
		},
	}}
	m := h.buildRouteToPool()
	assert.Equal(t, "origin1", m["api"])
	assert.Equal(t, "origin2", m["static"])
	assert.Equal(t, "origin3", m["/fallback"])
}

// TestClientNode verifies the client architecture node.
func TestClientNode(t *testing.T) {
	t.Parallel()
	n := clientNode()
	assert.Equal(t, "client", n.ID)
	assert.Equal(t, "Clients", n.Label)
	assert.Equal(t, "healthy", n.Status)
}

// TestCDNNode verifies the CDN architecture node.
func TestCDNNode(t *testing.T) {
	t.Parallel()
	card := &templates.CFStatusCard{ZoneID: "zone123", Async: true}
	n := cdnNode(card)
	assert.Equal(t, "cdn", n.ID)
	assert.Contains(t, n.Detail, "zone123")
	assert.Contains(t, n.Detail, "async")
}

// TestClusterNode verifies the cluster architecture node.
func TestClusterNode(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{ClusterMeta: templates.ClusterMeta{Mode: "full"}}}
	peers := []PeerResult{
		{NodeName: "node1", Stale: false},
		{NodeName: "node2", Stale: true},
	}
	n := h.clusterNode(peers, api.Stats{})
	assert.Equal(t, "bouine", n.ID)
	assert.Equal(t, "degraded", n.Status)
	assert.Equal(t, 2, len(n.Peers))
}

// TestClusterNode_AllStale verifies all-stale peers result in unhealthy status.
func TestClusterNode_AllStale(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	peers := []PeerResult{
		{NodeName: "node1", Stale: true},
		{NodeName: "node2", Stale: true},
	}
	n := h.clusterNode(peers, api.Stats{})
	assert.Equal(t, "unhealthy", n.Status)
}

// TestClusterNode_NoPeers verifies no peers results in healthy status.
func TestClusterNode_NoPeers(t *testing.T) {
	t.Parallel()
	h := &Handler{cfg: Config{}}
	n := h.clusterNode(nil, api.Stats{})
	assert.Equal(t, "healthy", n.Status)
}

// TestPoolNodeStatus verifies pool health status mapping.
func TestPoolNodeStatus(t *testing.T) {
	t.Parallel()
	poolHealth := map[string][]origin.TargetStatus{
		"origin1": {
			{Healthy: true},
			{Healthy: true},
		},
		"origin2": {
			{Healthy: false},
		},
	}
	status, detail := poolNodeStatus("origin1", poolHealth)
	assert.Equal(t, "healthy", status)
	assert.NotEmpty(t, detail)

	status2, detail2 := poolNodeStatus("origin2", poolHealth)
	assert.Equal(t, "unhealthy", status2)
	assert.NotEmpty(t, detail2)

	// Unknown pool defaults to healthy.
	status3, _ := poolNodeStatus("unknown", poolHealth)
	assert.Equal(t, "healthy", status3)
}

// TestStorageTiers verifies the storage tier status mapping.
func TestStorageTiers(t *testing.T) {
	t.Parallel()
	tiers := storageTiers(5000, 1000, api.Stats{HotBytes: 100, WarmBytes: 500})
	assert.NotEmpty(t, tiers)
}

// TestStorageTiers_ZeroMax verifies zero max returns 0%.
func TestStorageTiers_ZeroMax(t *testing.T) {
	t.Parallel()
	tiers := storageTiers(0, 0, api.Stats{})
	assert.NotEmpty(t, tiers)
}
