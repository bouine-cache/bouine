// Package insights evaluates operational and configuration data against
// a set of rules to produce actionable insights for dashboard operators.
// Each rule is a pure function — no side effects, no I/O. The Engine
// collects results and deduplicates insights that fire for multiple routes.
package insights

import (
	"context"
	"sort"

	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/internal/origin"
	"github.com/thylong/bouine/pkg/api"
)

// Severity ranks insights by urgency.
type Severity string

// Severity values rank insights by urgency. HIGH insights require
// immediate attention; MED are recommended improvements; LOW are
// informational.
const (
	SeverityHigh Severity = "HIGH"
	SeverityMed  Severity = "MED"
	SeverityLow  Severity = "LOW"
)

// Category groups insights by domain.
type Category string

// Category values group insights by domain: cache policy, upstream
// health, CDN integration, cluster topology, configuration, and
// anomaly detection.
const (
	CategoryCache    Category = "cache"
	CategoryUpstream Category = "upstream"
	CategoryCDN      Category = "cdn"
	CategoryCluster  Category = "cluster"
	CategoryConfig   Category = "config"
	CategoryAnomaly  Category = "anomaly"
)

// Insight is a single actionable finding.
type Insight struct {
	ID       string
	Severity Severity
	Category Category
	Title    string
	Detail   string
	Evidence string
	Routes   []string
	Action   string
}

// InsightData is the aggregated data collected by the dashboard handler
// before calling Engine.Evaluate. It holds all the inputs the rules need.
type InsightData struct {
	Config              *config.Config
	StoreStats          api.Stats
	RouteStats          []observability.RouteStat
	RequestBuckets      []observability.RequestBucket
	PeerResults         []PeerInfo
	PeerHealth          map[string]float64 // peer name → uptime % (0-100)
	CFStatus            CFStatus
	PoolHealth          map[string][]origin.TargetStatus
	HeaderAudit         map[string]observability.HeaderAuditSummary
	VaryCapHits         int64
	PrevStoreStats      api.Stats
	ReplicationLastRecv int64 // unix seconds of last replication receive (0 = never)
	ReplicationBytes    int64 // total bytes sent + received since startup
	BroadcastFailures   int64 // total cluster broadcast failures
	CFPurgeSkipped      int64 // total CF purges skipped
}

// PeerInfo is a simplified peer status for insight evaluation.
type PeerInfo struct {
	Name  string
	Stale bool
}

// CFStatus is the Cloudflare integration status.
type CFStatus struct {
	Enabled   bool
	Async     bool
	LastLagMs int64
	LastError string // empty when no error
}

// Engine evaluates all registered insight rules against the provided data.
type Engine struct {
	prevStoreStats api.Stats
}

// New creates a new Engine.
func New() *Engine {
	return &Engine{}
}

// Evaluate runs all insight rules and returns the results, sorted by
// severity (HIGH first, then MED, then LOW).
func (e *Engine) Evaluate(_ context.Context, data InsightData) []Insight {
	data.PrevStoreStats = e.prevStoreStats
	e.prevStoreStats = data.StoreStats

	var results []Insight
	for _, rule := range rules {
		if insight := rule(data); insight != nil {
			results = append(results, *insight)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return severityRank(results[i].Severity) < severityRank(results[j].Severity)
	})

	return results
}

func severityRank(s Severity) int {
	switch s {
	case SeverityHigh:
		return 0
	case SeverityMed:
		return 1
	default:
		return 2
	}
}

// ruleFunc is a single insight rule. It returns nil if the insight
// is not triggered by the current data.
type ruleFunc func(data InsightData) *Insight

var rules []ruleFunc
