package templates

import (
	"fmt"
	"time"

	"github.com/thylong/bouine/internal/observability"
)

// LayoutProps contains the fields required by every page to render the
// shared nav/header/footer.
type LayoutProps struct {
	Page      string // "overview" | "routes" | "cluster" | "invalidation" | "config"
	PageTitle string
	NodeName  string
}

// CacheSplitData holds per-category counts for the cache-breakdown donut.
type CacheSplitData struct {
	Hits        int64
	Misses      int64
	Stales      int64
	Bypasses    int64
	Revalidated int64
}

// Total returns the sum of all cache-result categories.
func (c CacheSplitData) Total() int64 {
	return c.Hits + c.Misses + c.Stales + c.Bypasses + c.Revalidated
}

// OverviewData is the view model for the overview page.
type OverviewData struct {
	LayoutProps
	ReqPerSec   float64
	HitPct      float64
	P99MS       int64
	ErrPct      float64
	CacheSplit  CacheSplitData
	ChartLabels []string
	ChartReqs   []int64
	ChartHits   []int64
	RouteStats  []observability.RouteStat
	PeerResults []PeerResult
	HotBytes    int64
	HotMaxBytes int64
	HotEntries  int64
	Token       string // for quick-purge auth header
}

// HotFillPct returns the hot-tier fill percentage (0–100), clamped.
func (o OverviewData) HotFillPct() float64 {
	if o.HotMaxBytes <= 0 {
		return 0
	}
	p := float64(o.HotBytes) / float64(o.HotMaxBytes) * 100
	if p > 100 {
		return 100
	}
	return p
}

// RoutesData is the view model for the routes page.
type RoutesData struct {
	LayoutProps
	RouteStats []observability.RouteStat
}

// ClusterData is the view model for the cluster page.
type ClusterData struct {
	LayoutProps
	PeerResults []PeerResult
}

// InvalidationData is the view model for the invalidation page.
type InvalidationData struct {
	LayoutProps
	OpsLog []observability.OpsLogEntry
}

// ConfigData is the view model for the config page.
type ConfigData struct {
	LayoutProps
	SnapshotPath string
	Flash        string
}

// PeerResult holds one peer's fan-out result.
type PeerResult struct {
	NodeName string
	Summary  observability.MetricsSummary
	Stale    bool
}

// ── Formatting helpers used inside templates ──────────────────────────

// FmtReqS formats a requests-per-second value for dashboard cards.
func FmtReqS(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.1fk", v/1000)
	}
	return fmt.Sprintf("%.0f", v)
}

// FmtHitPct formats a hit-percentage value for dashboard cards.
func FmtHitPct(v float64) string { return fmt.Sprintf("%.1f%%", v) }

// FmtLatMs formats a latency value in milliseconds.
func FmtLatMs(ms int64) string {
	if ms == 0 {
		return "—"
	}
	return fmt.Sprintf("%dms", ms)
}

// FmtBytes formats a byte count with SI unit suffix.
func FmtBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.1fGB", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.1fMB", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0fKB", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%dB", b)
	}
}

// TimeAgo formats a past time as a human-readable relative duration.
func TimeAgo(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}

// OpsLogOpClass returns the CSS badge class for an ops-log operation.
func OpsLogOpClass(op string) string {
	switch op {
	case "purge":
		return "lz r"
	case "ban":
		return "lz y"
	case "refresh":
		return "lz g"
	default:
		return "lz d"
	}
}
