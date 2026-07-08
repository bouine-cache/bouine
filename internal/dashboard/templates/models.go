package templates

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// ── Layout ──────────────────────────────────────────────────────────

// LayoutProps contains the fields required by every page to render the
// shared nav/header/footer sidebar and tabs bar.
type LayoutProps struct {
	Page      string // "overview" | "routes" | "cluster" | "invalidation" | "config"
	PageTitle string
	NodeName  string
	Version   string // build version shown in the sidebar (buildinfo.Version)
	// TimeRange is shown in the tabs-bar range selector and preserved
	// across page navigations (M2/M3).
	TimeRange string // "1h" | "6h" | "24h"; default "6h"
	// Sidebar live stats (M2).
	PeerCount     int     // total peers including self
	LivePeers     int     // non-stale peers
	SidebarReqS   float64 // requests/s from local ring
	SidebarHitPct float64 // hit % from local ring
}

// ── Overview ─────────────────────────────────────────────────────────

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

// TrendData carries delta vs the prior window for one metric.
// Positive = improvement (more requests, better hit %, lower error).
type TrendData struct {
	Delta float64 // absolute change
	Up    bool    // direction arrow
	Down  bool
	Label string // e.g. "↑ 12% vs prior"
}

// OverviewData is the view model for the overview page.
type OverviewData struct {
	LayoutProps
	ReqPerSec   float64
	HitPct      float64
	P99MS       int64
	P50MS       int64
	P90MS       int64
	ErrPct      float64
	TrendReq    TrendData
	TrendHit    TrendData
	TrendLat    TrendData
	TrendErr    TrendData
	CacheSplit  CacheSplitData
	ChartLabels []string
	ChartReqs   []int64
	ChartErrs   []int64 // error count per bucket (second chart line)
	RouteRows   []RouteRow
	PeerResults []PeerResult
	// Storage tier stats.
	HotBytes     int64
	HotMaxBytes  int64
	HotEntries   int64
	WarmBytes    int64
	WarmMaxBytes int64
	WarmEntries  int64
	Evictions    int64
	// Ring for the compact circular SVG on the overview bottom row.
	RingSegs []api.RingSegment
	// ClusterMode is the active cluster consistency model:
	// "strong", "eventual", "full", or "single-node" when cluster is disabled.
	// Used to conditionally render ring SVG vs. mode info on overview.
	ClusterMode string
}

// CFStatusCard is the view model for the Cloudflare status card on the
// overview page.
type CFStatusCard struct {
	Enabled       bool
	ZoneID        string
	Async         bool
	LastError     string // empty when no error
	LastSuccessAt string // RFC 3339 or empty
	LastLagMs     int64  // async propagation latency (0 when sync or disabled)
}

// HotFillPct returns the hot-tier fill percentage (0–100), clamped.
func (o OverviewData) HotFillPct() float64 { return fillPct(o.HotBytes, o.HotMaxBytes) }

// WarmFillPct returns the warm-tier fill percentage (0–100), clamped.
func (o OverviewData) WarmFillPct() float64 { return fillPct(o.WarmBytes, o.WarmMaxBytes) }

func fillPct(used, max int64) float64 {
	if max <= 0 {
		return 0
	}
	p := float64(used) / float64(max) * 100
	if p > 100 {
		return 100
	}
	return p
}

// sharePct returns part's percentage of total (0–100), or 0 when total
// is zero. Used for doughnut chart legend labels.
func sharePct(part, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total) * 100
}

// ── Performance ──────────────────────────────────────────────────────

// PerformanceData is the view model for the performance page. It focuses
// exclusively on request-latency signals so the overview stays lean.
type PerformanceData struct {
	LayoutProps
	// Latency percentiles over the recent window (last ~60s).
	P50MS, P90MS, P99MS, AvgMS int64
	TrendP99                   TrendData
	// LatHist is the latency distribution (request counts per bucket) for
	// the recent window; bucket bounds are observability.LatencyBoundsMs.
	LatHist []int64
	// Per-bucket latency series over the selected time range.
	ChartLabels []string
	P99Series   []int64
	AvgSeries   []int64
	// Apdex application-performance index over the recent window.
	Apdex          float64 // 0..1
	ApdexTargetMS  int64   // satisfied threshold T
	ApdexToleratMS int64   // tolerating threshold (≈4T, bucket-aligned)
	// SLO compliance: share of requests at or under each latency target.
	SLO          []SLOBucket
	TotalSamples int64
}

// SLOBucket is the share of requests served at or under a latency target.
type SLOBucket struct {
	Label string  // e.g. "10ms"
	Pct   float64 // 0..100
}

// ApdexLabel returns a qualitative rating for the Apdex score.
func (p PerformanceData) ApdexLabel() string {
	switch {
	case p.TotalSamples == 0:
		return "no data"
	case p.Apdex >= 0.94:
		return "excellent"
	case p.Apdex >= 0.85:
		return "good"
	case p.Apdex >= 0.70:
		return "fair"
	case p.Apdex >= 0.50:
		return "poor"
	default:
		return "unacceptable"
	}
}

// ApdexClass returns the CSS state class (g/y/r) for the Apdex score.
func (p PerformanceData) ApdexClass() string {
	switch {
	case p.TotalSamples == 0:
		return "d"
	case p.Apdex >= 0.85:
		return "g"
	case p.Apdex >= 0.70:
		return "y"
	default:
		return "r"
	}
}

// ── Routes ───────────────────────────────────────────────────────────

// RouteRow joins a configured route's policy with its live ring stats.
type RouteRow struct {
	// From config.Route
	Name        string
	PathPrefix  string
	Host        string
	Pool        string
	TTL         string // formatted NegativeTTL or "—"
	SWR         string // StaleWhileRevalidate or "—"
	SIE         string // StaleIfError or "—"
	NegTTL      string
	StayinAlive bool
	Jitter      string
	// v0.1.0 route features
	Methods  string         // "GET·HEAD" or "" (all methods)
	Features []RouteFeature // compact badges for set per-route features
	IsBypass bool           // private or no-store: no cache policy
	// From observability.RouteStat
	Requests  int64
	Hits      int64
	Misses    int64
	HitPct    float64
	Sparkline []int64
}

// RouteFeature is a compact badge describing a per-route capability that
// is enabled (ttl_override, allow_set_cookie, max_object_size,
// strip_prefix, strip_query_params).
type RouteFeature struct {
	Label string // short text shown in the badge, e.g. "override 1h"
	Title string // tooltip with the full explanation
	Warn  bool   // amber/red styling (security-relevant, e.g. allow_set_cookie)
}

// RoutesData is the view model for the routes page.
type RoutesData struct {
	LayoutProps
	RouteCount int
	RouteRows  []RouteRow
	URLStats   []observability.URLStat
}

// ── Cluster ──────────────────────────────────────────────────────────

// ClusterMeta holds static cluster configuration shown in the ring stats box.
type ClusterMeta struct {
	VirtualNodes     int
	LoadFactor       float64
	HopLimit         int
	PeerFetchTimeout string
	ProtocolVersion  string
	GossipInterval   string
	JoinRetryBudget  string
	Mode             string // "strong" | "eventual" | "full" | "single-node"
}

// PeerFetchStats holds aggregated peer fetch telemetry for the cluster page.
type PeerFetchStats struct {
	Hits6h       int64
	Misses6h     int64
	AvgLatMs     float64
	HopLimitHits int64
	DigestCount  int64
}

// ReplicationStats holds aggregated full-mode replication telemetry for
// the cluster page. Bytes/objects are totals over the dashboard window;
// the chart series carry per-bucket rates for the throughput graph.
type ReplicationStats struct {
	ObjectsSent  int64
	ObjectsRecv  int64
	BytesSent    int64
	BytesRecv    int64
	SentPerMin   float64 // objects replicated out per minute (recent)
	RecvPerMin   float64 // objects replicated in per minute (recent)
	LastActivity string  // human "12s ago" since last replication received, or "—"
	Idle         bool    // true when no replication has been observed
	// Chart series (oldest→newest), bytes per bucket.
	SentSeries []int64
	RecvSeries []int64
}

// ClusterData is the view model for the cluster page.
type ClusterData struct {
	LayoutProps
	PeerResults []PeerResult
	PeerHealth  map[string]float64 // uptime % over last 30 min
	RingSegs    []api.RingSegment
	Meta        ClusterMeta
	FetchStats  PeerFetchStats
	Replication ReplicationStats
	// Store stats (same fields as OverviewData).
	HotBytes     int64
	HotMaxBytes  int64
	HotEntries   int64
	WarmBytes    int64
	WarmMaxBytes int64
	WarmEntries  int64
	Evictions    int64
}

// HotFillPct returns the hot-tier fill percentage (0–100), clamped.
func (d ClusterData) HotFillPct() float64 { return fillPct(d.HotBytes, d.HotMaxBytes) }

// WarmFillPct returns the warm-tier fill percentage (0–100), clamped.
func (d ClusterData) WarmFillPct() float64 { return fillPct(d.WarmBytes, d.WarmMaxBytes) }

// TotalBytes returns the combined hot + warm bytes.
func (d ClusterData) TotalBytes() int64 { return d.HotBytes + d.WarmBytes }

// TotalEntries returns the combined hot + warm entry count.
func (d ClusterData) TotalEntries() int64 { return d.HotEntries + d.WarmEntries }

// HasStoreData reports whether any store stats are populated.
func (d ClusterData) HasStoreData() bool {
	return d.HotBytes > 0 || d.WarmBytes > 0 || d.HotEntries > 0 || d.WarmEntries > 0
}

// WarmAvgObjSize returns the average warm-tier object size in bytes,
// or 0 when there are no warm entries.
func (d ClusterData) WarmAvgObjSize() int64 {
	if d.WarmEntries == 0 {
		return 0
	}
	return d.WarmBytes / d.WarmEntries
}

// ── Invalidation ─────────────────────────────────────────────────────

// InvalidationData is the view model for the invalidation page.
type InvalidationData struct {
	LayoutProps
	OpsLog []observability.OpsLogEntry
	// CFStatus is the Cloudflare propagation status (nil when CF is not
	// configured). Cache invalidation on bouine propagates to the CDN, so
	// the CF status belongs with the invalidation controls.
	CFStatus *CFStatusCard
}

// HistTypeClass maps an ops-log op to the .ht-* CSS class.
func HistTypeClass(op string) string {
	switch op {
	case "purge":
		return "ht-purge"
	case "ban":
		return "ht-ban"
	case "refresh":
		return "ht-refresh"
	default:
		return "ht-purge"
	}
}

// ── Config ───────────────────────────────────────────────────────────

// ConfigRow is one key:value row in the config viewer.
type ConfigRow struct {
	Key   string
	Value string
	Kind  string // "str" | "num" | "bool-t" | "bool-f" | "dur"
	Hint  string
}

// ConfigSection is one collapsible section in the config viewer.
type ConfigSection struct {
	Icon      string
	Title     string
	Badge     string
	BadgeKind string // "" | "g" | "y"
	Rows      []ConfigRow
	Routes    []ConfigRouteEntry
}

// ConfigRouteEntry renders one route inside the routes section.
type ConfigRouteEntry struct {
	PathPrefix string
	Pool       string
	Rows       []ConfigRow
}

// ConfigData is the view model for the config page.
type ConfigData struct {
	LayoutProps
	ConfigPath   string
	SnapshotPath string
	Flash        string
	LastReload   string // "2h ago · success" or ""
	Uptime       string // "3d 14h 22m"
	Sections     []ConfigSection
	RawJSON      string // pretty-printed JSON of the running config
	RawYAML      string // pretty-printed YAML of the running config
	// Storage capacity (live usage vs configured max).
	HotBytes     int64
	HotMaxBytes  int64
	HotEntries   int64
	WarmBytes    int64
	WarmMaxBytes int64
	WarmEntries  int64
}

// HotFillPct returns the hot-tier fill percentage (0–100), clamped.
func (c ConfigData) HotFillPct() float64 { return fillPct(c.HotBytes, c.HotMaxBytes) }

// WarmFillPct returns the warm-tier fill percentage (0–100), clamped.
func (c ConfigData) WarmFillPct() float64 { return fillPct(c.WarmBytes, c.WarmMaxBytes) }

// BuildConfigSections converts a *config.Config into structured viewer sections.
func BuildConfigSections(cfg *config.Config) []ConfigSection {
	if cfg == nil {
		return nil
	}
	sections := []ConfigSection{
		{
			Icon: "⟁", Title: "listen", Badge: "listeners",
			Rows: []ConfigRow{
				{Key: "http", Value: fmt.Sprintf("%q", cfg.Listen.HTTP), Kind: "str", Hint: "HTTP/1.1 + h2c data plane"},
				{Key: "https", Value: fmt.Sprintf("%q", cfg.Listen.HTTPS), Kind: "str", Hint: "TLS data plane"},
				{Key: "admin", Value: fmt.Sprintf("%q", cfg.Listen.Admin), Kind: "str", Hint: "admin API · metrics · health"},
				{Key: "cluster", Value: fmt.Sprintf("%q", cfg.Listen.Cluster), Kind: "str", Hint: "gossip · peer fetch"},
			},
		},
		{
			Icon: "◫", Title: "storage", Badge: "hot + warm tiers", BadgeKind: "y",
			Rows: []ConfigRow{
				{Key: "hot_max_bytes", Value: cfg.Storage.HotMaxBytes.String(), Kind: "num", Hint: "in-RAM SIEVE cache"},
				{Key: "warm_dir", Value: fmt.Sprintf("%q", cfg.Storage.WarmDir), Kind: "str", Hint: "mmap segments path"},
				{Key: "warm_max_bytes", Value: cfg.Storage.WarmMaxBytes.String(), Kind: "num", Hint: "max warm tier size"},
				{Key: "eviction", Value: cfg.Storage.Eviction, Kind: "str", Hint: "sieve"},
			},
		},
	}

	clusterBadgeKind := ""
	clusterBadge := "disabled"
	if cfg.Cluster.Enabled {
		clusterBadgeKind = "g"
		clusterBadge = cfg.Cluster.Mode
	}
	modeHint := "strong: ring-sharded · eventual: local cache, gossip invalidation · full: full replication"
	sections = append(sections, ConfigSection{
		Icon: "◎", Title: "cluster", Badge: clusterBadge, BadgeKind: clusterBadgeKind,
		Rows: []ConfigRow{
			{Key: "enabled", Value: fmt.Sprintf("%v", cfg.Cluster.Enabled), Kind: boolKind(cfg.Cluster.Enabled), Hint: "gossip membership"},
			{Key: "mode", Value: cfg.Cluster.Mode, Kind: "str", Hint: modeHint},
			{Key: "replicas", Value: fmt.Sprintf("%d", cfg.Cluster.Replicas), Kind: "num", Hint: "write replication factor"},
			{Key: "hop_limit", Value: fmt.Sprintf("%d", cfg.Cluster.HopLimit), Kind: "num", Hint: "max peer-fetch hops (strong only)"},
		},
	})

	var routeEntries []ConfigRouteEntry
	for _, rc := range cfg.Routes {
		label := rc.Name
		if label == "" {
			label = rc.Match.PathPrefix
		}
		rows := buildRouteCacheRows(rc)
		routeEntries = append(routeEntries, ConfigRouteEntry{
			PathPrefix: label,
			Pool:       rc.Pool,
			Rows:       rows,
		})
	}
	sections = append(sections, ConfigSection{
		Icon: "⬡", Title: "routes", Badge: fmt.Sprintf("%d configured", len(cfg.Routes)),
		Routes: routeEntries,
	})
	return sections
}

func boolKind(v bool) string {
	if v {
		return "bool-t"
	}
	return "bool-f"
}

// ── Peer results ──────────────────────────────────────────────────────

// PeerResult holds one peer's fan-out result.
type PeerResult struct {
	NodeName  string
	DataAddr  string
	AdminAddr string
	Weight    float64
	JoinedAt  time.Time
	Summary   observability.MetricsSummary
	Stale     bool
}

// ── Ring SVG helpers ──────────────────────────────────────────────────

// RingArc is a pre-computed arc for a circular ring SVG.
type RingArc struct {
	NodeName   string
	Color      string
	DashArray  string // "arcLen circumference"
	DashOffset string // "-cumulativeLen"
	LabelX     float64
	LabelY     float64
	LabelX2    float64
	LabelY2    float64
	PctLabel   string
}

// RingColors cycles through the palette used in the reference.
var RingColors = []string{"#8b5cf6", "#34d399", "#fb7185", "#fbbf24", "#60a5fa", "#c4b5fd", "#a78bfa"}

// BuildRingArcs converts RingSegments into pre-computed SVG arc parameters.
// r is the stroke-center radius.
func BuildRingArcs(segs []api.RingSegment, r float64) []RingArc {
	if len(segs) == 0 {
		return nil
	}
	c := 2 * math.Pi * r
	arcs := make([]RingArc, 0, len(segs))
	var cumFrac float64
	for i, seg := range segs {
		arcLen := seg.Frac * c
		offset := -(cumFrac * c)
		// Label position: midpoint of the arc on the outer circle
		midAngle := (cumFrac + seg.Frac/2) * 2 * math.Pi
		labelR := r + 18 // outside the stroke
		lx := labelR * math.Sin(midAngle)
		ly := -labelR * math.Cos(midAngle)
		arcs = append(arcs, RingArc{
			NodeName:   seg.NodeName,
			Color:      RingColors[i%len(RingColors)],
			DashArray:  fmt.Sprintf("%.2f %.2f", arcLen, c),
			DashOffset: fmt.Sprintf("%.2f", offset),
			LabelX:     lx,
			LabelY:     ly,
			LabelX2:    lx,
			LabelY2:    ly + 11,
			PctLabel:   fmt.Sprintf("%.0f%%", seg.Frac*100),
		})
		cumFrac += seg.Frac
	}
	return arcs
}

// ── Formatting helpers ────────────────────────────────────────────────

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

// FmtBytes formats a byte count with IEC suffix.
func FmtBytes(b int64) string {
	switch {
	case b >= 1<<30:
		return fmt.Sprintf("%.2f Go", float64(b)/(1<<30))
	case b >= 1<<20:
		return fmt.Sprintf("%.0f Mo", float64(b)/(1<<20))
	case b >= 1<<10:
		return fmt.Sprintf("%.0f Ko", float64(b)/(1<<10))
	default:
		return fmt.Sprintf("%d B", b)
	}
}

// FmtDuration formats a time.Duration for config display.
func FmtDuration(d time.Duration) string {
	if d == 0 {
		return "—"
	}
	if d >= time.Hour*24 {
		return fmt.Sprintf("%.0fh", d.Hours())
	}
	if d >= time.Minute {
		return fmt.Sprintf("%.0fm", d.Minutes())
	}
	if d >= time.Second {
		return fmt.Sprintf("%.0fs", d.Seconds())
	}
	return fmt.Sprintf("%dms", d.Milliseconds())
}

// FmtUptime formats a duration as "Xd Yh Zm".
func FmtUptime(d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	days := int(d.Hours()) / 24
	hours := int(d.Hours()) % 24
	mins := int(d.Minutes()) % 60
	if days > 0 {
		return fmt.Sprintf("%dd %dh %dm", days, hours, mins)
	}
	if hours > 0 {
		return fmt.Sprintf("%dh %dm", hours, mins)
	}
	return fmt.Sprintf("%dm", mins)
}

// fmtVersion normalises the build version for the sidebar badge.
// Empty → "dev"; a bare semver gets a leading "v".
func fmtVersion(v string) string {
	if v == "" || v == "dev" {
		return "dev"
	}
	if v[0] >= '0' && v[0] <= '9' {
		return "v" + v
	}
	return v
}

// TimeAgo formats a past time as a human-readable relative duration.
func TimeAgo(t time.Time) string {
	if t.IsZero() {
		return "—"
	}
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

// BuildTrend computes a TrendData from current and prior values.
// For latency, higher is worse; for others, higher is better when
// positiveIsGood is true.
func BuildTrend(current, prior float64, positiveIsGood bool) TrendData {
	if prior == 0 {
		return TrendData{Label: "→ stable"}
	}
	delta := current - prior
	pct := delta / prior * 100
	if math.Abs(pct) < 1 {
		return TrendData{Label: "→ stable"}
	}
	if positiveIsGood {
		if delta > 0 {
			return TrendData{Delta: pct, Up: true, Label: fmt.Sprintf("↑ %.1f%%", pct)}
		}
		return TrendData{Delta: pct, Down: true, Label: fmt.Sprintf("↓ %.1f%%", math.Abs(pct))}
	}
	// For latency/errors: lower is better
	if delta < 0 {
		return TrendData{Delta: pct, Up: true, Label: fmt.Sprintf("↓ %.1f%%", math.Abs(pct))}
	}
	return TrendData{Delta: pct, Down: true, Label: fmt.Sprintf("↑ %.1f%%", pct)}
}

// BuildRouteRows joins configured routes with live ring stats.
func BuildRouteRows(cfgRoutes []config.Route, stats []observability.RouteStat) []RouteRow {
	byName := make(map[string]observability.RouteStat, len(stats))
	for _, s := range stats {
		byName[s.Route] = s
	}

	rows := make([]RouteRow, 0, len(cfgRoutes))
	for _, rc := range cfgRoutes {
		label := rc.Name
		if label == "" {
			switch {
			case rc.Match.Host != "":
				label = rc.Match.Host + ":" + rc.Match.PathPrefix
			case rc.Match.PathPrefix != "":
				label = rc.Match.PathPrefix
			default:
				label = "_catch-all"
			}
		}
		stat := byName[label]
		row := RouteRow{
			Name:        label,
			PathPrefix:  rc.Match.PathPrefix,
			Host:        rc.Match.Host,
			Pool:        rc.Pool,
			TTL:         FmtDuration(rc.Cache.NegativeTTL),
			SWR:         FmtDuration(rc.Cache.StaleWhileRevalidate),
			SIE:         FmtDuration(rc.Cache.StaleIfError),
			NegTTL:      FmtDuration(rc.Cache.NegativeTTL),
			StayinAlive: rc.Cache.StayinAlive,
			Jitter:      jitterStr(rc.Cache.JitterPercent),
			Methods:     methodsLabel(rc.Match.Methods),
			Features:    routeFeatures(rc),
			Requests:    stat.Requests,
			Hits:        stat.Hits,
			Misses:      stat.Misses,
			HitPct:      stat.HitPct,
			Sparkline:   stat.Sparkline,
		}
		rows = append(rows, row)
	}
	// Append live routes not in config (e.g. _catch-all)
	inConfig := make(map[string]bool, len(cfgRoutes))
	for _, r := range rows {
		inConfig[r.Name] = true
	}
	for _, s := range stats {
		if !inConfig[s.Route] {
			rows = append(rows, RouteRow{
				Name:       s.Route,
				PathPrefix: s.Route,
				Pool:       "—",
				TTL:        "—", SWR: "—", SIE: "—", NegTTL: "—", Jitter: "—",
				Requests:  s.Requests,
				Hits:      s.Hits,
				Misses:    s.Misses,
				HitPct:    s.HitPct,
				Sparkline: s.Sparkline,
			})
		}
	}
	return rows
}

func jitterStr(pct int) string {
	if pct == 0 {
		return "—"
	}
	return fmt.Sprintf("%d%%", pct)
}

// methodsLabel renders the route's HTTP-method restriction as a compact
// middot-separated chip ("GET·HEAD"), or "" when all methods are allowed.
func methodsLabel(methods []string) string {
	if len(methods) == 0 {
		return ""
	}
	return strings.Join(methods, "·")
}

// routeFeatures returns badges for the per-route capabilities that are
// enabled on this route (the features added in v0.1.0).
func routeFeatures(rc config.Route) []RouteFeature {
	var f []RouteFeature
	if rc.Cache.TTLOverride > 0 {
		f = append(f, RouteFeature{
			Label: "override " + FmtDuration(rc.Cache.TTLOverride),
			Title: "ttl_override: forces bouine's internal TTL to this value regardless of the upstream Cache-Control/Expires headers (forwarded unaltered).",
		})
	}
	if rc.Cache.AllowSetCookie != nil && *rc.Cache.AllowSetCookie {
		f = append(f, RouteFeature{
			Label: "set-cookie",
			Title: "allow_set_cookie: caches responses carrying Set-Cookie (the cookie is stripped from the stored copy). Security-relevant — only safe for non-user-specific cookies.",
			Warn:  true,
		})
	}
	if rc.Cache.MaxObjectSize > 0 {
		f = append(f, RouteFeature{
			Label: "max " + rc.Cache.MaxObjectSize.String(),
			Title: "max_object_size: responses larger than this are proxied but not cached.",
		})
	}
	if rc.Request.StripPrefix != "" {
		f = append(f, RouteFeature{
			Label: "strip " + rc.Request.StripPrefix,
			Title: "strip_prefix: this path prefix is removed before forwarding to the upstream. The cache key keeps the original path.",
		})
	}
	if n := len(rc.Cache.Key.StripQueryParams); n > 0 {
		f = append(f, RouteFeature{
			Label: fmt.Sprintf("q-strip ×%d", n),
			Title: "strip_query_params (still forwarded to origin): " + strings.Join(rc.Cache.Key.StripQueryParams, ", "),
		})
	}
	return f
}

// FmtAddrPort strips the scheme and returns host:port from a listen address.
// FmtInt formats an integer for display.

// FmtInt formats an integer for display.
func FmtInt(v int) string { return strconv.Itoa(v) }

// FmtInt64 formats an int64 for display.
func FmtInt64(v int64) string { return strconv.FormatInt(v, 10) }

// FmtRate formats a per-minute rate compactly ("0", "3.4", "120").
func FmtRate(v float64) string {
	if v == 0 {
		return "0"
	}
	if v < 10 {
		return fmt.Sprintf("%.1f", v)
	}
	return fmt.Sprintf("%.0f", v)
}

// LatencyBucketLabels returns the x-axis labels for the latency
// distribution chart, derived from observability.LatencyBoundsMs. The
// final label is the overflow bucket (">1s").
func LatencyBucketLabels() []string {
	bounds := observability.LatencyBoundsMs
	out := make([]string, 0, len(bounds)+1)
	for _, b := range bounds {
		out = append(out, "≤"+FmtLatMs(b))
	}
	out = append(out, ">"+FmtLatMs(bounds[len(bounds)-1]))
	return out
}

// boolStr converts a bool to "true"/"false" string.
func boolStr(v bool) string {
	if v {
		return "true"
	}
	return "false"
}

// ringDotStyle returns the inline style for a ring legend colored dot.
func ringDotStyle(i int) string {
	return "background:" + RingColors[i%len(RingColors)]
}

// modeLabel returns a human-readable label for the cluster mode.
func modeLabel(mode string) string {
	switch mode {
	case "strong":
		return "strong"
	case "eventual":
		return "eventual"
	case "full":
		return "full"
	default:
		return "single-node"
	}
}

// FmtAddrPort strips the scheme and returns host:port from a listen address.
func FmtAddrPort(addr string) string {
	addr = strings.TrimPrefix(addr, "http://")
	addr = strings.TrimPrefix(addr, "https://")
	if addr == "" {
		return "—"
	}
	return addr
}

func buildRouteCacheRows(rc config.Route) []ConfigRow {
	var rows []ConfigRow
	if rc.Cache.NegativeTTL > 0 {
		rows = append(rows, ConfigRow{Key: "negative_ttl", Value: rc.Cache.NegativeTTL.String(), Kind: "dur"})
	}
	if rc.Cache.TTLOverride > 0 {
		rows = append(rows, ConfigRow{Key: "ttl_override", Value: rc.Cache.TTLOverride.String(), Kind: "dur"})
	}
	if rc.Cache.StaleWhileRevalidate > 0 {
		rows = append(rows, ConfigRow{Key: "stale_while_revalidate", Value: rc.Cache.StaleWhileRevalidate.String(), Kind: "dur"})
	}
	if rc.Cache.StaleIfError > 0 {
		rows = append(rows, ConfigRow{Key: "stale_if_error", Value: rc.Cache.StaleIfError.String(), Kind: "dur"})
	}
	if rc.Cache.JitterPercent > 0 {
		rows = append(rows, ConfigRow{Key: "jitter_percent", Value: fmt.Sprintf("%d", rc.Cache.JitterPercent), Kind: "num"})
	}
	if rc.Cache.StayinAlive {
		rows = append(rows, ConfigRow{Key: "stayin_alive", Value: "true", Kind: "bool-t"})
	}
	if rc.Cache.AllowSetCookie != nil && *rc.Cache.AllowSetCookie {
		rows = append(rows, ConfigRow{Key: "allow_set_cookie", Value: "true", Kind: "bool-t"})
	}
	if rc.Cache.MaxObjectSize > 0 {
		rows = append(rows, ConfigRow{Key: "max_object_size", Value: rc.Cache.MaxObjectSize.String(), Kind: "size"})
	}
	if rc.Cache.MaxResponseBytes > 0 {
		rows = append(rows, ConfigRow{Key: "max_response_bytes", Value: rc.Cache.MaxResponseBytes.String(), Kind: "size"})
	}
	if rc.Cache.MaxFetchConcurrency > 0 {
		rows = append(rows, ConfigRow{Key: "max_fetch_concurrency", Value: strconv.Itoa(rc.Cache.MaxFetchConcurrency), Kind: "number"})
	}
	if len(rc.Cache.Key.StripQueryParams) > 0 {
		rows = append(rows, ConfigRow{Key: "strip_query_params", Value: strings.Join(rc.Cache.Key.StripQueryParams, ", "), Kind: "list"})
	}
	return rows
}

// ── Insights ────────────────────────────────────────────────────────

// InsightsData is the view model for /dashboard/insights.
type InsightsData struct {
	LayoutProps
	// Architecture nodes for the flow diagram.
	Nodes []ArchNode
	// Insights sorted by severity (HIGH first).
	Insights []InsightCard
	// InsightCount per severity for the filter chips.
	HighCount, MedCount, LowCount int
}

// ArchNode represents a single component in the architecture flow diagram.
type ArchNode struct {
	ID           string // "client", "cdn", "bouine", "pool:api-pool"
	Type         string // "client" | "cdn" | "bouine" | "pool"
	Label        string
	Status       string // "healthy" | "degraded" | "unhealthy" | "disabled"
	Detail       string
	Peers        []PeerNode    // only populated for "bouine" type
	StorageTiers []StorageTier // only populated for "bouine" type
}

// PeerNode is a single bouine cluster member shown inside the cluster
// container in the architecture diagram.
type PeerNode struct {
	Name   string
	Status string // "healthy" | "stale"
}

// StorageTier represents a storage tier (hot or warm) drawn as a cylinder
// inside the bouine cluster container.
type StorageTier struct {
	Name   string // "Hot" or "Warm"
	Status string // "healthy" | "degraded" | "unhealthy"
	Detail string
}

// InsightCard is a single insight rendered as a card in the sidebar.
type InsightCard struct {
	ID       string
	Severity string // "HIGH" | "MED" | "LOW"
	Category string
	Title    string
	Detail   string
	Evidence string
	Routes   []string
	Action   string
	NodeIDs  []string // related ArchNode IDs for click-to-focus filtering
}
