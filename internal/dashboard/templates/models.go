package templates

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// ── Layout ──────────────────────────────────────────────────────────

// LayoutProps contains the fields required by every page to render the
// shared nav/header/footer sidebar and tabs bar.
type LayoutProps struct {
	Page      string // "overview" | "routes" | "cluster" | "invalidation" | "config"
	PageTitle string
	NodeName  string
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
	// Cloudflare propagation status (nil when CF not configured).
	CFStatus *CFStatusCard
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
	IsBypass    bool // private or no-store: no cache policy
	// From observability.RouteStat
	Requests  int64
	Hits      int64
	Misses    int64
	HitPct    float64
	Sparkline []int64
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

// ClusterData is the view model for the cluster page.
type ClusterData struct {
	LayoutProps
	PeerResults []PeerResult
	PeerHealth  map[string]float64 // uptime % over last 30 min
	RingSegs    []api.RingSegment
	Meta        ClusterMeta
	FetchStats  PeerFetchStats
}

// ── Invalidation ─────────────────────────────────────────────────────

// InvalidationData is the view model for the invalidation page.
type InvalidationData struct {
	LayoutProps
	OpsLog []observability.OpsLogEntry
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
}

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
		var rows []ConfigRow
		if rc.Cache.NegativeTTL > 0 {
			rows = append(rows, ConfigRow{Key: "negative_ttl", Value: rc.Cache.NegativeTTL.String(), Kind: "dur"})
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

// FmtAddrPort strips the scheme and returns host:port from a listen address.
// FmtInt formats an integer for display.

// FmtInt formats an integer for display.
func FmtInt(v int) string { return strconv.Itoa(v) }

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
