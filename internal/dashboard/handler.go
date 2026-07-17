package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/a-h/templ"
	"gopkg.in/yaml.v3"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/dashboard/insights"
	"github.com/bouine-cache/bouine/internal/dashboard/templates"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/origin"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// Config controls the dashboard server.
type Config struct {
	Rings        *observability.Rings
	PeersFn      func() []api.PeerInfo
	SelfAddr     string
	Token        string
	Logger       observability.Logger
	SnapshotPath string
	// Version is the build version shown in the dashboard sidebar.
	Version string
	// Storage stats.
	StoreFn      func() api.Stats
	HotMaxBytes  int64
	WarmMaxBytes int64
	// Invalidation proxies (bearer token never in HTML).
	PurgeFn   func(ctx context.Context, urlStr string) error
	BanFn     func(ctx context.Context, hostRegex, pathRegex string) (int, error)
	RefreshFn func(ctx context.Context, urlStr string) error
	// Config viewer + reload.
	Config     *config.Config
	ConfigPath string
	StartTime  time.Time
	ReloadFn   func(*config.Config) error
	// Cluster metadata for the cluster page ring stats box.
	ClusterMeta templates.ClusterMeta
	// RingFn returns consistent-hash ring ownership segments.
	RingFn func() []api.RingSegment
	// PeerFetchStatsFn returns live peer fetch telemetry (M7).
	PeerFetchStatsFn func() templates.PeerFetchStats
	// CFStatusFn returns the current Cloudflare propagation status.
	// nil means CF is not configured.
	CFStatusFn func() templates.CFStatusCard
	// PoolHealthFn returns per-pool target health for the insights diagram.
	// nil means pool health is not available (single-node or no pools).
	PoolHealthFn func() map[string][]origin.TargetStatus
	// OriginHeaderAuditFn returns per-pool origin header audit stats.
	OriginHeaderAuditFn func() map[string]observability.HeaderAuditSummary
	// VaryCapHitsFn returns the total Vary cap hit count.
	VaryCapHitsFn func() int64
	// BroadcastFailuresFn returns total cluster broadcast failures.
	BroadcastFailuresFn func() int64
	// CFPurgeSkippedFn returns total CF purges skipped.
	CFPurgeSkippedFn func() int64
}

// Handler is the dashboard HTTP handler. Mount at /dashboard/.
type Handler struct {
	cfg              Config
	auth             *sessionAuth
	agg              *Aggregator
	insightEngine    *insights.Engine
	prevStoreStatsMu sync.Mutex
	prevStoreStats   api.Stats
}

// New creates and registers dashboard routes on mux.
func New(cfg Config, mux *http.ServeMux) *Handler {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.StartTime.IsZero() {
		cfg.StartTime = time.Now()
	}
	h := &Handler{
		cfg:           cfg,
		auth:          newSessionAuth(cfg.Token),
		agg:           NewAggregator(cfg.Rings, cfg.PeersFn, cfg.SelfAddr, cfg.Token, cfg.Logger),
		insightEngine: insights.New(),
	}

	mux.HandleFunc("GET /dashboard/login", h.auth.LoginHandler)
	mux.HandleFunc("POST /dashboard/login", h.auth.LoginHandler)

	protected := http.NewServeMux()
	protected.HandleFunc("GET /dashboard/", h.overview)
	protected.HandleFunc("GET /dashboard/performance", h.performance)
	protected.HandleFunc("GET /dashboard/routes", h.routes)
	protected.HandleFunc("GET /dashboard/cluster", h.cluster)
	protected.HandleFunc("GET /dashboard/invalidation", h.invalidation)
	protected.HandleFunc("GET /dashboard/config", h.config)
	protected.HandleFunc("GET /dashboard/insights", h.insights)
	protected.HandleFunc("POST /dashboard/config/reload", h.configReload)
	protected.HandleFunc("POST /dashboard/api/purge", h.apiPurge)
	protected.HandleFunc("POST /dashboard/api/ban", h.apiBan)
	protected.HandleFunc("POST /dashboard/api/refresh", h.apiRefresh)

	mux.Handle("/dashboard/", h.auth.Middleware(protected))
	return h
}

// ── Helpers ──────────────────────────────────────────────────────────

func (h *Handler) nodeName() string {
	if h.cfg.Rings != nil {
		return h.cfg.Rings.NodeName
	}
	return "unknown"
}

func (h *Handler) render(w http.ResponseWriter, r *http.Request, c templ.Component) {
	w.Header().Set(header.ContentType, "text/html; charset=utf-8")
	if err := c.Render(r.Context(), w); err != nil {
		h.cfg.Logger.Error("dashboard render error", "error", err)
	}
}

// sidebarProps computes live sidebar stats from the local rings (no fan-out,
// so it never adds latency to page renders).
func (h *Handler) sidebarProps(_ string) (reqs float64, hitPct float64, peerCount, live int) {
	peerCount, live = 1, 1
	if h.cfg.PeersFn != nil {
		peers := h.cfg.PeersFn()
		peerCount = len(peers) // Members() already includes self
		live = peerCount       // optimistic; stale known from agg cache
	}
	if h.cfg.Rings == nil {
		return 0, 0, peerCount, live
	}
	snap := h.cfg.Rings.Request.Snapshot(6) // last 60s
	var totalReq, totalHit int64
	for _, b := range snap {
		totalReq += b.Requests
		totalHit += b.Hits
	}
	reqs = float64(totalReq) / 60.0
	if totalReq > 0 {
		hitPct = float64(totalHit) / float64(totalReq) * 100
	}
	return
}

func (h *Handler) layoutProps(page, title, timeRange string) templates.LayoutProps {
	reqs, hitPct, peerCount, live := h.sidebarProps(timeRange)
	return templates.LayoutProps{
		Page:          page,
		PageTitle:     title,
		NodeName:      h.nodeName(),
		Version:       h.cfg.Version,
		TimeRange:     timeRange,
		PeerCount:     peerCount,
		LivePeers:     live,
		SidebarReqS:   reqs,
		SidebarHitPct: hitPct,
	}
}

func (h *Handler) cfStatusCard() *templates.CFStatusCard {
	if h.cfg.CFStatusFn == nil {
		return nil
	}
	card := h.cfg.CFStatusFn()
	return &card
}

func (h *Handler) storeStats() (hotBytes, hotEntries, hotMax, warmBytes, warmEntries, warmMax, evictions int64) {
	if h.cfg.StoreFn != nil {
		st := h.cfg.StoreFn()
		hotBytes = st.HotBytes
		hotEntries = st.HotEntries
		warmBytes = st.WarmBytes
		warmEntries = st.WarmEntries
		evictions = st.Evictions
	}
	hotMax = h.cfg.HotMaxBytes
	warmMax = h.cfg.WarmMaxBytes
	return
}

func sortRouteStats(stats []observability.RouteStat) []observability.RouteStat {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Requests > stats[j].Requests
	})
	return stats
}

func sortURLStats(stats []observability.URLStat) []observability.URLStat {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Requests > stats[j].Requests
	})
	return stats
}

func parseTimeRange(s string) (buckets int, label string) {
	switch s {
	case "1h":
		return 360, "1h"
	case "24h":
		return 2160, "24h"
	default:
		return 2160, "6h"
	}
}

// ── Page handlers ─────────────────────────────────────────────────────

// overviewStats holds pre-computed windowed statistics for the overview page.
type overviewStats struct {
	totalReq, totalHit, totalErr, maxP99                                 int64
	splitHits, splitMisses, splitStales, splitBypasses, splitRevalidated int64
	priorReq, priorHit, priorErr, priorP99                               int64
	hitPct, errPct, priorHitPct, priorErrPct                             float64
	reqPerSec, priorReqPerSec                                            float64
	// Latency percentiles over the recent window.
	latHist          observability.LatencyHistogram
	p50, p90, p99    int64
	priorP99FromHist int64
}

// buildOverviewStats computes the recent and prior windowed stats from the ring snapshot.
func buildOverviewStats(snap []observability.RequestBucket, n int) overviewStats {
	recent := min(6, n)
	var s overviewStats
	var priorLat observability.LatencyHistogram
	for _, b := range snap[n-recent:] {
		s.totalReq += b.Requests
		s.totalHit += b.Hits
		s.totalErr += b.Errors
		s.splitHits += b.Hits
		s.splitMisses += b.Misses
		s.splitStales += b.StaleHits
		s.splitBypasses += b.Bypasses
		s.splitRevalidated += b.Revalidated
		if b.P99MS > s.maxP99 {
			s.maxP99 = b.P99MS
		}
		for i := range b.LatHist {
			s.latHist[i] += b.LatHist[i]
		}
	}
	priorStart := max(0, n-12)
	for _, b := range snap[priorStart:max(priorStart, n-recent)] {
		s.priorReq += b.Requests
		s.priorHit += b.Hits
		s.priorErr += b.Errors
		if b.P99MS > s.priorP99 {
			s.priorP99 = b.P99MS
		}
		for i := range b.LatHist {
			priorLat[i] += b.LatHist[i]
		}
	}
	if s.totalReq > 0 {
		s.hitPct = float64(s.totalHit) / float64(s.totalReq) * 100
		s.errPct = float64(s.totalErr) / float64(s.totalReq) * 100
	}
	if s.priorReq > 0 {
		s.priorHitPct = float64(s.priorHit) / float64(s.priorReq) * 100
		s.priorErrPct = float64(s.priorErr) / float64(s.priorReq) * 100
	}
	windowSecs := float64(max(1, recent) * 10)
	s.reqPerSec = float64(s.totalReq) / windowSecs
	s.priorReqPerSec = float64(s.priorReq) / windowSecs
	s.p50 = s.latHist.Percentile(0.50)
	s.p90 = s.latHist.Percentile(0.90)
	s.p99 = s.latHist.Percentile(0.99)
	s.priorP99FromHist = priorLat.Percentile(0.99)
	return s
}

// latHistToInts copies the fixed-size latency histogram into a slice for
// the view model.
func latHistToInts(h observability.LatencyHistogram) []int64 {
	out := make([]int64, len(h))
	copy(out, h[:])
	return out
}

func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	merged, peers := h.agg.Collect(r.Context())

	chartBuckets, timeRange := parseTimeRange(r.URL.Query().Get("range"))
	snap := merged.RequestSnap
	n := len(snap)
	start := max(0, n-chartBuckets)
	labels := make([]string, 0, chartBuckets)
	reqData := make([]int64, 0, chartBuckets)
	errData := make([]int64, 0, chartBuckets)
	for _, b := range snap[start:] {
		if b.Timestamp == 0 {
			continue // unfilled ring slot
		}
		labels = append(labels, time.Unix(b.Timestamp, 0).Format("15:04:05"))
		reqData = append(reqData, b.Requests)
		errData = append(errData, b.Errors)
	}
	if len(reqData) == 0 {
		labels = []string{"—"}
		reqData = []int64{0}
		errData = []int64{0}
	}

	st := buildOverviewStats(snap, n)

	hotBytes, hotEntries, hotMax, warmBytes, warmEntries, warmMax, evictions := h.storeStats()

	var routeRows []templates.RouteRow
	if h.cfg.Config != nil {
		routeRows = templates.BuildRouteRows(h.cfg.Config.Routes, sortRouteStats(merged.RouteStats))
	} else {
		for _, rs := range sortRouteStats(merged.RouteStats) {
			if len(routeRows) >= 5 {
				break
			}
			routeRows = append(routeRows, templates.RouteRow{
				Name: rs.Route, Pool: "—", TTL: "—", SWR: "—", SIE: "—", NegTTL: "—", Jitter: "—",
				Requests: rs.Requests, Hits: rs.Hits, Misses: rs.Misses,
				HitPct: rs.HitPct, Sparkline: rs.Sparkline,
			})
		}
	}

	var ringSegs []api.RingSegment
	if h.cfg.RingFn != nil {
		ringSegs = h.cfg.RingFn()
	}

	lprops := h.layoutProps("overview", "Overview", timeRange)
	h.render(w, r, templates.Overview(templates.OverviewData{
		LayoutProps: lprops,
		ReqPerSec:   st.reqPerSec,
		HitPct:      st.hitPct,
		P99MS:       st.p99,
		P50MS:       st.p50,
		P90MS:       st.p90,
		ErrPct:      st.errPct,
		TrendReq:    templates.BuildTrend(st.reqPerSec, st.priorReqPerSec, true),
		TrendHit:    templates.BuildTrend(st.hitPct, st.priorHitPct, true),
		TrendLat:    templates.BuildTrend(float64(st.p99), float64(st.priorP99FromHist), false),
		TrendErr:    templates.BuildTrend(st.errPct, st.priorErrPct, false),
		CacheSplit: templates.CacheSplitData{
			Hits: st.splitHits, Misses: st.splitMisses, Stales: st.splitStales,
			Bypasses: st.splitBypasses, Revalidated: st.splitRevalidated,
		},
		ChartLabels: labels,
		ChartReqs:   reqData,
		ChartErrs:   errData,
		RouteRows:   routeRows,
		PeerResults: toPeerResultsEnriched(peers, h.cfg.PeersFn),
		HotBytes:    hotBytes, HotMaxBytes: hotMax, HotEntries: hotEntries,
		WarmBytes: warmBytes, WarmMaxBytes: warmMax, WarmEntries: warmEntries,
		Evictions:   evictions,
		RingSegs:    ringSegs,
		ClusterMode: h.cfg.ClusterMeta.Mode,
	}))
}

// performance renders the latency-focused performance page.
func (h *Handler) performance(w http.ResponseWriter, r *http.Request) {
	merged, _ := h.agg.Collect(r.Context())
	chartBuckets, timeRange := parseTimeRange(r.URL.Query().Get("range"))
	snap := merged.RequestSnap
	n := len(snap)

	// Per-bucket latency series over the selected range.
	start := max(0, n-chartBuckets)
	labels := make([]string, 0, chartBuckets)
	p99Series := make([]int64, 0, chartBuckets)
	avgSeries := make([]int64, 0, chartBuckets)
	for _, b := range snap[start:] {
		if b.Timestamp == 0 {
			continue // unfilled ring slot
		}
		labels = append(labels, time.Unix(b.Timestamp, 0).Format("15:04:05"))
		p99Series = append(p99Series, b.LatHist.Percentile(0.99))
		var avg int64
		if b.DurN > 0 {
			avg = b.DurSumMs / b.DurN
		}
		avgSeries = append(avgSeries, avg)
	}
	if len(labels) == 0 {
		labels = []string{"—"}
		p99Series = []int64{0}
		avgSeries = []int64{0}
	}

	// Recent window (last ~60s) aggregates for KPIs, distribution, Apdex, SLO.
	recent := min(6, n)
	var hist observability.LatencyHistogram
	var priorHist observability.LatencyHistogram
	var durSum, durN, totalReq int64
	for _, b := range snap[n-recent:] {
		for i := range b.LatHist {
			hist[i] += b.LatHist[i]
		}
		durSum += b.DurSumMs
		durN += b.DurN
		totalReq += b.Requests
	}
	priorStart := max(0, n-12)
	for _, b := range snap[priorStart:max(priorStart, n-recent)] {
		for i := range b.LatHist {
			priorHist[i] += b.LatHist[i]
		}
	}
	var avgMs int64
	if durN > 0 {
		avgMs = durSum / durN
	}
	p99 := hist.Percentile(0.99)

	h.render(w, r, templates.Performance(templates.PerformanceData{
		LayoutProps:    h.layoutProps("performance", "Performance", timeRange),
		P50MS:          hist.Percentile(0.50),
		P90MS:          hist.Percentile(0.90),
		P99MS:          p99,
		AvgMS:          avgMs,
		TrendP99:       templates.BuildTrend(float64(p99), float64(priorHist.Percentile(0.99)), false),
		LatHist:        latHistToInts(hist),
		ChartLabels:    labels,
		P99Series:      p99Series,
		AvgSeries:      avgSeries,
		Apdex:          apdexScore(hist, totalReq),
		ApdexTargetMS:  apdexTargetMS,
		ApdexToleratMS: apdexToleratMS,
		SLO:            sloBuckets(hist, totalReq),
		TotalSamples:   totalReq,
	}))
}

// Apdex thresholds (bucket-aligned to observability.LatencyBoundsMs). A
// request is "satisfied" at or under the target and "tolerating" up to the
// tolerating bound; anything slower is "frustrated".
const (
	apdexTargetMS  = 100 // bound index 6
	apdexToleratMS = 250 // bound index 7
)

// maxAdminFormBytes caps the request body for admin form handlers (login,
// purge, ban, refresh, config reload). These accept only short tokens, URLs,
// and regex patterns — 4 KiB is generous and prevents memory exhaustion.
const maxAdminFormBytes = 4 << 10

// apdexScore computes the Apdex index (0..1) from a latency histogram.
func apdexScore(h observability.LatencyHistogram, total int64) float64 {
	if total == 0 {
		return 0
	}
	var satisfied, tolerating int64
	for i := 0; i <= 6; i++ { // buckets with bound ≤ 100ms
		satisfied += h[i]
	}
	tolerating = h[7] // (100ms, 250ms]
	return (float64(satisfied) + float64(tolerating)/2) / float64(total)
}

// sloBuckets reports the share of requests served at or under fixed latency
// targets that align with the histogram bucket bounds.
func sloBuckets(h observability.LatencyHistogram, total int64) []templates.SLOBucket {
	targets := []struct {
		label string
		idx   int // inclusive bucket index for the target bound
	}{
		{"10ms", 3},
		{"100ms", 6},
		{"1s", 9},
	}
	out := make([]templates.SLOBucket, 0, len(targets))
	for _, t := range targets {
		var pct float64
		if total > 0 {
			var c int64
			for i := 0; i <= t.idx && i < len(h); i++ {
				c += h[i]
			}
			pct = float64(c) / float64(total) * 100
		}
		out = append(out, templates.SLOBucket{Label: t.label, Pct: pct})
	}
	return out
}

// toPeerResultsEnriched joins PeerResult with PeerInfo (for DataAddr/AdminAddr/Weight/JoinedAt).
func toPeerResultsEnriched(in []PeerResult, peersFn func() []api.PeerInfo) []templates.PeerResult {
	// Build name → PeerInfo map
	infoMap := map[string]api.PeerInfo{}
	if peersFn != nil {
		for _, p := range peersFn() {
			infoMap[p.Name] = p
		}
	}
	out := make([]templates.PeerResult, len(in))
	for i, p := range in {
		pi := infoMap[p.NodeName]
		out[i] = templates.PeerResult{
			NodeName:  p.NodeName,
			DataAddr:  pi.DataAddr,
			AdminAddr: pi.AdminAddr,
			Weight:    pi.Weight,
			JoinedAt:  pi.JoinedAt,
			Summary:   p.Summary,
			Stale:     p.Stale,
		}
	}
	return out
}

func (h *Handler) routes(w http.ResponseWriter, r *http.Request) {
	merged, _ := h.agg.Collect(r.Context())
	_, timeRange := parseTimeRange(r.URL.Query().Get("range"))

	var routeRows []templates.RouteRow
	routeCount := 0
	if h.cfg.Config != nil {
		routeCount = len(h.cfg.Config.Routes)
		routeRows = templates.BuildRouteRows(h.cfg.Config.Routes, sortRouteStats(merged.RouteStats))
	} else {
		for _, rs := range sortRouteStats(merged.RouteStats) {
			routeRows = append(routeRows, templates.RouteRow{
				Name: rs.Route, Pool: "—", TTL: "—", SWR: "—", SIE: "—", NegTTL: "—", Jitter: "—",
				Requests: rs.Requests, Hits: rs.Hits, Misses: rs.Misses,
				HitPct: rs.HitPct, Sparkline: rs.Sparkline,
			})
		}
		routeCount = len(routeRows)
	}

	h.render(w, r, templates.Routes(templates.RoutesData{
		LayoutProps: h.layoutProps("routes", "Routes", timeRange),
		RouteCount:  routeCount,
		RouteRows:   routeRows,
		URLStats:    sortURLStats(merged.URLStats),
	}))
}

func (h *Handler) cluster(w http.ResponseWriter, r *http.Request) {
	_, peers := h.agg.Collect(r.Context())
	_, timeRange := parseTimeRange(r.URL.Query().Get("range"))

	var peerHealth map[string]float64
	if h.cfg.Rings != nil {
		peerHealth = h.cfg.Rings.Peer.PeerHealth()
	}

	var ringSegs []api.RingSegment
	if h.cfg.RingFn != nil {
		ringSegs = h.cfg.RingFn()
	}

	var fetchStats templates.PeerFetchStats
	if h.cfg.PeerFetchStatsFn != nil {
		fetchStats = h.cfg.PeerFetchStatsFn()
	}

	hotBytes, hotEntries, hotMax, warmBytes, warmEntries, warmMax, evictions := h.storeStats()

	h.render(w, r, templates.Cluster(templates.ClusterData{
		LayoutProps:  h.layoutProps("cluster", "Cluster", timeRange),
		PeerResults:  toPeerResultsEnriched(peers, h.cfg.PeersFn),
		PeerHealth:   peerHealth,
		RingSegs:     ringSegs,
		Meta:         h.cfg.ClusterMeta,
		FetchStats:   fetchStats,
		HotBytes:     hotBytes,
		HotMaxBytes:  hotMax,
		HotEntries:   hotEntries,
		WarmBytes:    warmBytes,
		WarmMaxBytes: warmMax,
		WarmEntries:  warmEntries,
		Evictions:    evictions,
	}))
}

func (h *Handler) invalidation(w http.ResponseWriter, r *http.Request) {
	_, timeRange := parseTimeRange(r.URL.Query().Get("range"))
	var opsLog []observability.OpsLogEntry
	if h.cfg.Rings != nil {
		opsLog = h.cfg.Rings.OpsLog.Snapshot(20)
	}
	h.render(w, r, templates.Invalidation(templates.InvalidationData{
		LayoutProps: h.layoutProps("invalidation", "Invalidation", timeRange),
		OpsLog:      opsLog,
		CFStatus:    h.cfStatusCard(),
	}))
}

func (h *Handler) config(w http.ResponseWriter, r *http.Request) {
	_, timeRange := parseTimeRange(r.URL.Query().Get("range"))
	uptime := templates.FmtUptime(time.Since(h.cfg.StartTime))
	var rawJSON string
	var rawYAML string
	if h.cfg.Config != nil {
		if b, err := json.MarshalIndent(h.cfg.Config, "", "  "); err == nil {
			rawJSON = string(b)
		}
		if b, err := yaml.Marshal(h.cfg.Config); err == nil {
			rawYAML = string(b)
		}
	}
	hotBytes, hotEntries, hotMax, warmBytes, warmEntries, warmMax, _ := h.storeStats()
	h.render(w, r, templates.Config(templates.ConfigData{
		LayoutProps:  h.layoutProps("config", "Config", timeRange),
		ConfigPath:   h.cfg.ConfigPath,
		SnapshotPath: h.cfg.SnapshotPath,
		Uptime:       uptime,
		Sections:     templates.BuildConfigSections(h.cfg.Config),
		RawJSON:      rawJSON,
		RawYAML:      rawYAML,
		HotBytes:     hotBytes,
		HotMaxBytes:  hotMax,
		HotEntries:   hotEntries,
		WarmBytes:    warmBytes,
		WarmMaxBytes: warmMax,
		WarmEntries:  warmEntries,
	}))
}

func (h *Handler) configReload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(header.ContentType, "text/html")
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminFormBytes)

	if h.cfg.ConfigPath == "" {
		_, _ = fmt.Fprint(w, `<div class="flash-err">✗ Config path not configured</div>`)
		return
	}

	parsed, err := config.Load(h.cfg.ConfigPath)
	if err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprintf(w, `<div class="flash-err">✗ Config parse error: %s</div>`, err.Error())
		return
	}

	confirmed := r.FormValue("confirm") == "1"
	if !confirmed {
		_, _ = fmt.Fprint(w, `<div id="reload-confirm" class="confirm-box show">
  <p>Config validated successfully. Apply new configuration?</p>
  <form hx-post="/dashboard/config/reload" hx-target="#reload-section" hx-swap="outerHTML">
    <input type="hidden" name="confirm" value="1"/>
    <div style="display:flex;gap:.5rem;margin-top:.75rem">
      <button class="btn bp" type="submit">Apply</button>
      <button class="btn bo" type="button" onclick="this.closest('.confirm-box').remove()">Cancel</button>
    </div>
  </form>
</div>`)
		return
	}

	if h.cfg.ReloadFn != nil {
		if err := h.cfg.ReloadFn(parsed); err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = fmt.Fprintf(w, `<div id="reload-section"><div class="flash-err">✗ Apply failed: %s</div></div>`, err.Error())
			return
		}
	}
	_, _ = fmt.Fprintf(w, `<div id="reload-section"><div class="flash-ok">✓ Config reloaded at %s</div></div>`,
		time.Now().Format("15:04:05"))
}

// ── Proxy handlers ────────────────────────────────────────────────────

func (h *Handler) apiPurge(w http.ResponseWriter, r *http.Request) {
	if h.cfg.PurgeFn == nil {
		h.apiError(w, "purge not configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminFormBytes)
	_ = r.ParseForm()
	rawURL := r.FormValue("url")
	if rawURL == "" {
		var req struct {
			URL string `json:"url"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rawURL = req.URL
	}
	if msg := validateCacheURL(rawURL); msg != "" {
		h.apiError(w, msg)
		return
	}
	if err := h.cfg.PurgeFn(r.Context(), rawURL); err != nil {
		h.cfg.Rings.OpsLog.Record("purge", rawURL, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("purge", rawURL, "ok")
	h.apiOK(w, "purged")
}

func (h *Handler) apiBan(w http.ResponseWriter, r *http.Request) {
	if h.cfg.BanFn == nil {
		h.apiError(w, "ban not configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminFormBytes)
	_ = r.ParseForm()
	hostRegex := r.FormValue("host_regex")
	pathRegex := r.FormValue("path_regex")
	if hostRegex == "" && pathRegex == "" {
		var req struct {
			HostRegex string `json:"host_regex"`
			PathRegex string `json:"path_regex"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		hostRegex, pathRegex = req.HostRegex, req.PathRegex
	}
	if hostRegex == "" && pathRegex == "" {
		h.apiError(w, "provide at least one of host regex or path regex")
		return
	}
	if msg := validateRegex("host regex", hostRegex); msg != "" {
		h.apiError(w, msg)
		return
	}
	if msg := validateRegex("path regex", pathRegex); msg != "" {
		h.apiError(w, msg)
		return
	}
	n, err := h.cfg.BanFn(r.Context(), hostRegex, pathRegex)
	arg := hostRegex + " " + pathRegex
	if err != nil {
		h.cfg.Rings.OpsLog.Record("ban", arg, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("ban", arg, fmt.Sprintf("ok, %d evicted", n))
	h.apiOK(w, fmt.Sprintf("banned, %d entries evicted", n))
}

func (h *Handler) apiRefresh(w http.ResponseWriter, r *http.Request) {
	if h.cfg.RefreshFn == nil {
		h.apiError(w, "refresh not configured")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxAdminFormBytes)
	_ = r.ParseForm()
	rawURL := r.FormValue("url")
	if rawURL == "" {
		var req struct {
			URL string `json:"url"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		rawURL = req.URL
	}
	if msg := validateCacheURL(rawURL); msg != "" {
		h.apiError(w, msg)
		return
	}
	if err := h.cfg.RefreshFn(r.Context(), rawURL); err != nil {
		h.cfg.Rings.OpsLog.Record("refresh", rawURL, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("refresh", rawURL, "ok")
	h.apiOK(w, "refreshed")
}

func (h *Handler) apiOK(w http.ResponseWriter, msg string) {
	w.Header().Set(header.ContentType, "text/html")
	w.Header().Set(header.HXTrigger, "refreshOpsLog")
	_, _ = fmt.Fprintf(w, `<span class="flash-ok">✓ %s</span>`, msg)
}

func (h *Handler) apiError(w http.ResponseWriter, msg string) {
	w.Header().Set(header.ContentType, "text/html")
	_, _ = fmt.Fprintf(w, `<span class="flash-err">✗ %s</span>`, msg)
}

// ── Input validation ──────────────────────────────────────────────────

func validateCacheURL(rawURL string) string {
	if rawURL == "" {
		return "URL is required"
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Sprintf("invalid URL: %s", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "URL must begin with http:// or https://"
	}
	if u.Host == "" {
		return "URL must include a host, e.g. https://example.com/path"
	}
	return ""
}

func validateRegex(fieldName, s string) string {
	if s == "" {
		return ""
	}
	if _, err := regexp.Compile(s); err != nil {
		return fmt.Sprintf("%s is not a valid regex — %s", fieldName, err)
	}
	return ""
}

// ── Insights ──────────────────────────────────────────────────────────

func (h *Handler) insights(w http.ResponseWriter, r *http.Request) {
	_, timeRange := parseTimeRange(r.URL.Query().Get("range"))
	merged, peers := h.agg.Collect(r.Context())

	data := h.collectInsightData(merged, peers)
	h.prevStoreStatsMu.Lock()
	data.PrevStoreStats = h.prevStoreStats
	h.prevStoreStats = data.StoreStats
	h.prevStoreStatsMu.Unlock()
	rawInsights := h.insightEngine.Evaluate(r.Context(), data)
	routeToPool := h.buildRouteToPool()
	cards, highCount, medCount, lowCount := convertInsightCards(rawInsights, routeToPool)
	nodes := h.buildArchNodes(data.PoolHealth, h.cfStatusCard(), data.StoreStats, peers)

	h.render(w, r, templates.Insights(templates.InsightsData{
		LayoutProps: h.layoutProps("insights", "Insights", timeRange),
		Nodes:       nodes,
		Insights:    cards,
		HighCount:   highCount,
		MedCount:    medCount,
		LowCount:    lowCount,
	}))
}

// collectInsightData gathers all telemetry inputs needed by the insight
// engine from the dashboard config closures and merged cluster data.
func (h *Handler) collectInsightData(merged observability.MetricsSummary, peers []PeerResult) insights.InsightData {
	var storeStats api.Stats
	if h.cfg.StoreFn != nil {
		storeStats = h.cfg.StoreFn()
	}
	var poolHealth map[string][]origin.TargetStatus
	if h.cfg.PoolHealthFn != nil {
		poolHealth = h.cfg.PoolHealthFn()
	}
	var headerAudit map[string]observability.HeaderAuditSummary
	if h.cfg.OriginHeaderAuditFn != nil {
		headerAudit = h.cfg.OriginHeaderAuditFn()
	}
	var varyCapHits int64
	if h.cfg.VaryCapHitsFn != nil {
		varyCapHits = h.cfg.VaryCapHitsFn()
	}
	cfCard := h.cfStatusCard()
	cfStatus := insights.CFStatus{}
	if cfCard != nil {
		cfStatus.Enabled = cfCard.Enabled
		cfStatus.Async = cfCard.Async
		cfStatus.LastLagMs = cfCard.LastLagMs
		cfStatus.LastError = cfCard.LastError
	}
	peerInfos := make([]insights.PeerInfo, len(peers))
	for i, p := range peers {
		peerInfos[i] = insights.PeerInfo{Name: p.NodeName, Stale: p.Stale}
	}
	var peerHealth map[string]float64
	if h.cfg.Rings != nil && h.cfg.Rings.Peer != nil {
		peerHealth = h.cfg.Rings.Peer.PeerHealth()
	}
	var broadcastFailures int64
	if h.cfg.BroadcastFailuresFn != nil {
		broadcastFailures = h.cfg.BroadcastFailuresFn()
	}
	var cfPurgeSkipped int64
	if h.cfg.CFPurgeSkippedFn != nil {
		cfPurgeSkipped = h.cfg.CFPurgeSkippedFn()
	}
	return insights.InsightData{
		Config:            h.cfg.Config,
		StoreStats:        storeStats,
		RouteStats:        merged.RouteStats,
		RequestBuckets:    merged.RequestSnap,
		PeerResults:       peerInfos,
		PeerHealth:        peerHealth,
		CFStatus:          cfStatus,
		PoolHealth:        poolHealth,
		HeaderAudit:       headerAudit,
		VaryCapHits:       varyCapHits,
		BroadcastFailures: broadcastFailures,
		CFPurgeSkipped:    cfPurgeSkipped,
	}
}

// convertInsightCards maps insight engine results to template cards and
// counts per severity for the filter chips.
func convertInsightCards(raw []insights.Insight, routeToPool map[string]string) (cards []templates.InsightCard, high, med, low int) {
	cards = make([]templates.InsightCard, len(raw))
	for i, ins := range raw {
		cards[i] = templates.InsightCard{
			ID:       ins.ID,
			Severity: string(ins.Severity),
			Category: string(ins.Category),
			Title:    ins.Title,
			Detail:   ins.Detail,
			Evidence: ins.Evidence,
			Routes:   ins.Routes,
			Action:   ins.Action,
			NodeIDs:  insightNodeIDs(ins, routeToPool),
		}
		switch ins.Severity {
		case insights.SeverityHigh:
			high++
		case insights.SeverityMed:
			med++
		default:
			low++
		}
	}
	return
}

// insightNodeIDs maps an insight to the architecture node IDs it relates
// to, enabling click-to-focus filtering on the diagram.
func insightNodeIDs(ins insights.Insight, routeToPool map[string]string) []string {
	var ids []string
	switch ins.Category {
	case insights.CategoryCDN:
		ids = append(ids, "cdn")
	case insights.CategoryCluster, insights.CategoryConfig:
		ids = append(ids, "bouine")
	default:
		for _, route := range ins.Routes {
			if pool, ok := routeToPool[route]; ok && pool != "" {
				ids = append(ids, "pool:"+pool)
			}
		}
		if len(ids) == 0 {
			ids = append(ids, "bouine")
		}
	}
	return ids
}

// buildRouteToPool creates a route-name → pool-name mapping from config.
func (h *Handler) buildRouteToPool() map[string]string {
	m := make(map[string]string)
	if h.cfg.Config == nil {
		return m
	}
	for _, rc := range h.cfg.Config.Routes {
		name := rc.Name
		if name == "" {
			name = rc.Match.PathPrefix
		}
		m[name] = rc.Pool
	}
	return m
}

// buildArchNodes constructs the architecture flow diagram nodes from the
// running config, live pool health, CF status, and storage stats.
func (h *Handler) buildArchNodes(
	poolHealth map[string][]origin.TargetStatus,
	cfCard *templates.CFStatusCard,
	storeStats api.Stats,
	peers []PeerResult,
) []templates.ArchNode {
	var nodes []templates.ArchNode
	nodes = append(nodes, clientNode())
	if cfCard != nil && cfCard.Enabled {
		nodes = append(nodes, cdnNode(cfCard))
	}
	nodes = append(nodes, h.clusterNode(peers, storeStats))
	if h.cfg.Config != nil {
		seen := make(map[string]bool)
		for _, rc := range h.cfg.Config.Routes {
			if rc.Pool == "" || seen[rc.Pool] {
				continue
			}
			seen[rc.Pool] = true
			status, detail := poolNodeStatus(rc.Pool, poolHealth)
			nodes = append(nodes, templates.ArchNode{
				ID:     "pool:" + rc.Pool,
				Type:   "pool",
				Label:  rc.Pool,
				Status: status,
				Detail: detail,
			})
		}
	}
	return nodes
}

func clientNode() templates.ArchNode {
	return templates.ArchNode{
		ID:     "client",
		Type:   "client",
		Label:  "Clients",
		Status: "healthy",
		Detail: "HTTP/1.1 + h2c + h3",
	}
}

func cdnNode(cfCard *templates.CFStatusCard) templates.ArchNode {
	detail := "zone " + cfCard.ZoneID
	if cfCard.Async {
		detail += " · async"
	}
	return templates.ArchNode{
		ID:     "cdn",
		Type:   "cdn",
		Label:  "Cloudflare CDN",
		Status: "healthy",
		Detail: detail,
	}
}

func (h *Handler) clusterNode(peers []PeerResult, storeStats api.Stats) templates.ArchNode {
	clusterMode := "single-node"
	if h.cfg.ClusterMeta.Mode != "" {
		clusterMode = h.cfg.ClusterMeta.Mode
	}
	var peerNodes []templates.PeerNode
	staleCount := 0
	for _, p := range peers {
		st := "healthy"
		if p.Stale {
			st = "stale"
			staleCount++
		}
		peerNodes = append(peerNodes, templates.PeerNode{Name: p.NodeName, Status: st})
	}
	clusterStatus := "healthy"
	if len(peers) > 0 {
		if staleCount == len(peers) {
			clusterStatus = "unhealthy"
		} else if staleCount > 0 {
			clusterStatus = "degraded"
		}
	}
	return templates.ArchNode{
		ID:           "bouine",
		Type:         "bouine",
		Label:        "bouine cluster",
		Status:       clusterStatus,
		Detail:       "mode: " + clusterMode,
		Peers:        peerNodes,
		StorageTiers: storageTiers(h.cfg.WarmMaxBytes, h.cfg.HotMaxBytes, storeStats),
	}
}

// storageTiers builds the storage tier list shown inside the cluster
// container. If warm storage is configured (warmMax > 0), both hot and
// warm tiers are returned. Otherwise only the hot tier is shown.
// Tier status degrades when fill exceeds 90%.
func storageTiers(warmMax, hotMax int64, storeStats api.Stats) []templates.StorageTier {
	hotStatus := "healthy"
	if hotMax > 0 {
		hotPct := float64(storeStats.HotBytes) / float64(hotMax) * 100
		if hotPct > 100 {
			hotPct = 100
		}
		if hotPct > 90 {
			hotStatus = "degraded"
		}
	}
	warmStatus := "healthy"
	if warmMax > 0 {
		warmPct := float64(storeStats.WarmBytes) / float64(warmMax) * 100
		if warmPct > 100 {
			warmPct = 100
		}
		if warmPct > 90 {
			warmStatus = "degraded"
		}
	}
	tiers := []templates.StorageTier{
		{Name: "Hot", Status: hotStatus, Detail: templates.FmtBytes(storeStats.HotBytes)},
	}
	if warmMax > 0 {
		tiers = append(tiers, templates.StorageTier{
			Name:   "Warm",
			Status: warmStatus,
			Detail: templates.FmtBytes(storeStats.WarmBytes),
		})
	}
	return tiers
}

// poolNodeStatus computes the health status and detail string for a single
// upstream pool from the live target health map.
func poolNodeStatus(poolName string, poolHealth map[string][]origin.TargetStatus) (status, detail string) {
	status = "healthy"
	detail = poolName
	targets, ok := poolHealth[poolName]
	if !ok {
		return
	}
	healthy, total := 0, len(targets)
	for _, t := range targets {
		if t.Healthy {
			healthy++
		}
	}
	if healthy == 0 {
		status = "unhealthy"
	} else if healthy < total {
		status = "degraded"
	}
	detail = fmt.Sprintf("%s · %d/%d targets", poolName, healthy, total)
	return
}
