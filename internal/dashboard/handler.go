package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"time"

	"github.com/a-h/templ"

	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/dashboard/templates"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// Config controls the dashboard server.
type Config struct {
	Rings        *observability.Rings
	PeersFn      func() []api.PeerInfo
	SelfAddr     string
	Token        string
	Logger       *slog.Logger
	SnapshotPath string
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
}

// Handler is the dashboard HTTP handler. Mount at /dashboard/.
type Handler struct {
	cfg  Config
	auth *sessionAuth
	agg  *Aggregator
}

// New creates and registers dashboard routes on mux.
func New(cfg Config, mux *http.ServeMux) *Handler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if cfg.StartTime.IsZero() {
		cfg.StartTime = time.Now()
	}
	h := &Handler{
		cfg:  cfg,
		auth: newSessionAuth(cfg.Token),
		agg:  NewAggregator(cfg.Rings, cfg.PeersFn, cfg.SelfAddr, cfg.Token, cfg.Logger),
	}

	mux.HandleFunc("GET /dashboard/login", h.auth.LoginHandler)
	mux.HandleFunc("POST /dashboard/login", h.auth.LoginHandler)

	protected := http.NewServeMux()
	protected.HandleFunc("GET /dashboard/", h.overview)
	protected.HandleFunc("GET /dashboard/routes", h.routes)
	protected.HandleFunc("GET /dashboard/cluster", h.cluster)
	protected.HandleFunc("GET /dashboard/invalidation", h.invalidation)
	protected.HandleFunc("GET /dashboard/config", h.config)
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
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
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
		TimeRange:     timeRange,
		PeerCount:     peerCount,
		LivePeers:     live,
		SidebarReqS:   reqs,
		SidebarHitPct: hitPct,
	}
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
		labels = append(labels, time.Unix(b.Timestamp, 0).Format("15:04:05"))
		reqData = append(reqData, b.Requests)
		errData = append(errData, b.Errors)
	}
	if len(reqData) == 0 {
		labels = []string{"—"}
		reqData = []int64{0}
		errData = []int64{0}
	}

	// Current 60s window.
	recent := min(6, n)
	var totalReq, totalHit, totalErr, maxP99 int64
	var splitHits, splitMisses, splitStales, splitBypasses, splitRevalidated int64
	for _, b := range snap[n-recent:] {
		totalReq += b.Requests
		totalHit += b.Hits
		totalErr += b.Errors
		splitHits += b.Hits
		splitMisses += b.Misses
		splitStales += b.StaleHits
		splitBypasses += b.Bypasses
		splitRevalidated += b.Revalidated
		if b.P99MS > maxP99 {
			maxP99 = b.P99MS
		}
	}

	// Prior 60s window for trend deltas.
	priorStart := max(0, n-12)
	var priorReq, priorHit, priorErr, priorP99 int64
	for _, b := range snap[priorStart:max(priorStart, n-recent)] {
		priorReq += b.Requests
		priorHit += b.Hits
		priorErr += b.Errors
		if b.P99MS > priorP99 {
			priorP99 = b.P99MS
		}
	}

	var hitPct, errPct float64
	if totalReq > 0 {
		hitPct = float64(totalHit) / float64(totalReq) * 100
		errPct = float64(totalErr) / float64(totalReq) * 100
	}
	var priorHitPct, priorErrPct float64
	if priorReq > 0 {
		priorHitPct = float64(priorHit) / float64(priorReq) * 100
		priorErrPct = float64(priorErr) / float64(priorReq) * 100
	}
	reqPerSec := float64(totalReq) / float64(max(1, recent)*10)
	priorReqPerSec := float64(priorReq) / float64(max(1, recent)*10)

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
		ReqPerSec:   reqPerSec,
		HitPct:      hitPct,
		P99MS:       maxP99,
		ErrPct:      errPct,
		TrendReq:    templates.BuildTrend(reqPerSec, priorReqPerSec, true),
		TrendHit:    templates.BuildTrend(hitPct, priorHitPct, true),
		TrendLat:    templates.BuildTrend(float64(maxP99), float64(priorP99), false),
		TrendErr:    templates.BuildTrend(errPct, priorErrPct, false),
		CacheSplit: templates.CacheSplitData{
			Hits: splitHits, Misses: splitMisses, Stales: splitStales,
			Bypasses: splitBypasses, Revalidated: splitRevalidated,
		},
		ChartLabels: labels,
		ChartReqs:   reqData,
		ChartErrs:   errData,
		RouteRows:   routeRows,
		PeerResults: toPeerResultsEnriched(peers, h.cfg.PeersFn),
		HotBytes:    hotBytes, HotMaxBytes: hotMax, HotEntries: hotEntries,
		WarmBytes: warmBytes, WarmMaxBytes: warmMax, WarmEntries: warmEntries,
		Evictions: evictions,
		RingSegs:  ringSegs,
	}))
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

	h.render(w, r, templates.Cluster(templates.ClusterData{
		LayoutProps: h.layoutProps("cluster", "Cluster", timeRange),
		PeerResults: toPeerResultsEnriched(peers, h.cfg.PeersFn),
		PeerHealth:  peerHealth,
		RingSegs:    ringSegs,
		Meta:        h.cfg.ClusterMeta,
		FetchStats:  fetchStats,
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
	}))
}

func (h *Handler) config(w http.ResponseWriter, r *http.Request) {
	_, timeRange := parseTimeRange(r.URL.Query().Get("range"))
	uptime := templates.FmtUptime(time.Since(h.cfg.StartTime))
	var rawJSON string
	if h.cfg.Config != nil {
		if b, err := json.MarshalIndent(h.cfg.Config, "", "  "); err == nil {
			rawJSON = string(b)
		}
	}
	h.render(w, r, templates.Config(templates.ConfigData{
		LayoutProps:  h.layoutProps("config", "Config", timeRange),
		ConfigPath:   h.cfg.ConfigPath,
		SnapshotPath: h.cfg.SnapshotPath,
		Uptime:       uptime,
		Sections:     templates.BuildConfigSections(h.cfg.Config),
		RawJSON:      rawJSON,
	}))
}

func (h *Handler) configReload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

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
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("HX-Trigger", "refreshOpsLog")
	_, _ = fmt.Fprintf(w, `<span class="flash-ok">✓ %s</span>`, msg)
}

func (h *Handler) apiError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/html")
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
