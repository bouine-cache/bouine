package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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
	// StoreFn, if non-nil, is called to fetch hot/warm tier stats.
	StoreFn     func() api.Stats
	HotMaxBytes int64
	// Invalidation proxies — called by /dashboard/api/* so bearer token
	// never appears in HTML (RFC §6.3 / PLAN.md §6.5).
	PurgeFn   func(ctx context.Context, urlStr string) error
	BanFn     func(ctx context.Context, hostRegex, pathRegex string) (int, error)
	RefreshFn func(ctx context.Context, urlStr string) error
	// ConfigPath is the absolute path to the YAML config file.
	// Required for the config-reload validate→confirm→apply flow.
	ConfigPath string
	// ReloadFn, if non-nil, is called after operator confirmation to apply
	// a validated config. Receives the already-parsed *config.Config.
	ReloadFn func(*config.Config) error
	// RingFn, if non-nil, returns the consistent-hash ring ownership for
	// the cluster SVG visualization.
	RingFn func() []api.RingSegment
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
	h := &Handler{
		cfg:  cfg,
		auth: newSessionAuth(cfg.Token),
		agg:  NewAggregator(cfg.Rings, cfg.PeersFn, cfg.SelfAddr, cfg.Token, cfg.Logger),
	}

	// Login (unprotected).
	mux.HandleFunc("GET /dashboard/login", h.auth.LoginHandler)
	mux.HandleFunc("POST /dashboard/login", h.auth.LoginHandler)

	// All other dashboard routes require the session cookie.
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

func sortRouteStats(stats []observability.RouteStat) []observability.RouteStat {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Requests > stats[j].Requests
	})
	return stats
}

func toPeerResults(in []PeerResult) []templates.PeerResult {
	out := make([]templates.PeerResult, len(in))
	for i, p := range in {
		out[i] = templates.PeerResult{
			NodeName: p.NodeName,
			Summary:  p.Summary,
			Stale:    p.Stale,
		}
	}
	return out
}

// parseTimeRange maps the query-param string to a request-bucket count.
// 1h = 360 buckets, 6h = 2160 (all), 24h = 2160 (capped at ring size).
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
	reqs := make([]int64, 0, chartBuckets)
	hits := make([]int64, 0, chartBuckets)
	for _, b := range snap[start:] {
		labels = append(labels, time.Unix(b.Timestamp, 0).Format("15:04:05"))
		reqs = append(reqs, b.Requests)
		hits = append(hits, b.Hits)
	}
	if len(reqs) == 0 {
		labels = []string{"—"}
		reqs = []int64{0}
		hits = []int64{0}
	}

	var totalReq, totalHit, totalErr, maxP99 int64
	var splitHits, splitMisses, splitStales, splitBypasses, splitRevalidated int64
	recentBuckets := min(6, n)
	for _, b := range snap[n-recentBuckets:] {
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
	var hitPct, errPct float64
	if totalReq > 0 {
		hitPct = float64(totalHit) / float64(totalReq) * 100
		errPct = float64(totalErr) / float64(totalReq) * 100
	}
	reqPerSec := float64(totalReq) / float64(max(1, recentBuckets)*10)

	var hotBytes, hotEntries int64
	if h.cfg.StoreFn != nil {
		st := h.cfg.StoreFn()
		hotBytes = st.HotBytes
		hotEntries = st.HotEntries
	}

	top5 := sortRouteStats(merged.RouteStats)
	if len(top5) > 5 {
		top5 = top5[:5]
	}

	h.render(w, r, templates.Overview(templates.OverviewData{
		LayoutProps: templates.LayoutProps{Page: "overview", PageTitle: "Overview", NodeName: h.nodeName()},
		TimeRange:   timeRange,
		ReqPerSec:   reqPerSec,
		HitPct:      hitPct,
		P99MS:       maxP99,
		ErrPct:      errPct,
		CacheSplit: templates.CacheSplitData{
			Hits: splitHits, Misses: splitMisses, Stales: splitStales,
			Bypasses: splitBypasses, Revalidated: splitRevalidated,
		},
		ChartLabels: labels,
		ChartReqs:   reqs,
		ChartHits:   hits,
		RouteStats:  top5,
		PeerResults: toPeerResults(peers),
		HotBytes:    hotBytes,
		HotMaxBytes: h.cfg.HotMaxBytes,
		HotEntries:  hotEntries,
	}))
}

func (h *Handler) routes(w http.ResponseWriter, r *http.Request) {
	merged, _ := h.agg.Collect(r.Context())
	h.render(w, r, templates.Routes(templates.RoutesData{
		LayoutProps: templates.LayoutProps{Page: "routes", PageTitle: "Routes", NodeName: h.nodeName()},
		RouteStats:  sortRouteStats(merged.RouteStats),
	}))
}

func (h *Handler) cluster(w http.ResponseWriter, r *http.Request) {
	_, peers := h.agg.Collect(r.Context())

	var peerHealth map[string]float64
	if h.cfg.Rings != nil {
		peerHealth = h.cfg.Rings.Peer.PeerHealth()
	}

	var ringSegs []api.RingSegment
	if h.cfg.RingFn != nil {
		ringSegs = h.cfg.RingFn()
	}

	h.render(w, r, templates.Cluster(templates.ClusterData{
		LayoutProps: templates.LayoutProps{Page: "cluster", PageTitle: "Cluster", NodeName: h.nodeName()},
		PeerResults: toPeerResults(peers),
		PeerHealth:  peerHealth,
		RingSegs:    ringSegs,
	}))
}

func (h *Handler) invalidation(w http.ResponseWriter, r *http.Request) {
	var opsLog []observability.OpsLogEntry
	if h.cfg.Rings != nil {
		opsLog = h.cfg.Rings.OpsLog.Snapshot(20)
	}
	h.render(w, r, templates.Invalidation(templates.InvalidationData{
		LayoutProps: templates.LayoutProps{Page: "invalidation", PageTitle: "Invalidation", NodeName: h.nodeName()},
		OpsLog:      opsLog,
	}))
}

func (h *Handler) config(w http.ResponseWriter, r *http.Request) {
	h.render(w, r, templates.Config(templates.ConfigData{
		LayoutProps:  templates.LayoutProps{Page: "config", PageTitle: "Config", NodeName: h.nodeName()},
		SnapshotPath: h.cfg.SnapshotPath,
	}))
}

// configReload implements the validate → confirm → apply flow per §6.5:
//  1. POST without confirm: parse config file; 422 on error, confirm dialog on success.
//  2. POST with confirm=1: re-parse and apply via ReloadFn; 422 on error.
func (h *Handler) configReload(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")

	if h.cfg.ConfigPath == "" {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprint(w, `<div class="flash-err">✗ Config path not configured (set admin.config_path)</div>`)
		return
	}

	// Step 1 and 2 both parse the file — validate first, apply on confirm.
	parsed, err := config.Load(h.cfg.ConfigPath)
	if err != nil {
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = fmt.Fprintf(w, `<div class="flash-err">✗ Config parse error: %s</div>`, err.Error())
		return
	}

	confirmed := r.FormValue("confirm") == "1"
	if !confirmed {
		// Valid — show confirmation dialog.
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

	// Step 2 — apply.
	if h.cfg.ReloadFn != nil {
		if err := h.cfg.ReloadFn(parsed); err != nil {
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = fmt.Fprintf(w, `<div id="reload-section"><div class="flash-err">✗ Config apply failed: %s</div></div>`, err.Error())
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
	// ParseForm first: htmx sends application/x-www-form-urlencoded.
	// json.Decode is a fallback for direct API callers.
	_ = r.ParseForm()
	url := r.FormValue("url")
	if url == "" {
		var req struct {
			URL string `json:"url"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		url = req.URL
	}
	if url == "" {
		h.apiError(w, "missing url")
		return
	}
	if err := h.cfg.PurgeFn(r.Context(), url); err != nil {
		h.cfg.Rings.OpsLog.Record("purge", url, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("purge", url, "ok")
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
	url := r.FormValue("url")
	if url == "" {
		var req struct {
			URL string `json:"url"`
		}
		_ = json.NewDecoder(r.Body).Decode(&req)
		url = req.URL
	}
	if url == "" {
		h.apiError(w, "missing url")
		return
	}
	if err := h.cfg.RefreshFn(r.Context(), url); err != nil {
		h.cfg.Rings.OpsLog.Record("refresh", url, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("refresh", url, "ok")
	h.apiOK(w, "refreshed")
}

func (h *Handler) apiOK(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/html")
	w.Header().Set("HX-Trigger", "refreshOpsLog")
	_, _ = fmt.Fprintf(w, `<span class="flash-ok">✓ %s</span>`, msg)
}

func (h *Handler) apiError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(w, `<span class="flash-err">✗ %s</span>`, msg)
}
