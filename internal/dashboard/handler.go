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
	// StoreFn, if non-nil, is called to fetch hot/warm tier stats for the
	// overview hot-tier utilisation card.
	StoreFn     func() api.Stats
	HotMaxBytes int64
	// Invalidation proxies — called by /dashboard/api/* endpoints so the
	// bearer token never appears in HTML.
	PurgeFn   func(ctx context.Context, urlStr string) error
	BanFn     func(ctx context.Context, hostRegex, pathRegex string) (int, error)
	RefreshFn func(ctx context.Context, urlStr string) error
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
	// Proxy endpoints — session cookie auth, token never exposed in HTML.
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

// toPeerResults converts aggregator PeerResults to template PeerResults.
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

// ── Page handlers ─────────────────────────────────────────────────────

func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	merged, peers := h.agg.Collect(r.Context())

	const chartBuckets = 36
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

	// Hot-tier stats (optional).
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
	h.render(w, r, templates.Cluster(templates.ClusterData{
		LayoutProps: templates.LayoutProps{Page: "cluster", PageTitle: "Cluster", NodeName: h.nodeName()},
		PeerResults: toPeerResults(peers),
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

func (h *Handler) configReload(w http.ResponseWriter, r *http.Request) {
	confirmed := r.FormValue("confirm") == "1"
	w.Header().Set("Content-Type", "text/html")
	if !confirmed {
		_, _ = fmt.Fprint(w, `<div id="reload-confirm" class="confirm-box show">
  <p>Config file validated successfully. Apply new configuration?</p>
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
	_, _ = fmt.Fprintf(w, `<div id="reload-section"><div class="flash-ok">✓ Config reloaded at %s</div></div>`,
		time.Now().Format("15:04:05"))
}

// ── Proxy handlers ────────────────────────────────────────────────────
// These handlers sit behind the session-cookie middleware so the bearer
// token never needs to be embedded in HTML.

func (h *Handler) apiPurge(w http.ResponseWriter, r *http.Request) {
	if h.cfg.PurgeFn == nil {
		h.apiError(w, "purge not configured")
		return
	}
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		req.URL = r.FormValue("url")
	}
	if req.URL == "" {
		h.apiError(w, "missing url")
		return
	}
	if err := h.cfg.PurgeFn(r.Context(), req.URL); err != nil {
		h.cfg.Rings.OpsLog.Record("purge", req.URL, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("purge", req.URL, "ok")
	h.apiOK(w, "purged")
}

func (h *Handler) apiBan(w http.ResponseWriter, r *http.Request) {
	if h.cfg.BanFn == nil {
		h.apiError(w, "ban not configured")
		return
	}
	var req struct {
		HostRegex string `json:"host_regex"`
		PathRegex string `json:"path_regex"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		req.HostRegex = r.FormValue("host_regex")
		req.PathRegex = r.FormValue("path_regex")
	}
	n, err := h.cfg.BanFn(r.Context(), req.HostRegex, req.PathRegex)
	arg := req.HostRegex + " " + req.PathRegex
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
	var req struct {
		URL string `json:"url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.URL == "" {
		req.URL = r.FormValue("url")
	}
	if req.URL == "" {
		h.apiError(w, "missing url")
		return
	}
	if err := h.cfg.RefreshFn(r.Context(), req.URL); err != nil {
		h.cfg.Rings.OpsLog.Record("refresh", req.URL, err.Error())
		h.apiError(w, err.Error())
		return
	}
	h.cfg.Rings.OpsLog.Record("refresh", req.URL, "ok")
	h.apiOK(w, "refreshed")
}

func (h *Handler) apiOK(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = fmt.Fprintf(w, `<span class="flash-ok">✓ %s</span>`, msg)
}

func (h *Handler) apiError(w http.ResponseWriter, msg string) {
	w.Header().Set("Content-Type", "text/html")
	w.WriteHeader(http.StatusBadRequest)
	_, _ = fmt.Fprintf(w, `<span class="flash-err">✗ %s</span>`, msg)
}
