package dashboard

import (
	"encoding/json"
	"fmt"
	"html/template"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// Config controls the dashboard server.
type Config struct {
	Rings        *observability.Rings
	PeersFn      func() []api.PeerInfo
	SelfAddr     string // own admin addr for self-exclusion in fan-out
	Token        string
	Logger       *slog.Logger
	SnapshotPath string
}

// Handler is the dashboard HTTP handler. Mount at /dashboard/.
type Handler struct {
	cfg   Config
	auth  *sessionAuth
	agg   *Aggregator
	tmpls *template.Template
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
	h.tmpls = template.Must(template.New("").Funcs(template.FuncMap{
		"fmtReqS":   fmtReqS,
		"fmtHitPct": fmtHitPct,
		"fmtLatMs":  fmtLatMs,
		"timeAgo":   timeAgo,
		"mod":       func(a, b int) int { return a % b },
		"add":       func(a, b int) int { return a + b },
		"seq": func(n int) []int {
			s := make([]int, n)
			for i := range s {
				s[i] = i
			}
			return s
		},
		"toJSON": func(v any) (template.JS, error) {
			b, err := json.Marshal(v)
			if err != nil {
				return "", err
			}
			return template.JS(b), nil //nolint:gosec // chart data is server-generated
		},
	}).Parse(allTemplates))

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

	mux.Handle("/dashboard/", h.auth.Middleware(protected))
	return h
}

// ── View data models ─────────────────────────────────────────────────

// pageBase holds fields present on every page data struct.
type pageBase struct {
	Page      string // "overview" | "routes" | "cluster" | "invalidation" | "config"
	PageTitle string
	NodeName  string
}

type overviewData struct {
	pageBase
	Token       string
	PeerResults []PeerResult
	Merged      observability.MetricsSummary
	ReqPerSec   float64
	HitPct      float64
	P99MS       int64
	ErrPct      float64
	ChartLabels []string
	ChartReqs   []int64
	ChartHits   []int64
	RouteStats  []observability.RouteStat
}

type routesData struct {
	pageBase
	RouteStats []observability.RouteStat
}

type clusterData struct {
	pageBase
	PeerResults []PeerResult
	Merged      observability.MetricsSummary
}

type invalidationData struct {
	pageBase
	Token string
	Flash string
}

type configData struct {
	pageBase
	Flash        string
	SnapshotPath string
}

// ── Handlers ─────────────────────────────────────────────────────────

func (h *Handler) nodeName() string {
	if h.cfg.Rings != nil {
		return h.cfg.Rings.NodeName
	}
	return "unknown"
}

func (h *Handler) overview(w http.ResponseWriter, r *http.Request) {
	merged, peers := h.agg.Collect(r.Context())

	// Build chart data from last 36 buckets (6 min).
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
	// Ensure slices are non-empty to avoid JS errors.
	if len(reqs) == 0 {
		labels = []string{"—"}
		reqs = []int64{0}
		hits = []int64{0}
	}

	// Aggregate last 60s for headline numbers.
	var totalReq, totalHit, totalErr, maxP99 int64
	recentBuckets := min(6, n) // last 60s
	for _, b := range snap[n-recentBuckets:] {
		totalReq += b.Requests
		totalHit += b.Hits
		totalErr += b.Errors
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

	h.render(w, "overview", overviewData{
		pageBase:    pageBase{Page: "overview", PageTitle: "Overview", NodeName: h.nodeName()},
		Token:       h.cfg.Token,
		PeerResults: peers,
		Merged:      merged,
		ReqPerSec:   reqPerSec,
		HitPct:      hitPct,
		P99MS:       maxP99,
		ErrPct:      errPct,
		ChartLabels: labels,
		ChartReqs:   reqs,
		ChartHits:   hits,
		RouteStats:  sortRouteStats(merged.RouteStats),
	})
}

func (h *Handler) routes(w http.ResponseWriter, r *http.Request) {
	merged, _ := h.agg.Collect(r.Context())
	h.render(w, "routes", routesData{
		pageBase:   pageBase{Page: "routes", PageTitle: "Routes", NodeName: h.nodeName()},
		RouteStats: sortRouteStats(merged.RouteStats),
	})
}

func (h *Handler) cluster(w http.ResponseWriter, r *http.Request) {
	merged, peers := h.agg.Collect(r.Context())
	h.render(w, "cluster", clusterData{
		pageBase:    pageBase{Page: "cluster", PageTitle: "Cluster", NodeName: h.nodeName()},
		PeerResults: peers,
		Merged:      merged,
	})
}

func (h *Handler) invalidation(w http.ResponseWriter, _ *http.Request) {
	h.render(w, "invalidation", invalidationData{
		pageBase: pageBase{Page: "invalidation", PageTitle: "Invalidation", NodeName: h.nodeName()},
		Token:    h.cfg.Token,
	})
}

func (h *Handler) config(w http.ResponseWriter, _ *http.Request) {
	h.render(w, "config", configData{
		pageBase:     pageBase{Page: "config", PageTitle: "Config", NodeName: h.nodeName()},
		SnapshotPath: h.cfg.SnapshotPath,
	})
}

func (h *Handler) configReload(w http.ResponseWriter, r *http.Request) {
	// Returns a partial fragment for htmx swap.
	confirmed := r.FormValue("confirm") == "1"
	if !confirmed {
		w.Header().Set("Content-Type", "text/html")
		_, _ = fmt.Fprint(w, `<div id="reload-confirm" class="confirm-box show">
  <p>Config file validated successfully. Apply new configuration?</p>
  <form hx-post="/dashboard/config/reload" hx-target="#reload-section" hx-swap="outerHTML">
    <input type="hidden" name="confirm" value="1">
    <div style="display:flex;gap:.5rem;margin-top:.75rem">
      <button class="btn bp" type="submit">Apply</button>
      <button class="btn bo" type="button" onclick="this.closest('.confirm-box').remove()">Cancel</button>
    </div>
  </form>
</div>`)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	_, _ = fmt.Fprintf(w, `<div id="reload-section"><div class="flash-ok">✓ Config reloaded at %s</div></div>`,
		time.Now().Format("15:04:05"))
}

// ── Helpers ──────────────────────────────────────────────────────────

func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.tmpls.ExecuteTemplate(w, name, data); err != nil {
		h.cfg.Logger.Error("dashboard render error", "template", name, "error", err)
		http.Error(w, "render error", http.StatusInternalServerError)
	}
}

func sortRouteStats(stats []observability.RouteStat) []observability.RouteStat {
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].Requests > stats[j].Requests
	})
	return stats
}

func fmtReqS(v float64) string {
	if v >= 1000 {
		return fmt.Sprintf("%.1fk", v/1000)
	}
	return fmt.Sprintf("%.0f", v)
}

func fmtHitPct(v float64) string { return fmt.Sprintf("%.1f%%", v) }

func fmtLatMs(ms int64) string {
	if ms == 0 {
		return "—"
	}
	return fmt.Sprintf("%dms", ms)
}

func timeAgo(t time.Time) string {
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
