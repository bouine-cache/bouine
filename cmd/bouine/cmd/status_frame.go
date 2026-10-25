package cmd

import (
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

// frameMeta carries the rendering options that differ between the
// fullscreen TUI and the inline one-shot frame.
type frameMeta struct {
	fetchEr  error
	cluster  bool
	keys     bool
	interval time.Duration
	width    int
}

// renderStatusFrame renders the whole status document as a plain-text
// frame with semantic styling. It is pure: given (palette, report,
// prev, meta) it always produces the same bytes, so the TUI and the
// inline one-shot share one layout and both are unit-testable.
func renderStatusFrame(p tuiPalette, r *bouineapi.StatusReport, prev *bouineapi.StatusReport, meta frameMeta) string {
	width := meta.width
	if width < 40 {
		width = 40
	}
	var b strings.Builder
	b.WriteString(statusHeader(p, r, meta, width))
	b.WriteString(statusCacheSection(p, r, prev, meta))
	b.WriteString(statusOriginsSection(p, r, width))
	b.WriteString(statusClusterSection(p, r, width))
	b.WriteString(statusInsightsSection(p, r, width))
	if meta.keys {
		b.WriteString("\n" + p.dim.Render(strings.Repeat("─", width-2)) + "\n")
		clusterState := "off"
		if meta.cluster {
			clusterState = "on"
		}
		b.WriteString(p.dim.Render(fmt.Sprintf(
			" q quit · r refresh · c cluster (%s) · Ctrl-C quit", clusterState)))
	}
	return b.String()
}

func statusHeader(p tuiPalette, r *bouineapi.StatusReport, meta frameMeta, width int) string {
	var b strings.Builder
	ready := p.healthy.Render("READY")
	if !r.Node.Ready {
		ready = p.sick.Render("NOT-READY")
	}
	fmt.Fprintf(&b, " %s  %s  up %s  %s",
		p.title.Render(r.Node.Name), r.Node.Version, formatUptime(r.Node.UptimeSec), ready)
	if meta.cluster {
		b.WriteString(p.dim.Render(fmt.Sprintf("  every %s · cluster on", meta.interval)))
	} else if meta.interval > 0 {
		b.WriteString(p.dim.Render(fmt.Sprintf("  every %s", meta.interval)))
	}
	if meta.fetchEr != nil {
		b.WriteString("\n " + p.sick.Render("fetch error: "+meta.fetchEr.Error()))
	}
	b.WriteString("\n" + p.dim.Render(strings.Repeat("─", width-2)) + "\n")
	return b.String()
}

func statusCacheSection(p tuiPalette, r *bouineapi.StatusReport, prev *bouineapi.StatusReport, meta frameMeta) string {
	var b strings.Builder
	b.WriteString(" CACHE\n")
	drift := ""
	if prev != nil && meta.interval > 0 {
		deltaReq := (r.Cache.Hits + r.Cache.Misses) - (prev.Cache.Hits + prev.Cache.Misses)
		rate := float64(deltaReq) / meta.interval.Seconds()
		driftPoints := (r.Cache.HitRatio - prev.Cache.HitRatio) * 100
		drift = fmt.Sprintf("  %s/s  %+.1f%% drift", humanCount(rate), driftPoints)
	}
	fmt.Fprintf(&b, "    hit %s  %s hits / %s misses%s  evictions %s\n",
		humanPct(r.Cache.HitRatio), humanCount(float64(r.Cache.Hits)),
		humanCount(float64(r.Cache.Misses)), drift, humanCount(float64(r.Cache.Evictions)))
	b.WriteString(renderTierBar(p, "hot", r.Cache.Hot))
	b.WriteString(renderTierBar(p, "warm", r.Cache.Warm))
	return b.String()
}

// renderTierBar renders one tier's fill bar with the name padded so
// hot and warm bars start at the same column. An unbudgeted tier
// (MaxBytes 0) renders an empty bar labeled "unlimited": a full bar
// would read as full when the tier is empty and has no budget to
// measure against.
func renderTierBar(p tuiPalette, name string, t bouineapi.StatusTier) string {
	var b strings.Builder
	fmt.Fprintf(&b, "    %-4s ", name)
	frac := 0.0
	detail := fmt.Sprintf(" %s", humanBytes(t.Bytes))
	if t.MaxBytes > 0 {
		frac = float64(t.Bytes) / float64(t.MaxBytes)
		detail += fmt.Sprintf(" / %s (%s)", humanBytes(t.MaxBytes), humanPct(frac))
	} else {
		detail += " / unlimited"
	}
	b.WriteString(renderBar(p, frac, 20))
	if t.DiskBytes > 0 {
		detail += fmt.Sprintf("  disk %s", humanBytes(t.DiskBytes))
	}
	b.WriteString(p.dim.Render(detail))
	b.WriteString("\n")
	return b.String()
}

func renderBar(p tuiPalette, frac float64, width int) string {
	if frac < 0 {
		frac = 0
	}
	if frac > 1 {
		frac = 1
	}
	fill := int(frac * float64(width))
	return p.barFill.Render(strings.Repeat("█", fill)) + p.barEmpty.Render(strings.Repeat("░", width-fill))
}

func statusOriginsSection(p tuiPalette, r *bouineapi.StatusReport, width int) string {
	if len(r.Origins) == 0 {
		return ""
	}
	type originRow struct{ head, change string }
	rows := make([]originRow, 0, len(r.Origins))
	maxw := 0
	for _, pool := range r.Origins {
		for _, t := range pool.Targets {
			state := p.healthy.Render("healthy")
			if !t.Healthy {
				state = p.sick.Render("SICK")
			}
			head := fmt.Sprintf("   %s/%s  %s", pool.Name, t.Addr, state)
			if cs := targetCounterSignals(t); cs != "" {
				head += "  " + cs
			}
			if w := lipgloss.Width(head); w > maxw {
				maxw = w
			}
			rows = append(rows, originRow{head: head, change: statusRelTime(r, t.LastChange)})
		}
	}
	var b strings.Builder
	b.WriteString(" ORIGINS\n")
	for _, row := range rows {
		line := row.head + strings.Repeat(" ", maxw-lipgloss.Width(row.head)) +
			"  last change " + row.change
		b.WriteString(truncateLine(p, line, width))
		b.WriteString("\n")
	}
	return b.String()
}

// targetCounterSignals renders one target's non-zero health counters
// with human-readable labels; zero counters stay hidden so a healthy
// target reads as just its state and last change. The counters are
// consecutive streaks since the last health transition, not totals.
func targetCounterSignals(t bouineapi.StatusTarget) string {
	var parts []string
	if t.PassiveErrors > 0 {
		parts = append(parts, fmt.Sprintf("passive errors %d", t.PassiveErrors))
	}
	if t.ProbeErrors > 0 {
		parts = append(parts, fmt.Sprintf("probe errors %d", t.ProbeErrors))
	}
	if t.ProbeSuccesses > 0 {
		parts = append(parts, fmt.Sprintf("probe streak %d", t.ProbeSuccesses))
	}
	return strings.Join(parts, "  ")
}

// statusRelTime renders a report timestamp as a short human-relative
// string ("just now", "42m ago", "7h ago", "3d ago"; "never" when
// empty). It anchors to the server clock (StartedAt + UptimeSec, the
// same clock that stamped the event) instead of the client's wall
// clock, so a skewed or stepped client clock cannot distort the age
// of a server-side transition.
func statusRelTime(r *bouineapi.StatusReport, ts string) string {
	if ts == "" {
		return "never"
	}
	stamped, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		return ts
	}
	now := time.Now()
	if started, perr := time.Parse(time.RFC3339, r.Node.StartedAt); perr == nil {
		now = started.Add(time.Duration(r.Node.UptimeSec) * time.Second)
	}
	d := now.Sub(stamped)
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

func statusClusterSection(p tuiPalette, r *bouineapi.StatusReport, width int) string {
	var b strings.Builder
	b.WriteString(" CLUSTER\n")
	fmt.Fprintf(&b, "    peer-fetch %s hits / %s misses  avg %.1fms\n",
		humanCount(float64(r.Cluster.PeerFetch.Hits)),
		humanCount(float64(r.Cluster.PeerFetch.Misses)),
		r.Cluster.PeerFetch.AvgLatencyMs)
	if len(r.Cluster.Peers) == 0 {
		b.WriteString("    no peers (single node)\n")
		return b.String()
	}
	for _, peer := range r.Cluster.Peers {
		state := "live"
		style := p.healthy
		if peer.Stale {
			state = "stale"
			style = p.stale
		}
		line := fmt.Sprintf("    %s  %s  %s", peer.Name, style.Render(state), peer.Version)
		if peer.Requests > 0 {
			line = fmt.Sprintf("    %s  %s  %s  %s req  %s hit",
				peer.Name, style.Render(state), peer.Version,
				humanCount(float64(peer.Requests)), humanPct(hitPeerRatio(peer)))
		}
		b.WriteString(truncateLine(p, line, width))
		b.WriteString("\n")
	}
	return b.String()
}

// statusInsightsSection aligns the " - action" column: every head
// (severity + title) pads to the widest head so the dash column is
// straight across rows.
func statusInsightsSection(p tuiPalette, r *bouineapi.StatusReport, width int) string {
	if len(r.Insights) == 0 {
		return " INSIGHTS  none\n"
	}
	head := make([]string, len(r.Insights))
	maxw := 0
	for i, ins := range r.Insights {
		head[i] = fmt.Sprintf("%s %s",
			severityStyle(p, ins.Severity).Render(
				fmt.Sprintf("[%s]", strings.ToLower(ins.Severity))),
			ins.Title)
		if w := lipgloss.Width(head[i]); w > maxw {
			maxw = w
		}
	}
	var b strings.Builder
	b.WriteString(" INSIGHTS\n")
	for i, ins := range r.Insights {
		line := "   " + head[i]
		if ins.Action != "" {
			line += strings.Repeat(" ", maxw-lipgloss.Width(head[i])) + " - " + ins.Action
		}
		b.WriteString(truncateLine(p, line, width))
		b.WriteString("\n")
	}
	return b.String()
}

func severityStyle(p tuiPalette, severity string) lipgloss.Style {
	switch strings.ToLower(severity) {
	case "high":
		return p.high
	case "medium", "med":
		return p.med
	default:
		return p.low
	}
}

func hitPeerRatio(p bouineapi.StatusPeer) float64 {
	if p.Requests == 0 {
		return 0
	}
	return float64(p.Hits) / float64(p.Requests)
}

func truncateLine(p tuiPalette, line string, width int) string {
	if lipgloss.Width(line) <= width {
		return line
	}
	return p.dim.MaxWidth(width).Render(line)
}

func humanPct(frac float64) string {
	return fmt.Sprintf("%.1f%%", frac*100)
}

func humanCount(n float64) string {
	switch {
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", n/1_000_000)
	case n >= 10_000:
		return fmt.Sprintf("%.0fk", n/1_000)
	case n >= 1_000:
		return fmt.Sprintf("%.1fk", n/1_000)
	default:
		return fmt.Sprintf("%.0f", n)
	}
}
