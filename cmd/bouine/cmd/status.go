package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/charmbracelet/lipgloss"
	"github.com/spf13/cobra"

	"github.com/bouine-cache/bouine/pkg/bouineapi"
)

func newStatusCmd() *cobra.Command {
	var cluster, watch bool
	var interval time.Duration

	c := &cobra.Command{
		Use:   "status",
		Short: "Node overview: cache, origins, cluster, insights",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if interval <= 0 {
				return fmt.Errorf("--interval must be positive, got %s", interval)
			}
			// Ctrl-C must exit the watch loop cleanly: cancel the
			// command context on SIGINT so the in-flight fetch aborts
			// and no partial frame is written.
			ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt)
			defer stop()
			return runStatus(ctx, cmd, cluster, watch, interval)
		},
	}
	c.Flags().BoolVar(&cluster, "cluster", false,
		"aggregate peer stats (one extra peer-metrics round-trip)")
	c.Flags().BoolVar(&watch, "watch", false,
		"keep rendering until interrupted")
	c.Flags().DurationVar(&interval, "interval", 2*time.Second,
		"watch poll interval")
	return c
}

// runStatus fetches one status document and renders it; --watch loops
// on a ticker. Each frame is rendered whole (buffered) so an
// interrupted frame never leaves partial output.
func runStatus(ctx context.Context, cmd *cobra.Command, cluster, watch bool, interval time.Duration) error {
	format, err := outputFormat(cmd)
	if err != nil {
		return err
	}
	client := adminClient(cmd)
	// Terminal routing (issue #813): on a TTY, a one-shot prints the
	// styled frame inline and --watch runs the fullscreen alternate-
	// screen TUI. Piped output and --format json keep the line-based
	// rendering so scripts never see escape sequences.
	if format == formatText && statusIsTTY(os.Stdout.Fd()) {
		if watch {
			return runStatusTUI(ctx, client, cluster, interval)
		}
		report, err := client.Status(ctx, cluster)
		if err != nil {
			return err
		}
		return renderInlineFrame(cmd.OutOrStdout(), report, inlineWidth())
	}
	render := func() error {
		report, err := client.Status(ctx, cluster)
		if err != nil {
			return err
		}
		if format == formatJSON {
			out, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				return fmt.Errorf("marshal status: %w", err)
			}
			_, err = fmt.Fprintln(cmd.OutOrStdout(), string(out))
			return err
		}
		var b strings.Builder
		renderStatusText(&b, report)
		_, err = fmt.Fprint(cmd.OutOrStdout(), b.String())
		return err
	}
	if !watch {
		return render()
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	if err := render(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := render(); err != nil {
				return err
			}
		}
	}
}

// renderStatusText writes the five sections (node, cache, origins,
// cluster, insights) as plain text. Split from renderStatusTier and
// friends so each stays under the 80-line function budget.
func renderStatusText(b *strings.Builder, r *bouineapi.StatusReport) {
	fmt.Fprintf(b, "node     %s  version %s  uptime %s",
		r.Node.Name, r.Node.Version, formatUptime(r.Node.UptimeSec))
	if !r.Node.Ready {
		b.WriteString("  NOT-READY")
	}
	b.WriteString("\n")

	fmt.Fprintf(b, "cache\n    hit ratio %.1f%%  (%d hits / %d misses)  evictions %d\n",
		r.Cache.HitRatio*100, r.Cache.Hits, r.Cache.Misses, r.Cache.Evictions)
	renderTierLine(b, "hot", r.Cache.Hot)
	renderTierLine(b, "warm", r.Cache.Warm)

	if len(r.Origins) > 0 {
		b.WriteString("origins\n")
		for _, pool := range r.Origins {
			for _, t := range pool.Targets {
				state := "healthy"
				if !t.Healthy {
					state = "SICK"
				}
				fmt.Fprintf(b, "    %s/%s  %s  %s\n",
					pool.Name, t.Addr, state, targetSignals(r, t))
			}
		}
	}

	b.WriteString("cluster\n")
	fmt.Fprintf(b, "    peer-fetch %d hits / %d misses  avg %.1fms\n",
		r.Cluster.PeerFetch.Hits, r.Cluster.PeerFetch.Misses, r.Cluster.PeerFetch.AvgLatencyMs)
	if len(r.Cluster.Peers) > 0 {
		for _, p := range r.Cluster.Peers {
			state := "live"
			if p.Stale {
				state = "stale"
			}
			line := fmt.Sprintf("    %s  %s  %s", p.Name, state, p.Version)
			if p.Requests > 0 {
				line = fmt.Sprintf("    %s  %s  %s  %d req  %.1f%% hit",
					p.Name, state, p.Version, p.Requests, hitPct(p))
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
	} else {
		b.WriteString("    no peers (single node)\n")
	}

	if len(r.Insights) > 0 {
		b.WriteString("insights\n")
		head := make([]string, len(r.Insights))
		maxw := 0
		for i, ins := range r.Insights {
			head[i] = fmt.Sprintf("[%s] %s", strings.ToLower(ins.Severity), ins.Title)
			if w := lipgloss.Width(head[i]); w > maxw {
				maxw = w
			}
		}
		for i, ins := range r.Insights {
			line := "  " + head[i]
			if ins.Action != "" {
				line += strings.Repeat(" ", maxw-lipgloss.Width(head[i])) + " - " + ins.Action
			}
			b.WriteString(line)
			b.WriteString("\n")
		}
	} else {
		b.WriteString("insights none\n")
	}
}

func renderTierLine(b *strings.Builder, name string, t bouineapi.StatusTier) {
	line := fmt.Sprintf("    %s: %d entries, %s", name, t.Entries, humanBytes(t.Bytes))
	if t.MaxBytes > 0 {
		line += fmt.Sprintf(" / %s (%.1f%%)", humanBytes(t.MaxBytes), float64(t.Bytes)/float64(t.MaxBytes)*100)
	}
	if t.DiskBytes > 0 {
		line += fmt.Sprintf("  disk %s", humanBytes(t.DiskBytes))
	}
	b.WriteString(line + "\n")
}

func hitPct(p bouineapi.StatusPeer) float64 {
	if p.Requests == 0 {
		return 0
	}
	return float64(p.Hits) / float64(p.Requests) * 100
}

func formatUptime(sec int64) string {
	d := time.Duration(sec) * time.Second
	if d < time.Minute {
		return d.Truncate(time.Second).String()
	}
	if d < time.Hour {
		return d.Truncate(time.Minute).String()
	}
	return d.Truncate(time.Hour).String()
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%ciB", float64(n)/float64(div), "KMGTPE"[exp])
}
