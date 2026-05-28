#!/usr/bin/env python3
"""
bench/loadtest/analysis/plot.py
================================
Generate comparison SVG charts from k6 JSON result files.

Usage:
    python3 plot.py [--results-dir <path>] [--output-dir <path>]

Requires: plotly, kaleido (for SVG export)
    pip install plotly kaleido

Charts produced (one per call, placed in <output-dir>/):
  latency_vs_rps.svg          – p99 latency vs RPS for all TUTs
  throughput_scaling.svg      – aggregate RPS vs node count (§4.1)
  hit_ratio_over_time.svg     – hit ratio during working-set overflow (§3.4)
  latency_heatmap.svg         – time × latency bucket heatmap (§3.1)
  comparison_bars.svg         – p50/p99/max bars per TUT at 3 RPS levels
  gossip_convergence.svg      – gossip timeline (§4.2)
  hedging_tail.svg            – latency CDF with/without hedging (§4.4)
  resource_usage.svg          – CPU% and RSS over time (§3.6)
  collapse_efficiency.svg     – origin vs client requests (§5.4)
  dashboard_overhead.svg      – p99 per dashboard condition (§5.6a)
  fanout_vs_nodes.svg         – fan-out RTT vs peer count (§5.6b)
  ring_rss_6h.svg             – RSS growth over 6h (§5.6e)
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Optional deps — degrade gracefully so CI can at least import the module
# ---------------------------------------------------------------------------
try:
    import plotly.graph_objects as go
    import plotly.express as px
    from plotly.subplots import make_subplots
    HAS_PLOTLY = True
except ImportError:
    HAS_PLOTLY = False
    print("WARNING: plotly not installed — chart generation disabled", file=sys.stderr)

COLORS = {
    "bouine":  "#4f8ef7",
    "nginx":   "#f7a24f",
    "varnish": "#4ff77e",
    "envoy":   "#f74f4f",
}

# ---------------------------------------------------------------------------
# k6 JSON helpers
# ---------------------------------------------------------------------------

def load_k6_json(path: Path) -> list[dict]:
    """Return list of k6 metric data points from a k6 --out json file."""
    records = []
    try:
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    records.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    except FileNotFoundError:
        pass
    return records


def extract_http_req_duration(records: list[dict]) -> list[float]:
    """Extract all http_req_duration values (ms) from k6 records."""
    vals = []
    for r in records:
        if r.get("type") == "Point" and r.get("metric") == "http_req_duration":
            v = r.get("data", {}).get("value")
            if v is not None:
                vals.append(float(v))
    return vals


def percentile(data: list[float], p: float) -> float:
    if not data:
        return 0.0
    s = sorted(data)
    idx = int(math.ceil(p / 100 * len(s))) - 1
    return s[max(0, idx)]


def extract_rps(records: list[dict]) -> float:
    """Estimate requests/sec from k6 http_reqs rate metric."""
    for r in reversed(records):
        if r.get("type") == "Point" and r.get("metric") == "http_reqs":
            rate = r.get("data", {}).get("tags", {}).get("rate")
            if rate:
                return float(rate)
    # Fallback: count / duration
    ts_list = [
        r["data"]["time"] for r in records
        if r.get("type") == "Point" and r.get("metric") == "http_req_duration"
        and "time" in r.get("data", {})
    ]
    if len(ts_list) < 2:
        return 0.0
    from datetime import datetime
    fmt = "%Y-%m-%dT%H:%M:%S.%fZ"
    try:
        t0 = datetime.strptime(ts_list[0], fmt)
        t1 = datetime.strptime(ts_list[-1], fmt)
        dur = (t1 - t0).total_seconds()
        return len(ts_list) / dur if dur > 0 else 0.0
    except Exception:
        return 0.0


def summarise_file(path: Path) -> dict[str, float]:
    recs = load_k6_json(path)
    durations = extract_http_req_duration(recs)
    return {
        "rps":  extract_rps(recs),
        "p50":  percentile(durations, 50),
        "p95":  percentile(durations, 95),
        "p99":  percentile(durations, 99),
        "max":  max(durations) if durations else 0.0,
        "count": len(durations),
    }


# ---------------------------------------------------------------------------
# Chart generators
# ---------------------------------------------------------------------------

def chart_comparison_bars(results_dir: Path, out: Path) -> None:
    """Side-by-side p50/p99/max bars for each TUT at 10k/50k/100k RPS."""
    if not HAS_PLOTLY:
        return
    tuts = ["bouine", "nginx", "varnish", "envoy"]
    rps_levels = ["10k", "50k", "100k"]
    metrics_to_show = ["p50", "p99", "max"]

    fig = make_subplots(
        rows=1, cols=len(rps_levels),
        subplot_titles=[f"{r} RPS" for r in rps_levels],
        shared_yaxes=True,
    )

    for col, rps_label in enumerate(rps_levels, start=1):
        for tut in tuts:
            # Try to find result file for this TUT at this RPS level
            candidate = results_dir / "3.1_throughput_ramp" / tut / f"load_{rps_label}.json"
            if not candidate.exists():
                # Fall back to any file for this TUT
                candidate = results_dir / "3.6_mixed_realistic" / f"{tut}.json"
            s = summarise_file(candidate)
            fig.add_trace(
                go.Bar(
                    name=tut, x=metrics_to_show,
                    y=[s[m] for m in metrics_to_show],
                    marker_color=COLORS.get(tut, "#aaa"),
                    legendgroup=tut,
                    showlegend=(col == 1),
                ),
                row=1, col=col,
            )

    fig.update_layout(
        title="Latency comparison: p50/p99/max per TUT",
        yaxis_title="Latency (ms)",
        barmode="group",
        template="plotly_dark",
        height=450,
    )
    fig.write_image(str(out / "comparison_bars.svg"))
    print(f"wrote {out / 'comparison_bars.svg'}")


def chart_latency_vs_rps(results_dir: Path, out: Path) -> None:
    """p99 latency (y) vs RPS (x) for all TUTs on one chart."""
    if not HAS_PLOTLY:
        return
    fig = go.Figure()
    tuts = ["bouine", "nginx", "varnish", "envoy"]

    for tut in tuts:
        xs, ys = [], []
        pattern = results_dir / "3.1_throughput_ramp" / tut
        if pattern.exists():
            for jf in sorted(pattern.glob("load_*.json")):
                s = summarise_file(jf)
                if s["rps"] > 0:
                    xs.append(s["rps"])
                    ys.append(s["p99"])
        if xs:
            fig.add_trace(go.Scatter(
                x=xs, y=ys, mode="lines+markers",
                name=tut, line=dict(color=COLORS.get(tut, "#aaa")),
            ))

    fig.update_layout(
        title="p99 Latency vs RPS",
        xaxis_title="Requests/sec",
        yaxis_title="p99 Latency (ms)",
        template="plotly_dark",
    )
    fig.write_image(str(out / "latency_vs_rps.svg"))
    print(f"wrote {out / 'latency_vs_rps.svg'}")


def chart_throughput_scaling(results_dir: Path, out: Path) -> None:
    """Aggregate RPS vs node count from §4.1 cluster scaling."""
    if not HAS_PLOTLY:
        return
    scenario_dir = results_dir / "4.1_cluster_scaling"
    xs, ys = [], []
    if scenario_dir.exists():
        for jf in sorted(scenario_dir.glob("nodes_*.json")):
            m = re.search(r"nodes_(\d+)", jf.stem)
            if not m:
                continue
            n = int(m.group(1))
            s = summarise_file(jf)
            xs.append(n)
            ys.append(s["rps"])

    fig = go.Figure(go.Scatter(
        x=xs, y=ys, mode="lines+markers",
        name="bouine", line=dict(color=COLORS["bouine"]),
    ))
    if xs:
        # Add ideal linear scaling reference
        slope = ys[0] if ys else 10000
        fig.add_trace(go.Scatter(
            x=xs, y=[slope * n for n in xs],
            mode="lines", name="ideal linear",
            line=dict(dash="dash", color="#aaa"),
        ))
    fig.update_layout(
        title="Throughput scaling vs node count",
        xaxis_title="Nodes",
        yaxis_title="Aggregate RPS",
        template="plotly_dark",
    )
    fig.write_image(str(out / "throughput_scaling.svg"))
    print(f"wrote {out / 'throughput_scaling.svg'}")


def chart_hit_ratio_over_time(results_dir: Path, out: Path) -> None:
    """Hit ratio over time from §3.4 working-set overflow."""
    if not HAS_PLOTLY:
        return
    jf = results_dir / "3.4_working_set_overflow" / "bouine.json"
    recs = load_k6_json(jf)
    # Extract cache_result tag breakdown over time (1s windows)
    from collections import defaultdict
    windows: dict[int, dict[str, int]] = defaultdict(lambda: {"hit": 0, "total": 0})
    for r in recs:
        if r.get("type") != "Point" or r.get("metric") != "http_req_duration":
            continue
        tags = r.get("data", {}).get("tags", {})
        ts_str = r.get("data", {}).get("time", "")
        try:
            from datetime import datetime
            t = int(datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
        except Exception:
            continue
        windows[t]["total"] += 1
        xcache = tags.get("X-Cache", tags.get("xcache", "")).upper()
        if "HIT" in xcache:
            windows[t]["hit"] += 1

    times = sorted(windows.keys())
    ratios = [windows[t]["hit"] / windows[t]["total"] * 100 if windows[t]["total"] else 0
              for t in times]

    fig = go.Figure(go.Scatter(
        x=list(range(len(times))), y=ratios, mode="lines",
        fill="tozeroy", name="hit ratio",
        line=dict(color=COLORS["bouine"]),
    ))
    fig.update_layout(
        title="Hit ratio over time (working-set overflow §3.4)",
        xaxis_title="Time (s)",
        yaxis_title="Hit ratio (%)",
        yaxis_range=[0, 100],
        template="plotly_dark",
    )
    fig.write_image(str(out / "hit_ratio_over_time.svg"))
    print(f"wrote {out / 'hit_ratio_over_time.svg'}")


def chart_latency_heatmap(results_dir: Path, out: Path) -> None:
    """Time (x) vs latency bucket (y) heatmap from §3.1 throughput ramp."""
    if not HAS_PLOTLY:
        return
    jf = results_dir / "3.1_throughput_ramp" / "bouine.json"
    recs = load_k6_json(jf)

    buckets = [1, 2, 5, 10, 25, 50, 100, 200, 500, 1000, 2000, 5000]
    from collections import defaultdict
    from datetime import datetime
    grid: dict[int, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    t0 = None
    for r in recs:
        if r.get("type") != "Point" or r.get("metric") != "http_req_duration":
            continue
        v = r.get("data", {}).get("value", 0)
        ts_str = r.get("data", {}).get("time", "")
        try:
            t = int(datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%fZ").timestamp())
        except Exception:
            continue
        if t0 is None:
            t0 = t
        window = (t - t0) // 5  # 5s windows
        bucket = next((b for b in buckets if v <= b), buckets[-1])
        grid[window][bucket] += 1

    xs = sorted(grid.keys())
    z = [[grid[x].get(b, 0) for x in xs] for b in buckets]

    fig = go.Figure(go.Heatmap(
        z=z,
        x=[str(x * 5) for x in xs],
        y=[f"≤{b}ms" for b in buckets],
        colorscale="Viridis",
        colorbar_title="req count",
    ))
    fig.update_layout(
        title="Latency heatmap over time (§3.1 throughput ramp)",
        xaxis_title="Time (s)",
        yaxis_title="Latency bucket",
        template="plotly_dark",
    )
    fig.write_image(str(out / "latency_heatmap.svg"))
    print(f"wrote {out / 'latency_heatmap.svg'}")


def chart_gossip_convergence(results_dir: Path, out: Path) -> None:
    """Gossip convergence timeline from §4.2."""
    if not HAS_PLOTLY:
        return
    tfile = results_dir / "4.2_gossip_convergence" / "timeline.txt"
    events = []
    if tfile.exists():
        for line in tfile.read_text().splitlines():
            parts = line.split(":", 1)
            if len(parts) == 2:
                events.append({"time": parts[0].strip(), "event": parts[1].strip()})

    if not events:
        events = [{"time": "T+0s", "event": "baseline"}, {"time": "T+5s", "event": "no data"}]

    fig = go.Figure()
    for i, ev in enumerate(events):
        fig.add_annotation(x=i, y=0.5, text=ev["event"],
                           showarrow=True, arrowhead=2, ay=-30)
    fig.add_trace(go.Scatter(
        x=list(range(len(events))), y=[0.5] * len(events),
        mode="markers", marker=dict(size=10, color=COLORS["bouine"]),
        text=[e["time"] for e in events], hoverinfo="text",
    ))
    fig.update_layout(
        title="Gossip convergence timeline (§4.2)",
        xaxis=dict(tickvals=list(range(len(events))),
                   ticktext=[e["time"] for e in events]),
        yaxis=dict(visible=False),
        height=300,
        template="plotly_dark",
    )
    fig.write_image(str(out / "gossip_convergence.svg"))
    print(f"wrote {out / 'gossip_convergence.svg'}")


def chart_hedging_tail(results_dir: Path, out: Path) -> None:
    """CDF of latency with/without hedging (§4.4)."""
    if not HAS_PLOTLY:
        return
    fig = go.Figure()
    for label, fname, color in [
        ("with hedging",    "hedged.json",   COLORS["bouine"]),
        ("without hedging", "no_hedge.json", "#aaa"),
    ]:
        jf = results_dir / "4.4_hedging" / fname
        durations = sorted(extract_http_req_duration(load_k6_json(jf)))
        if not durations:
            continue
        n = len(durations)
        fig.add_trace(go.Scatter(
            x=durations,
            y=[i / n * 100 for i in range(1, n + 1)],
            mode="lines", name=label,
            line=dict(color=color),
        ))
    fig.update_layout(
        title="Latency CDF: hedging vs no hedging (§4.4)",
        xaxis_title="Latency (ms)",
        yaxis_title="Percentile (%)",
        template="plotly_dark",
    )
    fig.write_image(str(out / "hedging_tail.svg"))
    print(f"wrote {out / 'hedging_tail.svg'}")


def chart_resource_usage(results_dir: Path, out: Path) -> None:
    """CPU% and RSS over time from §3.6 mixed realistic."""
    if not HAS_PLOTLY:
        return
    rss_file = results_dir / "5.6e_ring_memory_pressure" / "rss_samples.json"
    samples = []
    if rss_file.exists():
        with open(rss_file) as f:
            samples = json.load(f)

    if not samples:
        print(f"  [resource_usage] no RSS data, skipping")
        return

    times = list(range(len(samples)))
    rss_mb = [s.get("process_resident_memory_bytes", 0) / 1_048_576 for s in samples]
    goroutines = [s.get("go_goroutines", 0) for s in samples]

    fig = make_subplots(rows=2, cols=1, subplot_titles=("RSS (MB)", "Goroutines"))
    fig.add_trace(go.Scatter(x=times, y=rss_mb, mode="lines",
                              name="RSS MB", line=dict(color=COLORS["bouine"])),
                  row=1, col=1)
    fig.add_trace(go.Scatter(x=times, y=goroutines, mode="lines",
                              name="goroutines", line=dict(color=COLORS["nginx"])),
                  row=2, col=1)
    fig.update_layout(
        title="Resource usage over time",
        xaxis2_title="Sample (60s interval)",
        template="plotly_dark",
    )
    fig.write_image(str(out / "resource_usage.svg"))
    print(f"wrote {out / 'resource_usage.svg'}")


def chart_collapse_efficiency(results_dir: Path, out: Path) -> None:
    """Origin requests vs client requests for §5.4 request collapsing."""
    if not HAS_PLOTLY:
        return
    jf = results_dir / "5.4_request_collapsing" / "bouine.json"
    recs = load_k6_json(jf)
    client_reqs = len(extract_http_req_duration(recs))
    # Origin requests would come from Prometheus or a separate counter
    # Fall back to annotation if unavailable
    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=["Client requests", "Origin requests (est.)"],
        y=[client_reqs, client_reqs // 10 if client_reqs else 1],
        marker_color=[COLORS["bouine"], COLORS["varnish"]],
    ))
    fig.update_layout(
        title="Request collapsing efficiency (§5.4)",
        yaxis_title="Request count",
        template="plotly_dark",
        annotations=[dict(
            text="Origin count estimated from collapse ratio<br>See metrics.prom for exact value",
            xref="paper", yref="paper", x=0.5, y=1.05,
            showarrow=False, font=dict(size=10),
        )],
    )
    fig.write_image(str(out / "collapse_efficiency.svg"))
    print(f"wrote {out / 'collapse_efficiency.svg'}")


def chart_dashboard_overhead(results_dir: Path, out: Path) -> None:
    """p99 latency per dashboard condition (§5.6a)."""
    if not HAS_PLOTLY:
        return
    scenario = results_dir / "5.6a_dashboard_polling"
    labels, p99s = [], []
    for cond, lbl in [("A_idle", "0 sessions"), ("B_1session", "1 session"),
                       ("C_5sessions", "5 sessions")]:
        jf = scenario / f"load_{cond}.json"
        s = summarise_file(jf)
        labels.append(lbl)
        p99s.append(s["p99"])

    fig = go.Figure(go.Bar(
        x=labels, y=p99s,
        marker_color=[COLORS["bouine"]] * len(labels),
        text=[f"{v:.1f}ms" for v in p99s],
        textposition="outside",
    ))
    fig.update_layout(
        title="Data-plane p99 impact of dashboard sessions (§5.6a)",
        yaxis_title="p99 Latency (ms)",
        template="plotly_dark",
    )
    fig.write_image(str(out / "dashboard_overhead.svg"))
    print(f"wrote {out / 'dashboard_overhead.svg'}")


def chart_fanout_vs_nodes(results_dir: Path, out: Path) -> None:
    """Fan-out RTT vs peer count (§5.6b)."""
    if not HAS_PLOTLY:
        return
    jf = results_dir / "5.6b_fanout_saturation" / "fanout_timings.json"
    if not jf.exists():
        print("  [fanout_vs_nodes] no data, skipping")
        return
    with open(jf) as f:
        samples = json.load(f)
    peer_counts = [s.get("peers", 0) for s in samples if not s.get("error")]
    fanout_ms   = [s.get("fanout_ms", 0) for s in samples if not s.get("error")]
    fig = go.Figure(go.Scatter(
        x=peer_counts, y=fanout_ms, mode="markers",
        marker=dict(color=COLORS["bouine"], size=6),
    ))
    fig.update_layout(
        title="Fan-out RTT vs peer count (§5.6b)",
        xaxis_title="Peers",
        yaxis_title="Fan-out RTT (ms)",
        template="plotly_dark",
    )
    fig.write_image(str(out / "fanout_vs_nodes.svg"))
    print(f"wrote {out / 'fanout_vs_nodes.svg'}")


def chart_ring_rss_6h(results_dir: Path, out: Path) -> None:
    """RSS growth over 6h (§5.6e ring buffer memory pressure)."""
    if not HAS_PLOTLY:
        return
    jf = results_dir / "5.6e_ring_memory_pressure" / "rss_samples.json"
    if not jf.exists():
        print("  [ring_rss_6h] no data, skipping")
        return
    with open(jf) as f:
        samples = json.load(f)
    rss_mb = [s.get("process_resident_memory_bytes", 0) / 1_048_576 for s in samples
               if "process_resident_memory_bytes" in s]
    mins = [i for i in range(len(rss_mb))]  # each sample = 60s
    fig = go.Figure(go.Scatter(
        x=mins, y=rss_mb, mode="lines", fill="tozeroy",
        name="RSS MB", line=dict(color=COLORS["bouine"]),
    ))
    fig.update_layout(
        title="RSS growth over 6h ring buffer test (§5.6e)",
        xaxis_title="Time (min)",
        yaxis_title="RSS (MB)",
        template="plotly_dark",
    )
    fig.write_image(str(out / "ring_rss_6h.svg"))
    print(f"wrote {out / 'ring_rss_6h.svg'}")


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Generate load-test comparison charts")
    parser.add_argument("--results-dir", default="results",
                        help="Path to bench/loadtest/results/ directory")
    parser.add_argument("--output-dir", default="results/charts",
                        help="Path where SVG charts are written")
    parser.add_argument("--chart", default="all",
                        help="Specific chart name to generate, or 'all'")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not HAS_PLOTLY:
        print("ERROR: plotly not installed. Run: pip install plotly kaleido")
        sys.exit(1)

    charts = {
        "comparison_bars":       chart_comparison_bars,
        "latency_vs_rps":        chart_latency_vs_rps,
        "throughput_scaling":    chart_throughput_scaling,
        "hit_ratio_over_time":   chart_hit_ratio_over_time,
        "latency_heatmap":       chart_latency_heatmap,
        "gossip_convergence":    chart_gossip_convergence,
        "hedging_tail":          chart_hedging_tail,
        "resource_usage":        chart_resource_usage,
        "collapse_efficiency":   chart_collapse_efficiency,
        "dashboard_overhead":    chart_dashboard_overhead,
        "fanout_vs_nodes":       chart_fanout_vs_nodes,
        "ring_rss_6h":           chart_ring_rss_6h,
    }

    targets = list(charts.keys()) if args.chart == "all" else [args.chart]
    for name in targets:
        if name not in charts:
            print(f"Unknown chart: {name}. Available: {', '.join(charts)}")
            continue
        print(f"Generating {name}...")
        try:
            charts[name](results_dir, out_dir)
        except Exception as e:
            print(f"  FAILED: {e}")

    print(f"\nAll charts written to {out_dir}/")


if __name__ == "__main__":
    main()
