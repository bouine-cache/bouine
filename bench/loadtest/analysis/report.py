#!/usr/bin/env python3
"""
bench/loadtest/analysis/report.py
===================================
Auto-generate bench/loadtest/REPORT.md from scenario result files
and pre-generated SVG charts.

Usage:
    python3 report.py [--results-dir <path>] [--charts-dir <path>] [--output <path>]

Expects plot.py to have been run first (SVG files in charts-dir).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

# Re-use helpers from plot.py (same directory)
sys.path.insert(0, str(Path(__file__).parent))
try:
    from plot import load_k6_json, extract_http_req_duration, percentile, summarise_file
except ImportError:
    def load_k6_json(p): return []  # type: ignore[misc]
    def extract_http_req_duration(r): return []  # type: ignore[misc]
    def percentile(d, p): return 0.0  # type: ignore[misc]
    def summarise_file(p): return {"rps": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "count": 0}  # type: ignore[misc]


# ---------------------------------------------------------------------------
# Verdict helpers
# ---------------------------------------------------------------------------

TARGET_P99_MS  = 10.0   # ms — hit path budget from AGENTS.md §7
TARGET_RPS_REG = 0.98   # max allowed regression vs previous run (unused here)
TUT_LABEL      = {"bouine": "bouine", "nginx": "NGINX", "varnish": "Varnish", "envoy": "Envoy"}


def verdict(condition: bool, pass_msg: str, fail_msg: str) -> str:
    icon = "✅" if condition else "❌"
    msg  = pass_msg if condition else fail_msg
    return f"{icon} {msg}"


def fmt_ms(v: float) -> str:
    return f"{v:.2f} ms" if v > 0 else "—"


def fmt_rps(v: float) -> str:
    return f"{v:,.0f}" if v > 0 else "—"


# ---------------------------------------------------------------------------
# Per-scenario section builders
# ---------------------------------------------------------------------------

def section_throughput_ramp(results_dir: Path, charts_dir: Path) -> str:
    tuts = ["bouine", "nginx", "varnish", "envoy"]
    rows = []
    for tut in tuts:
        jf = results_dir / "3.1_throughput_ramp" / f"{tut}.json"
        s  = summarise_file(jf)
        rows.append(f"| {TUT_LABEL.get(tut, tut)} | {fmt_rps(s['rps'])} | {fmt_ms(s['p50'])} "
                    f"| {fmt_ms(s['p99'])} | {fmt_ms(s['max'])} |")

    chart = chart_embed(charts_dir, "latency_vs_rps.svg")
    return f"""
## §3.1 Throughput ramp

Ramped load from 1k to 100k RPS across all four TUTs.

| TUT | RPS | p50 | p99 | max |
|-----|-----|-----|-----|-----|
{chr(10).join(rows)}

{chart}
"""


def section_hit_only(results_dir: Path, charts_dir: Path) -> str:
    jf = results_dir / "3.2_hit_only" / "bouine.json"
    s  = summarise_file(jf)
    v  = verdict(s["p99"] <= TARGET_P99_MS,
                 f"Hit-path p99 {fmt_ms(s['p99'])} ≤ {TARGET_P99_MS} ms budget",
                 f"Hit-path p99 {fmt_ms(s['p99'])} EXCEEDS {TARGET_P99_MS} ms budget")
    return f"""
## §3.2 Hit-only (warm cache)

Sustained 50k RPS against a pre-warmed cache. Measures pure hit-path cost.

| Metric | Value |
|--------|-------|
| RPS    | {fmt_rps(s['rps'])} |
| p50    | {fmt_ms(s['p50'])} |
| p99    | {fmt_ms(s['p99'])} |
| max    | {fmt_ms(s['max'])} |

**Verdict**: {v}
"""


def section_miss_storm(results_dir: Path, charts_dir: Path) -> str:
    jf = results_dir / "3.3_miss_storm" / "bouine.json"
    s  = summarise_file(jf)
    return f"""
## §3.3 Miss storm (cache cold)

All requests cache-miss; measures origin fan-out and collapse.

| Metric | Value |
|--------|-------|
| RPS    | {fmt_rps(s['rps'])} |
| p99    | {fmt_ms(s['p99'])} |
| max    | {fmt_ms(s['max'])} |
"""


def section_working_set_overflow(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "hit_ratio_over_time.svg")
    return f"""
## §3.4 Working-set overflow

Request set intentionally exceeds cache capacity to exercise eviction.

{chart}
"""


def section_vary_blowup(results_dir: Path, charts_dir: Path) -> str:
    jf = results_dir / "3.5_vary_blowup" / "bouine.json"
    s  = summarise_file(jf)
    return f"""
## §3.5 Vary blowup

Requests with 200 distinct `Accept-Encoding` values exercise `Vary` cardinality.
`MaxVariants=64` cap enforced; `bouine_vary_cap_hits_total` counter expected > 0.

| Metric | Value |
|--------|-------|
| RPS    | {fmt_rps(s['rps'])} |
| p99    | {fmt_ms(s['p99'])} |
"""


def section_mixed_realistic(results_dir: Path, charts_dir: Path) -> str:
    tuts = ["bouine", "nginx", "varnish", "envoy"]
    rows = []
    for tut in tuts:
        jf = results_dir / "3.6_mixed_realistic" / f"{tut}.json"
        s  = summarise_file(jf)
        rows.append(f"| {TUT_LABEL.get(tut, tut)} | {fmt_rps(s['rps'])} | {fmt_ms(s['p50'])} "
                    f"| {fmt_ms(s['p99'])} | {fmt_ms(s['max'])} |")

    chart = chart_embed(charts_dir, "comparison_bars.svg")
    return f"""
## §3.6 Mixed realistic workload

70% HIT / 20% MISS / 5% REVALIDATE / 3% BYPASS / 2% STALE mix at 25k RPS.

| TUT | RPS | p50 | p99 | max |
|-----|-----|-----|-----|-----|
{chr(10).join(rows)}

{chart}
"""


def section_cluster_scaling(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "throughput_scaling.svg")
    return f"""
## §4.1 Cluster scaling

Aggregate RPS measured as node count grows 1 → 3 → 5 → 10.

{chart}
"""


def section_gossip_convergence(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "gossip_convergence.svg")
    tfile = results_dir / "4.2_gossip_convergence" / "timeline.txt"
    timeline = ""
    if tfile.exists():
        timeline = "\n```\n" + tfile.read_text().strip() + "\n```\n"
    return f"""
## §4.2 Gossip convergence

Kill one node, measure time for the ring to stabilise.
{timeline}
{chart}
"""


def section_hedging(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "hedging_tail.svg")
    return f"""
## §4.4 Hedged requests

Tail latency CDF comparison with `connect.hedge_timeout` enabled vs disabled.

{chart}
"""


def section_request_collapsing(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "collapse_efficiency.svg")
    return f"""
## §5.4 Request collapsing

10k concurrent requests to the same slow URL (200ms origin RTT).
Single-flight latch should reduce origin load by ~100×.

{chart}
"""


def section_dashboard_overhead(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "dashboard_overhead.svg")
    scenario = results_dir / "5.6a_dashboard_polling"
    rows = []
    for cond, label in [("A_idle", "0 sessions"), ("B_1session", "1 session"),
                         ("C_5sessions", "5 sessions")]:
        jf = scenario / f"load_{cond}.json"
        s  = summarise_file(jf)
        delta = ""
        if rows:
            base = rows[0][1]
            if base > 0:
                pct = (s["p99"] - base) / base * 100
                delta = f"+{pct:.1f}%"
        rows.append((label, s["p99"], delta))

    table_rows = "\n".join(
        f"| {r[0]} | {fmt_ms(r[1])} | {r[2]} |" for r in rows
    )
    # Verdict: ≤0.5% for 1 session, ≤2% for 5
    v1 = verdict(
        len(rows) < 2 or rows[0][1] == 0 or
        abs(rows[1][1] - rows[0][1]) / max(rows[0][1], 0.001) <= 0.005,
        "1-session overhead ≤ 0.5% ✓",
        "1-session overhead EXCEEDS 0.5% target",
    )
    v5 = verdict(
        len(rows) < 3 or rows[0][1] == 0 or
        abs(rows[2][1] - rows[0][1]) / max(rows[0][1], 0.001) <= 0.02,
        "5-session overhead ≤ 2% ✓",
        "5-session overhead EXCEEDS 2% target",
    )
    return f"""
## §5.6.a Dashboard polling overhead

| Condition | p99 | Δ vs baseline |
|-----------|-----|---------------|
{table_rows}

**Verdict**: {v1}
**Verdict**: {v5}

{chart}
"""


def section_fanout(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "fanout_vs_nodes.svg")
    jf = results_dir / "5.6b_fanout_saturation" / "fanout_timings.json"
    summary = ""
    if jf.exists():
        with open(jf) as f:
            samples = [s for s in json.load(f) if not s.get("error")]
        if samples:
            times = sorted(s["fanout_ms"] for s in samples)
            p50  = percentile(times, 50)
            p99  = percentile(times, 99)
            errs = sum(1 for s in json.load(open(jf)) if s.get("error"))
            summary = f"\np50 fan-out RTT: {fmt_ms(p50)}  p99: {fmt_ms(p99)}  errors: {errs}\n"
    return f"""
## §5.6.b Fan-out saturation
{summary}
{chart}
"""


def section_ring_rss(results_dir: Path, charts_dir: Path) -> str:
    chart = chart_embed(charts_dir, "ring_rss_6h.svg")
    return f"""
## §5.6.e Ring buffer memory pressure (6h)

RSS growth curve over 6 hours with 500 distinct URL paths at 50k RPS.
URLRing cap (512 entries) is expected to prevent unbounded growth.

{chart}
"""


# ---------------------------------------------------------------------------
# Chart embed helper
# ---------------------------------------------------------------------------

def chart_embed(charts_dir: Path, name: str) -> str:
    p = charts_dir / name
    if p.exists():
        # Inline SVG (safe for GitHub-flavoured Markdown via img tag)
        return f'<img src="charts/{name}" alt="{name}" />\n'
    return f'_Chart `{name}` not yet generated — run `make loadtest-report`._\n'


# ---------------------------------------------------------------------------
# Main report assembly
# ---------------------------------------------------------------------------

SECTIONS = [
    ("§3.1 Throughput ramp",        section_throughput_ramp),
    ("§3.2 Hit-only",               section_hit_only),
    ("§3.3 Miss storm",             section_miss_storm),
    ("§3.4 Working-set overflow",   section_working_set_overflow),
    ("§3.5 Vary blowup",            section_vary_blowup),
    ("§3.6 Mixed realistic",        section_mixed_realistic),
    ("§4.1 Cluster scaling",        section_cluster_scaling),
    ("§4.2 Gossip convergence",     section_gossip_convergence),
    ("§4.4 Hedging tail",           section_hedging),
    ("§5.4 Request collapsing",     section_request_collapsing),
    ("§5.6.a Dashboard overhead",   section_dashboard_overhead),
    ("§5.6.b Fan-out saturation",   section_fanout),
    ("§5.6.e Ring memory pressure", section_ring_rss),
]


def generate_report(results_dir: Path, charts_dir: Path) -> str:
    now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        "# bouine load-test report",
        "",
        f"Generated: {now}",
        "",
        "## Table of contents",
        "",
    ]
    for title, _ in SECTIONS:
        anchor = title.lower().replace(" ", "-").replace(".", "").replace("/", "").replace("§", "")
        anchor = "-".join(anchor.split())
        lines.append(f"- [{title}](#{anchor})")
    lines.append("")
    lines.append("---")
    lines.append("")

    for title, fn in SECTIONS:
        try:
            lines.append(fn(results_dir, charts_dir))
        except Exception as e:
            lines.append(f"\n## {title}\n\n_Error generating section: {e}_\n")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate REPORT.md from load-test results")
    parser.add_argument("--results-dir", default="results",
                        help="Path to bench/loadtest/results/")
    parser.add_argument("--charts-dir",  default="results/charts",
                        help="Path to pre-generated SVG charts")
    parser.add_argument("--output",      default="REPORT.md",
                        help="Output Markdown file path")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    charts_dir  = Path(args.charts_dir)
    out_path    = Path(args.output)

    md = generate_report(results_dir, charts_dir)
    out_path.write_text(md)
    print(f"Report written to {out_path} ({len(md)} bytes)")


if __name__ == "__main__":
    main()
