#!/usr/bin/env python3
"""
bench/loadtest/analysis/visualize.py
======================================
Generate a self-contained HTML report from k6 --summary-export JSON files.
Opens in any browser with no Python dependencies beyond stdlib.

Usage (from repo root):
    python3 bench/loadtest/analysis/visualize.py \
        --results-dir bench/loadtest/results \
        --output      bench/loadtest/results/report.html

Then open bench/loadtest/results/report.html in a browser.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from pathlib import Path

# ─── helpers ──────────────────────────────────────────────────────────────────

TUTS   = ["bouine", "nginx", "varnish", "envoy"]
COLORS = {
    "bouine":  "#4f8ef7",
    "nginx":   "#f7a24f",
    "varnish": "#4ff77e",
    "envoy":   "#f74f4f",
}

ROUNDS = [
    ("baseline",  "",        "Baseline (pre-fixes)"),
    ("round1",    "after_",  "Round 1 – race fix + age pool"),
    ("round2",    "new_",    "Round 2 – ResponseWriter pool, route label, ReverseProxy"),
    ("round3",    "v3_",     "Round 3 – CC cache, OriginAge, SWR bg refresh"),
]

SCENARIOS = [
    ("3.2", "§3.2 Hit-only (warm cache, 3k RPS)"),
    ("3.3", "§3.3 Miss storm (no-store, 1.5k RPS)"),
    ("3.6", "§3.6 Mixed realistic (60/15/…, 3k RPS)"),
]


def load_summary(path: Path) -> dict | None:
    if not path.exists():
        return None
    try:
        with open(path) as f:
            d = json.load(f)
        m = d.get("metrics", {})
        dur = m.get("http_req_duration", {})
        reqs = m.get("http_reqs", {})
        failed = m.get("http_req_failed", {})
        hit_m = m.get("hit_rate", m.get("cache_hit_rate", {}))
        count = reqs.get("count", 0)
        duration_s = 30  # all tests ran for 30 s
        return {
            "rps":      count / duration_s if count else 0,
            "avg":      dur.get("avg", 0) * 1000,
            "med":      dur.get("med", 0) * 1000,
            "p90":      dur.get("p(90)", 0) * 1000,
            "p95":      dur.get("p(95)", 0) * 1000,
            "max":      dur.get("max", 0) * 1000,
            "fail_pct": failed.get("value", 0) * 100,
            "hit_pct":  hit_m.get("value", 0) * 100,
            "count":    count,
        }
    except Exception:
        return None


def collect_all(results_dir: Path) -> dict:
    """Return nested dict: data[scenario][tut][round_id] = summary | None"""
    data: dict = {}
    for sc_id, _ in SCENARIOS:
        data[sc_id] = {}
        for tut in TUTS:
            data[sc_id][tut] = {}
            for round_id, prefix, _ in ROUNDS:
                fname = results_dir / f"{prefix}{sc_id}_{tut}.json"
                data[sc_id][tut][round_id] = load_summary(fname)
    return data


def safe(v: float | None, decimals: int = 0) -> str:
    if v is None:
        return "—"
    return f"{v:.{decimals}f}"


# ─── HTML generation ──────────────────────────────────────────────────────────

CHART_JS = "https://cdn.jsdelivr.net/npm/chart.js@4/dist/chart.umd.min.js"


def build_html(data: dict, results_dir: Path) -> str:
    charts_js = []   # list of JS chart definitions
    sections_html = []

    # ── §1: bouine progression over rounds (per scenario) ────────────────────
    for sc_id, sc_label in SCENARIOS:
        round_labels = [label for _, _, label in ROUNDS]
        bouine_avg  = []
        bouine_p95  = []
        nginx_avg   = []
        varnish_avg = []

        for round_id, _, _ in ROUNDS:
            b = data[sc_id]["bouine"].get(round_id)
            n = data[sc_id]["nginx"].get(round_id)
            v = data[sc_id]["varnish"].get(round_id)
            bouine_avg.append(round(b["avg"],  1) if b else None)
            bouine_p95.append(round(b["p95"],  1) if b else None)
            nginx_avg.append( round(n["avg"],  1) if n else None)
            varnish_avg.append(round(v["avg"], 1) if v else None)

        cid = f"chart_prog_{sc_id.replace('.','_')}"
        charts_js.append(f"""
  new Chart(document.getElementById('{cid}'), {{
    type: 'bar',
    data: {{
      labels: {json.dumps(round_labels)},
      datasets: [
        {{ label: 'bouine avg',    data: {bouine_avg},   backgroundColor: '{COLORS["bouine"]}', borderRadius: 4 }},
        {{ label: 'bouine p95',    data: {bouine_p95},   backgroundColor: '{COLORS["bouine"]}88', borderRadius: 4 }},
        {{ label: 'nginx avg',     data: {nginx_avg},    backgroundColor: '{COLORS["nginx"]}', borderRadius: 4 }},
        {{ label: 'varnish avg',   data: {varnish_avg},  backgroundColor: '{COLORS["varnish"]}', borderRadius: 4 }},
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ title: {{ display: true, text: '{sc_label} — latency across optimisation rounds (ms)', color: '#ccc', font: {{ size: 14 }} }},
                  legend: {{ labels: {{ color: '#ccc' }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#aaa', maxRotation: 30 }}, grid: {{ color: '#333' }} }},
        y: {{ title: {{ display: true, text: 'µs (ms × 1000 shown as ms)', color: '#aaa' }},
              ticks: {{ color: '#aaa', callback: v => v + ' ms' }}, grid: {{ color: '#333' }} }}
      }}
    }}
  }});""")
        sections_html.append(f"""
  <section>
    <h2>{sc_label}</h2>
    <div class="chart-wrap"><canvas id="{cid}"></canvas></div>
  </section>""")

    # ── §2: TUT comparison in latest round (all scenarios) ───────────────────
    for sc_id, sc_label in SCENARIOS:
        latest_round = ROUNDS[-1][0]
        tut_labels   = TUTS
        avgs  = []
        p95s  = []
        for tut in TUTS:
            s = data[sc_id][tut].get(latest_round)
            avgs.append( round(s["avg"], 1) if s else None)
            p95s.append( round(s["p95"], 1) if s else None)
        colors_bg  = [COLORS[t] for t in TUTS]
        colors_p95 = [COLORS[t] + "88" for t in TUTS]

        cid = f"chart_tut_{sc_id.replace('.','_')}"
        charts_js.append(f"""
  new Chart(document.getElementById('{cid}'), {{
    type: 'bar',
    data: {{
      labels: {json.dumps(tut_labels)},
      datasets: [
        {{ label: 'avg latency',  data: {avgs},  backgroundColor: {json.dumps(colors_bg)},  borderRadius: 4 }},
        {{ label: 'p95 latency',  data: {p95s},  backgroundColor: {json.dumps(colors_p95)}, borderRadius: 4 }},
      ]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ title: {{ display: true, text: '{sc_label} — TUT comparison (latest round, ms)', color: '#ccc', font: {{ size: 14 }} }},
                  legend: {{ labels: {{ color: '#ccc' }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#aaa' }}, grid: {{ color: '#333' }} }},
        y: {{ ticks: {{ color: '#aaa', callback: v => v + ' ms' }}, grid: {{ color: '#333' }} }}
      }}
    }}
  }});""")
        sections_html.append(f"""
  <section>
    <h2>{sc_label} — TUT comparison (latest round)</h2>
    <div class="chart-wrap"><canvas id="{cid}"></canvas></div>
  </section>""")

    # ── §3: bouine hit rate across rounds for mixed workload ─────────────────
    sc_id = "3.6"
    sc_label = "§3.6 Mixed — bouine cache hit rate across optimisation rounds"
    hit_rates = []
    for round_id, _, _ in ROUNDS:
        b = data[sc_id]["bouine"].get(round_id)
        hit_rates.append(round(b["hit_pct"], 1) if b else None)
    round_labels = [label for _, _, label in ROUNDS]

    cid = "chart_hitrate"
    charts_js.append(f"""
  new Chart(document.getElementById('{cid}'), {{
    type: 'line',
    data: {{
      labels: {json.dumps(round_labels)},
      datasets: [{{
        label: 'bouine hit rate (%)',
        data: {hit_rates},
        borderColor: '{COLORS["bouine"]}',
        backgroundColor: '{COLORS["bouine"]}33',
        fill: true, tension: 0.3, pointRadius: 6,
        pointBackgroundColor: '{COLORS["bouine"]}'
      }}]
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ title: {{ display: true, text: '{sc_label}', color: '#ccc', font: {{ size: 14 }} }},
                  legend: {{ labels: {{ color: '#ccc' }} }} }},
      scales: {{
        x: {{ ticks: {{ color: '#aaa', maxRotation: 30 }}, grid: {{ color: '#333' }} }},
        y: {{ min: 0, max: 100, ticks: {{ color: '#aaa', callback: v => v + '%' }}, grid: {{ color: '#333' }} }}
      }}
    }}
  }});""")
    sections_html.append(f"""
  <section>
    <h2>{sc_label}</h2>
    <div class="chart-wrap"><canvas id="{cid}"></canvas></div>
  </section>""")

    # ── §4: full data table ───────────────────────────────────────────────────
    table_rows = []
    for sc_id, sc_label in SCENARIOS:
        for tut in TUTS:
            for round_id, _, round_label in ROUNDS:
                s = data[sc_id][tut].get(round_id)
                if s is None:
                    continue
                table_rows.append(
                    f"<tr><td>{sc_id}</td><td>{tut}</td><td>{round_label}</td>"
                    f"<td>{safe(s['rps'],0)}</td>"
                    f"<td>{safe(s['avg'],3)}</td>"
                    f"<td>{safe(s['med'],3)}</td>"
                    f"<td>{safe(s['p90'],3)}</td>"
                    f"<td>{safe(s['p95'],3)}</td>"
                    f"<td>{safe(s['max'],3)}</td>"
                    f"<td>{safe(s['fail_pct'],2)}%</td>"
                    f"<td>{safe(s['hit_pct'],1)}%</td></tr>"
                )

    table_html = f"""
  <section>
    <h2>Raw data table</h2>
    <div style="overflow-x:auto">
    <table>
      <thead><tr>
        <th>Scenario</th><th>TUT</th><th>Round</th>
        <th>RPS</th><th>avg (ms)</th><th>med (ms)</th>
        <th>p90 (ms)</th><th>p95 (ms)</th><th>max (ms)</th>
        <th>fail%</th><th>hit%</th>
      </tr></thead>
      <tbody>{''.join(table_rows)}</tbody>
    </table>
    </div>
  </section>"""
    sections_html.append(table_html)

    charts_js_block = "\n".join(charts_js)
    sections_block  = "\n".join(sections_html)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>bouine load-test results</title>
<script src="{CHART_JS}"></script>
<style>
  :root {{ --bg:#1a1a2e; --surface:#16213e; --border:#0f3460; --text:#e0e0e0; --muted:#888; }}
  * {{ box-sizing:border-box; margin:0; padding:0 }}
  body {{ background:var(--bg); color:var(--text); font-family: 'Segoe UI',system-ui,sans-serif; padding:2rem }}
  h1 {{ font-size:1.8rem; margin-bottom:.5rem; color:#fff }}
  .subtitle {{ color:var(--muted); margin-bottom:2.5rem; font-size:.9rem }}
  section {{ background:var(--surface); border:1px solid var(--border); border-radius:8px;
             padding:1.5rem; margin-bottom:2rem }}
  h2 {{ font-size:1rem; color:#ccc; margin-bottom:1rem; font-weight:600 }}
  .chart-wrap {{ position:relative; height:340px }}
  table {{ width:100%; border-collapse:collapse; font-size:.78rem }}
  th, td {{ padding:.5rem .75rem; text-align:left; border-bottom:1px solid var(--border) }}
  th {{ background:#0f3460; color:#ccc; font-weight:600; position:sticky; top:0 }}
  tr:hover td {{ background:#1e2a4a }}
  td:nth-child(n+4) {{ font-variant-numeric:tabular-nums; font-family:monospace }}
  .legend {{ display:flex; gap:1.5rem; margin-top:1rem; flex-wrap:wrap }}
  .litem {{ display:flex; align-items:center; gap:.4rem; font-size:.82rem; color:#ccc }}
  .lswatch {{ width:14px; height:14px; border-radius:3px }}
</style>
</head>
<body>
<h1>bouine · load-test results</h1>
<p class="subtitle">
  k6 --summary-export JSON &nbsp;·&nbsp; 3k RPS hit/mixed, 1.5k RPS miss &nbsp;·&nbsp;
  30 s per run &nbsp;·&nbsp; Docker bridge network (macOS)
</p>

<div class="legend">
  {''.join(f'<div class="litem"><div class="lswatch" style="background:{COLORS[t]}"></div>{t}</div>' for t in TUTS)}
</div>

{sections_block}

<script>
window.addEventListener('DOMContentLoaded', () => {{
  Chart.defaults.color = '#aaa';
{charts_js_block}
}});
</script>
</body>
</html>"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--results-dir", default="bench/loadtest/results")
    ap.add_argument("--output",       default="bench/loadtest/results/report.html")
    args = ap.parse_args()

    results_dir = Path(args.results_dir)
    out_path    = Path(args.output)

    data = collect_all(results_dir)
    html = build_html(data, results_dir)
    out_path.write_text(html)
    print(f"Report written → {out_path}  ({len(html):,} bytes)")
    print(f"Open with:  open {out_path}")


if __name__ == "__main__":
    main()
