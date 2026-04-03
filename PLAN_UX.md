# PLAN_UX.md — Dashboard UX Gap Analysis

Reference: `docs/assets/dashboard-reference.html`
Compared against: `internal/dashboard/templates/*.templ`

Every item is classified by:
- **Effort**: S = < 30 min · M = 1–3 h · L = > 3 h
- **Data available**: ✅ already reachable from existing Go code · ⚠️ partial · ❌ new wiring or new metric needed
- **Type**: Display | Data | Structure | CSS | Copy

---

## Global / Layout

### G-L1 · Sidebar footer — live stats missing
**Reference** shows three stat rows:
```
peers   3/3
hit     87.4%
req/s   2,847
```
**Current** shows only `node: <nodename>`.
Wire `ReqPerSec`, `HitPct`, and live peer count (len(PeerResults)) into `LayoutProps`.
- Effort: S · Data: ✅ (all computable from ring + aggregator) · Type: Data + Display

### G-L2 · Header pill — dynamic peer health text
**Reference**: `● 3 peers healthy`
**Current**: `● dashboard` (static text, never changes).
Wire peer count into `LayoutProps.PeerCount` and render "N peers healthy" / "stale peers" when any are stale.
- Effort: S · Data: ✅ · Type: Copy + Display

### G-L3 · Time-range selector — wrong placement
**Reference**: range buttons (1H / 6H / 24H) live inside the `.tabs` bar as a right-aligned `.tg` group, present on **every** page.
**Current**: selector is rendered inline next to the metric cards on the Overview page only.
Move `.tg` into `layout.templ` tabs bar; pass `TimeRange` through `LayoutProps`.
- Effort: S · Data: ✅ · Type: Structure

### G-L4 · Light-mode active nav shadow missing
**Reference** CSS:
```css
html[data-theme="light"] nav a.active { box-shadow: inset 2px 0 0 var(--a); }
```
**Current** CSS only has the dark-mode version (`inset 1px 0 0 var(--a2)`).
- Effort: S · Data: ✅ · Type: CSS

### G-L5 · Sort icon CSS — current implementation diverges
**Reference** uses a proper `th.sortable` class with `<span class="sort-icon">` and `::after` pseudo-element showing ↑/↓/↕ based on `th.asc` / `th.desc` class toggle.
**Current** bakes `↕` as static text into `<th>` strings and uses `data-asc` dataset on a custom `sortTable()` function that doesn't add `asc`/`desc` classes — sort direction feedback is absent.
Update `tableHelperScript` and all `<th>` elements to match reference pattern.
- Effort: M · Data: ✅ · Type: CSS + Display

### G-L6 · `.br` (red/danger button) CSS class missing
**Reference** defines `.br { background:rgba(251,113,133,.08); border:1px solid rgba(251,113,133,.25); color:var(--r); }`.
**Current** has `.bp`, `.bo`, `.bg2`, `.by` but no `.br`.
Needed for destructive actions.
- Effort: S · Data: ✅ · Type: CSS

### G-L7 · Tier bar label row missing from CSS
**Reference** has `.tier-bar-wrap`, `.tier-bar-label` (a flex row showing "hot used" + "1.21 Go / 2 Go"), then `.tier-bar`.
**Current** only has `.tier-bar` + `.tier-bar-fill` with no label.
- Effort: S · Data: ✅ · Type: CSS + Display

### G-L8 · Theme toggle placement
**Reference**: fixed `position:fixed; bottom:1rem; right:1rem` `.tog` button (always visible regardless of scroll).
**Current**: inline in the header toolbar (disappears with content on narrow viewports).
- Effort: S · Data: ✅ · Type: Structure

---

## Overview page

### G-O1 · Metric cards — trend indicators missing
**Reference** metric card `.md` lines:
```
↑ 12% vs prior
↑ 2.1pp
→ stable
↓ improving
```
**Current**: all four show "last 60s" static text.
Compute delta vs the previous 60s window (bucket[n-12..n-6] vs bucket[n-6..n]) and express as percentage change. Add `TrendReqPct`, `TrendHitPp`, `TrendLatMs`, `TrendErrPct` int to `OverviewData`.
- Effort: M · Data: ✅ (ring has the buckets) · Type: Data + Copy

### G-O2 · Route performance table inline in Overview
**Reference** includes a full sortable route table **inside the overview page** with columns:
`Prefix | Pool | Hit % | Req/min | TTL | Trend`
The subtitle reads: `↻ 10s · click header to sort`.
Bypass routes show `<span class="lz d">bypass</span>` for hit% and `—` for TTL.
**Current** only shows top-5 static stat-rows (`route / hit%`). Pool and TTL are completely absent.
Requires joining `[]config.Route` (for Pool/TTL/bypass detection) with `[]observability.RouteStat` by route name. Add `[]RouteRow` (combined view) to `OverviewData`.
- Effort: M · Data: ⚠️ (config.Route needs threading to dashboard handler) · Type: Data + Display

### G-O3 · Bottom row tile 2 — Hot & Warm store (completely missing)
**Reference** "Hot & Warm store" tile shows:
```
hot entries     142,831
[tier bar]      hot used   1.21 Go / 2 Go
warm entries    38,204
[tier bar]      warm used  12.4 Go / 50 Go
evictions / min 124
```
**Current** shows only hot entries + hot tier bar in the same slot, without warm tier data or evictions/min.
`api.Stats` already has `WarmEntries`, `WarmBytes`, `Evictions`. Need:
1. `WarmMaxBytes` plumbed through `dashboard.Config` (from `config.Storage.WarmMaxBytes`)
2. Evictions/min derived from comparing two `Stats()` samples 60s apart (ring snapshot) or an `EvictionsRing`
3. Tier bar label row CSS (G-L7 above)
- Effort: M · Data: ⚠️ (evictions/min needs a rate ring) · Type: Data + Display

### G-O4 · Bottom row tile 3 — Compact hash ring (replaced by Quick Purge)
**Reference** shows a **compact circular SVG** (120×120) with `stroke-dasharray` arcs per node, node labels (b-0 / b-1 / b-2) and a dot legend with `nodename · XX%`.
**Current** puts Quick Purge in this slot; the ring SVG is only on the Cluster page as a horizontal band.
Quick Purge should move to Invalidation page only (consistent with reference).
Overview should render a compact version of the circular ring.
- Effort: M · Data: ✅ (RingSegments() available) · Type: Structure + Display

### G-O5 · Cluster peers tile — address and join time missing
**Reference** peer rows show:
```
bouine-0   10.42.0.5:8443 · joined 2h ago   ●
```
**Current** only shows node name + live/stale label. `api.PeerInfo.Addr` and `api.PeerInfo.JoinedAt` are available but not rendered.
- Effort: S · Data: ✅ · Type: Display + Copy

### G-O6 · Chart: throughput — second dataset label missing
**Reference** legend: dataset 1 unlabelled (req/s), dataset 2 unlabelled (errors).
**Current**: datasets are labelled "req/s" and "hits" — hits is correct but reference shows errors as the second line.
Verify design intent: reference likely shows errors (red line). Switch second dataset from hits to error rate.
- Effort: S · Data: ✅ · Type: Data + Copy

---

## Routes page

### G-R1 · Page subtitle missing route count
**Reference**: `5 configured routes · polled every 10s`
**Current**: `cluster-wide aggregated · polled every 10s`
Include configured route count from `len(cfg.Routes)` in subtitle.
- Effort: S · Data: ⚠️ (need to pass route count) · Type: Copy

### G-R2 · Config columns entirely missing from route table
**Reference** columns: `Prefix | Pool | Hit % | Req/min | TTL | SWR | SIE | neg_ttl | stayin_alive | jitter`
**Current** columns: `Route | Trend | Requests | Hits | Hit %`
All cache policy columns are absent. Requires joining live `RouteStat` with `config.Route` by name. Add `[]RouteRow` carrying both live stats and config fields to `RoutesData`.
- Effort: M · Data: ⚠️ (config.Routes needs threading) · Type: Data + Display

### G-R3 · Two Chart.js charts missing from Routes page
**Reference** shows below the table:
1. "hit ratio by route (6h)" — bar chart
2. "req/min by route" — bar chart
**Current** has no charts on the Routes page.
Data is in `merged.RouteStats`. Add two bar charts via templ `script` component.
- Effort: S · Data: ✅ · Type: Display

### G-R4 · Search bar placeholder text
**Reference**: `Filter by prefix or pool…`
**Current**: `Filter…`
- Effort: S · Data: ✅ · Type: Copy

---

## Cluster page

### G-C1 · Page subtitle
**Reference**: `gossip membership · consistent hash ring · peer fetch`
**Current**: `fan-out aggregated · 200ms timeout · stale = last-known data`
- Effort: S · Data: ✅ · Type: Copy

### G-C2 · Peer table column set entirely wrong
**Reference** columns: `Node | Data addr | Admin addr | Weight | Joined | Status`
**Current** columns: `Node | Status | Uptime 30m | Routes | Total req | Hit %`
All address, weight, and joined columns are absent. All of `DataAddr`, `AdminAddr`, `Weight`, `JoinedAt` are in `api.PeerInfo` and already in `PeerResult.Summary`. Uptime, Routes, Hit% move to a secondary view or drop.
- Effort: S · Data: ✅ · Type: Display + Copy

### G-C3 · Ring stats box missing entirely
**Reference** right-side box shows:
```
virtual nodes / real   256
load factor            1.25
hop limit              2
peer fetch timeout     500ms
protocol version       v1
```
**Current**: nothing equivalent.
`cluster.Config.VirtualNodes`, `cluster.Config.LoadFactor`, `config.Cluster.HopLimit` are available. Add `ClusterMeta` struct to dashboard.Config / LayoutProps.
- Effort: S · Data: ⚠️ (need to pass ClusterMeta) · Type: Data + Display

### G-C4 · Ring SVG shape — circular donut vs horizontal band
**Reference** uses a large (220×220) `<circle stroke-dasharray>` donut with:
- Node name labels outside the ring (with `font-weight:700`)
- Percentage and vnode count on a second line: `34% · 87 vnodes`
- Center circle overlay showing "256 / vnodes"
- Arcs computed from `Frac` × circumference (C = 2π×90 = 565.5px)
**Current** uses a horizontal rectangular band (`<rect>` elements in a 600×28 SVG) with a separate text legend below.
Replace `ringBand()` component with a circular `ringDonut()` that mirrors the reference geometry.
- Effort: M · Data: ✅ (RingSegments() provides Frac) · Type: Display

### G-C5 · Peer fetch stats box missing entirely
**Reference** shows:
```
peer hits (6h)       24,182
peer misses (6h)     3,041
avg peer latency     0.31ms
hop limit hits       0
gossip interval      5s
digest gossiped      4,302
join retry budget    60s · 2s step
```
**Current**: nothing equivalent.
`peer hits/misses` requires new counters in `internal/cluster` (or derived from ring if peer fetches record `X-Cache` like origin fetches). `gossip interval` = 5s constant. `join retry budget` = 60s/2s constant from engine. `avg peer latency` needs a latency ring in cluster.
- Effort: L · Data: ❌ (new cluster counters needed) · Type: Data + Display

### G-C6 · Layout — reference uses `.r21` not full-width `.tc`
**Reference** puts the peer table + ring stats in a `.r21` (2/3 + 1/3 grid) and the ring + peer fetch in a `.r2`.
**Current** stacks everything vertically in a single `.tc`.
- Effort: S · Data: ✅ · Type: Structure

---

## Invalidation page

### G-I1 · History item style — table vs div list
**Reference** uses semantic div items:
```html
<div class="hist-item">
  <span class="hist-type ht-purge">PURGE</span>
  <span class="hist-url">https://…</span>
  <span class="hist-time">2 min ago</span>
</div>
```
CSS classes `.hist-item`, `.hist-type`, `.ht-purge`, `.ht-ban`, `.ht-refresh`, `.hist-url`, `.hist-time` defined with `overflow:hidden; text-overflow:ellipsis; white-space:nowrap` on the URL.
**Current** renders a `<table>` with columns Time / Op / Argument / Result — more data but loses the visual style.
Option: keep the table (more information) but add `.hist-type` badge CSS so the Op column uses `ht-purge` / `ht-ban` / `ht-refresh` instead of the generic `.lz` badge.
- Effort: S · Data: ✅ · Type: CSS + Display

### G-I2 · History section title
**Reference**: `Recent invalidations`
**Current**: `Ops log`
- Effort: S · Data: ✅ · Type: Copy

### G-I3 · Quick Purge location
**Reference**: Quick Purge lives only in the Invalidation page.
**Current**: Quick Purge also appears in the Overview page (bottom-right tile), duplicating it. The Overview slot should instead be the ring SVG (G-O4).
- Effort: S · Data: ✅ · Type: Structure

### G-I4 · Ban button style
**Reference**: uses inline style `background:rgba(251,191,36,.1); border:1px solid rgba(251,191,36,.25); color:var(--y)` (yellow tint).
**Current**: uses `.by` CSS class — which is correct but only defined inline in styles. Verify `.by` is in `styles.templ`.
- Effort: S · Data: ✅ · Type: CSS

---

## Config page

### G-CF1 · Running config viewer — entirely missing
**Reference** left panel titled "Current config (read-only)" with subtitle showing:
```
/etc/bouine/config.yaml · reloaded 2h ago
```
Contains four collapsible sections with structured key-value rows, type hints, and badges:

**listen** section (icon ⟁, badge "listeners"):
```
http     : ":80"      # HTTP/1.1 + h2c data plane
admin    : ":9000"    # admin API · metrics · health
cluster  : ":8443"    # gossip · peer fetch mTLS
```

**storage** section (icon ◫, badge "hot + warm tiers" yellow):
```
hot_max_bytes : 2Go              # in-RAM SIEVE cache
warm_dir      : "/var/lib/bouine" # mmap segments path
warm_max_bytes: 20Go             # max warm tier size
eviction      : sieve            # or w-tinylfu
```

**cluster** section (icon ◎, badge "enabled" green or "disabled" grey):
```
enabled   : true   # gossip membership active
replicas  : 2      # write replication factor
hop_limit : 2      # max peer-fetch hops
```

**routes** section (icon ⬡, badge "N configured"):
Each route shows `path_prefix → pool` header with indented cache config rows (TTL, SWR, jitter, stayin_alive). Footer: `+ N more · click Routes to see all`.

CSS classes needed (all absent from `styles.templ`):
`.cfg-section`, `.cfg-section-hd`, `.cfg-section-icon`, `.cfg-badge`, `.cfg-badge-g`, `.cfg-badge-y`, `.cfg-grid`, `.cfg-grid-dense`, `.cfg-row`, `.cfg-key`, `.cfg-sep`, `.cfg-str`, `.cfg-num`, `.cfg-bool`, `.cfg-bool-t`, `.cfg-bool-f`, `.cfg-hint`, `.cfg-route`, `.cfg-route-hd`, `.cfg-route-path`, `.cfg-route-pool`

Pass `*config.Config` (read-only) to `dashboard.Config` so `configData` can render it.
- Effort: L · Data: ⚠️ (config pointer needs threading) · Type: Data + Display + CSS + Copy

### G-CF2 · Right panel stats missing
**Reference** right panel shows additional metadata rows:
```
config path   /etc/bouine/config.yaml
last reload   2h ago · success
uptime        3d 14h 22m
```
**Current** only shows `node` and `snapshot path`. `ConfigPath` already in `dashboard.Config`. Need:
- `LastReloadAt time.Time` stored in engine on each successful reload
- Process uptime from `time.Since(startTime)` (store `startTime` in engine)
- Copy: "2h ago · success" vs "2h ago · failed" depending on last reload result
- Effort: S · Data: ⚠️ (lastReload timestamp + startTime need engine storage) · Type: Data + Copy

### G-CF3 · Config page subtitle
**Reference**: `validate · confirm · apply — running config is never altered by parse errors`
**Current**: `validate → confirm → apply`
- Effort: S · Data: ✅ · Type: Copy

---

## Implementation milestones

### M1 · CSS + copy pass (all S-effort items)
G-L4, G-L6, G-L7, G-L8, G-O5, G-O6, G-R4, G-C1, G-C6, G-I1, G-I2, G-I3, G-I4, G-CF3
**What**: CSS additions to `styles.templ` (`.br`, `.tier-bar-label`, `.hist-*`, `.cfg-*`), copy string fixes, theme toggle relocation, light-mode nav shadow.
**Output**: `styles.templ`, all five page `.templ` files touched.

### M2 · Layout live data (sidebar + header + time range)
G-L1, G-L2, G-L3
**What**: Move `TimeRange` into `LayoutProps`. Add `PeerCount`, `LivePeers`, `ReqPerSec`, `HitPct` to `LayoutProps`. Update `layout.templ` sidebar footer and header pill. Update all page handlers to pass sidebar stats.

### M3 · Overview completeness
G-O1, G-O2, G-O3, G-O4
**What**:
- Trend deltas in metric cards (compare two 60s windows)
- Route performance table with Pool + TTL columns (join config.Routes with RouteStat)
- Replace overview tile 2 (hot-only) with full Hot & Warm store tile
- Replace overview tile 3 (Quick Purge) with compact circular ring SVG
- Move Quick Purge to Invalidation page only

### M4 · Routes page completeness
G-R1, G-R2, G-R3, G-L5 (sort icons)
**What**:
- Thread `[]config.Route` to dashboard handler and join with `[]RouteStat`
- Add Pool/TTL/SWR/SIE/neg_ttl/stayin_alive/jitter columns
- Add two bar charts (hit ratio by route, req/min by route)
- Fix sort icon `::after` CSS

### M5 · Cluster page completeness
G-C2, G-C3, G-C4, G-C6
**What**:
- Peer table redesign: DataAddr/AdminAddr/Weight/JoinedAt columns
- Ring stats box: vnodes, load factor, hop limit, timeout
- Circular ring donut SVG replacing horizontal band
- `.r21` layout for peer table + ring stats

### M6 · Config viewer
G-CF1, G-CF2, G-CF3
**What**:
- Pass `*config.Config` read-only to `ConfigData`
- Render all four sections with `.cfg-*` CSS classes
- Store `startTime` + `lastReloadAt` + `lastReloadOK` in engine; thread to dashboard

### M7 · Peer fetch stats (requires new counters)
G-C5
**What**:
- Add `PeerFetchHits`, `PeerFetchMisses`, `PeerFetchLatencyMs` atomics to `cluster.Cluster`
- Record on each `PeerFetcher.Fetch()` call
- Expose via `Cluster.PeerFetchStats()` → wire into dashboard ClusterData
- Gossip interval (5s constant), join budget (60s/2s) from engine config

---

## Priority order

| Milestone | Gaps | Effort total | Dependency |
|---|---|---|---|
| M1 | G-L4,L6,L7,L8,O5,O6,R4,C1,C6,I1,I2,I3,I4,CF3 | ~3h | none |
| M2 | G-L1,L2,L3 | ~2h | none |
| M3 | G-O1,O2,O3,O4 | ~4h | M2 (LayoutProps) |
| M4 | G-R1,R2,R3,L5 | ~3h | config threading |
| M5 | G-C2,C3,C4,C6 | ~3h | ClusterMeta |
| M6 | G-CF1,CF2,CF3 | ~4h | config pointer |
| M7 | G-C5 | ~5h | new cluster counters |

Total: ~24h. M1–M3 unblock the most visible regressions.
