# bouine — Accessibility Statement

bouine is committed to making its project sites and software usable by
people with disabilities. This statement describes the accessibility
measures in place and known limitations.

## Scope

- **CLI** (`bouine` binary) — text-based, operates via standard input,
  output, and exit codes. It is inherently accessible with screen
  readers and terminal accessibility tools.
- **Dashboard** (embedded web UI at `/dashboard/`) — the operator-facing
  web interface rendered from `internal/dashboard/templates/*.templ`.
- **Project documentation** (README, docs/, GitHub issue templates) —
  Markdown rendered by GitHub and the docs site at bouine.org.

## Dashboard accessibility measures

The embedded dashboard follows WCAG 2.0 AA guidelines where practical:

### Implemented

- **Semantic HTML and landmarks** — `<html lang="en">`, `<main>`,
  `<nav>`, `<aside>`, `<h1>`/`<h2>` heading hierarchy.
- **ARIA roles and labels** — `role="tablist"`/`tab`/`tabpanel` for
  navigation tabs, `role="progressbar"` with `aria-valuenow/min/max` for
  tier bars, `aria-live="polite"` for invalidation response regions,
  `aria-label` on all charts, forms, and interactive controls.
- **Skip-to-content link** — keyboard users can bypass the sidebar and
  jump directly to main content.
- **Keyboard focus indicators** — `:focus-visible` outline on all
  interactive elements (links, buttons, tabs, sortable headers, filter
  chips).
- **Keyboard-operable sort** — sortable table headers have `tabindex="0"`
  and respond to Enter/Space.
- **Decorative elements hidden** — `aria-hidden="true"` on noise canvas,
  tint overlay, nav icons, and status dots.
- **Image alt text** — logo has `alt="bouine"`; SVG diagrams use
  `role="img"` with `aria-label` and `<title>` elements.
- **Color contrast** — dark theme meets 4.5:1 contrast for text. Light
  theme muted text (`--m`) adjusted to meet AA.
- **Reduced motion** — `@media (prefers-reduced-motion: reduce)` disables
  transitions and the animated noise canvas.
- **Form labels** — all form inputs have associated `<label>` elements
  via `for`/`id`.
- **Theme toggle** — exposes current state via `aria-pressed`.

### Known limitations

- **Charts lack data table fallback** — Chart.js canvases have
  `aria-label` summaries but no full textual data table alternative.
  Screen readers announce the chart title but not individual data points.
- **Insights SVG nodes are click-only** — the architecture diagram nodes
  in the Insights page are activated by `onclick` and are not fully
  keyboard-accessible yet.
- **No screen-reader testing has been conducted** — the dashboard has
  been built following WCAG guidelines but has not been formally tested
  with NVDA, JAWS, VoiceOver, or Orca.

## CLI accessibility

The `bouine` CLI is a standard command-line tool:
- All output is text-based, sent to stdout/stderr.
- Exit codes follow Unix conventions.
- No interactive TUI or ncurses interface is used.
- Help text (`--help`) and man-page-style documentation are available
  for all subcommands.

## Reporting accessibility issues

If you encounter an accessibility barrier, please open a
[GitHub issue](https://github.com/bouine-cache/bouine/issues) with the
`accessibility` label, or report privately via
[GitHub Private Vulnerability Reporting](https://github.com/bouine-cache/bouine/security/advisories/new)
if the issue has security implications.
