# ADR-0065: bubbletea for the status TUI

- **Status**: Accepted
- **Date**: 2026-10-24
- **Deciders**: @thylong
- **Phase**: admin-surface epic (#812..#817)
- **Informed**: reviewers with the `deps` label

## Context

Issue #813 makes `bouine status --watch` a fullscreen alternate-screen
TUI on any terminal, with keys (quit, refresh, live cluster toggle),
SIGWINCH resize handling, semantic colors, and tier bars. The
alternatives considered:

1. **Hand-rolled over `golang.org/x/sys`** (already allow-listed):
   zero new modules, but we would own the ANSI *input* state machine
   (Esc vs key runes vs escape sequences), termios raw-mode
   setup/restore including panic-safe teardown, SIGWINCH plumbing,
   and color-profile detection (NO_COLOR, TERM=dumb, 256-color
   degradation). This is the highest-bug-density surface in terminal
   code and none of it is bouine's core competency.
2. **`golang.org/x/term` only**: removes the termios edge cases but
   leaves the event loop, input parsing, and styling to us.
3. **charmbracelet/bubbletea**: the Cmd/Msg/Update event loop is
   exactly the interaction model #813 chose; raw mode, resize, TTY
   detection, and panic-safe teardown are handled; lipgloss provides
   styling with automatic color-profile degradation.

Measured on this tree (Go 1.27.3, `-trimpath -ldflags "-s -w"`): the
linked cost with the full status TUI is +1.06 MB on a 30.93 MB
binary (+3.4%; the bare framework alone links to +0.28 MB, the rest
is lipgloss styling views), +19 modules in the graph (190 → 209),
+38 go.sum lines, ~13 MB of module cache for contributors. The TUI
is a
default-on interactive surface for every operator, not a niche flag,
which is what tilts the trade-off away from hand-rolling.

## Decision

- We use `github.com/charmbracelet/bubbletea` v1.3.x (pinned exact
  minor; not the v2 alphas) for `bouine status --watch` on a terminal.
- We use `github.com/charmbracelet/lipgloss` for styling (already a
  bubbletea dependency; imported directly for the frame renderer).
- We use `golang.org/x/term` for TTY detection and terminal sizing.
- Piped/scripted output and `--format json` never take the TUI path;
  no escape sequence can reach a pipe by construction.
- The TUI's frame renderer stays a pure function of
  (report, prev, meta) so it is unit-testable without a pty; the
  bubbletea model is a plain state struct.

## Consequences

### Positive
- Terminal edge cases (raw mode teardown, resize races, Esc handling,
  color degradation) are maintained upstream, not in bouine.
- The pure-frame design keeps the TUI under the normal test policy.

### Negative / trade-offs
- +19 modules, +38 go.sum lines, ~0.5-1 MB binary, ~13 MB module
  cache. Recorded here so future re-evaluations have the numbers.
- Charm release cadence is fast; upgrades are deliberate and pinned.

### Risks
- bubbletea v2 may eventually break v1 API; mitigation is the narrow
  integration surface (one program, one model, one renderer) and the
  pure-frame tests that survive a framework swap.
