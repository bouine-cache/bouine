# Code Comment Condensation

Reusable prompt for trimming code comments so they express *why*, not *how*,
and stay too short to go stale. Invoke it with a target path or package
(none to survey everything under `internal/`, `cmd/`, `pkg/`).

## Goal

A comment earns its line only if it states something a reader cannot get from
the code, the test names, or the signature in seconds. Everything else is
context-load that drifts: the code moves, the comment stays. Bias to deletion
— a missing why-comment is rediscovered in `git blame`; a stale how-comment
actively misleads.

## Hard constraints (never violate)

- Every exported identifier keeps a godoc (AGENTS.md §4), starting with the
  identifier name. Trim it, never delete it.
- Spec-driven behavior keeps its RFC clause citation (AGENTS.md §4). "Cite
  the clause in a code comment and add a test that pins the chosen behavior"
  (AGENTS.md §2.9) means the citation is part of the pinned decision — keep
  it even when the test name already hints at it.
- No behavior change. Comments only. If trimming reveals a code smell, note
  it in the report instead of editing code.
- Lint gates stay green: `gofmt -s`, doc-comment format (`golangci-lint`
  runs `godot`-style checks where configured).

## Keep (why-comments)

- RFC / spec citations with clause numbers.
- Non-obvious decisions and tradeoffs: why this algorithm, why this limit,
  why the naive-looking alternative is wrong.
- External constraints the code cannot express: wire-format requirements,
  benchmark-proven choices ("allocs/op = 0 requires..."), bug references.
- Warnings about non-obvious invariants ("caller must hold mu", "not
  goroutine-safe", ordering constraints).
- Workarounds with a reason ("klauspost/compress requires X").

## Delete (how-comments)

- Restatements of the next line of code or the signature
  (`// increment the counter`, `// ParseX parses x`).
- Translation of idiomatic Go into English (`// range over entries`).
- Commentary on the obvious: guard clauses, error returns, unlocks.
- History ("previously this was...", "added in...") — that is CHANGELOG and
  ADR territory.
- TODO/FIXME without a tracking issue — either add the reference or delete.
- Commented-out code, always.

## Rewrite (condense to one why-line)

When a block mixes a how-restatement with a real why, delete the how part
and keep one line for the why. A multi-line comment collapses to its
non-obvious core; if the core is already in the test name or an adjacent
doc, delete instead.

## Procedure

1. **Inventory.** Per target file: comment-line count, split into the four
   buckets above.
2. **Sweep.** Work file by file, largest comment density first. Apply
   keep/delete/rewrite. Prefer deleting the comment over rewording it.
3. **Verify.** `make lint`, `go build ./...`, `go test -short ./...`
   (or `make test`). No test may change behavior — comment-only diff.
4. **Report.** Table: file | before → after comment lines. List flagged
   code smells and any comment you were unsure about.

## Report format

- Table: file | before → after comment lines | dominant action
- Flagged code smells (not fixed)
- Borderline comments kept or deleted, with one-line justification
