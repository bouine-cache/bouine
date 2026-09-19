# Documentation Review & Condensation

Reusable prompt for trimming documentation context-load without losing
knowledge. Invoke it with a target path (or none to survey everything).

## Goal

Reduce context-load from documentation without losing knowledge. Documentation
should be scannable by humans and cheap for AI agents to load. Work in this
order: (1) enhance/restructure, (2) reduce, (3) eliminate — escalating only
when the lighter option clearly isn't enough.

## Target

The path(s) given when invoking this prompt. If none given, survey all of
`docs/`, `AGENTS.md`, and top-level `*.md`, then propose a work plan before
editing.

## Hard constraints (never violate)

- Every cross-reference cited elsewhere must keep resolving: section anchors,
  `§` references, ADR numbers, file paths, `make` targets, `doc.go` links.
  Before deleting or moving anything, grep the whole repo (including
  `AGENTS.md`, `.golangci.yaml`, CI configs, templates) for references to it.
  If a reference exists, the content stays (it may move, with the reference
  updated in the same change).
- ADR numbering is immutable: never renumber, renumber-gapped, or merge ADRs
  into one another. New ADRs only via the normal process.
- `CHANGELOG.md` is append-only; never touch existing entries.
- Never weaken a rule: if a doc states a gate, budget, or invariant
  (benchmarks, coverage, security), the tightened version must enforce at
  least the same thing.
- No content loss in enhance phase: facts, decisions, and rationale must
  survive. Cutting is for *redundancy and verbosity*, not information.

## Procedure

1. **Inventory.** For each target file: line count, who cites it, and what
   overlaps with other docs. Build a redundancy map (same rule stated in 2+
   places = pick one home, reference it from the others).
2. **Enhance.** Restructure: dedup, move operational detail out of overview
   docs into runbooks/appendices, convert prose lists into tables, put the
   normative rule first and rationale second. Fix stale references found in
   step 1.
3. **Reduce.** Within each file, cut: restated context, examples that
   duplicate adjacent sections, hedging, and history (old behavior belongs
   in ADRs/CHANGELOG, not living docs). Target meaningfully shorter, not a
   fixed percentage.
4. **Eliminate (propose only).** For files or sections that are fully
   superseded, dead (zero inbound references, describe removed features),
   or duplicated elsewhere: do NOT delete. List them in the report with
   evidence (inbound-reference grep results, superseding ADR) and wait for
   approval.
5. **Verify.** After edits, re-run the cross-reference greps from step 1:
   every previously-resolving reference must still resolve. Report before/
   after line counts per file.

## Report format

- Table: file | before → after lines | action taken
- Elimination candidates (with evidence) awaiting approval
- Any reference you could not verify
