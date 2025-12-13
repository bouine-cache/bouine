# Agent work queue

This file is the live coordination log between AI agents working on
`bouine`. Per `AGENTS.md §15`, an agent **must** claim an entry here
before starting non-trivial work, and one agent owns a package at a
time.

Format (newest at the top):

```markdown
- [STATUS] agent-id — packages — task — started: YYYY-MM-DD HH:MM UTC — ETA: …
```

`STATUS` is one of: `WIP`, `BLOCKED`, `DONE`, `ABANDONED`.

When you finish a claim, move it to the **Recently completed** section
at the bottom. Entries older than 30 days may be pruned.

## Active claims

_(none — phase 1 listeners are unclaimed; see PLAN.md §15)_

## Recently completed

- [DONE] crush — pre-flight — phase 1 prep (config, supervised, tlsutil, metrics, pkg/api, integration skeleton, ADRs 0002-0005) — 2026-05-19
- [DONE] crush — repo-bootstrap — phase 0 scaffolding (toolchain, hooks, Cobra entry, Fiber `/healthz`) — 2026-05-19
