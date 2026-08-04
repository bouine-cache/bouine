## Summary
<!-- 1–3 sentences. Why is this change needed? What does it do? -->

## Linked issue
<!-- Closes #N, Fixes #N, Refs #N -->

## Type of change
- [ ] feat — new feature
- [ ] fix — bug fix
- [ ] perf — performance improvement
- [ ] refactor — internal change with no behavior diff
- [ ] docs — documentation only
- [ ] test — tests only
- [ ] chore / build / ci — tooling

## Architectural layer(s) touched
<!-- See AGENTS.md §3 and docs/architecture.md §2 -->
- [ ] L1 server (HTTP/1.1, /2, TLS, routing)
- [ ] L2 storage (RAM hot tier, mmap warm tier, WAL, eviction)
- [ ] L3 cache engine (RFC 9111, Vary, conditionals, negative cache)
- [ ] L4 origin (upstream pool, health, hedge, circuit breaker)
- [ ] L5 cluster (gossip, consistent hash, peer fetch)
- [ ] L6 control plane (admin API, purge, dashboard)
- [ ] L7 observability (metrics, traces, logs, pprof)
- [ ] L8 AI / dashboard (design target, not yet implemented)
- [ ] config / SDK / CLI / docs

## Checklist (mirrors AGENTS.md §17)

- [ ] `prek run --all-files` passes locally.
- [ ] Layer dependencies respected (`depguard` clean).
- [ ] `make ci` is green locally.
- [ ] Tests added/updated; coverage not reduced.
- [ ] If hot path: zero-alloc benchmark proves it (`benchstat` diff in
      the description).
- [ ] If cache logic: `cache-tests` score not regressed
      (attach harness output).
- [ ] If config: `config.Validate` updated and new field documented.
- [ ] If public API: SDK types updated, semver impact noted below.
- [ ] If a threat row is affected: `docs/security/threat-model.md`
      updated in this PR (cite Txx IDs).
- [ ] Godoc updated.
- [ ] ADR added under `docs/decisions/` if architectural.
- [ ] No secrets, no PII, no production hostnames.
- [ ] `HANDOFF.md` removed if it was created.

## Performance impact
<!-- Required if you touched L1–L6. Paste benchstat output. -->

```
$ benchstat main.txt head.txt
```

## Compliance impact
<!-- Required if you touched cache logic. -->

`cache-tests` score: `before` → `after`.

## Threat-model impact
<!-- Required if you touched a Txx row. List the IDs and the new
     control state (shipped / deferred / unchanged). -->

## Semver impact
<!-- For public API changes only (pkg/api, pkg/bouineapi, wire protocol). -->
- [ ] Patch (no API change)
- [ ] Minor (additive)
- [ ] Major (breaking) — migration note below

### Migration note
<!-- Only if Major. -->

## Test plan
<!-- How did you verify this works? Commands, scenarios, integration runs. -->

## Notes for reviewers
<!-- Anything reviewers should focus on, known follow-ups, deferred items. -->
