# ADR-0073: Store-fill body ownership transfer

- **Status**: Accepted
- **Date**: 2026-11-01
- **Deciders**: @thylong
- **Phase**: phase 6

## Context

The rc1 5krps stress run (results archived in
`cache-stress-test/results/bouine-pr890rc1-cf-30m-5krps-1n-prof/`)
shed 5,868 foreground fetches to 503 at a TTL-expiry wave. The heap
profile taken at the shed attributes **479.86 MB — 84.7% of inuse heap
— to `api.Object.CloneForStorage` inlined in the `HotStore.Put` path**,
cumulative through `teeStreamToClient` (493 MB, 87%). Over the 30-minute
window the same copy churned **5.1 GB of alloc_space**.

The reason is a double copy by construction:

- every fill path already builds an **exact-size, cache-owned body
  copy** for the singleflight flight (`teeStreamToClient`'s
  `bodyCopy`, `streamMissBuffered`'s bounded copy, `doFetch`/`doFetchBg`/
  `doRefillFetch`/`upstreamFetchResult`'s exact-size copies) — followers
  share it read-only and the hot tier must not pin fasthttp's
  doubling-growth slack, which is exactly why these copies exist
  (pinned by `TestFetchStoresRightSizedBody`);
- `HotStore.Put` then **clones that copy again** via
  `CloneForStorage`, because Put's contract is defensive: the *typical*
  caller may reuse its response buffers, so Put cannot know which
  bodies are safe to alias.

Two full body allocations per stored byte of working set, on the path
that saturates during TTL-expiry waves: allocation-rate spikes into a
near-GOMEMLIMIT live set, GC assist steals CPU from fetch goroutines
holding `fetchSem` slots, and queued fetches shed after
`fetchWaitTimeout` (the wave's 503s; the goroutine dump shows 304
runnable goroutines at capture — scheduler saturation, not blocked
I/O).

## Decision

**Split the ownership boundary instead of weakening it.**

1. `HotStore.Put` keeps the defensive clone unchanged — callers that
   may reuse buffers (revalidation refresh, peer promote, soft purge)
   are unaffected.
2. A new `PutOwned` method on `HotStore` and `TieredStore` takes the
   body as handed over: exact-size, cache-owned, immutable after the
   call. With the slab disabled (default) the stored entry **aliases**
   the transferred bytes — zero body copies on the fill path. With the
   slab enabled the body is still copied into slab memory (moving off
   the Go heap is the point of the slab); an `Alloc` failure keeps the
   transferred slice, which is as safe as the slab-off case.
3. The cache layer reaches it through a **consumer-declared capability
   interface** (`ownedBodyStore`, in `internal/cache`): stores without
   the capability fall back to the cloning `Put`. `fetchResult` carries
   a `bodyOwned` flag set at every fill site that builds the exact-size
   copy, and false everywhere a body aliases a caller-reusable buffer or
   bytes shared with a still-stored object.
4. The header map stays deep-cloned on both paths: the serialized head
   served on every hit is built from stored headers.
5. `teeStreamToClient` and `streamMissBuffered` release the
   fetch-semaphore slot (and the pooled origin response) at
   origin-body-complete, **before** the settle/store tail, so the store
   admission (shard lock, inline SIEVE eviction, stored-object build)
   never extends the fetch-queue wait.
6. The tee's 32 KiB read scratch and `streamCopyFlush`'s chunk move to a
   `sync.Pool` (fixed-size, fully overwritten per Read, reset-free).

Safety argument for aliasing: the transferred slice is a fresh heap
allocation that no writer ever touches after construction — the leader
has written it to the client, followers only writev it, the warm-tier
encode only reads it. Heap slices are GC-pinned for as long as any
reader holds them, so eviction and Put-overwrite cannot invalidate
in-flight reads. This is the same immutability the hit path already
relies on (writev zero-copy aliases stored bodies); the fill path now
shares one backing array between the flight and the stored entry
instead of two.

## Consequences

### Positive

- One body allocation per stored fill instead of two: the gate
  benchmark drops 18→17 allocs/op and −24% B/op on the cacheable miss;
  ~5 GB/30m less churn on the profiled shape, directly shrinking the
  GC-assist pressure that starved the fetch queue at the wave.
- The store admission leaves the fetch-critical section: slot-hold time
  no longer includes shard-lock eviction work, so TTL-expiry waves
  drain the semaphore faster and shed less.
- Pooled read scratch removes a 32 KiB allocation per streaming fill
  (~10 GB of the 30-minute alloc_space window).

### Negative / trade-offs

- `PutOwned` is a second storage entry point with a sharper contract;
  callers must prove the body is exact-size, cache-owned, and
  never-written after the call. The flag is threaded from construction
  sites, so a future fill site that reuses buffers must remember to
  leave it false.
- Tee buffers can now outlive their fetch-slot hold (bounded by the
  global streaming cap and per-response cap instead of the slot
  count); the streaming-fallback counter may tick slightly earlier
  under bursts.
- `fetchSem` no longer bounds concurrent *store attempts* — SIEVE
  admission and the hot budget bound those instead.

### Risks

- A future call site setting `bodyOwned` on a reused buffer would let
  the store alias bytes the caller overwrites — the same class as the
  bug `TestHotStore_PutOwnsCallerBody` pins for `Put`. Mitigated by
  construction-site threading and the aliasing/transfer tests
  (`fillheap_test.go`, `hot_putowned_test.go`).
- With the slab on, aliasing the transferred slice into the stored
  entry would be a use-after-free under eviction (slab regions are
  returned to the free list); `PutOwned` copies into slab memory, and
  `TestHotStore_PutOwnedWithSlabStoresSlabMemory` pins it on Linux.

## Alternatives considered

- **Enable the slab by default** to move stored bodies off the Go heap:
  changes an operator-visible memory profile and the eviction story;
  the copy into slab memory remains one full body copy per fill.
- **GOGC/GOMEMLIMIT tuning** (the report's third lever): pacing the GC
  softer trades latency for memory headroom and does not remove the
  double copy; tracked as a follow-up experiment.
- **Right-size in `Put`**: `Put` cannot distinguish fill-owned bodies
  from caller-reusable buffers without exactly the ownership signal
  this ADR adds.

## References

- `docs/architecture.md` §3.5 (response streaming), §7 performance
  rules.
- `api.Object.CloneForStorage` godoc (the pre-existing ownership
  boundary this ADR splits).
- rc1 5krps profile:
  `cache-stress-test/results/bouine-pr890rc1-cf-30m-5krps-1n-prof/pprof-capture/`.
- ADR-0066/0067 (fill flights and settlement lifecycle the early slot
  release must respect).
