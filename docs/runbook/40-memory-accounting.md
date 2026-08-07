# 40 — Memory accounting

How to interpret the three memory-related numbers bouine exposes, and when to
trust each.

---

## The three numbers

| Metric | Source | What it measures | What it does NOT measure |
|--------|--------|------------------|--------------------------|
| `bouine_hot_store_bytes` | `objSize` estimate (`internal/storage/hot.go`) | Body bytes + struct sizes + header map overhead per cached object | Go allocator size-class rounding, non-cache heap, GC fragmentation |
| `go_memstats_heap_alloc_bytes` | Go runtime (via `prometheus/client_golang` default gatherer) | All live Go heap objects (cache + everything else) | Free slots in in-use spans; recently-dead objects not yet swept |
| `go_memstats_heap_inuse_bytes` | Go runtime | Bytes in in-use heap spans (includes fragmentation + unswept dead) | Objects in idle spans (already returned to OS) |

`bouine_hot_store_bytes` is an **eviction budgeting estimate**, not a runtime
memory metric. It will always be lower than `go_memstats_heap_alloc_bytes`
because the heap contains the cache plus HTTP server buffers, goroutine
metadata, memberlist state, warm-tier index, request-path transients, and GC
overhead. A significant gap (observed at 3-4× in production under traffic
pressure) between `hot_store_bytes` and `heap_inuse_bytes` is expected.

---

## When to trust which

| Task | Use | Why |
|------|-----|-----|
| Eviction / cache sizing | `bouine_hot_store_bytes` | It tracks what the cache actually holds; eviction uses it for `OverBudget` |
| OOM investigation | `go_memstats_heap_alloc_bytes` + pprof | The heap alloc metric is the GC's view of live memory; pprof attributes it |
| Capacity planning / RSS | `go_memstats_heap_inuse_bytes` | In-use spans approximate RSS contribution from the Go heap |

---

## Capturing a heap profile

Enable pprof on the admin port (opt-in, default off):

```yaml
admin:
  pprof_enabled: true
```

Then capture a heap profile:

```bash
go tool pprof http://<admin-addr>/debug/pprof/heap
```

The pprof endpoints are auth-exempt; the admin port should be network-isolated
via K8s NetworkPolicy in production.

For a goroutine snapshot:

```bash
curl http://<admin-addr>/debug/pprof/goroutine?debug=1
```

---

## Known `objSize` accuracy

`objSize` has two known blind spots (addressed in Step 4 of the #180 plan):

1. **Orphaned `header.Map.values` slots from `Del`** — `Set-Cookie` is always
   deleted from cached objects, orphaning one value string per affected object.
   `objSize` counts active entries (`Len()`) not total slots (`len(values)`).
2. **`mapPerEntryOverhead` was recalibrated for 128-bit keys** (32 B/entry
   for 8-slot buckets with 16 B keys at load factor 6.5, up from 22 B
   with 8 B keys) — partially offsetting the underestimate above.

Back-of-envelope net effect at 1.07M entries: ~43 MB underestimate from
orphaned values (16 B string header + ~24 B average value per Set-Cookie
deletion) minus ~30 MB overestimate from the map constant = ~73 MB net
underestimate. Not worth closing further; the gap is dominated by non-cache
heap consumers and GC behavior, not per-entry accounting error.
