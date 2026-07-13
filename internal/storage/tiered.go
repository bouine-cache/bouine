package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage/wal"
	"github.com/bouine-cache/bouine/internal/storage/warm"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

// TieredStore wraps a hot tier (RAM) and an optional warm tier (disk)
// into a single Store. Objects smaller than a threshold live in the
// hot tier only; larger objects are demoted to the warm tier. The WAL
// ensures the index can be rebuilt after a crash.
//
// When warm is nil the store is ephemeral (equivalent to --no-persist).
//
// Stable.
type TieredStore struct {
	hot    *HotStore
	warm   *warm.Store
	wal    *wal.Log
	walMu  sync.Mutex // guards wal field access during rewriteWAL vs concurrent Enqueue/EnqueueBatch
	logger observability.Logger

	// bodyThreshold: objects with Body <= this stay hot-only.
	bodyThreshold int64

	// warmSyncInterval controls how often hot→warm background sync runs.
	warmSyncInterval time.Duration
	// warmSyncBatchSize caps entries written per sync cycle.
	warmSyncBatchSize int
	// warmSyncOffset rotates through the hot key set across cycles.
	warmSyncOffset int

	// walSyncInterval controls the async WAL fsync batching interval.
	// <= 0 means synchronous mode (per-entry fsync). See ADR-0024.
	walSyncInterval time.Duration

	// tombstoneQueue receives keys evicted from hot that had a warm
	// backup. Drained by warmSyncLoop and tombstoned in warm + WAL.
	// Buffered; non-blocking sends — overflow drops the tombstone.
	tombstoneQueue chan api.Key
	// droppedTombstones counts tombstones dropped (queue full).
	droppedTombstones atomic.Int64

	// warmEvictQueue receives keys evicted from the warm tier by its
	// eviction policy. Drained by warmSyncLoop to append WAL delete
	// entries so warm eviction survives restart. The hot tier's
	// hasBackup flag is cleared immediately in the callback (no I/O).
	// Buffered; non-blocking sends — overflow drops the WAL entry (the
	// tombstone is already on disk; the WAL is a fast-replay optimization).
	warmEvictQueue chan api.Key
	// droppedWarmEvicts counts warm-eviction WAL entries dropped.
	droppedWarmEvicts atomic.Int64

	// walPath is the WAL file path, stored for WAL rewrite after compaction.
	walPath string

	// done is closed by Close to stop the background goroutines.
	done chan struct{}
	// compactWg tracks the compaction goroutine for join-on-close.
	compactWg sync.WaitGroup
	// CompactStartupDelay delays the first compaction after startup.
	compactStartupDelay time.Duration
	// checkpointInterval controls how often a snapshot + WAL truncate
	// checkpoint runs. Default 5m. 0 disables periodic checkpointing.
	checkpointInterval time.Duration
	// checkpointWALThreshold triggers a checkpoint when the WAL entry
	// count exceeds this value, regardless of the interval. Default 100K.
	checkpointWALThreshold int64
	// checkpointing is true during the WAL truncate window. WAL enqueues
	// spin until it is false. See checkpoint() for the sequence.
	checkpointing atomic.Bool
	// walEntryCount tracks entries since the last checkpoint. Used for
	// the threshold trigger.
	walEntryCount atomic.Int64
	// checkpointWg tracks the checkpoint goroutine for join-on-close.
	checkpointWg sync.WaitGroup
	// syncWg tracks the warm sync goroutine for join-on-close.
	syncWg sync.WaitGroup
}

// TieredConfig configures a TieredStore.
type TieredConfig struct {
	Hot    HotConfig
	Warm   *warm.Config // nil = no warm tier (ephemeral mode)
	WALDir string       // empty = no WAL
	Logger observability.Logger

	// WarmMetrics holds the warm-tier Prometheus collectors. When non-nil
	// and Warm is also set, the warm store increments the over-budget,
	// eviction, and compaction counters inline. The caller is responsible
	// for polling TieredStore.Stats() and calling SetDiskBytes on this
	// handle to update the disk_bytes gauge.
	WarmMetrics *warm.Metrics

	// BodyThreshold controls the hot/warm admission boundary. Objects
	// with BodySize <= this value stay in the hot tier only. Objects
	// above this value are also written to the warm tier so they can
	// be evicted from RAM without data loss.
	// Default: 64 KiB.
	BodyThreshold int64
	// WarmSyncInterval controls how often hot→warm background sync runs.
	// Set to 0 to disable (warm tier only stores objects above body_threshold).
	// Operators should set this explicitly (e.g. 60s) when they want
	// small objects to survive restarts.
	WarmSyncInterval time.Duration
	// WarmSyncBatchSize caps entries written to warm per sync cycle.
	// Default 5000.
	WarmSyncBatchSize int
	// WALSyncInterval controls the async WAL fsync batching interval.
	// Default 100ms. Set to -1 for synchronous mode (per-entry fsync,
	// same as pre-ADR-0024 behavior). See ADR-0024.
	WALSyncInterval time.Duration
	// CompactStartupDelay delays the first compaction check after
	// startup. Compaction scans all segments and can take seconds with
	// millions of keys — running it during startup (when WAL replay,
	// cluster join, and initial traffic compete for I/O) causes probe
	// timeouts and CrashLoopBackOff. Default 5 minutes.
	CompactStartupDelay time.Duration
	// CheckpointInterval controls how often a snapshot + WAL truncate
	// checkpoint runs. Default 5m. 0 disables periodic checkpointing.
	CheckpointInterval time.Duration
	// CheckpointWALThreshold triggers a checkpoint when the WAL entry
	// count exceeds this value. Default 100000.
	CheckpointWALThreshold int64
}

// NewTieredStore creates a tiered store. If WALDir is non-empty, the
// WAL is replayed on open to rebuild the warm-tier index.
func NewTieredStore(cfg TieredConfig) (*TieredStore, error) {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if cfg.BodyThreshold <= 0 {
		cfg.BodyThreshold = 64 << 10
	}
	if cfg.WarmSyncBatchSize <= 0 {
		cfg.WarmSyncBatchSize = 5000
	}
	// WarmSyncInterval defaults to 60s when unset (0). Operators can
	// explicitly disable by setting -1. A value of 0 is treated as
	// "use the default" because Go's zero-value Duration can't
	// distinguish unset from explicitly-zero in YAML.
	if cfg.WarmSyncInterval == 0 {
		cfg.WarmSyncInterval = 60 * time.Second
	}
	warmSyncInterval := cfg.WarmSyncInterval
	if warmSyncInterval < 0 {
		warmSyncInterval = 0 // -1 means disabled
	}

	walSyncInterval := cfg.WALSyncInterval
	if walSyncInterval == 0 {
		walSyncInterval = wal.DefaultSyncInterval
	}

	checkpointInterval := cfg.CheckpointInterval
	if checkpointInterval == 0 {
		checkpointInterval = 5 * time.Minute
	}
	checkpointWALThreshold := cfg.CheckpointWALThreshold
	if checkpointWALThreshold == 0 {
		checkpointWALThreshold = 100_000
	}

	ts := &TieredStore{
		bodyThreshold:          cfg.BodyThreshold,
		warmSyncInterval:       warmSyncInterval,
		warmSyncBatchSize:      cfg.WarmSyncBatchSize,
		tombstoneQueue:         make(chan api.Key, 4096),
		warmEvictQueue:         make(chan api.Key, 4096),
		done:                   make(chan struct{}),
		logger:                 cfg.Logger,
		walSyncInterval:        walSyncInterval,
		compactStartupDelay:    cfg.CompactStartupDelay,
		checkpointInterval:     checkpointInterval,
		checkpointWALThreshold: checkpointWALThreshold,
	}

	// Wire the eviction callback so backed evictions enqueue
	// tombstones for async processing by warmSyncLoop.
	cfg.Hot.OnEvict = func(key api.Key) {
		select {
		case ts.tombstoneQueue <- key:
		default:
			ts.droppedTombstones.Add(1)
		}
	}
	ts.hot = NewHotStore(cfg.Hot)

	if cfg.Warm != nil {
		if err := ts.initWarm(cfg.Warm, cfg.WarmMetrics); err != nil {
			return nil, err
		}
	}

	// Background compaction: check every 30 minutes and compact if the
	// warm tier has accumulated significant tombstone/dead byte waste.
	if ts.warm != nil {
		ts.compactWg.Add(1)
		go ts.compactLoop()
	}

	if cfg.WALDir != "" {
		if err := ts.initWAL(cfg.WALDir); err != nil {
			return nil, err
		}
	}

	// Start the warm sync goroutine if warm tier and sync are enabled.
	if ts.warm != nil && warmSyncInterval > 0 {
		ts.syncWg.Add(1)
		go ts.warmSyncLoop()
	}

	// Start the checkpoint loop if warm tier and WAL are both enabled.
	if ts.warm != nil && ts.wal != nil && ts.checkpointInterval > 0 {
		ts.checkpointWg.Add(1)
		go ts.checkpointLoop()
	}

	return ts, nil
}

// initWarm opens the warm store, injects metrics, and wires the
// eviction callback.
func (t *TieredStore) initWarm(cfg *warm.Config, metrics *warm.Metrics) error {
	warmCfg := *cfg
	if metrics != nil {
		warmCfg.Metrics = metrics
	}
	w, err := warm.NewStore(warmCfg)
	if err != nil {
		return err
	}
	// Wire warm-tier eviction callback: clear the backup flag on the hot
	// entry immediately (no I/O), and enqueue a WAL delete entry
	// for async persistence. Crash recovery relies on
	// rebuildIndexFromScan honoring tombstones in segment order —
	// the WAL delete is a fast-replay optimization, not the sole
	// durability guarantee.
	w.OnEvict = func(key uint64) {
		t.hot.ClearBacked(api.Key(key))
		select {
		case t.warmEvictQueue <- api.Key(key):
		default:
			t.droppedWarmEvicts.Add(1)
		}
	}
	t.warm = w
	return nil
}

// initWAL opens the WAL, replays it to rebuild the warm-tier index, and
// falls back to a segment scan if the replayed index is empty. When an
// index snapshot exists, it is loaded first to skip the segment scan —
// the WAL is replayed on top of the snapshot as a delta. When the WAL
// contains v2 entries (opPutV2 with record size), the index is
// populated with size information directly, and RecomputeStats is skipped
// — eliminating the multi-second segment scan on startup with millions of
// keys. v1-only WALs fall back to RecomputeStats as before.
func (t *TieredStore) initWAL(walDir string) error {
	t.walPath = walDir
	l, err := wal.OpenAsync(walDir, t.walSyncInterval)
	if err != nil {
		return err
	}
	t.wal = l
	if t.warm == nil {
		return nil
	}

	// Try to load the index snapshot first. If successful, the WAL is
	// replayed as a delta on top of the snapshot instead of from scratch.
	snapshotLoaded := false
	if snapPath := t.warm.SnapshotPath(); snapPath != "" {
		if err := t.warm.LoadSnapshot(snapPath); err != nil {
			t.logger.Warn("index snapshot load failed; falling back to WAL replay",
				"error", err)
		} else {
			snapshotLoaded = true
			t.logger.Info("index snapshot loaded",
				"entries", t.warm.IndexLen())
		}
	}

	allHaveSize := true
	rErr := wal.Replay(walDir, func(e wal.Entry) error {
		switch {
		case e.IsPut():
			if e.HasSize() {
				t.warm.SetIndexWithSize(e.Key, int(e.SegID), e.Offset, e.Size)
			} else {
				t.warm.SetIndex(e.Key, int(e.SegID), e.Offset)
				allHaveSize = false
			}
		case e.IsDelete():
			t.warm.DelIndex(e.Key)
		}
		return nil
	})
	if rErr != nil {
		t.logger.Warn("wal replay failed; warm-tier index may be incomplete",
			"error", rErr)
	}

	if snapshotLoaded {
		// Snapshot has correct sizes. WAL replay failure means recent
		// writes may be lost, but the base index is consistent. No
		// RecomputeStats needed.
		if rErr != nil {
			t.logger.Warn("wal replay failed after snapshot; recent writes may be lost",
				"error", rErr)
		}
		t.walEntryCount.Store(0)
		return nil
	}

	// No snapshot — may need full scan if WAL was empty/corrupt.
	needRebuild := rErr != nil || t.warm.IndexLen() == 0
	if needRebuild {
		if rErr != nil {
			t.logger.Warn("rebuilding index from segment scan after WAL replay error")
		} else {
			t.logger.Warn("wal replay produced empty index; rebuilding from segment scan")
		}
		if err := t.rebuildIndexFromScan(); err != nil {
			t.logger.Warn("segment scan index rebuild failed", "error", err)
		}
		// rebuildIndexFromScan populates sizes, so we can skip RecomputeStats.
		t.walEntryCount.Store(0)
		return nil
	}
	// Skip RecomputeStats when all WAL entries had size (v2 format).
	// This is the key startup optimization: with v2 WAL, we avoid a full
	// segment scan that takes seconds with millions of keys.
	if allHaveSize && t.warm.IndexLen() > 0 {
		t.warm.RecomputeStatsFromIndex()
		t.walEntryCount.Store(0)
		return nil
	}
	if err := t.warm.RecomputeStats(); err != nil {
		t.logger.Warn("warm-tier stats recompute failed; counters may be stale",
			"error", err)
	}
	t.walEntryCount.Store(0)
	return nil
}

// Get looks up the hot tier first. On a miss, the warm tier is
// consulted and the object is promoted back into the hot tier so
// the next hit is served from RAM. Returns api.SourceHot or
// api.SourceWarm depending on which tier served the hit.
func (t *TieredStore) Get(ctx context.Context, key api.Key) (*api.Object, api.Source, error) {
	obj, src, err := t.hot.Get(ctx, key)
	if err != nil {
		return nil, "", err
	}
	if obj != nil {
		return obj, src, nil
	}
	if t.warm == nil {
		return nil, "", nil
	}
	body, wErr := t.warm.Get(uint64(key))
	if wErr != nil {
		return nil, "", wErr
	}
	if body == nil {
		return nil, "", nil
	}
	loaded, decErr := decodeObject(body)
	if decErr != nil {
		// Unreadable blob (legacy codec version or corruption): evict
		// durably so the next Put rewrites it in the current format.
		// The warm tier has no TTL/LRU reaper, so without this the stale
		// blob poisons lookups forever (issue #171).
		t.logger.Warn("evicting undecodable warm blob",
			"key", key, "error", decErr)
		t.evictWarm(key)
		return nil, "", nil
	}
	// Re-derive transient fields not serialised to disk (tagged json:"-").
	// These fields exist purely for hit-path performance; recalculating them
	// once on warm-tier load restores the fast path for subsequent hits.
	if cc := loaded.Header.Get(header.CacheControl); cc != "" {
		loaded.CacheControl = cc
	}
	if age := loaded.Header.Get(header.Age); age != "" {
		var secs int64
		for _, b := range []byte(age) {
			if b < '0' || b > '9' {
				break
			}
			secs = secs*10 + int64(b-'0')
		}
		loaded.OriginAge = time.Duration(secs) * time.Second
	}
	// Promote to hot tier (best-effort: ignore error).
	_ = t.hot.Put(ctx, key, loaded)
	// Check lazy bans before serving: Ban() only touches the hot tier,
	// so warm-tier hits must be checked against the hot tier's active
	// ban list to avoid serving stale objects that were banned after
	// being demoted to warm.
	if t.hot.MatchesActiveBan(loaded) {
		_ = t.hot.Delete(ctx, key)
		return nil, "", nil
	}
	return loaded, api.SourceWarm, nil
}

// Put stores an object in the hot tier and, for large objects, also
// in the warm tier (with a WAL record).
func (t *TieredStore) Put(ctx context.Context, key api.Key, obj *api.Object) error {
	if err := t.hot.Put(ctx, key, obj); err != nil {
		return err
	}

	if t.warm != nil && obj.BodySize > t.bodyThreshold {
		body := encodeObject(obj)
		segID, offset, err := t.warm.Put(uint64(key), body) //nolint:gosec // segID fits int32
		if err != nil {
			if errors.Is(err, warm.ErrOverBudget) {
				// Hot tier already holds the object; warm-tier
				// rejection is non-fatal. The object won't survive
				// a restart, so log at Warn for operator visibility.
				t.logger.Warn("warm tier over budget, skipping warm write",
					"key", key)
				return nil
			}
			return err
		}
		// Mark the hot entry as having a backup so eviction
		// can prefer it under memory pressure.
		t.hot.SetBacked(key)
		// Mark the warm entry as protected so warm-tier eviction
		// skips it (the hot tier will re-sync it if evicted from warm).
		t.warm.Protect(uint64(key))
		if t.wal != nil {
			if err := t.warm.SyncSegment(segID); err != nil {
				return fmt.Errorf("warm: sync before wal append: %w", err)
			}
			recSize := int64(warm.HeaderLen + len(body) + warm.FooterLen)
			t.walEnqueue(wal.PutEntryWithSize(uint64(key), int32(segID), offset, recSize)) //nolint:gosec // segID bounded
			return nil
		}
	}
	return nil
}

// Delete removes from both tiers.
func (t *TieredStore) Delete(ctx context.Context, key api.Key) error {
	if err := t.hot.Delete(ctx, key); err != nil {
		return err
	}
	return t.evictWarmErr(key)
}

// evictWarm removes a key from the warm tier and appends a WAL delete
// record so the eviction survives restart. Best-effort: errors are
// logged, not returned, because callers (Get on decode failure) treat
// warm eviction as a hint to miss-and-refetch, not a hard failure.
func (t *TieredStore) evictWarm(key api.Key) {
	if err := t.evictWarmErr(key); err != nil {
		t.logger.Warn("warm evict failed on decode error",
			"key", key, "error", err)
	}
}

// evictWarmErr removes a key from the warm tier and appends a WAL delete
// record so the eviction survives restart. Errors are returned so
// callers (Delete) can surface them; Get uses the best-effort evictWarm
// wrapper instead.
func (t *TieredStore) evictWarmErr(key api.Key) error {
	if t.warm == nil {
		return nil
	}
	segID, err := t.warm.Delete(uint64(key))
	if err != nil {
		return err
	}
	if t.wal != nil {
		if err := t.warm.SyncSegment(segID); err != nil {
			return fmt.Errorf("warm: sync before wal append: %w", err)
		}
		t.walEnqueue(wal.DeleteEntry(uint64(key)))
		return nil
	}
	return nil
}

// Ban delegates to the hot tier's eager scan and registers a lazy ban
// that is also checked on warm-tier hits via MatchesActiveBan in Get.
func (t *TieredStore) Ban(ctx context.Context, expr api.BanExpr) (int, error) {
	return t.hot.Ban(ctx, expr)
}

// Keys returns the union of hot-tier and warm-tier cache keys.
// Reporting only hot-tier keys (as this method once did) caused a
// feedback loop with SIEVE eviction: evicted
// backed keys were seen as "missing" and backfilled via Put,
// re-overfilling the hot tier. The union reports keys the node *owns*,
// not just those currently in RAM (#175).
func (t *TieredStore) Keys() []api.Key {
	hotKeys := t.hot.Keys()
	if t.warm == nil {
		return hotKeys
	}
	warmKeys := t.warm.Keys()
	// Fast path: nothing in warm (common during cold start or ephemeral).
	if len(warmKeys) == 0 {
		return hotKeys
	}
	seen := make(map[api.Key]struct{}, len(hotKeys)+len(warmKeys))
	out := make([]api.Key, 0, len(hotKeys)+len(warmKeys))
	for _, k := range hotKeys {
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	for _, wk := range warmKeys {
		k := api.Key(wk)
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}
	return out
}

// WALStats returns async WAL metrics for Prometheus export. The engine
// polls this on the same ticker as Stats(). DroppedEntries is a delta
// (counter reset to zero on read). LastSyncTime is the timestamp of the
// last successful fsync. Returns zero values when WAL is not configured.
func (t *TieredStore) WALStats() (dropped int64, lastSync time.Time) {
	t.walMu.Lock()
	w := t.wal
	t.walMu.Unlock()
	if w == nil {
		return 0, time.Time{}
	}
	return w.DroppedEntries(), w.LastSyncTime()
}

// OverBudget reports whether the hot tier is over its configured byte
// budget. TieredStore consults this before promoting warm→hot to avoid
// fighting the eviction policy: promoting into an already-full hot
// tier causes SIEVE to evict the newly inserted keys, which then look
// "missing" again next round — the self-sustaining loop from #175.
// The warm tier is not budget-checked here because it has its own
// eviction policy (SIEVE) and compaction path — see warm.evictToFit.
func (t *TieredStore) OverBudget() bool {
	return t.hot.OverBudget()
}

// Stats merges hot + warm stats.
func (t *TieredStore) Stats() api.Stats {
	st := t.hot.Stats()
	if t.warm != nil {
		wEnt, wBytes := t.warm.Stats()
		st.WarmEntries = wEnt
		st.WarmBytes = wBytes
		st.WarmDiskBytes = t.warm.DiskBytes()
		st.WarmMaxBytes = t.warm.MaxBytes()
		st.WarmSelfHeals = t.warm.SelfHeals()
	}
	return st
}

// compactLoop runs the periodic warm-tier compaction until done is closed.
// After a successful compaction the WAL is rewritten with the live index
// so it stays bounded and restart replay is fast.
func (t *TieredStore) compactLoop() {
	defer t.compactWg.Done()
	startupDelay := t.compactStartupDelay
	if startupDelay <= 0 {
		startupDelay = 5 * time.Minute
	}
	startupTimer := time.NewTimer(startupDelay)
	defer startupTimer.Stop()
	ticker := time.NewTicker(30 * time.Minute)
	defer ticker.Stop()
	startupDone := false
	for {
		select {
		case <-t.done:
			return
		case <-startupTimer.C:
			startupDone = true
		case <-ticker.C:
			if !startupDone {
				continue // still in startup delay period
			}
			if t.warm.NeedsCompaction() {
				if err := t.warm.Compact(); err != nil {
					t.logger.Error("warm tier compaction failed", "error", err)
				} else {
					t.logger.Info("warm tier compaction complete")
					if err := t.rewriteWAL(); err != nil {
						t.logger.Error("wal rewrite after compaction failed", "error", err)
					} else {
						// Write a fresh snapshot after compaction —
						// segIDs and offsets changed, the old
						// snapshot is stale.
						if err := t.warm.WriteSnapshot(); err != nil {
							t.logger.Error("snapshot write after compaction failed", "error", err)
						}
						t.walEntryCount.Store(0)
					}
				}
			}
		}
	}
}

// warmSyncLoop periodically batches hot-only entries into the warm tier
// so they survive restarts. It also drains the tombstone queue to remove
// warm copies of evicted (unpopular) entries. Terminates when done is closed.
//
//nolint:contextcheck // creates its own context tied to t.done — no parent ctx available at construction time
func (t *TieredStore) warmSyncLoop() {
	defer t.syncWg.Done()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticker := time.NewTicker(t.warmSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.done:
			cancel()
			return
		case <-ticker.C:
			t.runWarmSyncCycle(ctx)
		}
	}
}

// runWarmSyncCycle performs one warm sync cycle: drain tombstones and warm
// eviction WAL entries, then batch-write hot-only entries to warm. All
// writes are batched with a single warm.Sync and a single WAL AppendBatch
// to minimise fsync.
func (t *TieredStore) runWarmSyncCycle(ctx context.Context) {
	if t.warm == nil {
		return
	}
	start := time.Now()

	var walEntries []wal.Entry
	tombstoned := t.drainTombstones(&walEntries)
	warmEvicted := t.drainWarmEvicts(&walEntries)

	// Skip promotion when the warm tier is over its byte budget — every
	// Put would return ErrOverBudget, wasting I/O and log noise. Tombstone
	// draining still runs because deletions free space, not consume it.
	// Re-checked each cycle so promotion resumes as soon as eviction or
	// compaction frees enough space (#205).
	var synced, skipped, skippedOverBudget int
	promotionSkipped := false
	if t.warm.OverBudget() {
		promotionSkipped = true
	} else {
		hotOnlyKeys := t.collectHotOnlyKeys()
		synced, skipped, skippedOverBudget = t.writeHotOnlyToWarm(ctx, hotOnlyKeys, &walEntries)
	}

	if len(walEntries) > 0 {
		if err := t.warm.Sync(); err != nil {
			t.logger.Warn("warm sync: warm sync failed", "error", err)
		}
		if t.wal != nil {
			t.walEnqueueBatch(walEntries)
		}
	}

	droppedTomb := t.droppedTombstones.Swap(0)
	droppedEvict := t.droppedWarmEvicts.Swap(0)

	t.logger.Info("warm sync cycle complete",
		"synced", synced,
		"tombstoned", tombstoned,
		"warm_evicted", warmEvicted,
		"skipped", skipped,
		"promotion_skipped", promotionSkipped,
		"skipped_over_budget", skippedOverBudget,
		"dropped_tombstones", droppedTomb,
		"dropped_warm_evicts", droppedEvict,
		"dur_ms", time.Since(start).Milliseconds(),
	)
	if droppedTomb > 0 || droppedEvict > 0 {
		t.logger.Warn("warm sync: eviction/tombstone WAL entries dropped (queue full)",
			"dropped_tombstones", droppedTomb,
			"dropped_warm_evicts", droppedEvict,
			"note", "evicted keys rely on rebuildIndexFromScan tombstones for durability; dropped WAL deletes delay replay recovery")
	}
}

// collectHotOnlyKeys returns hot keys that are not in the warm tier,
// capped at warmSyncBatchSize with rotation across cycles.
func (t *TieredStore) collectHotOnlyKeys() []api.Key {
	hotKeys := t.hot.Keys()
	warmKeys := t.warm.Keys()
	warmSet := make(map[uint64]struct{}, len(warmKeys))
	for _, wk := range warmKeys {
		warmSet[wk] = struct{}{}
	}
	var hotOnlyKeys []api.Key
	for _, k := range hotKeys {
		if _, inWarm := warmSet[uint64(k)]; !inWarm {
			hotOnlyKeys = append(hotOnlyKeys, k)
		}
	}
	// Apply batch size cap with rotation.
	total := len(hotOnlyKeys)
	if total > t.warmSyncBatchSize {
		startIdx := t.warmSyncOffset % total
		endIdx := startIdx + t.warmSyncBatchSize
		if endIdx <= total {
			hotOnlyKeys = hotOnlyKeys[startIdx:endIdx]
		} else {
			hotOnlyKeys = append(hotOnlyKeys[startIdx:], hotOnlyKeys[:endIdx-total]...)
		}
		t.warmSyncOffset = endIdx % total
	}
	return hotOnlyKeys
}

// drainTombstones removes all pending tombstone keys from the warm tier
// and collects WAL delete entries. The warm tier is synced once after
// all tombstones are processed (by the caller's warm.Sync call), and the
// WAL delete entries are appended in a single batch. If a crash occurs
// between warm.Sync and wal.AppendBatch, the tombstoned keys could be
// resurrected on restart — but rebuildIndexFromScan honours tombstones
// in segment order, so the fallback path self-corrects.
func (t *TieredStore) drainTombstones(walEntries *[]wal.Entry) int {
	tombstoned := 0
	for {
		select {
		case key := <-t.tombstoneQueue:
			if _, err := t.warm.Delete(uint64(key)); err != nil {
				t.logger.Debug("warm sync: tombstone delete failed",
					"key", key, "error", err)
				continue
			}
			*walEntries = append(*walEntries, wal.DeleteEntry(uint64(key)))
			tombstoned++
		default:
			return tombstoned
		}
	}
}

// drainWarmEvicts appends WAL delete entries for keys evicted by the warm
// tier's eviction policy. The tombstone is already on disk (Evict calls
// Delete internally); the WAL entry ensures the eviction survives restart
// so replay doesn't resurrect the evicted key.
func (t *TieredStore) drainWarmEvicts(walEntries *[]wal.Entry) int {
	evicted := 0
	for {
		select {
		case key := <-t.warmEvictQueue:
			*walEntries = append(*walEntries, wal.DeleteEntry(uint64(key)))
			evicted++
		default:
			return evicted
		}
	}
}

// writeHotOnlyToWarm writes the given hot-only keys to the warm tier and
// collects WAL put entries. Returns synced, skipped (non-budget errors),
// and skippedOverBudget counts. When warm.Put returns ErrOverBudget the
// loop stops immediately — the warm tier is full, so every subsequent Put
// would also fail, wasting I/O. The caller logs the count so operators
// see backpressure (#205).
func (t *TieredStore) writeHotOnlyToWarm(ctx context.Context, hotOnlyKeys []api.Key, walEntries *[]wal.Entry) (synced, skipped, skippedOverBudget int) {
	for _, key := range hotOnlyKeys {
		obj, _, err := t.hot.Get(ctx, key)
		if err != nil || obj == nil {
			skipped++
			continue
		}
		body := encodeObject(obj)
		segID, offset, err := t.warm.Put(uint64(key), body)
		if err != nil {
			if errors.Is(err, warm.ErrOverBudget) {
				// Remaining keys including this one — not yet
				// counted in synced or skipped.
				skippedOverBudget = len(hotOnlyKeys) - synced - skipped
				t.logger.Info("warm sync: warm put over budget, stopping promotion",
					"key", key,
					"skipped_over_budget", skippedOverBudget,
				)
				return synced, skipped, skippedOverBudget
			}
			t.logger.Debug("warm sync: warm put failed",
				"key", key, "error", err)
			skipped++
			continue
		}
		recSize := int64(warm.HeaderLen + len(body) + warm.FooterLen)
		*walEntries = append(*walEntries, wal.PutEntryWithSize(uint64(key), int32(segID), offset, recSize)) //nolint:gosec // segID bounded
		t.hot.SetBacked(key)
		t.warm.Protect(uint64(key))
		synced++
	}
	return synced, skipped, skippedOverBudget
}

// rebuildIndexFromScan rebuilds the warm-tier index by scanning all
// segment records in append order. Tombstones remove the key from the
// index so deleted keys are not resurrected after WAL loss.
func (t *TieredStore) rebuildIndexFromScan() error {
	if t.warm == nil {
		return nil
	}
	return t.warm.Scan(func(r warm.Record) error {
		if r.IsTomb {
			t.warm.DelIndex(r.Key)
		} else {
			t.warm.SetIndex(r.Key, r.SegID, r.Offset)
		}
		return nil
	})
}

// walEnqueue wraps wal.Enqueue with checkpoint awareness. While a
// checkpoint is in progress (checkpointing == true), WAL enqueues spin
// via runtime.Gosched until the checkpoint unblocks. The spin window
// is ~2ms (second Sync + Truncate). After enqueuing, the entry count
// is incremented for the checkpoint threshold trigger.
func (t *TieredStore) walEnqueue(entry wal.Entry) {
	for t.checkpointing.Load() {
		runtime.Gosched()
	}
	t.walMu.Lock()
	defer t.walMu.Unlock()
	if t.wal != nil {
		_ = t.wal.Enqueue(entry)
		t.walEntryCount.Add(1)
	}
}

// walEnqueueBatch wraps wal.EnqueueBatch with checkpoint awareness.
// Same spin semantics as walEnqueue. The entry count is incremented by
// the batch size.
func (t *TieredStore) walEnqueueBatch(entries []wal.Entry) {
	for t.checkpointing.Load() {
		runtime.Gosched()
	}
	t.walMu.Lock()
	defer t.walMu.Unlock()
	if t.wal != nil {
		t.wal.EnqueueBatch(entries)
		t.walEntryCount.Add(int64(len(entries)))
	}
}

// checkpoint performs a crash-safe WAL checkpoint: flush the WAL, block
// writes, flush again, write the index snapshot, truncate the WAL, then
// unblock writes. The snapshot is written before the WAL is truncated so
// that a crash between snapshot and truncate leaves the WAL intact —
// restart loads the old snapshot and replays the WAL (idempotent). If the
// snapshot write fails, the WAL is not truncated and no data is lost.
// After a successful checkpoint, the WAL is empty and the snapshot
// captures the full index state — restart loads the snapshot + replays
// the (empty or small) WAL instead of scanning all segments.
func (t *TieredStore) checkpoint() error {
	// Step 1: Flush all pending WAL entries to disk.
	t.walMu.Lock()
	if t.wal != nil {
		if err := t.wal.Sync(); err != nil {
			t.walMu.Unlock()
			return fmt.Errorf("checkpoint: flush wal: %w", err)
		}
	}
	t.walMu.Unlock()

	// Step 2: Block WAL writes. Any Enqueue call now spins until
	// the block is released. The block window covers the second
	// flush + snapshot I/O (~350ms at 10M entries).
	t.checkpointing.Store(true)

	// Step 3: Flush any WAL entries written between step 1 and step 2.
	// These entries' index updates are already in the in-memory index
	// (Put updates index before Enqueue). The flush ensures they're on
	// disk before the snapshot captures the index.
	t.walMu.Lock()
	if t.wal != nil {
		if err := t.wal.Sync(); err != nil {
			t.walMu.Unlock()
			t.checkpointing.Store(false)
			return fmt.Errorf("checkpoint: second flush: %w", err)
		}
	}
	t.walMu.Unlock()

	// Step 4: Write the snapshot from the current index state. This
	// takes a consistent copy under idxMu.RLock (~50ms at 10M entries)
	// then writes lock-free (~300ms I/O). The snapshot captures all
	// writes up to this point. If this fails, the WAL is still intact
	// and no data is lost — we unblock and return an error.
	if err := t.warm.WriteSnapshot(); err != nil {
		t.checkpointing.Store(false)
		return fmt.Errorf("checkpoint: snapshot: %w", err)
	}

	// Step 5: Truncate the WAL. Safe now — the snapshot on disk
	// captures the full index state. All entries up to this point are
	// reflected in both the in-memory index and the snapshot.
	t.walMu.Lock()
	if t.wal != nil {
		if err := t.wal.Truncate(); err != nil {
			t.walMu.Unlock()
			t.checkpointing.Store(false)
			return fmt.Errorf("checkpoint: truncate wal: %w", err)
		}
		t.walEntryCount.Store(0)
	}
	t.walMu.Unlock()

	// Step 6: Unblock WAL writes. New entries go into the fresh WAL.
	// On restart, the snapshot is loaded first, then WAL replay
	// overwrites/extends it (idempotent).
	t.checkpointing.Store(false)

	return nil
}

// checkpointLoop runs periodic checkpoints and threshold-triggered
// checkpoints. A checkpoint writes an index snapshot and truncates
// the WAL, bounding WAL replay time on restart.
func (t *TieredStore) checkpointLoop() {
	defer t.checkpointWg.Done()
	// Check frequently enough to catch the threshold trigger in a
	// timely manner without burning CPU. The interval tick still
	// gates the periodic checkpoint; the check tick only decides
	// whether to evaluate the conditions.
	checkInterval := t.checkpointInterval
	if checkInterval > 10*time.Second {
		checkInterval = 10 * time.Second
	}
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	lastCheckpoint := time.Now()
	for {
		select {
		case <-t.done:
			return
		case <-ticker.C:
			count := t.walEntryCount.Load()
			if count == 0 {
				continue
			}
			elapsed := time.Since(lastCheckpoint)
			if elapsed >= t.checkpointInterval || count >= t.checkpointWALThreshold {
				if err := t.checkpoint(); err != nil {
					t.logger.Warn("checkpoint failed", "error", err)
				}
				lastCheckpoint = time.Now()
			}
		}
	}
}

// rewriteWAL writes a fresh WAL containing all live warm-tier index
// entries, then atomically replaces the old WAL file. Called after a
// successful warm-tier compaction so the WAL stays bounded.
func (t *TieredStore) rewriteWAL() error {
	t.walMu.Lock()
	defer t.walMu.Unlock()
	if t.wal == nil || t.walPath == "" {
		return nil
	}
	tmpPath := t.walPath + ".tmp"
	tmpLog, err := wal.Open(tmpPath)
	if err != nil {
		return fmt.Errorf("wal rewrite: open tmp: %w", err)
	}
	var walEntries []wal.Entry
	for _, key := range t.warm.Keys() {
		segID, offset, size, ok := t.warm.LookupWithSize(key)
		if !ok {
			continue
		}
		if size > 0 {
			walEntries = append(walEntries, wal.PutEntryWithSize(key, int32(segID), offset, size)) //nolint:gosec // segID bounded
		} else {
			walEntries = append(walEntries, wal.PutEntry(key, int32(segID), offset)) //nolint:gosec // segID bounded
		}
	}
	if err := tmpLog.AppendBatch(walEntries); err != nil {
		_ = tmpLog.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("wal rewrite: batch write: %w", err)
	}
	if err := tmpLog.Close(); err != nil {
		return fmt.Errorf("wal rewrite: close tmp: %w", err)
	}
	if err := t.wal.Close(); err != nil {
		t.wal = nil
		return fmt.Errorf("wal rewrite: close old: %w", err)
	}
	t.wal = nil // prevent double-close in Close() if rename fails
	if err := os.Rename(tmpPath, t.walPath); err != nil {
		// Rename failed: old WAL file is still at t.walPath but closed.
		// Clean up the leaked tmp file, then reopen the old WAL so it
		// remains usable for subsequent appends and Close.
		_ = os.Remove(tmpPath)
		reopened, reopenErr := wal.OpenAsync(t.walPath, t.walSyncInterval)
		if reopenErr != nil {
			return fmt.Errorf("wal rewrite: rename: %w; reopen old WAL: %v", err, reopenErr)
		}
		t.wal = reopened
		return fmt.Errorf("wal rewrite: rename: %w", err)
	}
	t.wal, err = wal.OpenAsync(t.walPath, t.walSyncInterval)
	if err != nil {
		return fmt.Errorf("wal rewrite: reopen: %w", err)
	}
	t.logger.Info("wal rewritten after compaction", "path", t.walPath)
	return nil
}

// WindowHits returns the per-window hit count for key from the hot tier.
// Returns 0 if the key is not in the hot tier (warm-only objects have no
// hot-tier counter). Delegates to HotStore.WindowHits.
func (t *TieredStore) WindowHits(key api.Key) int64 {
	return t.hot.WindowHits(key)
}

// Close shuts down the WAL, warm tier, and hot tier in order.
// The compaction and warm-sync goroutines are stopped and joined
// before the warm store is closed, preventing use-after-close on file handles.
func (t *TieredStore) Close(ctx context.Context) error {
	close(t.done)
	t.compactWg.Wait()
	t.syncWg.Wait()
	t.checkpointWg.Wait()

	t.walMu.Lock()
	if t.wal != nil {
		lastSync := t.wal.LastSyncTime()
		if !lastSync.IsZero() && time.Since(lastSync) > 2*t.walSyncInterval {
			t.logger.Warn("wal sync loop appears stuck before close",
				"last_sync_ago", time.Since(lastSync))
		}
		if err := t.wal.Sync(); err != nil {
			t.logger.Warn("wal sync on close failed", "error", err)
		}
		if err := t.wal.Close(); err != nil {
			t.logger.Warn("wal close error", "error", err)
		}
	}
	t.walMu.Unlock()
	if t.warm != nil {
		if err := t.warm.Close(); err != nil {
			t.logger.Warn("warm close error", "error", err)
		}
	}
	return t.hot.Close(ctx)
}
