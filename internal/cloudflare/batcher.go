package cloudflare

import (
	"context"
	"sync"
	"time"
)

// BatchConfig configures the CF purge batcher.
type BatchConfig struct {
	// MaxBatchSize is the maximum number of items (URLs, tags, prefixes, or
	// hosts) coalesced into a single CF API call. CF's PurgeSingleFile
	// supports up to 30 URLs per call; tags/prefixes/hosts have higher limits.
	// When MaxBatchSize is reached the batch flushes immediately.
	// 0 disables batching (passthrough mode — one API call per request).
	MaxBatchSize int
	// MaxWait is the maximum time an item waits in the batch before a flush
	// is triggered, even if MaxBatchSize is not reached. This bounds latency
	// for low-traffic periods. Default 500ms when 0 and batching is enabled.
	MaxWait time.Duration
}

// defaultBatchMaxWait is the default max wait when BatchConfig.MaxWait is 0.
const defaultBatchMaxWait = 500 * time.Millisecond

// BatchKind identifies which CF purge operation a batch entry belongs to.
// Entries of different kinds are never mixed in a single CF API call.
type BatchKind int

const (
	// KindURLs represents URL purge items (PurgeSingleFile).
	KindURLs BatchKind = iota
	// KindTags represents cache tag purge items (PurgeByTags).
	KindTags
	// KindPrefixes represents URL prefix purge items (PurgeByPrefixes).
	KindPrefixes
	// KindHosts represents hostname purge items (PurgeByHostnames).
	KindHosts
)

// batchBucket collects entries of a single kind. Each kind has its own
// bucket so flushes map cleanly to one CF API call per kind.
type batchBucket struct {
	items   map[string]struct{}
	flushCh chan struct{}
	mu      sync.Mutex
}

func newBatchBucket() *batchBucket {
	return &batchBucket{
		items:   make(map[string]struct{}),
		flushCh: make(chan struct{}, 1),
	}
}

func (b *batchBucket) add(value string) bool {
	b.mu.Lock()
	if _, exists := b.items[value]; exists {
		b.mu.Unlock()
		return false // duplicate
	}
	b.items[value] = struct{}{}
	b.mu.Unlock()
	return true
}

func (b *batchBucket) drain() []string {
	b.mu.Lock()
	items := make([]string, 0, len(b.items))
	for v := range b.items {
		items = append(items, v)
	}
	b.items = make(map[string]struct{})
	b.mu.Unlock()
	return items
}

func (b *batchBucket) len() int {
	b.mu.Lock()
	n := len(b.items)
	b.mu.Unlock()
	return n
}

// FlushFn is the callback invoked when a bucket flushes. It receives the
// deduplicated, coalesced items and performs the actual CF API call.
type FlushFn func(ctx context.Context, items []string) error

// Batcher coalesces individual CF purge requests into batched, deduplicated
// API calls. Each kind (URLs, tags, prefixes, hosts) has its own bucket and
// flush goroutine. Items are flushed when MaxBatchSize is reached or MaxWait
// elapses, whichever comes first.
//
// When MaxBatchSize is 0 the batcher operates in passthrough mode: every
// add call immediately invokes the flush function without dedup or delay.
type Batcher struct {
	closeCtx context.Context
	buckets  map[BatchKind]*batchBucket
	flushFns map[BatchKind]FlushFn
	cancel   context.CancelFunc
	// Metrics callbacks (optional, nil-safe).
	onFlush    func(kind BatchKind, count int)
	onDedup    func(kind BatchKind)
	onFlushErr func(kind BatchKind, err error)
	cfg        BatchConfig
	wg         sync.WaitGroup
}

// NewBatcher creates a Batcher. The flush functions map defines one callback
// per kind. A nil flushFn for a kind means items of that kind are silently
// dropped. The batcher starts background flush goroutines for each kind.
func NewBatcher(
	ctx context.Context,
	cfg BatchConfig,
	flushFns map[BatchKind]FlushFn,
	metrics BatchMetrics,
) *Batcher {
	if cfg.MaxBatchSize == 0 {
		// Passthrough mode — no goroutines, no buckets.
		return &Batcher{
			cfg:      cfg,
			flushFns: flushFns,
		}
	}

	if cfg.MaxWait <= 0 {
		cfg.MaxWait = defaultBatchMaxWait
	}

	closeCtx, cancel := context.WithCancel(ctx)

	b := &Batcher{
		cfg:      cfg,
		buckets:  make(map[BatchKind]*batchBucket),
		flushFns: flushFns,
		closeCtx: closeCtx,
		cancel:   cancel,
	}

	if metrics.OnFlush != nil {
		b.onFlush = metrics.OnFlush
	}
	if metrics.OnDedup != nil {
		b.onDedup = metrics.OnDedup
	}
	if metrics.OnFlushErr != nil {
		b.onFlushErr = metrics.OnFlushErr
	}

	for kind, fn := range flushFns {
		if fn == nil {
			continue
		}
		bucket := newBatchBucket()
		b.buckets[kind] = bucket
		b.wg.Add(1)
		go b.flushLoop(kind, bucket, fn)
	}

	return b
}

// BatchMetrics holds optional callbacks for batcher observability.
type BatchMetrics struct {
	OnFlush    func(kind BatchKind, count int)
	OnDedup    func(kind BatchKind)
	OnFlushErr func(kind BatchKind, err error)
}

// Add enqueues an item of the given kind. In passthrough mode (MaxBatchSize
// == 0), the flush function is called immediately and synchronously.
// In batched mode, the item is deduplicated against pending items of the
// same kind. If the bucket reaches MaxBatchSize the flush is triggered
// immediately.
func (b *Batcher) Add(ctx context.Context, kind BatchKind, value string) {
	if b.cfg.MaxBatchSize == 0 {
		// Passthrough: flush immediately.
		fn, ok := b.flushFns[kind]
		if ok && fn != nil {
			if err := fn(ctx, []string{value}); err != nil && b.onFlushErr != nil {
				b.onFlushErr(kind, err)
			}
			if b.onFlush != nil {
				b.onFlush(kind, 1)
			}
		}
		return
	}

	bucket, ok := b.buckets[kind]
	if !ok {
		// No bucket for this kind — drop silently.
		return
	}

	added := bucket.add(value)
	if !added {
		if b.onDedup != nil {
			b.onDedup(kind)
		}
		return
	}

	// Trigger immediate flush if batch is full.
	if bucket.len() >= b.cfg.MaxBatchSize {
		select {
		case bucket.flushCh <- struct{}{}:
		default:
		}
	}
}

// Flush forces an immediate flush of all buckets. It blocks until all
// pending items have been sent or ctx expires.
func (b *Batcher) Flush(ctx context.Context) {
	if b.cfg.MaxBatchSize == 0 {
		return
	}
	for _, bucket := range b.buckets {
		select {
		case bucket.flushCh <- struct{}{}:
		default:
		}
	}
}

// Close stops all flush goroutines and waits for them to exit. It does not
// flush pending items — call Flush before Close if draining is required.
func (b *Batcher) Close() {
	if b.cfg.MaxBatchSize == 0 {
		return
	}
	b.cancel()
	b.wg.Wait()
}

func (b *Batcher) flushLoop(kind BatchKind, bucket *batchBucket, fn FlushFn) {
	defer b.wg.Done()

	timer := time.NewTimer(b.cfg.MaxWait)
	defer timer.Stop()

	for {
		select {
		case <-b.closeCtx.Done():
			// Final flush on shutdown.
			b.doFlush(kind, bucket, fn)
			return
		case <-bucket.flushCh:
			b.doFlush(kind, bucket, fn)
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(b.cfg.MaxWait)
		case <-timer.C:
			b.doFlush(kind, bucket, fn)
			timer.Reset(b.cfg.MaxWait)
		}
	}
}

func (b *Batcher) doFlush(kind BatchKind, bucket *batchBucket, fn FlushFn) {
	items := bucket.drain()
	if len(items) == 0 {
		return
	}
	// Chunk items to respect CF API limits. PurgeSingleFile max 30 URLs;
	// tags/prefixes/hosts have higher limits but chunking is safe for all.
	maxChunk := b.cfg.MaxBatchSize
	if maxChunk <= 0 {
		maxChunk = len(items)
	}
	for start := 0; start < len(items); start += maxChunk {
		end := start + maxChunk
		if end > len(items) {
			end = len(items)
		}
		chunk := items[start:end]
		err := fn(b.closeCtx, chunk)
		if b.onFlush != nil {
			b.onFlush(kind, len(chunk))
		}
		if err != nil && b.onFlushErr != nil {
			b.onFlushErr(kind, err)
		}
	}
}
