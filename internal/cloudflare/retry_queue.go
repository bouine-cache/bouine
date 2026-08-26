package cloudflare

import (
	"context"
	"strconv"
	"sync"
	"time"
)

// RetryQueueConfig configures the DLQ retry queue.
type RetryQueueConfig struct {
	// MaxQueueSize is the maximum number of items the queue can hold.
	// When full, new failed items are dropped. Default 1000 when 0.
	MaxQueueSize int
	// MaxRetries is the maximum number of retry attempts per item.
	// After this, the item is dropped. Default 3 when 0.
	MaxRetries int
	// BaseDelay is the initial retry delay. Delays grow exponentially:
	// baseDelay * 2^attempt. Default 1s when 0.
	BaseDelay time.Duration
	// MaxDelay caps the exponential backoff. Default 30s when 0.
	MaxDelay time.Duration
}

const (
	defaultMaxQueueSize = 1000
	defaultMaxRetries   = 3
	defaultBaseDelay    = 1 * time.Second
	defaultMaxDelay     = 30 * time.Second
)

// queueItem holds a failed purge item and its retry metadata.
type queueItem struct {
	nextRetry time.Time
	value     string
	kind      BatchKind
	attempts  int
}

// RetryQueue holds failed purge items and retries them with exponential
// backoff. Items are deduplicated on enqueue — if an item is already in
// the queue, it is not added again. The queue is bounded; when full, new
// items are dropped.
//
// The retry goroutine reads items from the queue, waits until their
// nextRetry time, then re-adds them to the batcher for processing.
type RetryQueue struct {
	closeCtx context.Context
	onExpire func(kind BatchKind)
	items    map[string]*queueItem // key = kind:value
	nowFunc  func() time.Time
	// retryFn is called to retry an item. If it returns nil, the item
	// is removed from the queue (success). If it returns an error, the
	// item stays in the queue with an incremented attempts count and
	// a new nextRetry time (exponential backoff).
	retryFn func(ctx context.Context, kind BatchKind, value string) error
	// Metrics callbacks (optional, nil-safe).
	onEnqueue func(kind BatchKind)
	onDrop    func(kind BatchKind)
	onRetry   func(kind BatchKind)
	onDepth   func(depth int)
	cancel    context.CancelFunc
	order     []string // FIFO order for items with same nextRetry
	cfg       RetryQueueConfig
	wg        sync.WaitGroup
	mu        sync.Mutex
}

// NewRetryQueue creates a RetryQueue. The retry goroutine starts immediately.
// retryFn is called to retry items. If retryFn returns nil, the item is
// removed from the queue (success). If it returns an error, the item stays
// with incremented attempts and exponential backoff.
func NewRetryQueue(
	ctx context.Context,
	cfg RetryQueueConfig,
	retryFn func(ctx context.Context, kind BatchKind, value string) error,
	metrics RetryQueueMetrics,
) *RetryQueue {
	if cfg.MaxQueueSize <= 0 {
		cfg.MaxQueueSize = defaultMaxQueueSize
	}
	if cfg.MaxRetries <= 0 {
		cfg.MaxRetries = defaultMaxRetries
	}
	if cfg.BaseDelay <= 0 {
		cfg.BaseDelay = defaultBaseDelay
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = defaultMaxDelay
	}

	closeCtx, cancel := context.WithCancel(ctx)

	rq := &RetryQueue{
		cfg:      cfg,
		items:    make(map[string]*queueItem),
		nowFunc:  time.Now,
		retryFn:  retryFn,
		closeCtx: closeCtx,
		cancel:   cancel,
	}

	if metrics.OnEnqueue != nil {
		rq.onEnqueue = metrics.OnEnqueue
	}
	if metrics.OnDrop != nil {
		rq.onDrop = metrics.OnDrop
	}
	if metrics.OnRetry != nil {
		rq.onRetry = metrics.OnRetry
	}
	if metrics.OnExpire != nil {
		rq.onExpire = metrics.OnExpire
	}
	if metrics.OnDepth != nil {
		rq.onDepth = metrics.OnDepth
	}

	rq.wg.Add(1)
	go rq.retryLoop()

	return rq
}

// RetryQueueMetrics holds optional callbacks for retry queue observability.
type RetryQueueMetrics struct {
	OnEnqueue func(kind BatchKind)
	OnDrop    func(kind BatchKind)
	OnRetry   func(kind BatchKind)
	OnExpire  func(kind BatchKind)
	// OnDepth is called with the current queue depth after any change.
	OnDepth func(depth int)
}

// itemKey returns a unique key for an item in the queue.
func itemKey(kind BatchKind, value string) string {
	return strconv.Itoa(int(kind)) + ":" + value
}

// Enqueue adds a failed item to the retry queue. If the item is already
// in the queue, the retry count is not incremented (the item keeps its
// original attempts count). If the queue is full, the item is dropped.
// If the item has already exceeded MaxRetries, it is expired (dropped).
func (rq *RetryQueue) Enqueue(kind BatchKind, value string) {
	key := itemKey(kind, value)

	rq.mu.Lock()

	if _, ok := rq.items[key]; ok {
		rq.mu.Unlock()
		return
	}

	if len(rq.items) >= rq.cfg.MaxQueueSize {
		rq.mu.Unlock()
		if rq.onDrop != nil {
			rq.onDrop(kind)
		}
		return
	}

	now := rq.nowFunc()
	item := &queueItem{
		kind:      kind,
		value:     value,
		attempts:  0,
		nextRetry: now.Add(rq.cfg.BaseDelay),
	}
	rq.items[key] = item
	rq.order = append(rq.order, key)
	depth := len(rq.items)
	rq.mu.Unlock()

	if rq.onEnqueue != nil {
		rq.onEnqueue(kind)
	}
	if rq.onDepth != nil {
		rq.onDepth(depth)
	}
}

// Len returns the current queue depth.
func (rq *RetryQueue) Len() int {
	rq.mu.Lock()
	defer rq.mu.Unlock()
	return len(rq.items)
}

// Close stops the retry goroutine.
func (rq *RetryQueue) Close() {
	rq.cancel()
	rq.wg.Wait()
}

func (rq *RetryQueue) retryLoop() {
	defer rq.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-rq.closeCtx.Done():
			return
		case <-ticker.C:
			rq.processReady()
		}
	}
}

func (rq *RetryQueue) processReady() {
	now := rq.nowFunc()

	rq.mu.Lock()
	var ready []*queueItem
	var expired []string

	for key, item := range rq.items {
		if now.Before(item.nextRetry) {
			continue
		}
		if item.attempts >= rq.cfg.MaxRetries {
			expired = append(expired, key)
			continue
		}
		item.attempts++
		ready = append(ready, item)
	}

	for _, key := range expired {
		item := rq.items[key]
		delete(rq.items, key)
		rq.removeFromOrder(key)
		if rq.onExpire != nil && item != nil {
			rq.onExpire(item.kind)
		}
	}

	depth := len(rq.items)
	rq.mu.Unlock()

	if rq.onDepth != nil && (len(expired) > 0) {
		rq.onDepth(depth)
	}

	// Process ready items outside the lock.
	for _, item := range ready {
		if rq.onRetry != nil {
			rq.onRetry(item.kind)
		}
		err := rq.retryFn(rq.closeCtx, item.kind, item.value)
		if err != nil {
			delay := rq.exponentialDelay(item.attempts)
			rq.mu.Lock()
			item.nextRetry = rq.nowFunc().Add(delay)
			rq.mu.Unlock()
		} else {
			rq.mu.Lock()
			key := itemKey(item.kind, item.value)
			delete(rq.items, key)
			rq.removeFromOrder(key)
			depth = len(rq.items)
			rq.mu.Unlock()
			if rq.onDepth != nil {
				rq.onDepth(depth)
			}
		}
	}
}

func (rq *RetryQueue) exponentialDelay(attempts int) time.Duration {
	delay := rq.cfg.BaseDelay * time.Duration(1<<uint(attempts-1))
	if delay > rq.cfg.MaxDelay {
		delay = rq.cfg.MaxDelay
	}
	return delay
}

func (rq *RetryQueue) removeFromOrder(key string) {
	for i, k := range rq.order {
		if k == key {
			rq.order = append(rq.order[:i], rq.order[i+1:]...)
			return
		}
	}
}
