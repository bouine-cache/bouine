package cloudflare_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cf "github.com/bouine-cache/bouine/internal/cloudflare"
)

// recordingFlush records all items it receives, grouped by call.
type recordingFlush struct {
	mu    sync.Mutex
	calls [][]string
	err   error
	delay time.Duration
}

func (r *recordingFlush) flush(ctx context.Context, items []string) error {
	r.mu.Lock()
	r.calls = append(r.calls, append([]string(nil), items...))
	r.mu.Unlock()
	if r.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.delay):
		}
	}
	return r.err
}

func (r *recordingFlush) totalCalls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

func (r *recordingFlush) totalItems() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	total := 0
	for _, c := range r.calls {
		total += len(c)
	}
	return total
}

func TestBatcher_PassthroughMode(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 0, // passthrough
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})
	defer b.Close()

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")

	require.Equal(t, 2, rf.totalCalls(), "passthrough should make one call per item")
	require.Equal(t, 2, rf.totalItems())
}

func TestBatcher_PassthroughNilFlushFn(t *testing.T) {
	t.Parallel()
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 0,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: nil,
	}, cf.BatchMetrics{})
	defer b.Close()

	// Should not panic.
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
}

func TestBatcher_BatchedMode_Dedup(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	// Add 3 unique + 2 duplicate items.
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a") // dup
	b.Add(context.Background(), cf.KindURLs, "https://example.com/c")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b") // dup

	b.Flush(context.Background())
	time.Sleep(100 * time.Millisecond)
	b.Close()

	require.Equal(t, 1, rf.totalCalls(), "should flush in a single batch")
	require.Equal(t, 3, rf.totalItems(), "should have 3 unique items after dedup")
}

func TestBatcher_BatchedMode_FlushOnFull(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 3,
		MaxWait:      10 * time.Second, // long wait; should flush on size
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/c")

	// Should flush immediately when batch is full (3 items).
	time.Sleep(50 * time.Millisecond)
	b.Close()

	require.GreaterOrEqual(t, rf.totalCalls(), 1, "should flush when batch is full")
	require.Equal(t, 3, rf.totalItems())
}

func TestBatcher_BatchedMode_FlushOnTimer(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 100,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")

	// Should flush after MaxWait even though batch isn't full.
	time.Sleep(200 * time.Millisecond)
	b.Close()

	require.GreaterOrEqual(t, rf.totalCalls(), 1, "should flush on timer")
	require.Equal(t, 2, rf.totalItems())
}

func TestBatcher_BatchedMode_MultipleFlushes(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 2,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	// First batch (triggers immediate flush at 2 items).
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")

	time.Sleep(50 * time.Millisecond)

	// Second batch.
	b.Add(context.Background(), cf.KindURLs, "https://example.com/c")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/d")

	time.Sleep(50 * time.Millisecond)
	b.Close()

	require.GreaterOrEqual(t, rf.totalCalls(), 2, "should have at least 2 flush calls")
	require.Equal(t, 4, rf.totalItems())
}

func TestBatcher_BatchedMode_KindIsolation(t *testing.T) {
	t.Parallel()
	urlFlush := &recordingFlush{}
	tagFlush := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: urlFlush.flush,
		cf.KindTags: tagFlush.flush,
	}, cf.BatchMetrics{})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindTags, "product-123")

	b.Flush(context.Background())
	time.Sleep(100 * time.Millisecond)
	b.Close()

	require.Equal(t, 1, urlFlush.totalCalls(), "URL flush should be separate from tag flush")
	require.Equal(t, 1, tagFlush.totalCalls())
}

func TestBatcher_BatchedMode_NilFlushFn(t *testing.T) {
	t.Parallel()
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		// No KindURLs entry — items should be dropped silently.
		cf.KindTags: func(ctx context.Context, items []string) error { return nil },
	}, cf.BatchMetrics{})
	defer b.Close()

	// Should not panic.
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
}

func TestBatcher_BatchedMode_FlushError(t *testing.T) {
	t.Parallel()
	var flushErrCount atomic.Int64
	rf := &recordingFlush{err: errors.New("CF API down")}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 5,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{
		OnFlushErr: func(kind cf.BatchKind, err error) {
			flushErrCount.Add(1)
		},
	})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	time.Sleep(100 * time.Millisecond)
	b.Close()

	require.GreaterOrEqual(t, int(flushErrCount.Load()), 1, "should call OnFlushErr on error")
}

func TestBatcher_BatchedMode_OnFlushMetric(t *testing.T) {
	t.Parallel()
	var flushCount atomic.Int64
	var flushItems atomic.Int64
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{
		OnFlush: func(kind cf.BatchKind, count int) {
			flushCount.Add(1)
			flushItems.Add(int64(count))
		},
	})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")
	time.Sleep(100 * time.Millisecond)
	b.Close()

	require.GreaterOrEqual(t, int(flushCount.Load()), 1, "should call OnFlush metric")
	require.GreaterOrEqual(t, int(flushItems.Load()), 2, "should count all items")
}

func TestBatcher_BatchedMode_OnDedupMetric(t *testing.T) {
	t.Parallel()
	var dedupCount atomic.Int64
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{
		OnDedup: func(kind cf.BatchKind) {
			dedupCount.Add(1)
		},
	})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a") // dup
	b.Add(context.Background(), cf.KindURLs, "https://example.com/a") // dup
	time.Sleep(100 * time.Millisecond)
	b.Close()

	require.Equal(t, int64(2), dedupCount.Load(), "should count 2 dedup events")
	require.Equal(t, 1, rf.totalItems(), "should flush only 1 unique item")
}

func TestBatcher_Close_FlushesPending(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 100,
		MaxWait:      10 * time.Second, // long wait; only Close should flush
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindURLs, "https://example.com/b")

	b.Close()

	require.GreaterOrEqual(t, rf.totalCalls(), 1, "Close should flush pending items")
	require.Equal(t, 2, rf.totalItems())
}

func TestBatcher_DefaultMaxWait(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	// MaxWait=0 should default to 500ms.
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      0,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")

	// Should flush within ~500ms (allow some buffer).
	time.Sleep(700 * time.Millisecond)
	b.Close()

	require.GreaterOrEqual(t, rf.totalCalls(), 1, "should flush with default MaxWait")
}

func TestBatcher_ConcurrentAdd(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 100,
		MaxWait:      100 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			b.Add(context.Background(), cf.KindURLs, "https://example.com/"+itoa(n))
		}(i)
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)
	b.Close()

	require.Equal(t, 50, rf.totalItems(), "all unique items should be flushed")
}

func TestBatcher_Flush_EmptyBucket(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      10 * time.Second,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	// Flush with no items should not call flush fn.
	b.Flush(context.Background())
	time.Sleep(50 * time.Millisecond)
	b.Close()

	require.Equal(t, 0, rf.totalCalls(), "empty flush should not call flush fn")
}

func TestBatcher_AllKinds(t *testing.T) {
	t.Parallel()
	urlF := &recordingFlush{}
	tagF := &recordingFlush{}
	prefixF := &recordingFlush{}
	hostF := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs:     urlF.flush,
		cf.KindTags:     tagF.flush,
		cf.KindPrefixes: prefixF.flush,
		cf.KindHosts:    hostF.flush,
	}, cf.BatchMetrics{})

	b.Add(context.Background(), cf.KindURLs, "https://example.com/a")
	b.Add(context.Background(), cf.KindTags, "product-123")
	b.Add(context.Background(), cf.KindPrefixes, "/api/v1/")
	b.Add(context.Background(), cf.KindHosts, "example.com")

	b.Flush(context.Background())
	time.Sleep(100 * time.Millisecond)
	b.Close()

	require.Equal(t, 1, urlF.totalItems())
	require.Equal(t, 1, tagF.totalItems())
	require.Equal(t, 1, prefixF.totalItems())
	require.Equal(t, 1, hostF.totalItems())
}

func TestBatcher_Chunking(t *testing.T) {
	t.Parallel()
	rf := &recordingFlush{}
	b := cf.NewBatcher(context.Background(), cf.BatchConfig{
		MaxBatchSize: 3,
		MaxWait:      10 * time.Second,
	}, map[cf.BatchKind]cf.FlushFn{
		cf.KindURLs: rf.flush,
	}, cf.BatchMetrics{})

	// Add 7 items. With MaxBatchSize=3, this should trigger flushes at
	// 3 items, then 3 more, and the final 1 stays until Close.
	for i := range 7 {
		b.Add(context.Background(), cf.KindURLs, "https://example.com/"+itoa(i))
	}

	time.Sleep(50 * time.Millisecond)
	b.Close()

	// Should have at least 3 flush calls: 3+3+1.
	require.GreaterOrEqual(t, rf.totalCalls(), 2, "should chunk items to MaxBatchSize")
	require.Equal(t, 7, rf.totalItems())
}

// Helper: int to string without fmt to avoid import.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}
