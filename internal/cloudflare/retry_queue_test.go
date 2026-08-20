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

func TestRetryQueue_EnqueueAndRetry(t *testing.T) {
	t.Parallel()
	var retried atomic.Int64
	retryFn := func(ctx context.Context, kind cf.BatchKind, value string) error {
		retried.Add(1)
		return nil
	}
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   3,
		BaseDelay:    50 * time.Millisecond,
		MaxDelay:     1 * time.Second,
	}, retryFn, cf.RetryQueueMetrics{})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	require.Equal(t, 1, rq.Len())

	time.Sleep(300 * time.Millisecond)
	require.GreaterOrEqual(t, int(retried.Load()), 1, "should retry the item")
	require.Equal(t, 0, rq.Len(), "item should be removed after successful retry")
}

func TestRetryQueue_DedupOnEnqueue(t *testing.T) {
	t.Parallel()
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   3,
		BaseDelay:    10 * time.Second,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	rq.Enqueue(cf.KindURLs, "https://example.com/a")

	require.Equal(t, 1, rq.Len(), "duplicates should not increase queue depth")
}

func TestRetryQueue_DropWhenFull(t *testing.T) {
	t.Parallel()
	var drops atomic.Int64
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 2,
		MaxRetries:   3,
		BaseDelay:    10 * time.Second,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{
		OnDrop: func(kind cf.BatchKind) { drops.Add(1) },
	})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	rq.Enqueue(cf.KindURLs, "https://example.com/b")
	rq.Enqueue(cf.KindURLs, "https://example.com/c")

	require.Equal(t, 2, rq.Len())
	require.Equal(t, int64(1), drops.Load())
}

func TestRetryQueue_ExpireAfterMaxRetries(t *testing.T) {
	t.Parallel()
	var expired atomic.Int64
	var retries atomic.Int64
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   2,
		BaseDelay:    50 * time.Millisecond,
		MaxDelay:     200 * time.Millisecond,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error {
		retries.Add(1)
		return errors.New("still failing")
	}, cf.RetryQueueMetrics{
		OnExpire: func(kind cf.BatchKind) { expired.Add(1) },
	})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	time.Sleep(500 * time.Millisecond)

	require.Equal(t, int64(1), expired.Load(), "item should expire after max retries")
	require.GreaterOrEqual(t, int(retries.Load()), 1)
}

func TestRetryQueue_OnEnqueueMetric(t *testing.T) {
	t.Parallel()
	var enqueues atomic.Int64
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   3,
		BaseDelay:    10 * time.Second,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{
		OnEnqueue: func(kind cf.BatchKind) { enqueues.Add(1) },
	})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	require.Equal(t, int64(1), enqueues.Load())
}

func TestRetryQueue_OnRetryMetric(t *testing.T) {
	t.Parallel()
	var retries atomic.Int64
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   1,
		BaseDelay:    50 * time.Millisecond,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{
		OnRetry: func(kind cf.BatchKind) { retries.Add(1) },
	})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	time.Sleep(300 * time.Millisecond)
	require.GreaterOrEqual(t, int(retries.Load()), 1)
}

func TestRetryQueue_CloseStopsGoroutine(t *testing.T) {
	t.Parallel()
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   3,
		BaseDelay:    10 * time.Second,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{})

	rq.Close()
	require.Equal(t, 0, rq.Len())
}

func TestRetryQueue_ConcurrentEnqueue(t *testing.T) {
	t.Parallel()
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 1000,
		MaxRetries:   3,
		BaseDelay:    10 * time.Second,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{})
	defer rq.Close()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			rq.Enqueue(cf.KindURLs, "https://example.com/"+itoa(n))
		}(i)
	}
	wg.Wait()

	require.Equal(t, 100, rq.Len())
}

func TestRetryQueue_Defaults(t *testing.T) {
	t.Parallel()
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{}, func(ctx context.Context, kind cf.BatchKind, value string) error { return nil }, cf.RetryQueueMetrics{})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	require.Equal(t, 1, rq.Len())
}

func TestRetryQueue_RetryFailureStaysInQueue(t *testing.T) {
	t.Parallel()
	rq := cf.NewRetryQueue(context.Background(), cf.RetryQueueConfig{
		MaxQueueSize: 100,
		MaxRetries:   3,
		BaseDelay:    50 * time.Millisecond,
		MaxDelay:     200 * time.Millisecond,
	}, func(ctx context.Context, kind cf.BatchKind, value string) error {
		return errors.New("still failing")
	}, cf.RetryQueueMetrics{})
	defer rq.Close()

	rq.Enqueue(cf.KindURLs, "https://example.com/a")
	time.Sleep(200 * time.Millisecond)

	require.Equal(t, 1, rq.Len(), "item should stay in queue after failed retry")
}
