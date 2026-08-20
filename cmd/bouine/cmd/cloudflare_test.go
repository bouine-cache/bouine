package cmd

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/admin"
	bouinecf "github.com/bouine-cache/bouine/internal/cloudflare"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// fakeInvalidator records calls and can return a configurable error.
type fakeInvalidator struct {
	mu          sync.Mutex
	purgeURLs   int
	purgeTags   int
	purgePrefix int
	purgeHosts  int
	err         error
	delay       time.Duration
}

func (f *fakeInvalidator) PurgeURLs(ctx context.Context, urls []string) error {
	f.mu.Lock()
	f.purgeURLs++
	f.mu.Unlock()
	if f.delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.delay):
		}
	}
	return f.err
}

func (f *fakeInvalidator) PurgeTags(ctx context.Context, tags []string) error {
	f.mu.Lock()
	f.purgeTags++
	f.mu.Unlock()
	return f.err
}

func (f *fakeInvalidator) PurgePrefixes(ctx context.Context, prefixes []string) error {
	f.mu.Lock()
	f.purgePrefix++
	f.mu.Unlock()
	return f.err
}

func (f *fakeInvalidator) PurgeHosts(ctx context.Context, hosts []string) error {
	f.mu.Lock()
	f.purgeHosts++
	f.mu.Unlock()
	return f.err
}

func (f *fakeInvalidator) counts() (int, int, int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.purgeURLs, f.purgeTags, f.purgePrefix, f.purgeHosts
}

func testMetrics() *observability.DataPlaneMetrics {
	return observability.NewDataPlaneMetrics(observability.NewMetrics().Registry)
}

func boolPtr(b bool) *bool { return &b }

func TestCFPropagator_PropagateForPurge_Async(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")

	// Async: wait for the goroutine to finish.
	err := p.Close(context.Background())
	require.NoError(t, err, "Close")

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls)
}

func TestCFPropagator_PropagateForPurge_Sync(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Async:     boolPtr(false),
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")

	// Sync: call completed inline, no need to Close.
	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls)
}

func TestCFPropagator_PropagateForPurge_Disabled(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: false}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 0, urls)
}

func TestCFPropagator_PropagateForPurge_NilInvalidator(t *testing.T) {
	t.Parallel()
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(nil, cfg, testMetrics(), slog.Default(), context.Background())

	// Should be a no-op, not panic.
	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())
}

func TestCFPropagator_PropagateForBan_SurrogateKey(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{SurrogateKey: "product-123"})
	_ = p.Close(context.Background())

	_, tags, _, _ := inv.counts()
	require.Equal(t, 1, tags)
}

func TestCFPropagator_PropagateForBan_PathRegex(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{PathRegex: "^/api/v1/"})
	_ = p.Close(context.Background())

	_, _, prefixes, _ := inv.counts()
	require.Equal(t, 1, prefixes)
}

func TestCFPropagator_PropagateForBan_CompoundOverPurge(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{
		HostRegex: "example.com",
		PathRegex: "^/api/",
	})
	_ = p.Close(context.Background())

	// Compound bans should over-purge: both prefixes and hosts should be called.
	_, _, prefixes, hosts := inv.counts()
	require.Equal(t, 1, prefixes, "compound ban should purge by prefix (over-purge)")
	require.Equal(t, 1, hosts, "compound ban should purge by host (over-purge)")
}

func TestCFPropagator_PropagateForRefresh(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Refresh: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForRefresh(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls)
}

func TestCFPropagator_Status(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Before any propagation: no last error or success.
	status := p.Status()
	require.Nil(t, status.LastError)
	require.Nil(t, status.LastSuccessAt)

	// After a successful propagation.
	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	status = p.Status()
	require.Nil(t, status.LastError)
	require.NotNil(t, status.LastSuccessAt)
}

func TestCFPropagator_Status_ErrorRecorded(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	status := p.Status()
	require.NotNil(t, status.LastError)
	require.NotEqual(t, "", *status.LastError)
}

func TestCFPropagator_Close_WaitsForInFlight(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{delay: 100 * time.Millisecond}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")

	// Close should block until the delayed call finishes.
	start := time.Now()
	err := p.Close(context.Background())
	require.NoError(t, err, "Close")
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("Close returned too quickly (%v), expected to wait for in-flight call", elapsed)
	}

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls)
}

func TestCFPropagator_Close_ContextCancellation(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{delay: 5 * time.Second}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")

	// Close with a short timeout should return ctx.Err() since the
	// in-flight call takes 5s.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := p.Close(ctx)
	require.Error(t, err)
}

func TestCFPropagator_AsyncContextCancellation(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{delay: 5 * time.Second}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	closeCtx, closeCancel := context.WithCancel(context.Background())
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), closeCtx)

	p.PropagateForPurge(context.Background(), "https://example.com/page")

	// Cancel the close context — the in-flight async call should bail out
	// because its context is now cancelled.
	closeCancel()

	// Give the goroutine time to notice the cancellation.
	time.Sleep(200 * time.Millisecond)

	// Close should return quickly since the goroutine already exited.
	done := make(chan error, 1)
	go func() {
		done <- p.Close(context.WithoutCancel(context.Background()))
	}()
	select {
	case err := <-done:
		// Either nil (goroutine already done) or the error — both are fine.
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return within 2s after context cancellation")
	}
}

// TestCFPropagator_LastSuccessAtomic verifies no data race on lastSuccess
// when concurrent propagations update it.
func TestCFPropagator_LastSuccessAtomic(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.PropagateForPurge(context.Background(), "https://example.com/page")
		}()
	}
	wg.Wait()
	_ = p.Close(context.Background())

	// If we got here without -race panicking, the atomics are correct.
}

// --- Batching tests ---

func TestCFPropagator_BatchedPurge_Dedup(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Add 3 identical URLs — should dedup to 1.
	p.PropagateForPurge(context.Background(), "https://example.com/a")
	p.PropagateForPurge(context.Background(), "https://example.com/a")
	p.PropagateForPurge(context.Background(), "https://example.com/a")

	_ = p.Close(context.Background())

	// Only 1 PurgeURLs call with 1 URL.
	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls, "dedup should reduce 3 identical URLs to 1 call")
}

func TestCFPropagator_BatchedPurge_Coalesced(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Add 5 unique URLs — should coalesce into 1 batch flush.
	for i := range 5 {
		p.PropagateForPurge(context.Background(), "https://example.com/"+string(rune('a'+i)))
	}

	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls, "5 unique URLs should coalesce into 1 flush call")
}

func TestCFPropagator_BatchedBan_KindIsolation(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Ban: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Add a surrogate key ban and a path ban — should go to different buckets.
	p.PropagateForBan(context.Background(), api.BanExpr{SurrogateKey: "product-123"})
	p.PropagateForBan(context.Background(), api.BanExpr{PathRegex: "^/api/v1/"})

	_ = p.Close(context.Background())

	_, tags, prefixes, _ := inv.counts()
	require.Equal(t, 1, tags, "tags should be flushed separately")
	require.Equal(t, 1, prefixes, "prefixes should be flushed separately")
}

func TestCFPropagator_BatchedRefresh(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Refresh: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForRefresh(context.Background(), "https://example.com/a")
	p.PropagateForRefresh(context.Background(), "https://example.com/b")

	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls, "2 refresh URLs should coalesce into 1 flush")
}

func TestCFPropagator_BatchedCompoundBan_OverPurge(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Ban: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{
		HostRegex: "example.com",
		PathRegex: "^/api/",
	})

	_ = p.Close(context.Background())

	_, _, prefixes, hosts := inv.counts()
	require.Equal(t, 1, prefixes, "compound ban should purge by prefix")
	require.Equal(t, 1, hosts, "compound ban should purge by host")
}

func TestCFPropagator_BatchedDisabled_Passthrough(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 0, // passthrough
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/a")
	p.PropagateForPurge(context.Background(), "https://example.com/b")

	_ = p.Close(context.Background())

	// Passthrough: 2 separate calls, no batching.
	urls, _, _, _ := inv.counts()
	require.Equal(t, 2, urls, "passthrough mode should make 1 call per URL")
}

func TestCFPropagator_BatchedFlushOnFull(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 3,
			MaxWait:      10 * time.Second, // long wait; should flush on size
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/a")
	p.PropagateForPurge(context.Background(), "https://example.com/b")
	p.PropagateForPurge(context.Background(), "https://example.com/c")

	// Should flush immediately when batch reaches 3.
	time.Sleep(100 * time.Millisecond)
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.GreaterOrEqual(t, urls, 1, "should flush when batch is full")
}

func TestCFPropagator_BatchedClose_FlushesPending(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 100,
			MaxWait:      10 * time.Second, // long wait; only Close should flush
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/a")
	p.PropagateForPurge(context.Background(), "https://example.com/b")

	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.GreaterOrEqual(t, urls, 1, "Close should flush pending items")
}

func TestCFPropagator_BatchedSkippedNotAddedToBatch(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Empty URL should be skipped, not added to batch.
	p.PropagateForPurge(context.Background(), "")

	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 0, urls, "skipped purges should not reach the batch")
}

// --- Circuit breaker tests ---

func TestCFPropagator_CircuitBreaker_PassthroughRejects(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Circuit: config.CloudflareCircuitConfig{
			Enabled:          true,
			FailureThreshold: 2,
			OpenTimeout:      10 * time.Second,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Two failures should open the circuit.
	p.PropagateForPurge(context.Background(), "https://example.com/a")
	_ = p.Close(context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/b")
	_ = p.Close(context.Background())

	// Circuit should now be open. The third call should be rejected.
	inv2 := &fakeInvalidator{} // fresh invalidator to verify no calls
	p.inv = inv2
	p.PropagateForPurge(context.Background(), "https://example.com/c")
	_ = p.Close(context.Background())

	urls, _, _, _ := inv2.counts()
	require.Equal(t, 0, urls, "circuit open should reject call without hitting CF")
}

func TestCFPropagator_CircuitBreaker_BatchedRejects(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
		Circuit: config.CloudflareCircuitConfig{
			Enabled:          true,
			FailureThreshold: 2,
			OpenTimeout:      10 * time.Second,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// First batch: 1 URL triggers 1 flush failure.
	p.PropagateForPurge(context.Background(), "https://example.com/a")
	time.Sleep(100 * time.Millisecond)

	// Second batch: 1 URL triggers another flush failure → circuit opens.
	p.PropagateForPurge(context.Background(), "https://example.com/b")
	time.Sleep(100 * time.Millisecond)

	// Close the old batcher to stop its goroutines.
	p.batcher.Close()

	// Rebuild with a fresh invalidator and circuit still open.
	inv2 := &fakeInvalidator{}
	p.inv = inv2
	// Recreate batcher with fresh invalidator.
	p.batcher = bouinecf.NewBatcher(context.Background(), bouinecf.BatchConfig{
		MaxBatchSize: 10,
		MaxWait:      50 * time.Millisecond,
	}, p.buildFlushFns(), bouinecf.BatchMetrics{})
	p.PropagateForPurge(context.Background(), "https://example.com/c")
	time.Sleep(100 * time.Millisecond)
	p.batcher.Close()

	urls, _, _, _ := inv2.counts()
	require.Equal(t, 0, urls, "circuit open should reject batched call")
}

func TestCFPropagator_CircuitBreaker_Disabled(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Circuit: config.CloudflareCircuitConfig{
			Enabled: false,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Even after many failures, calls should not be rejected.
	for range 10 {
		p.PropagateForPurge(context.Background(), "https://example.com/a")
	}
	_ = p.Close(context.Background())

	// All 10 calls should have gone through (no circuit breaker).
	urls, _, _, _ := inv.counts()
	require.Equal(t, 10, urls, "disabled circuit should not reject calls")
}

// --- DLQ tests ---

func TestCFPropagator_DLQ_PassthroughEnqueuesOnFailure(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Retry: config.CloudflareRetryConfig{
			Enabled:      true,
			MaxQueueSize: 100,
			MaxRetries:   1,
			BaseDelay:    50 * time.Millisecond,
			MaxDelay:     200 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/a")
	_ = p.Close(context.Background())

	require.Equal(t, 1, p.retryQueue.Len(), "failed item should be enqueued to DLQ")
}

func TestCFPropagator_DLQ_BatchedEnqueuesOnFailure(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
		Retry: config.CloudflareRetryConfig{
			Enabled:      true,
			MaxQueueSize: 100,
			MaxRetries:   1,
			BaseDelay:    50 * time.Millisecond,
			MaxDelay:     200 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/a")

	// Poll for the DLQ to be populated (up to 2s for slow CI).
	require.Eventually(t, func() bool {
		return p.retryQueue.Len() >= 1
	}, 2*time.Second, 50*time.Millisecond, "failed batch item should be enqueued to DLQ")

	p.batcher.Close()
	p.retryQueue.Close()
}

func TestCFPropagator_DLQ_Disabled(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Retry: config.CloudflareRetryConfig{
			Enabled: false,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/a")
	_ = p.Close(context.Background())

	require.Nil(t, p.retryQueue, "DLQ should be disabled")
}

// --- PropagateExternal tests ---

func TestCFPropagator_PropagateExternal_URLs(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	err := p.PropagateExternal(context.Background(), admin.CFPropagateRequest{
		Kind:  "urls",
		Items: []string{"https://example.com/a"},
	})
	require.NoError(t, err)
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls)
}

func TestCFPropagator_PropagateExternal_Tags(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	err := p.PropagateExternal(context.Background(), admin.CFPropagateRequest{
		Kind:  "tags",
		Items: []string{"product-123"},
	})
	require.NoError(t, err)
	_ = p.Close(context.Background())

	_, tags, _, _ := inv.counts()
	require.Equal(t, 1, tags)
}

func TestCFPropagator_PropagateExternal_Batched(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{
		Propagate: config.CloudflarePropagation{Purge: true},
		Batch: config.CloudflareBatchConfig{
			MaxBatchSize: 10,
			MaxWait:      50 * time.Millisecond,
		},
	}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	err := p.PropagateExternal(context.Background(), admin.CFPropagateRequest{
		Kind:  "urls",
		Items: []string{"https://example.com/a", "https://example.com/b"},
	})
	require.NoError(t, err)
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	require.Equal(t, 1, urls, "2 URLs should coalesce into 1 batch flush")
}

func TestCFPropagator_PropagateExternal_InvalidKind(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	err := p.PropagateExternal(context.Background(), admin.CFPropagateRequest{
		Kind:  "invalid",
		Items: []string{"x"},
	})
	require.Error(t, err)
	_ = p.Close(context.Background())
}

func TestCFPropagator_PropagateExternal_Disabled(t *testing.T) {
	t.Parallel()
	cfg := config.CloudflareConfig{}
	p := buildCFPropagator(nil, cfg, testMetrics(), slog.Default(), context.Background())

	err := p.PropagateExternal(context.Background(), admin.CFPropagateRequest{
		Kind:  "urls",
		Items: []string{"https://example.com/a"},
	})
	require.Error(t, err)
}
