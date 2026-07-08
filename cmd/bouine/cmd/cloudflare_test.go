package cmd

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

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
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}

	urls, _, _, _ := inv.counts()
	if urls != 1 {
		t.Fatalf("expected 1 PurgeURLs call, got %d", urls)
	}
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
	if urls != 1 {
		t.Fatalf("expected 1 PurgeURLs call, got %d", urls)
	}
}

func TestCFPropagator_PropagateForPurge_Disabled(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: false}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	if urls != 0 {
		t.Fatalf("expected 0 calls when purge disabled, got %d", urls)
	}
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
	if tags != 1 {
		t.Fatalf("expected 1 PurgeTags call, got %d", tags)
	}
}

func TestCFPropagator_PropagateForBan_PathRegex(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{PathRegex: "^/api/v1/"})
	_ = p.Close(context.Background())

	_, _, prefixes, _ := inv.counts()
	if prefixes != 1 {
		t.Fatalf("expected 1 PurgePrefixes call, got %d", prefixes)
	}
}

func TestCFPropagator_PropagateForBan_CompoundSkipped(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{
		HostRegex: "example.com",
		PathRegex: "^/api/",
	})
	_ = p.Close(context.Background())

	// Compound bans should be skipped — no CF calls at all.
	urls, tags, prefixes, hosts := inv.counts()
	if urls+tags+prefixes+hosts != 0 {
		t.Fatalf("expected 0 calls for compound ban, got urls=%d tags=%d prefixes=%d hosts=%d",
			urls, tags, prefixes, hosts)
	}
}

func TestCFPropagator_PropagateForBan_NonLiteralRegexSkipped(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Ban: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForBan(context.Background(), api.BanExpr{PathRegex: "^/api/[0-9]+"})
	_ = p.Close(context.Background())

	urls, tags, prefixes, hosts := inv.counts()
	if urls+tags+prefixes+hosts != 0 {
		t.Fatalf("expected 0 calls for non-literal regex, got %d", urls+tags+prefixes+hosts)
	}
}

func TestCFPropagator_PropagateForRefresh(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Refresh: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForRefresh(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	urls, _, _, _ := inv.counts()
	if urls != 1 {
		t.Fatalf("expected 1 PurgeURLs call, got %d", urls)
	}
}

func TestCFPropagator_Status(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	// Before any propagation: no last error or success.
	status := p.Status()
	if status.LastError != nil {
		t.Fatalf("expected nil last error before any call, got %v", *status.LastError)
	}
	if status.LastSuccessAt != nil {
		t.Fatalf("expected nil last success before any call, got %v", *status.LastSuccessAt)
	}

	// After a successful propagation.
	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	status = p.Status()
	if status.LastError != nil {
		t.Fatalf("expected nil last error after success, got %v", *status.LastError)
	}
	if status.LastSuccessAt == nil {
		t.Fatal("expected non-nil last success after successful propagation")
	}
}

func TestCFPropagator_Status_ErrorRecorded(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{err: errors.New("CF API down")}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")
	_ = p.Close(context.Background())

	status := p.Status()
	if status.LastError == nil {
		t.Fatal("expected non-nil last error after failed propagation")
	}
	if *status.LastError == "" {
		t.Fatal("expected non-empty error message")
	}
}

func TestCFPropagator_Close_WaitsForInFlight(t *testing.T) {
	t.Parallel()
	inv := &fakeInvalidator{delay: 100 * time.Millisecond}
	cfg := config.CloudflareConfig{Propagate: config.CloudflarePropagation{Purge: true}}
	p := buildCFPropagator(inv, cfg, testMetrics(), slog.Default(), context.Background())

	p.PropagateForPurge(context.Background(), "https://example.com/page")

	// Close should block until the delayed call finishes.
	start := time.Now()
	if err := p.Close(context.Background()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed < 50*time.Millisecond {
		t.Fatalf("Close returned too quickly (%v), expected to wait for in-flight call", elapsed)
	}

	urls, _, _, _ := inv.counts()
	if urls != 1 {
		t.Fatalf("expected 1 PurgeURLs call after Close, got %d", urls)
	}
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
	if err == nil {
		t.Fatal("expected context deadline exceeded error from Close")
	}
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
