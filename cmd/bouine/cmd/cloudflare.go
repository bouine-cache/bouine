package cmd

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/admin"
	bouinecf "github.com/bouine-cache/bouine/internal/cloudflare"
	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
)

// errCircuitOpen is returned when the circuit breaker rejects a call.
var errCircuitOpen = errors.New("cloudflare: circuit breaker open")

// cfPropagator bridges bouine invalidation operations (purge/ban/refresh)
// to the Cloudflare Cache API. When async (the default), CF calls fire in
// background goroutines so admin API responses return immediately.
// When batching is enabled (cfg.Batch.MaxBatchSize > 0), individual
// purge requests are coalesced and deduplicated before reaching the CF API,
// significantly reducing API call volume under burst traffic.
type cfPropagator struct {
	inv     bouinecf.Invalidator
	cfg     config.CloudflareConfig
	metrics *observability.DataPlaneMetrics
	logger  observability.Logger

	lastErr     atomic.Pointer[string]
	lastSuccess atomic.Pointer[time.Time]
	lastLagMs   atomic.Int64

	wg       sync.WaitGroup
	closeCtx context.Context

	// batcher is non-nil when batching is enabled. In batched mode,
	// propagate methods add items to the batcher instead of calling the
	// invalidator directly. The batcher's flush goroutines handle the
	// actual CF API calls.
	batcher *bouinecf.Batcher
	// circuit is non-nil when the circuit breaker is enabled. It wraps
	// all CF API calls, failing fast when the circuit is open.
	circuit *bouinecf.CircuitBreaker
	// retryQueue is non-nil when the DLQ is enabled. Failed purge items
	// are enqueued and retried with exponential backoff.
	retryQueue *bouinecf.RetryQueue
}

// buildCFPropagator constructs a cfPropagator. inv may be nil when CF is
// disabled; all propagate methods become no-ops. closeCtx is the lifecycle
// context whose cancellation stops in-flight async propagations.
//
//nolint:funlen // 90 lines: CF propagator setup is sequential
func buildCFPropagator(
	inv bouinecf.Invalidator,
	cfg config.CloudflareConfig,
	metrics *observability.DataPlaneMetrics,
	logger observability.Logger,
	closeCtx context.Context,
) *cfPropagator {
	p := &cfPropagator{
		inv:      inv,
		cfg:      cfg,
		metrics:  metrics,
		logger:   logger,
		closeCtx: closeCtx,
	}

	if inv != nil && cfg.Circuit.Enabled {
		p.circuit = bouinecf.NewCircuitBreaker(bouinecf.CircuitConfig{
			FailureThreshold: cfg.Circuit.FailureThreshold,
			OpenTimeout:      cfg.Circuit.OpenTimeout,
			HalfOpenMaxCalls: cfg.Circuit.HalfOpenMaxCalls,
		})
		p.circuit.OnStateChange(func(from, to bouinecf.CircuitState) {
			p.logger.Warn("cloudflare circuit breaker state changed",
				"from", from.String(), "to", to.String())
			metrics.CFCircuitState.Set(float64(to))
		})
		p.circuit.OnReject(func() {
			metrics.CFCircuitRejected.Inc()
		})
		// Set initial state metric.
		metrics.CFCircuitState.Set(float64(bouinecf.CircuitClosed))
	}

	if inv != nil && cfg.Retry.Enabled {
		p.retryQueue = bouinecf.NewRetryQueue(
			closeCtx,
			bouinecf.RetryQueueConfig{
				MaxQueueSize: cfg.Retry.MaxQueueSize,
				MaxRetries:   cfg.Retry.MaxRetries,
				BaseDelay:    cfg.Retry.BaseDelay,
				MaxDelay:     cfg.Retry.MaxDelay,
			},
			func(ctx context.Context, kind bouinecf.BatchKind, value string) error {
				// In batched mode, add to the batcher. The batcher's flush
				// will call the CF API. If it fails again, the flush callback
				// will re-enqueue. We return nil here because the batcher
				// is async — we can't know the result synchronously.
				// The item is removed from the DLQ on success (nil return).
				// If the batcher flush fails, it will re-enqueue the item
				// via the flush callback's error handling.
				if p.batcher != nil {
					p.batcher.Add(ctx, kind, value)
					return nil
				}
				// In passthrough mode, attempt the CF API call directly.
				return p.retryCallDirect(ctx, kind, value)
			},
			bouinecf.RetryQueueMetrics{
				OnEnqueue: func(kind bouinecf.BatchKind) {
					metrics.CFDLQEnqueued.WithLabelValues(kindLabel(kind)).Inc()
				},
				OnDrop: func(kind bouinecf.BatchKind) {
					metrics.CFDLQDropped.WithLabelValues(kindLabel(kind)).Inc()
				},
				OnRetry: func(kind bouinecf.BatchKind) {
					metrics.CFDLQRetried.WithLabelValues(kindLabel(kind)).Inc()
				},
				OnExpire: func(kind bouinecf.BatchKind) {
					metrics.CFDLQExpired.WithLabelValues(kindLabel(kind)).Inc()
				},
				OnDepth: func(depth int) {
					metrics.CFDLQDepth.Set(float64(depth))
				},
			},
		)
	}

	if inv != nil && cfg.Batch.MaxBatchSize > 0 {
		p.batcher = bouinecf.NewBatcher(
			closeCtx,
			bouinecf.BatchConfig{
				MaxBatchSize: cfg.Batch.MaxBatchSize,
				MaxWait:      cfg.Batch.MaxWait,
			},
			p.buildFlushFns(),
			bouinecf.BatchMetrics{
				OnFlush: func(kind bouinecf.BatchKind, count int) {
					metrics.CFBatchFlushed.WithLabelValues(kindLabel(kind)).Add(float64(count))
				},
				OnDedup: func(kind bouinecf.BatchKind) {
					metrics.CFBatchDeduped.WithLabelValues(kindLabel(kind)).Inc()
				},
				OnFlushErr: func(kind bouinecf.BatchKind, err error) {
					metrics.CFBatchFlushErr.WithLabelValues(kindLabel(kind), bouinecf.ErrorType(err)).Inc()
				},
			},
		)
	}

	return p
}

// buildFlushFns returns the flush callbacks for each batch kind. Each
// callback performs the actual CF API call and records success/failure
// metrics. The circuit breaker (if enabled) gates each call. On failure,
// items are enqueued to the retry queue (if enabled).
func (p *cfPropagator) buildFlushFns() map[bouinecf.BatchKind]bouinecf.FlushFn {
	return map[bouinecf.BatchKind]bouinecf.FlushFn{
		bouinecf.KindURLs: func(ctx context.Context, items []string) error {
			return p.recordFlush(ctx, "purge", bouinecf.KindURLs, items, func(ctx context.Context) error {
				return p.inv.PurgeURLs(ctx, items)
			})
		},
		bouinecf.KindTags: func(ctx context.Context, items []string) error {
			return p.recordFlush(ctx, "ban", bouinecf.KindTags, items, func(ctx context.Context) error {
				return p.inv.PurgeTags(ctx, items)
			})
		},
		bouinecf.KindPrefixes: func(ctx context.Context, items []string) error {
			return p.recordFlush(ctx, "ban", bouinecf.KindPrefixes, items, func(ctx context.Context) error {
				return p.inv.PurgePrefixes(ctx, items)
			})
		},
		bouinecf.KindHosts: func(ctx context.Context, items []string) error {
			return p.recordFlush(ctx, "ban", bouinecf.KindHosts, items, func(ctx context.Context) error {
				return p.inv.PurgeHosts(ctx, items)
			})
		},
	}
}

// recordFlush wraps a batched CF API call with circuit breaker gating,
// metrics/last-success/error tracking, and DLQ enqueue on failure.
// Delegates to runAndRecord for the actual metrics recording so the logic
// is shared between batched and passthrough modes. Returns the error so
// the batcher can report it via OnFlushErr.
// Note: lastLagMs in batched mode measures only the CF API call duration,
// not the total time from the original request to CF completion, because
// the batcher does not track per-item enqueue time.
func (p *cfPropagator) recordFlush(ctx context.Context, op string, kind bouinecf.BatchKind, items []string, fn func(context.Context) error) error {
	if p.circuit != nil && !p.circuit.Allow() {
		p.enqueueFailedItems(kind, items)
		return errCircuitOpen
	}
	err := p.runAndRecord(ctx, op, kindLabel(kind), fn, time.Now())
	if p.circuit != nil {
		if err != nil {
			p.circuit.RecordFailure()
		} else {
			p.circuit.RecordSuccess()
		}
	}
	if err != nil && p.retryQueue != nil {
		p.enqueueFailedItems(kind, items)
	}
	return err
}

// enqueueFailedItems adds all failed items to the retry queue (if enabled).
func (p *cfPropagator) enqueueFailedItems(kind bouinecf.BatchKind, items []string) {
	if p.retryQueue == nil {
		return
	}
	for _, item := range items {
		p.retryQueue.Enqueue(kind, item)
	}
}

// retryCallDirect attempts a single CF API call for a retried item in
// passthrough mode. Returns the error so the retry queue can decide
// whether to keep or expire the item.
func (p *cfPropagator) retryCallDirect(ctx context.Context, kind bouinecf.BatchKind, value string) error {
	if p.circuit != nil && !p.circuit.Allow() {
		return errCircuitOpen
	}
	var err error
	switch kind {
	case bouinecf.KindURLs:
		err = p.inv.PurgeURLs(ctx, []string{value})
	case bouinecf.KindTags:
		err = p.inv.PurgeTags(ctx, []string{value})
	case bouinecf.KindPrefixes:
		err = p.inv.PurgePrefixes(ctx, []string{value})
	case bouinecf.KindHosts:
		err = p.inv.PurgeHosts(ctx, []string{value})
	}
	if p.circuit != nil {
		if err != nil {
			p.circuit.RecordFailure()
		} else {
			p.circuit.RecordSuccess()
		}
	}
	return err
}

// kindLabel converts a BatchKind to a stable string for metric labels.
func kindLabel(kind bouinecf.BatchKind) string {
	switch kind {
	case bouinecf.KindURLs:
		return "urls"
	case bouinecf.KindTags:
		return "tags"
	case bouinecf.KindPrefixes:
		return "prefixes"
	case bouinecf.KindHosts:
		return "hosts"
	default:
		return "unknown"
	}
}

// Status returns a snapshot suitable for GET /v1/cloudflare/status.
func (p *cfPropagator) Status() admin.CloudflareStatus {
	enabled := p.inv != nil
	zoneID := ""
	if c, ok := p.inv.(*bouinecf.Client); ok && c != nil {
		zoneID = c.ZoneID()
	}
	var lastErr *string
	if ptr := p.lastErr.Load(); ptr != nil {
		lastErr = ptr
	}
	var lastSuccessAt *string
	if t := p.lastSuccess.Load(); t != nil {
		s := t.UTC().Format(time.RFC3339)
		lastSuccessAt = &s
	}
	return admin.CloudflareStatus{
		Enabled:       enabled,
		ZoneID:        zoneID,
		Async:         p.cfg.IsAsync(),
		LastError:     lastErr,
		LastSuccessAt: lastSuccessAt,
		LastLagMs:     p.lastLagMs.Load(),
		BatchEnabled:  p.batcher != nil,
		CircuitState:  p.circuitStateString(),
		DLQDepth:      p.dlqDepth(),
		TokenCount:    p.tokenCount(),
	}
}

// PropagateExternal handles a purge request from an external service
// (e.g. cache-lifecycle) via POST /v1/cloudflare/propagate. It routes
// the request through the same batching/circuit-breaker/DLQ pipeline as
// internal propagation requests.
func (p *cfPropagator) PropagateExternal(ctx context.Context, req admin.CFPropagateRequest) error {
	if p.inv == nil {
		return errors.New("cloudflare propagation is not enabled")
	}
	var kind bouinecf.BatchKind
	switch req.Kind {
	case "urls":
		kind = bouinecf.KindURLs
	case "tags":
		kind = bouinecf.KindTags
	case "prefixes":
		kind = bouinecf.KindPrefixes
	case "hosts":
		kind = bouinecf.KindHosts
	default:
		return errors.New("invalid kind: must be one of urls, tags, prefixes, hosts")
	}
	for _, item := range req.Items {
		if p.batcher != nil {
			p.batcher.Add(ctx, kind, item)
		} else {
			p.dispatch(ctx, kindToOp(kind), kind, []string{item}, func(ctx context.Context) error {
				return p.purgeByKind(ctx, kind, []string{item})
			})
		}
	}
	return nil
}

func (p *cfPropagator) purgeByKind(ctx context.Context, kind bouinecf.BatchKind, items []string) error {
	switch kind {
	case bouinecf.KindURLs:
		return p.inv.PurgeURLs(ctx, items)
	case bouinecf.KindTags:
		return p.inv.PurgeTags(ctx, items)
	case bouinecf.KindPrefixes:
		return p.inv.PurgePrefixes(ctx, items)
	case bouinecf.KindHosts:
		return p.inv.PurgeHosts(ctx, items)
	default:
		return errors.New("unknown kind")
	}
}

func kindToOp(kind bouinecf.BatchKind) string {
	switch kind {
	case bouinecf.KindURLs:
		return "purge"
	default:
		return "ban"
	}
}

func (p *cfPropagator) circuitStateString() string {
	if p.circuit == nil {
		return ""
	}
	return p.circuit.State().String()
}

func (p *cfPropagator) dlqDepth() int {
	if p.retryQueue == nil {
		return 0
	}
	return p.retryQueue.Len()
}

func (p *cfPropagator) tokenCount() int {
	if c, ok := p.inv.(*bouinecf.Client); ok && c != nil {
		if pool := c.TokenPool(); pool != nil {
			return pool.Len()
		}
		return 1
	}
	return 0
}

// PropagateForPurge forwards a URL purge to Cloudflare when propagation is
// enabled. In batched mode, the URL is added to the URL batch. In
// passthrough mode, the CF API call fires in a background goroutine (async)
// or inline (sync).
func (p *cfPropagator) PropagateForPurge(ctx context.Context, url string) {
	if p.inv == nil || !p.cfg.Propagate.Purge {
		return
	}
	result := bouinecf.MapURL(url)
	if result.Skipped {
		p.metrics.CFPurgeSkipped.WithLabelValues(bouinecf.SkipCategory(result.SkipReason)).Inc()
		p.logger.Warn("cloudflare propagation skipped",
			"op", "purge",
			"reason", result.SkipReason)
		return
	}
	if p.batcher != nil {
		for _, u := range result.URLs {
			p.batcher.Add(ctx, bouinecf.KindURLs, u)
		}
		return
	}
	p.dispatch(ctx, "purge", bouinecf.KindURLs, result.URLs, func(ctx context.Context) error {
		return p.inv.PurgeURLs(ctx, result.URLs)
	})
}

// PropagateForBan forwards a ban expression to Cloudflare when propagation is
// enabled. Surrogate keys map to PurgeByTags; literal path/host regexes map
// to PurgeByPrefixes/PurgeByHostnames; non-literal regexes are skipped.
// Compound bans (host AND path) are over-purged: both host and path are
// purged independently (OR semantics) to ensure the CF cache is invalidated.
//
//nolint:gocyclo // 20: ban propagation is inherently branchy
func (p *cfPropagator) PropagateForBan(ctx context.Context, expr api.BanExpr) {
	if p.inv == nil || !p.cfg.Propagate.Ban {
		return
	}
	var result bouinecf.MapResult
	switch {
	case expr.SurrogateKey != "":
		result = bouinecf.MapSurrogateKey(expr.SurrogateKey)
	case expr.PathRegex != "" && expr.HostRegex != "":
		// CF has no compound predicate (host AND path). Over-purge by
		// issuing both PurgeByPrefixes AND PurgeByHostnames (OR
		// semantics). This is safer for consistency than skipping.
		pathResult := bouinecf.MapPathRegex(expr.PathRegex)
		hostResult := bouinecf.MapHostRegex(expr.HostRegex)
		result = bouinecf.MergeResults(pathResult, hostResult)
		if result.Skipped {
			result.SkipReason = "compound ban (host AND path) — " + result.SkipReason
		}
	case expr.PathRegex != "":
		result = bouinecf.MapPathRegex(expr.PathRegex)
	case expr.HostRegex != "":
		result = bouinecf.MapHostRegex(expr.HostRegex)
	default:
		return
	}
	if result.Skipped {
		p.metrics.CFPurgeSkipped.WithLabelValues(bouinecf.SkipCategory(result.SkipReason)).Inc()
		p.logger.Warn("cloudflare propagation skipped",
			"op", "ban",
			"reason", result.SkipReason)
		return
	}
	if p.batcher != nil {
		for _, tag := range result.Tags {
			p.batcher.Add(ctx, bouinecf.KindTags, tag)
		}
		for _, prefix := range result.Prefixes {
			p.batcher.Add(ctx, bouinecf.KindPrefixes, prefix)
		}
		for _, host := range result.Hosts {
			p.batcher.Add(ctx, bouinecf.KindHosts, host)
		}
		return
	}
	p.dispatch(ctx, "ban", bouinecf.KindTags, result.Tags, func(ctx context.Context) error {
		if len(result.Tags) > 0 {
			if err := p.inv.PurgeTags(ctx, result.Tags); err != nil {
				return err
			}
		}
		if len(result.Prefixes) > 0 {
			if err := p.inv.PurgePrefixes(ctx, result.Prefixes); err != nil {
				return err
			}
		}
		if len(result.Hosts) > 0 {
			if err := p.inv.PurgeHosts(ctx, result.Hosts); err != nil {
				return err
			}
		}
		return nil
	})
}

// PropagateForRefresh forwards a soft-purge (refresh) URL to Cloudflare.
func (p *cfPropagator) PropagateForRefresh(ctx context.Context, url string) {
	if p.inv == nil || !p.cfg.Propagate.Refresh {
		return
	}
	result := bouinecf.MapURL(url)
	if result.Skipped {
		p.metrics.CFPurgeSkipped.WithLabelValues(bouinecf.SkipCategory(result.SkipReason)).Inc()
		p.logger.Warn("cloudflare propagation skipped",
			"op", "refresh",
			"reason", result.SkipReason)
		return
	}
	if p.batcher != nil {
		for _, u := range result.URLs {
			p.batcher.Add(ctx, bouinecf.KindURLs, u)
		}
		return
	}
	p.dispatch(ctx, "refresh", bouinecf.KindURLs, result.URLs, func(ctx context.Context) error {
		return p.inv.PurgeURLs(ctx, result.URLs)
	})
}

// Close waits for in-flight async propagations to finish or ctx to expire.
// In batched mode, also flushes pending items and stops the batcher.
// Must be called during shutdown to prevent goroutine leaks.
func (p *cfPropagator) Close(ctx context.Context) error {
	if p.batcher != nil {
		p.batcher.Flush(ctx)
		p.batcher.Close()
	}
	if p.retryQueue != nil {
		p.retryQueue.Close()
	}
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// dispatch executes fn either in a background goroutine (async=true, the
// default) or inline (async=false). Either way it records metrics.
// Only used in passthrough (non-batched) mode. The circuit breaker (if
// enabled) gates the call. On failure, items are enqueued to the DLQ (if
// enabled). The items parameter is used only for DLQ enqueue on failure.
func (p *cfPropagator) dispatch(ctx context.Context, op string, kind bouinecf.BatchKind, items []string, fn func(context.Context) error) {
	requestStart := time.Now()
	wrapped := func(ctx context.Context) error {
		if p.circuit != nil && !p.circuit.Allow() {
			p.metrics.CFCircuitRejected.Inc()
			p.enqueueFailedItems(kind, items)
			return errCircuitOpen
		}
		err := fn(ctx)
		if p.circuit != nil {
			if err != nil {
				p.circuit.RecordFailure()
			} else {
				p.circuit.RecordSuccess()
			}
		}
		if err != nil && p.retryQueue != nil {
			p.enqueueFailedItems(kind, items)
		}
		return err
	}
	if p.cfg.IsAsync() {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			p.run(p.closeCtx, op, wrapped, requestStart)
		}()
	} else {
		p.run(ctx, op, wrapped, requestStart)
	}
}

func (p *cfPropagator) run(ctx context.Context, op string, fn func(context.Context) error, requestStart time.Time) {
	_ = p.runAndRecord(ctx, op, "", fn, requestStart)
}

func (p *cfPropagator) runAndRecord(ctx context.Context, op, kind string, fn func(context.Context) error, requestStart time.Time) error {
	start := time.Now()
	err := fn(ctx)
	dur := time.Since(start)
	p.metrics.CFPurgeDuration.WithLabelValues(op).Observe(dur.Seconds())
	if err != nil {
		errStr := err.Error()
		p.lastErr.Store(&errStr)
		p.metrics.CFPurgeTotal.WithLabelValues(op, bouinecf.ErrorType(err)).Inc()
		logFields := []any{
			"op", op,
			"error", err,
			"error_type", bouinecf.ErrorType(err),
			"duration_ms", dur.Milliseconds(),
		}
		if kind != "" {
			logFields = append([]any{"kind", kind}, logFields...)
		}
		p.logger.Warn("cloudflare propagation failed", logFields...)
		return err
	}
	now := time.Now()
	p.lastSuccess.Store(&now)
	p.lastLagMs.Store(now.Sub(requestStart).Milliseconds())
	p.metrics.CFPurgeTotal.WithLabelValues(op, "ok").Inc()
	return nil
}
