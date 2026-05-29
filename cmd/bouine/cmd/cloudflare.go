package cmd

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/thylong/bouine/internal/admin"
	bouinecf "github.com/thylong/bouine/internal/cloudflare"
	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// cfPropagator bridges bouine invalidation operations (purge/ban/refresh)
// to the Cloudflare Cache API. When async (the default), CF calls fire in
// background goroutines so admin API responses return immediately.
type cfPropagator struct {
	inv     bouinecf.Invalidator
	cfg     config.CloudflareConfig
	metrics *observability.DataPlaneMetrics
	logger  *slog.Logger

	lastErr     atomic.Pointer[string]
	lastSuccess atomic.Pointer[time.Time]
}

// buildCFPropagator constructs a cfPropagator. inv may be nil when CF is
// disabled; all propagate methods become no-ops.
func buildCFPropagator(
	inv bouinecf.Invalidator,
	cfg config.CloudflareConfig,
	metrics *observability.DataPlaneMetrics,
	logger *slog.Logger,
) *cfPropagator {
	return &cfPropagator{
		inv:     inv,
		cfg:     cfg,
		metrics: metrics,
		logger:  logger,
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
	}
}

// PropagateForPurge forwards a URL purge to Cloudflare when propagation is
// enabled. Runs asynchronously when cfg.Async is true (the default).
func (p *cfPropagator) PropagateForPurge(ctx context.Context, url string) {
	if p.inv == nil || !p.cfg.Propagate.Purge {
		return
	}
	result := bouinecf.MapURL(url)
	p.dispatch(ctx, "purge", func(ctx context.Context) error {
		if result.Skipped {
			p.metrics.CFPurgeSkipped.WithLabelValues(result.SkipReason).Inc()
			return nil
		}
		return p.inv.PurgeURLs(ctx, result.URLs)
	})
}

// PropagateForBan forwards a ban expression to Cloudflare when propagation is
// enabled. Surrogate keys map to PurgeByTags; literal path/host regexes map
// to PurgeByPrefixes/PurgeByHostnames; non-literal regexes are skipped.
func (p *cfPropagator) PropagateForBan(ctx context.Context, expr api.BanExpr) {
	if p.inv == nil || !p.cfg.Propagate.Ban {
		return
	}
	var result bouinecf.MapResult
	switch {
	case expr.SurrogateKey != "":
		result = bouinecf.MapSurrogateKey(expr.SurrogateKey)
	case expr.PathRegex != "" && expr.HostRegex != "":
		result = bouinecf.MergeResults(
			bouinecf.MapPathRegex(expr.PathRegex),
			bouinecf.MapHostRegex(expr.HostRegex),
		)
	case expr.PathRegex != "":
		result = bouinecf.MapPathRegex(expr.PathRegex)
	case expr.HostRegex != "":
		result = bouinecf.MapHostRegex(expr.HostRegex)
	default:
		return
	}
	p.dispatch(ctx, "ban", func(ctx context.Context) error {
		if result.Skipped {
			p.metrics.CFPurgeSkipped.WithLabelValues(result.SkipReason).Inc()
			return nil
		}
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
	p.dispatch(ctx, "refresh", func(ctx context.Context) error {
		if result.Skipped {
			p.metrics.CFPurgeSkipped.WithLabelValues(result.SkipReason).Inc()
			return nil
		}
		return p.inv.PurgeURLs(ctx, result.URLs)
	})
}

// dispatch executes fn either in a background goroutine (async=true, the
// default) or inline (async=false). Either way it records metrics.
func (p *cfPropagator) dispatch(ctx context.Context, op string, fn func(context.Context) error) {
	if p.cfg.IsAsync() {
		go p.run(context.WithoutCancel(ctx), op, fn)
	} else {
		p.run(ctx, op, fn)
	}
}

func (p *cfPropagator) run(ctx context.Context, op string, fn func(context.Context) error) {
	start := time.Now()
	err := fn(ctx)
	dur := time.Since(start)
	p.metrics.CFPurgeDuration.WithLabelValues(op).Observe(dur.Seconds())
	if err != nil {
		errStr := err.Error()
		p.lastErr.Store(&errStr)
		p.metrics.CFPurgeTotal.WithLabelValues(op, "error").Inc()
		p.logger.Warn("cloudflare propagation failed",
			"op", op,
			"error", err,
			"duration_ms", dur.Milliseconds())
		return
	}
	now := time.Now()
	p.lastSuccess.Store(&now)
	p.metrics.CFPurgeTotal.WithLabelValues(op, "ok").Inc()
}
