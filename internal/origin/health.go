package origin

import (
	"context"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/thylong/bouine/internal/observability"
)

// ActiveHealthChecker runs periodic HTTP probes against every target
// in a pool. Healthy targets are marked up; unhealthy ones are ejected.
//
// Stable.
type ActiveHealthChecker struct {
	pool   *Pool
	cfg    ActiveHealthConfig
	logger observability.Logger
	client *http.Client
}

// ActiveHealthConfig controls the probe behavior.
type ActiveHealthConfig struct {
	Path               string
	Method             string
	Interval           time.Duration
	Timeout            time.Duration
	HealthyThreshold   int
	UnhealthyThreshold int
	ExpectedCodes      []int
}

// NewActiveHealthChecker creates a health checker for the given pool.
func NewActiveHealthChecker(pool *Pool, cfg ActiveHealthConfig, logger observability.Logger) *ActiveHealthChecker {
	logger = observability.ResolveLogger(logger)
	if cfg.Method == "" {
		cfg.Method = "GET"
	}
	if cfg.Path == "" {
		cfg.Path = "/healthz"
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.HealthyThreshold <= 0 {
		cfg.HealthyThreshold = 2
	}
	if cfg.UnhealthyThreshold <= 0 {
		cfg.UnhealthyThreshold = 3
	}
	if len(cfg.ExpectedCodes) == 0 {
		cfg.ExpectedCodes = []int{200}
	}

	return &ActiveHealthChecker{
		pool:   pool,
		cfg:    cfg,
		logger: logger,
		client: &http.Client{Timeout: cfg.Timeout},
	}
}

// Run starts probing in a loop until ctx is cancelled. It is designed
// to be launched inside a supervised.Group.
func (hc *ActiveHealthChecker) Run(ctx context.Context) error {
	// Jitter the first probe by up to half the interval so multiple
	// pools don't probe in lockstep.
	jitter := time.Duration(rand.Int63n(int64(hc.cfg.Interval / 2))) //nolint:gosec // not crypto
	select {
	case <-ctx.Done():
		return nil
	case <-time.After(jitter):
	}

	ticker := time.NewTicker(hc.cfg.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			hc.probeAll(ctx)
		}
	}
}

func (hc *ActiveHealthChecker) probeAll(ctx context.Context) {
	hc.pool.mu.RLock()
	targets := make([]*Target, len(hc.pool.targets))
	copy(targets, hc.pool.targets)
	hc.pool.mu.RUnlock()

	var wg sync.WaitGroup
	for _, t := range targets {
		wg.Add(1)
		go func(t *Target) {
			defer wg.Done()
			hc.probeOne(ctx, t)
		}(t)
	}
	wg.Wait()
}

func (hc *ActiveHealthChecker) probeOne(ctx context.Context, t *Target) {
	url := t.url.Scheme + "://" + t.url.Host + hc.cfg.Path
	req, err := http.NewRequestWithContext(ctx, hc.cfg.Method, url, nil)
	if err != nil {
		return
	}

	resp, err := hc.client.Do(req)
	if err != nil {
		hc.recordFailure(t)
		return
	}
	defer func() {
		_, _ = http.MaxBytesReader(nil, resp.Body, 4096).Read(make([]byte, 4096))
		_ = resp.Body.Close()
	}()

	if hc.isExpectedCode(resp.StatusCode) {
		hc.recordSuccess(t)
	} else {
		hc.recordFailure(t)
	}
}

func (hc *ActiveHealthChecker) isExpectedCode(code int) bool {
	for _, c := range hc.cfg.ExpectedCodes {
		if c == code {
			return true
		}
	}
	return false
}

// Track consecutive successes/failures per target using the existing
// atomic error counter. Successes decrement toward zero (healthy);
// failures increment toward the threshold.
func (hc *ActiveHealthChecker) recordSuccess(t *Target) {
	t.errors.Store(0)
	if !t.healthy.Load() {
		// Count consecutive successes needed to restore.
		// One success restores.
		t.healthy.Store(true)
		hc.logger.Info("target restored by active health check",
			"pool", hc.pool.Name,
			"target", t.addr)
	}
}

func (hc *ActiveHealthChecker) recordFailure(t *Target) {
	cnt := t.errors.Add(1)
	if cnt >= int64(hc.cfg.UnhealthyThreshold) && t.healthy.Load() {
		t.healthy.Store(false)
		hc.logger.Warn("target ejected by active health check",
			"pool", hc.pool.Name,
			"target", t.addr,
			"consecutive_failures", cnt)
	}
}
