package origin

import (
	"context"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"

	"github.com/valyala/fasthttp"
)

// ActiveHealthChecker runs periodic HTTP probes against every target
// in a pool. Healthy targets are marked up; unhealthy ones are ejected.
//
// Stable.
type ActiveHealthChecker struct {
	logger observability.Logger
	pool   *Pool
	client *fasthttp.Client
	cfg    ActiveHealthConfig
}

// ActiveHealthConfig controls the probe behavior.
type ActiveHealthConfig struct {
	Path               string
	Method             string
	ExpectedCodes      []int
	Interval           time.Duration
	Timeout            time.Duration
	HealthyThreshold   int
	UnhealthyThreshold int
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
		client: &fasthttp.Client{
			MaxConnsPerHost: 2,
			ReadTimeout:     cfg.Timeout,
			WriteTimeout:    cfg.Timeout,
			Dial: func(addr string) (net.Conn, error) {
				return (&net.Dialer{Timeout: cfg.Timeout}).Dial("tcp", addr)
			},
		},
	}
}

// Run starts probing in a loop until ctx is cancelled. It is designed
// to be launched inside a supervised.Group.
func (hc *ActiveHealthChecker) Run(ctx context.Context) error {
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

func (hc *ActiveHealthChecker) probeOne(_ context.Context, t *Target) { //nolint:unparam // ctx reserved for future use
	url := t.url.Scheme + "://" + t.url.Host + hc.cfg.Path

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	req.Header.SetMethod(hc.cfg.Method)
	req.SetRequestURI(url)

	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	if err := hc.client.DoTimeout(req, resp, hc.cfg.Timeout); err != nil {
		hc.recordFailure(t)
		return
	}

	if hc.isExpectedCode(resp.StatusCode()) {
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

func (hc *ActiveHealthChecker) recordSuccess(t *Target) {
	t.recordProbeSuccess(hc.cfg.HealthyThreshold, hc.logger, hc.pool.Name)
}

func (hc *ActiveHealthChecker) recordFailure(t *Target) {
	t.successes.Store(0)
	t.recordProbeError(hc.cfg.UnhealthyThreshold, hc.logger, hc.pool.Name)
}
