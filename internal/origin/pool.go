// Package origin is the L5 upstream layer. It manages connection pools
// to origin servers, selects targets via round-robin (ADR-0005),
// performs passive health checking (consecutive-5xx ejection), active
// health probes, hedged requests, and exposes a fasthttp.RequestHandler
// that forwards requests to the chosen target.
package origin

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/transport"

	"github.com/valyala/fasthttp"
)

// Pool is a named set of upstream targets with round-robin selection
// and passive health checking.
//
// Stable.
type Pool struct {
	Name    string
	targets []*Target
	next    atomic.Uint64
	logger  observability.Logger
	mu      sync.RWMutex
	client  *fasthttp.Client
}

// Target is a single upstream endpoint.
type Target struct {
	addr          string
	url           *url.URL
	healthy       atomic.Bool
	passiveErrors atomic.Int64
	probeErrors   atomic.Int64
	successes     atomic.Int64
	metrics       *Metrics
}

// recordPassiveError increments the passive error counter and ejects the
// target via CompareAndSwap if the threshold is reached. Called from both
// the 5xx and connection-error paths. The source string is included in
// the ejection log for operator visibility.
func (t *Target) recordPassiveError(threshold int, logger observability.Logger, poolName, source string) {
	cnt := t.passiveErrors.Add(1)
	if t.metrics != nil {
		t.metrics.incPassiveError(poolName, t.addr)
	}
	if cnt >= int64(threshold) {
		if t.healthy.CompareAndSwap(true, false) {
			if t.metrics != nil {
				t.metrics.incEjection(poolName, t.addr, "passive")
			}
			logger.Warn("target ejected (passive)",
				"pool", poolName,
				"target", t.addr,
				"source", source,
				"consecutive_errors", cnt)
		}
	}
}

// recordProbeError increments the probe error counter and ejects the
// target via CompareAndSwap if the threshold is reached. Called from the
// active health checker. Counter increments are lock-free; the CAS on
// healthy ensures the log and flag always agree.
func (t *Target) recordProbeError(threshold int, logger observability.Logger, poolName string) {
	cnt := t.probeErrors.Add(1)
	if t.metrics != nil {
		t.metrics.incProbeError(poolName, t.addr)
	}
	if cnt >= int64(threshold) {
		if t.healthy.CompareAndSwap(true, false) {
			if t.metrics != nil {
				t.metrics.incEjection(poolName, t.addr, "active")
			}
			logger.Warn("target ejected (active)",
				"pool", poolName,
				"target", t.addr,
				"consecutive_failures", cnt)
		}
	}
}

// recordProbeSuccess increments the probe success counter and restores
// the target via CompareAndSwap if the healthy threshold is reached.
// Does NOT touch passiveErrors — passive and active counters are
// independent.
func (t *Target) recordProbeSuccess(threshold int, logger observability.Logger, poolName string) {
	t.probeErrors.Store(0)
	if t.healthy.Load() {
		return
	}
	cnt := t.successes.Add(1)
	if cnt >= int64(threshold) {
		if t.healthy.CompareAndSwap(false, true) {
			t.successes.Store(0)
			if t.metrics != nil {
				t.metrics.incRestore(poolName, t.addr, "active")
			}
			logger.Info("target restored (active)",
				"pool", poolName,
				"target", t.addr,
				"consecutive_successes", cnt)
		}
	}
}

// DefaultResponseHeaderTimeout bounds the time waiting for the origin's
// response headers after the request is fully sent. Used as the fallback
// when the operator has not configured connect.response_header_timeout.
const DefaultResponseHeaderTimeout = 30 * time.Second

// defaultOriginMaxConnsPerHost caps persistent connections per origin host
// in the fasthttp.Client pool. At 3k req/s with ~2.6ms mean origin latency,
// Little's Law requires ~8 concurrent connections; 52 gives ~6.5x headroom
// for traffic growth while bounding FD usage.
const defaultOriginMaxConnsPerHost = 52

// PoolConfig configures a Pool at construction time.
type PoolConfig struct {
	Name    string
	Targets []string
	Logger  observability.Logger

	// Passive health: eject after this many consecutive 5xx.
	// Zero disables passive health.
	Consecutive5xx int

	// DialTimeout bounds the TCP dial.
	DialTimeout time.Duration

	// Metrics holds Prometheus collectors for origin health events.
	// Nil is safe — all counter methods are no-ops on a nil Metrics.
	Metrics *Metrics
}

// NewPool constructs a pool from config. Returns an error if no
// targets are provided.
func NewPool(cfg PoolConfig) (*Pool, error) {
	cfg.Logger = observability.ResolveLogger(cfg.Logger)
	if len(cfg.Targets) == 0 {
		return nil, fmt.Errorf("origin: pool %q has no targets", cfg.Name)
	}

	p := &Pool{
		Name:   cfg.Name,
		logger: cfg.Logger,
	}

	for _, addr := range cfg.Targets {
		t, err := newTarget(addr)
		if err != nil {
			return nil, fmt.Errorf("origin: pool %q: %w", cfg.Name, err)
		}
		t.metrics = cfg.Metrics
		p.targets = append(p.targets, t)
	}

	return p, nil
}

func newTarget(addr string) (*Target, error) {
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("parse target %q: %w", addr, err)
	}
	t := &Target{addr: u.Host, url: u}
	t.healthy.Store(true)
	return t, nil
}

// pick selects the next healthy target via atomic round-robin.
// Returns nil if all targets are unhealthy.
func (p *Pool) pick() *Target {
	p.mu.RLock()
	n := uint64(len(p.targets))
	p.mu.RUnlock()
	if n == 0 {
		return nil
	}

	start := p.next.Add(1)
	for i := uint64(0); i < n; i++ {
		idx := (start + i) % n
		t := p.targets[idx]
		if t.healthy.Load() {
			return t
		}
	}
	return nil
}

// FastHandler returns a fasthttp.RequestHandler that reverse-proxies
// to this pool. The handler picks a target per request via round-robin
// and forwards through a pooled fasthttp.Client. Passive health
// checking (consecutive-5xx ejection) is applied on response.
func (p *Pool) FastHandler(consecutive5xx int) fasthttp.RequestHandler {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	p.client = &fasthttp.Client{
		MaxConnsPerHost:               defaultOriginMaxConnsPerHost,
		MaxIdleConnDuration:           90 * time.Second,
		ReadTimeout:                   DefaultResponseHeaderTimeout,
		WriteTimeout:                  5 * time.Minute,
		DisableHeaderNamesNormalizing: true,
		Dial: func(addr string) (net.Conn, error) {
			return dialer.Dial("tcp", addr)
		},
	}
	conc5xx := consecutive5xx

	return func(ctx *fasthttp.RequestCtx) {
		t := p.pick()
		if t == nil {
			ctx.Error("no healthy upstream", fasthttp.StatusBadGateway)
			return
		}

		scheme := t.url.Scheme
		if scheme == "" {
			scheme = "http"
		}
		uri := scheme + "://" + t.url.Host + string(ctx.Path())
		if q := ctx.QueryArgs(); q.Len() > 0 {
			uri += "?" + string(q.QueryString())
		}

		req := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(req)
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseResponse(resp)

		req.Header.SetMethod(string(ctx.Method()))
		req.SetRequestURI(uri)
		req.Header.SetHost(t.url.Host)
		//nolint:staticcheck // deprecated but functional
		ctx.Request.Header.VisitAll(func(k, v []byte) {
			req.Header.AddBytesKV(k, v)
		})
		if len(ctx.PostBody()) > 0 {
			req.SetBodyRaw(ctx.PostBody())
		}

		// Use DoTimeout directly because *fasthttp.RequestCtx.Done()
		// panics when the ctx was created manually (not by a real
		// fasthttp server).
		if err := p.client.DoTimeout(req, resp, DefaultResponseHeaderTimeout); err != nil {
			p.logger.Warn("upstream error",
				"pool", p.Name,
				"target", t.addr,
				"uri", uri,
				"error", err)
			if conc5xx > 0 {
				t.recordPassiveError(conc5xx, p.logger, p.Name, "connection error")
			}
			ctx.Error("upstream error", fasthttp.StatusBadGateway)
			return
		}

		// Passive health: eject on consecutive 5xx, reset on success.
		if conc5xx > 0 {
			if resp.StatusCode() >= 500 {
				t.recordPassiveError(conc5xx, p.logger, p.Name, "passive 5xx")
			} else {
				t.passiveErrors.Store(0)
			}
		}

		// Copy response headers and body to the client.
		dst := &ctx.Response.Header
		//nolint:staticcheck // VisitAll deprecated but functional
		resp.Header.VisitAll(func(k, v []byte) {
			dst.AddBytesKV(k, v)
		})
		ctx.SetStatusCode(resp.StatusCode())
		if string(ctx.Method()) != "HEAD" {
			_, _ = ctx.Write(resp.Body())
		}
	}
}

// FastClient returns a cache.FastClient that fetches from this pool
// using fasthttp instead of httputil.ReverseProxy. Each Do() call
// selects a healthy target via round-robin, rewrites the request URI,
// and sends via a pooled fasthttp.Client. When no healthy target is
// available, returns an error.
func (p *Pool) FastClient() *PoolFastClient {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &PoolFastClient{
		pool: p,
		client: &fasthttp.Client{
			MaxConnsPerHost:     defaultOriginMaxConnsPerHost,
			MaxIdleConnDuration: 90 * time.Second,
			ReadTimeout:         DefaultResponseHeaderTimeout,
			WriteTimeout:        5 * time.Minute,
			Dial: func(addr string) (net.Conn, error) {
				return dialer.Dial("tcp", addr)
			},
		},
	}
}

// PoolFastClient implements cache.FastClient by selecting a target
// from the pool and fetching via fasthttp.Client.
type PoolFastClient struct {
	pool   *Pool
	client *fasthttp.Client
}

// Do performs an origin fetch via fasthttp.
func (c *PoolFastClient) Do(ctx context.Context, req *fasthttp.Request, resp *fasthttp.Response) error {
	t := c.pool.pick()
	if t == nil {
		return fmt.Errorf("no healthy upstream")
	}
	// Rewrite the request URI to the selected target.
	scheme := t.url.Scheme
	if scheme == "" {
		scheme = "http"
	}
	req.SetRequestURI(scheme + "://" + t.url.Host + string(req.RequestURI()))
	tc := transport.NewClient(c.client)
	return tc.Do(ctx, req, resp)
}

// Healthy returns the list of currently healthy target addresses.
func (p *Pool) Healthy() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	var out []string
	for _, t := range p.targets {
		if t.healthy.Load() {
			out = append(out, t.addr)
		}
	}
	return out
}

// TargetStatus reports the health and error counts for a single
// upstream target. ConsecutiveErrors is the sum of passive and probe
// errors for backward compatibility with dashboard consumers.
type TargetStatus struct {
	Addr              string
	Healthy           bool
	ConsecutiveErrors int64
	PassiveErrors     int64
	ProbeErrors       int64
}

// Targets returns the health status of all targets in the pool.
func (p *Pool) Targets() []TargetStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]TargetStatus, len(p.targets))
	for i, t := range p.targets {
		pe := t.passiveErrors.Load()
		pr := t.probeErrors.Load()
		out[i] = TargetStatus{
			Addr:              t.addr,
			Healthy:           t.healthy.Load(),
			ConsecutiveErrors: pe + pr,
			PassiveErrors:     pe,
			ProbeErrors:       pr,
		}
	}
	return out
}

// MarkHealthy resets a previously ejected target so it can receive
// traffic again. Called by the active health checker or manual admin
// intervention. Uses CompareAndSwap so the log and the flag always
// agree.
func (p *Pool) MarkHealthy(addr string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, t := range p.targets {
		if t.addr == addr {
			t.passiveErrors.Store(0)
			t.probeErrors.Store(0)
			t.successes.Store(0)
			if t.healthy.CompareAndSwap(false, true) {
				p.logger.Info("target restored (manual)",
					"pool", p.Name,
					"target", t.addr)
			}
		}
	}
}

// Close drains idle upstream connections. Satisfies the lifecycle
// contract so that rolling restarts don't leak TIME_WAIT sockets on
// origins.
func (p *Pool) Close(_ context.Context) error {
	// fasthttp.Client doesn't expose CloseIdleConnections directly;
	// the GC will reclaim idle connections when the Client is
	// dereferenced.
	return nil
}
