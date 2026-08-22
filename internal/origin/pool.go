// Package origin is the L5 upstream layer. It manages connection pools
// to origin servers, selects targets via round-robin (ADR-0005),
// performs passive health checking (consecutive-5xx ejection), active
// health probes, hedged requests, and exposes a reverse-proxy
// http.Handler that forwards requests to the chosen target.
package origin

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
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
	Name      string
	targets   []*Target
	next      atomic.Uint64
	logger    observability.Logger
	mu        sync.RWMutex
	transport http.RoundTripper
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
// the ModifyResponse (5xx) and ErrorHandler (connection error) paths. The
// source string is included in the ejection log for operator visibility.
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

// PoolConfig configures a Pool at construction time.
type PoolConfig struct {
	Name    string
	Targets []string
	Logger  observability.Logger

	// Passive health: eject after this many consecutive 5xx.
	// Zero disables passive health.
	Consecutive5xx int

	// Transport overrides the default http.Transport (useful for tests).
	Transport http.RoundTripper

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

// targetKey is the context key used to pass the selected *Target to
// the ReverseProxy Director / ModifyResponse / ErrorHandler without
// creating a new closure per request.
type targetKey struct{}

// Handler returns an http.Handler that reverse-proxies to this pool.
// The returned handler picks a target per request via round-robin and
// forwards through a single, shared httputil.ReverseProxy — avoiding
// the per-request allocation of a new ReverseProxy struct and its
// three function closures.
func (p *Pool) Handler(consecutive5xx int, transport http.RoundTripper) http.Handler {
	if transport == nil {
		transport = &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConnsPerHost:   64,
			IdleConnTimeout:       90 * time.Second,
			ResponseHeaderTimeout: DefaultResponseHeaderTimeout,
			ForceAttemptHTTP2:     true,
		}
	}
	p.transport = transport

	// Construct the ReverseProxy once. The selected target is stored in
	// the request context so Director/ModifyResponse/ErrorHandler can
	// read it without capturing a per-request pointer.
	proxy := &httputil.ReverseProxy{
		Transport: transport,

		Director: func(req *http.Request) {
			t, _ := req.Context().Value(targetKey{}).(*Target)
			if t == nil {
				return
			}
			req.URL.Scheme = t.url.Scheme
			req.URL.Host = t.url.Host
			if req.URL.Scheme == "" {
				req.URL.Scheme = "http"
			}
			req.Host = t.url.Host
		},

		ErrorHandler: func(w http.ResponseWriter, req *http.Request, err error) {
			t, _ := req.Context().Value(targetKey{}).(*Target)
			addr := ""
			if t != nil {
				addr = t.addr
			}
			p.logger.Warn("upstream error",
				"pool", p.Name,
				"target", addr,
				"error", err)
			if t != nil && consecutive5xx > 0 {
				t.recordPassiveError(consecutive5xx, p.logger, p.Name, "connection error")
			}
			http.Error(w, "upstream error", http.StatusBadGateway)
		},

		ModifyResponse: func(resp *http.Response) error {
			t, _ := resp.Request.Context().Value(targetKey{}).(*Target)
			if t == nil {
				return nil
			}
			if consecutive5xx > 0 && resp.StatusCode >= 500 {
				t.recordPassiveError(consecutive5xx, p.logger, p.Name, "passive 5xx")
			} else if consecutive5xx > 0 {
				t.passiveErrors.Store(0)
			}
			return nil
		},
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := p.pick()
		if t == nil {
			http.Error(w, "no healthy upstream", http.StatusBadGateway)
			return
		}
		// Attach the selected target to the request context so the shared
		// proxy functions above can read it. WithContext copies the
		// *http.Request struct and wraps the context — one allocation, but
		// far cheaper than the previous per-request ReverseProxy + 3 closures.
		proxy.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), targetKey{}, t)))
	})
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
			MaxConnsPerHost:     64,
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
// the origin side.
func (p *Pool) Close(_ context.Context) error {
	if t, ok := p.transport.(*http.Transport); ok {
		t.CloseIdleConnections()
	}
	return nil
}
