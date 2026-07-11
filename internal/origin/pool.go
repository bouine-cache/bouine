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
	addr      string
	url       *url.URL
	healthy   atomic.Bool
	errors    atomic.Int64
	successes atomic.Int64
}

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
			ResponseHeaderTimeout: 30 * time.Second,
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
			http.Error(w, "upstream error", http.StatusBadGateway)
		},

		ModifyResponse: func(resp *http.Response) error {
			t, _ := resp.Request.Context().Value(targetKey{}).(*Target)
			if t == nil {
				return nil
			}
			if consecutive5xx > 0 && resp.StatusCode >= 500 {
				cnt := t.errors.Add(1)
				if cnt >= int64(consecutive5xx) {
					t.healthy.Store(false)
					p.logger.Warn("target ejected",
						"pool", p.Name,
						"target", t.addr,
						"consecutive_5xx", cnt)
				}
			} else {
				t.errors.Store(0)
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

// TargetStatus reports the health and consecutive-error count for a
// single upstream target.
type TargetStatus struct {
	Addr              string
	Healthy           bool
	ConsecutiveErrors int64
}

// Targets returns the health status of all targets in the pool.
func (p *Pool) Targets() []TargetStatus {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := make([]TargetStatus, len(p.targets))
	for i, t := range p.targets {
		out[i] = TargetStatus{
			Addr:              t.addr,
			Healthy:           t.healthy.Load(),
			ConsecutiveErrors: t.errors.Load(),
		}
	}
	return out
}

// MarkHealthy resets a previously ejected target so it can receive
// traffic again. Called by the active health checker.
func (p *Pool) MarkHealthy(addr string) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, t := range p.targets {
		if t.addr == addr {
			t.healthy.Store(true)
			t.errors.Store(0)
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
