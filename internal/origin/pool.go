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
	"strconv"
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
	logger observability.Logger
	client *fasthttp.Client
	// streamClient serves requests that announced SSE intent
	// (Accept: text/event-stream): its connections use per-read idle
	// read deadlines so event streams are not cut by the absolute
	// fetch deadline (see sse.go).
	streamClient *fasthttp.Client
	Name         string
	targets      []*Target
	next         atomic.Uint64
	mu           sync.RWMutex

	// clientConfig holds the resolved connect settings the shared
	// client was built with. Kept after the pointer/lock fields to
	// preserve the struct's pointer-heavy layout (fieldalignment).
	clientConfig clientConfig
}

// Target is a single upstream endpoint.
type Target struct {
	url           *url.URL
	metrics       *Metrics
	addr          string
	passiveErrors atomic.Int64
	probeErrors   atomic.Int64
	successes     atomic.Int64
	healthy       atomic.Bool
}

// recordPassiveError increments the passive error counter and ejects the
// target via CompareAndSwap if the threshold is reached. Called from both
// the 5xx and connection-error paths. The source string is included in
// the ejection log for operator visibility.
func (t *Target) recordPassiveError(threshold int, logger observability.Logger, poolName, source, status string) {
	cnt := t.passiveErrors.Add(1)
	if t.metrics != nil {
		t.metrics.incPassiveError(poolName, t.addr, status)
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

// DefaultMaxIdleConnDuration is how long an idle pooled origin connection
// is kept before closing. Used as the fallback when the operator has not
// configured connect.max_idle_conn_duration.
const DefaultMaxIdleConnDuration = 90 * time.Second

// defaultDialTimeout bounds the TCP dial to an origin target. Fallback
// for connect.timeout.
const defaultDialTimeout = 10 * time.Second

// defaultKeepAlive is the TCP keep-alive probe interval on origin
// connections. Fallback for connect.keep_alive.
const defaultKeepAlive = 30 * time.Second

// classifyConnError maps an origin connection error to a short reason
// string for the bouine_origin_connection_errors_total metric label.
// Uses string matching on the error message to avoid platform-specific
// syscall imports. Returns "timeout", "refused", "reset", or "error".
func classifyConnError(err error) string {
	if err == nil {
		return ""
	}
	s := err.Error()
	if strings.Contains(s, "timeout") || strings.Contains(s, "deadline") {
		return "timeout"
	}
	if strings.Contains(s, "refused") {
		return "refused"
	}
	if strings.Contains(s, "reset") || strings.Contains(s, "broken pipe") {
		return "reset"
	}
	return "error"
}

// defaultOriginMaxConnsPerHost caps persistent connections per origin host
// in the fasthttp.Client pool. At 3k req/s with ~2.6ms mean origin latency,
// Little's Law requires ~8 concurrent connections; 64 gives ~8x headroom
// for traffic growth while bounding FD usage.
const defaultOriginMaxConnsPerHost = 64

// clientConfig holds the resolved (defaults applied) origin client
// settings. Produced once at pool construction and shared by every
// handler/client built from the pool.
type clientConfig struct {
	dialTimeout           time.Duration
	keepAlive             time.Duration
	maxConnsPerHost       int
	maxIdleConnDuration   time.Duration
	responseHeaderTimeout time.Duration
}

// PoolConfig configures a Pool at construction time.
type PoolConfig struct {
	Logger observability.Logger
	// Metrics holds Prometheus collectors for origin health events.
	// Nil is safe — all counter methods are no-ops on a nil Metrics.
	Metrics *Metrics
	Name    string
	Targets []string
	// Passive health: eject after this many consecutive 5xx.
	// Zero disables passive health.
	Consecutive5xx int
	// DialTimeout bounds the TCP dial. Zero applies a 10s default.
	DialTimeout time.Duration
	// KeepAlive is the TCP keep-alive probe interval. Zero applies a
	// 30s default.
	KeepAlive time.Duration
	// MaxConnsPerHost caps concurrent connections per origin host.
	// Zero applies a 64 default.
	MaxConnsPerHost int
	// MaxIdleConnDuration is how long idle pooled connections are kept.
	// Zero applies a 90s default.
	MaxIdleConnDuration time.Duration
	// ResponseHeaderTimeout bounds the wait for origin response headers.
	// Zero applies a 30s default.
	ResponseHeaderTimeout time.Duration
}

// resolveDefault returns def when v is zero, v otherwise. Zero
// configuration always yields the built-in default so existing configs
// keep today's behaviour.
func resolveDefault(v, def time.Duration) time.Duration {
	if v == 0 {
		return def
	}
	return v
}

// resolveDefaultInt returns def when v is not positive, v otherwise.
// Zero configuration always yields the built-in default so existing
// configs keep today's behaviour; negatives are rejected by the loader
// but clamped here too so a raw PoolConfig can't disable the cap.
func resolveDefaultInt(v, def int) int {
	if v <= 0 {
		return def
	}
	return v
}

// newOriginClient builds the shared fasthttp.Client for a pool from the
// resolved connect settings. One client per pool means MaxConnsPerHost
// is enforced per pool, not per route handler.
//
// Header-name normalizing must stay ENABLED: the cache layer reads
// origin responses with canonical Peek keys (ETag, Cache-Control, ...)
// and origins commonly emit lowercase names (Node.js does); with
// DisableHeaderNamesNormalizing fasthttp Peek misses them and the
// cache misclassifies freshness and conditional revalidation
// (http-tests/cache-tests drops to 283/365).
func newOriginClient(cc clientConfig) *fasthttp.Client {
	dialer := &net.Dialer{Timeout: cc.dialTimeout, KeepAlive: cc.keepAlive}
	return &fasthttp.Client{
		MaxConnsPerHost:     cc.maxConnsPerHost,
		MaxIdleConnDuration: cc.maxIdleConnDuration,
		ReadTimeout:         cc.responseHeaderTimeout,
		WriteTimeout:        5 * time.Minute,
		Dial: func(addr string) (net.Conn, error) {
			return dialer.Dial("tcp", addr)
		},
	}
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
		clientConfig: clientConfig{
			dialTimeout:           resolveDefault(cfg.DialTimeout, defaultDialTimeout),
			keepAlive:             resolveDefault(cfg.KeepAlive, defaultKeepAlive),
			maxConnsPerHost:       resolveDefaultInt(cfg.MaxConnsPerHost, defaultOriginMaxConnsPerHost),
			maxIdleConnDuration:   resolveDefault(cfg.MaxIdleConnDuration, DefaultMaxIdleConnDuration),
			responseHeaderTimeout: resolveDefault(cfg.ResponseHeaderTimeout, DefaultResponseHeaderTimeout),
		},
	}
	p.client = newOriginClient(p.clientConfig)
	p.streamClient = newOriginStreamClient(p.clientConfig)

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
// and forwards through the pool's shared fasthttp.Client. Passive
// health checking (consecutive-5xx ejection) is applied on response.
func (p *Pool) FastHandler(consecutive5xx int) fasthttp.RequestHandler {
	conc5xx := consecutive5xx

	return func(ctx *fasthttp.RequestCtx) {
		t := p.pick()
		if t == nil {
			ctx.Error("no healthy upstream", fasthttp.StatusBadGateway)
			return
		}

		t.metrics.incActiveConnection(p.Name, t.addr)
		defer t.metrics.decActiveConnection(p.Name, t.addr)
		originStart := time.Now()

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
		// fasthttp server). Bound by connect.response_header_timeout:
		// on this proxied (never-cached) path it doubles as the total
		// fetch bound, covering headers and body.
		if err := p.client.DoTimeout(req, resp, p.clientConfig.responseHeaderTimeout); err != nil {
			p.logger.Warn("upstream error",
				"pool", p.Name,
				"target", t.addr,
				"uri", uri,
				"error", err)
			connErrReason := classifyConnError(err)
			t.metrics.incConnectionError(p.Name, t.addr, connErrReason)
			t.metrics.observeRequestDuration(p.Name, t.addr, connErrReason, time.Since(originStart).Seconds())
			if conc5xx > 0 {
				t.recordPassiveError(conc5xx, p.logger, p.Name, "connection error", connErrReason)
			}
			ctx.Error("upstream error", fasthttp.StatusBadGateway)
			return
		}

		statusCode := resp.StatusCode()
		statusStr := strconv.Itoa(statusCode)
		t.metrics.observeRequestDuration(p.Name, t.addr, statusStr, time.Since(originStart).Seconds())

		// Passive health: eject on consecutive 5xx, reset on success.
		if conc5xx > 0 {
			if statusCode >= 500 {
				t.recordPassiveError(conc5xx, p.logger, p.Name, "passive 5xx", statusStr)
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
		ctx.SetStatusCode(statusCode)
		if string(ctx.Method()) != "HEAD" {
			_, _ = ctx.Write(resp.Body())
		}
	}
}

// ClientSettings exposes the pool's resolved origin client settings
// (defaults applied) for observability and wiring tests.
type ClientSettings struct {
	DialTimeout           time.Duration
	KeepAlive             time.Duration
	MaxConnsPerHost       int
	MaxIdleConnDuration   time.Duration
	ResponseHeaderTimeout time.Duration
}

// ResolvedClientConfig returns the connect settings this pool's shared
// fasthttp.Client was built with, after zero-value defaults were
// applied. Stable.
func (p *Pool) ResolvedClientConfig() ClientSettings {
	return ClientSettings{
		DialTimeout:           p.clientConfig.dialTimeout,
		KeepAlive:             p.clientConfig.keepAlive,
		MaxConnsPerHost:       p.clientConfig.maxConnsPerHost,
		MaxIdleConnDuration:   p.clientConfig.maxIdleConnDuration,
		ResponseHeaderTimeout: p.clientConfig.responseHeaderTimeout,
	}
}

// FastClient returns a cache.FastClient that fetches from this pool
// using fasthttp instead of httputil.ReverseProxy. Each Do() call
// selects a healthy target via round-robin, rewrites the request URI,
// and sends via the pool's shared fasthttp.Client. When no healthy
// target is available, returns an error.
func (p *Pool) FastClient() *PoolFastClient {
	return &PoolFastClient{
		pool:         p,
		client:       p.client,
		streamClient: p.streamClient,
	}
}

// PoolFastClient implements cache.FastClient by selecting a target
// from the pool and fetching via fasthttp.Client. Requests that
// announced SSE intent are routed to the pool's stream client, whose
// connections carry per-read idle read deadlines instead of the
// absolute fetch deadline (see sse.go).
type PoolFastClient struct {
	pool         *Pool
	client       *fasthttp.Client
	streamClient *fasthttp.Client
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

	t.metrics.incActiveConnection(c.pool.Name, t.addr)
	defer t.metrics.decActiveConnection(c.pool.Name, t.addr)
	originStart := time.Now()

	// SSE-intent requests go through the stream client (idle read
	// deadlines) so the event stream is not cut by the absolute fetch
	// deadline armed before the response headers (sse.go).
	fetchClient := c.client
	if c.streamClient != nil && isSSERequest(req) {
		fetchClient = c.streamClient
	}

	tc := transport.NewClient(fetchClient)
	err := tc.Do(ctx, req, resp)
	if err != nil {
		connErrReason := classifyConnError(err)
		t.metrics.incConnectionError(c.pool.Name, t.addr, connErrReason)
		t.metrics.observeRequestDuration(c.pool.Name, t.addr, connErrReason, time.Since(originStart).Seconds())
		return err
	}
	t.metrics.observeRequestDuration(c.pool.Name, t.addr, strconv.Itoa(resp.StatusCode()), time.Since(originStart).Seconds())
	return nil
}

// DoDeadline performs an origin fetch bounded by an absolute deadline,
// enforced via fasthttp's kernel-level connection read/write deadlines.
// It maps directly to fasthttp.Client.DoDeadline: no context, no timer
// goroutine, no timer allocation. The deadline aborts a hung origin at
// the socket level — the only mechanism that actually reaches the
// transport (transport.Client.Do contexts without a deadline fall
// back to a fixed 60s DoTimeout, so ctx-based timeouts alone never
// enforce fetch_timeout here).
func (c *PoolFastClient) DoDeadline(req *fasthttp.Request, resp *fasthttp.Response, deadline time.Time) error {
	t := c.pool.pick()
	if t == nil {
		return fmt.Errorf("no healthy upstream")
	}
	// Rewrite the request URI to the selected target.
	scheme := t.url.Scheme
	if scheme == "" {
		scheme = "http"
	}
	// The dialed host is always the operator-configured pool target;
	// only the request's own path/query is appended — the same data
	// flow as Do and FastHandler, which carry the already-accepted
	// go/request-forgery alerts. Suppressing here keeps this
	// duplicated sink from adding a third alert.
	// lgtm[go/request-forgery] — see docs/architecture.md §6 threat model
	req.SetRequestURI(scheme + "://" + t.url.Host + string(req.RequestURI()))

	t.metrics.incActiveConnection(c.pool.Name, t.addr)
	defer t.metrics.decActiveConnection(c.pool.Name, t.addr)
	originStart := time.Now()

	// SSE-intent requests go through the stream client (per-read idle
	// read deadlines) so the event stream is not cut by the absolute
	// deadline fasthttp arms before the response headers (sse.go).
	fetchClient := c.client
	if c.streamClient != nil && isSSERequest(req) {
		fetchClient = c.streamClient
	}

	err := fetchClient.DoDeadline(req, resp, deadline)
	if err != nil {
		connErrReason := classifyConnError(err)
		t.metrics.incConnectionError(c.pool.Name, t.addr, connErrReason)
		t.metrics.observeRequestDuration(c.pool.Name, t.addr, connErrReason, time.Since(originStart).Seconds())
		return err
	}
	t.metrics.observeRequestDuration(c.pool.Name, t.addr, strconv.Itoa(resp.StatusCode()), time.Since(originStart).Seconds())
	return nil
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
	p.client.CloseIdleConnections()
	return nil
}
