package cluster

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// PeerForwardPath is the shield-forward endpoint (ADR-0052): the
// non-owner sends the original client request here; the ring owner
// runs it through its standard miss path and proxies the response.
const PeerForwardPath = "/v1/peer/forward"

// shieldForwardTimeout bounds one forward on both sides (requester
// wait; owner clamp via X-Bouine-Deadline). config.Validate rejects
// route fetch_timeouts at or below this while the shield is on.
const shieldForwardTimeout = api.ShieldForwardTimeout

// shieldMaxBodyBytes caps the forward response body at the transport
// layer (the shield client's MaxResponseBodySize): a misbehaving peer
// cannot make the requester buffer unbounded bytes before the check.
const shieldMaxBodyBytes int64 = 64 << 20

// Shield reply marker values (header.XBouineShieldResult).
const (
	// shieldResultServed marks a reply the owner's replay produced —
	// any status, including an origin 404/5xx relayed through it.
	shieldResultServed = "served"
	// shieldResultRefused marks an endpoint-level refusal (disabled,
	// wrong method, gate failure). Refusals and origin answers can
	// share a status (404), so the marker is what the requester keys on.
	shieldResultRefused = "refused"
)

// shieldLaneKeySuffix namespaces the shield lane's cached client in the
// peer client map, away from the key-only fetch lane.
const shieldLaneKeySuffix = "\x00shield"

// ErrShieldForward is returned when the owner refused or failed the
// forward; callers fall back to their own origin fetch.
var ErrShieldForward = errors.New("shield forward failed")

// parseShieldDeadline reads the absolute unix-nano deadline. Mandatory
// (D4): an unbounded owner could outlive the requester's patience.
func parseShieldDeadline(b []byte) (time.Time, bool) {
	if len(b) == 0 {
		return time.Time{}, false
	}
	ns, err := strconv.ParseInt(string(b), 10, 64)
	if err != nil || ns <= 0 {
		return time.Time{}, false
	}
	//nolint:gosec // G115: a unix-nano value fits in int64 by construction.
	return time.Unix(0, ns), true
}

// parseShieldKey reads the hex shield key and hop header; both are
// mandatory (key feeds the ownership gate, hops bound the loop).
func parseShieldKey(b []byte) (api.Key, bool) {
	if len(b) != 2*len(api.Key{}) {
		return api.Key{}, false
	}
	var key api.Key
	if _, err := hex.Decode(key[:], b); err != nil {
		return api.Key{}, false
	}
	return key, true
}

// PeerForwardHandler serves shield forwards on the admin plane.
type PeerForwardHandler struct {
	DataPlane fasthttp.RequestHandler
	// OwnsKey: a non-owner answers 404 (loop prevention under ring
	// disagreement). Nil disables the gate (unit tests).
	OwnsKey func(key api.Key) bool
	logger  observability.Logger
	metrics *Metrics
	// FetchBudget: the carried deadline is clamped to min(its own,
	// the carried one) (D4). Zero applies shieldForwardTimeout.
	FetchBudget time.Duration
	HopLimit    int
	// Enabled mirrors cluster.origin_shield; a flag-off node answers
	// 404 (D9).
	Enabled bool
}

// NewPeerForwardHandler creates a shield-forward handler. metrics and
// logger may be nil.
func NewPeerForwardHandler(dataPlane fasthttp.RequestHandler, ownsKey func(api.Key) bool, fetchBudget time.Duration, hopLimit int, enabled bool, logger observability.Logger, metrics *Metrics) *PeerForwardHandler {
	if hopLimit <= 0 {
		hopLimit = MaxHops
	}
	if fetchBudget <= 0 {
		fetchBudget = shieldForwardTimeout
	}
	return &PeerForwardHandler{
		DataPlane:   dataPlane,
		OwnsKey:     ownsKey,
		FetchBudget: fetchBudget,
		HopLimit:    hopLimit,
		Enabled:     enabled,
		logger:      observability.ResolveLogger(logger),
		metrics:     metrics,
	}
}

// Handle is the fasthttp.RequestHandler for shield forwards.
//
//nolint:gocyclo // 12: gate/hop/deadline/method branches are a checklist by design (D3–D8)
func (h *PeerForwardHandler) Handle(ctx *fasthttp.RequestCtx) {
	if !h.Enabled {
		// 404, not 501: matches an endpoint-less old build so mixed
		// fleets behave identically during rolling deploys.
		h.refuse(ctx, "disabled", fasthttp.StatusNotFound)
		return
	}
	method := string(ctx.Method())
	if method != fasthttp.MethodGet && method != fasthttp.MethodHead {
		h.refuse(ctx, "method", fasthttp.StatusMethodNotAllowed)
		return
	}
	key, _, hops, ok := h.checkForwardGate(ctx)
	if !ok {
		return
	}

	// D1/D3: replay the ORIGINAL request bytes through the standard data
	// plane, so the route's rewrites apply once, on the owner. Fresh
	// UserValues: no fast-path hints — the router sees a plain client
	// request.
	start := time.Now()
	clientHTTPS := isHTTPSScheme(string(ctx.Request.Header.Peek(header.XBouineScheme)))
	fwdCtx, err := h.initReplayCtx(ctx, clientHTTPS)
	if err != nil {
		ctx.Error(err.Error(), fasthttp.StatusBadRequest)
		return
	}
	defer fwdCtx.Response.Reset()
	h.DataPlane(fwdCtx)

	h.serveForwardResponse(ctx, fwdCtx, key, hops, start)
}

// refuse answers an endpoint-level refusal: the marker tells the
// requester this is not an origin answer relayed through the replay —
// the two can share a status (404). The marker goes on after Error:
// ctx.Error resets the response.
func (h *PeerForwardHandler) refuse(ctx *fasthttp.RequestCtx, reason string, code int) {
	ctx.Error("shield refused: "+reason, code)
	ctx.Response.Header.Set(header.XBouineShieldResult, shieldResultRefused)
}

// checkForwardGate runs the admission checks (hop limit, deadline,
// key, ownership, expiry, SSE) and answers the error itself.
//
//nolint:gocyclo // 9: gate branches are a checklist by design (D3–D8)
func (h *PeerForwardHandler) checkForwardGate(ctx *fasthttp.RequestCtx) (api.Key, time.Time, int, bool) {
	// The counter starts at 1: a forward is one hop past the key-only
	// peer-fetch the requester already ran.
	hopBytes := ctx.Request.Header.Peek(BouineHopHeader)
	hops := 1
	if len(hopBytes) > 0 {
		if parsed, err := strconv.Atoi(string(hopBytes)); err == nil && parsed > 0 {
			hops = parsed
		}
	}
	if hops >= h.HopLimit {
		h.refuse(ctx, "hop limit", fasthttp.StatusLoopDetected)
		return api.Key{}, time.Time{}, 0, false
	}

	deadline, ok := parseShieldDeadline(ctx.Request.Header.Peek(header.XBouineDeadline))
	if !ok {
		h.refuse(ctx, "missing or invalid deadline", fasthttp.StatusBadRequest)
		return api.Key{}, time.Time{}, 0, false
	}
	// D4: under config skew the shorter budget wins, so the owner can
	// never be mid-fetch when the requester gives up.
	ownDeadline := time.Now().Add(h.FetchBudget)
	if ownDeadline.Before(deadline) {
		deadline = ownDeadline
	}

	key, ok := parseShieldKey(ctx.Request.Header.Peek(header.XBouineShieldKey))
	if !ok {
		h.refuse(ctx, "missing or invalid shield key", fasthttp.StatusBadRequest)
		return api.Key{}, time.Time{}, 0, false
	}
	if h.OwnsKey != nil && !h.OwnsKey(key) {
		// D5: answering 404 sends a stale-ring requester back to its own
		// origin fetch instead of doubling the cold-key fetch.
		h.logger.Info("refused shield forward: not the ring owner",
			"key", key, "hops", hops)
		h.refuse(ctx, "not owner", fasthttp.StatusNotFound)
		return api.Key{}, time.Time{}, 0, false
	}
	if time.Now().After(deadline) {
		// The requester's budget is already spent — a fetch could not
		// reach the client in time.
		h.refuse(ctx, "deadline exceeded", fasthttp.StatusGatewayTimeout)
		return api.Key{}, time.Time{}, 0, false
	}

	// SSE cannot be replayed into a second connection (ADR-0042); the
	// requester's plain SSE handling runs locally.
	if header.AcceptsEventStream(ctx.Request.Header.Peek(header.Accept)) {
		h.refuse(ctx, "sse not forwardable", fasthttp.StatusNotFound)
		return api.Key{}, time.Time{}, 0, false
	}
	return key, deadline, hops, true
}

// serveForwardResponse proxies the data plane's answer back to the
// requester (D6).
func (h *PeerForwardHandler) serveForwardResponse(ctx *fasthttp.RequestCtx, fwdCtx *fasthttp.RequestCtx, key api.Key, hops int, start time.Time) {
	// The scratch ctx is synchronous; by the time it returns, the only
	// body form still open is a tee'd streaming fill. Reading Body()
	// runs that writer to completion HERE: it finishes the client copy
	// AND the store tee the fill was promised. Forwarding the stream
	// itself would tie the store to the requester's connection.
	_ = fwdCtx.Response.Body()
	fwdCtx.Response.CopyTo(&ctx.Response)
	// A tee'd fill carries Transfer-Encoding: chunked; the body is now
	// buffered. Leaving chunked framing would misframe this response
	// and every pipelined response after it on the same connection.
	// SetContentLength also deletes the Transfer-Encoding header.
	ctx.Response.Header.SetContentLength(len(ctx.Response.Body()))
	// Mark the reply as replay-produced (any status): the requester keys
	// its relay-or-fallback decision on this marker, not the status —
	// refusals and origin answers can share one (404).
	ctx.Response.Header.Set(header.XBouineShieldResult, shieldResultServed)
	if h.metrics != nil {
		// A replayed 5xx means the owner's miss path degraded (shed,
		// origin failure) — the requester falls back, so separate it from
		// the served case instead of counting both as "owner".
		if fwdCtx.Response.StatusCode() >= fasthttp.StatusInternalServerError {
			h.metrics.IncShield("failure")
		} else {
			h.metrics.IncShield("owner")
		}
		h.metrics.ObserveShieldDuration(time.Since(start))
	}
	h.logger.Debug("served shield forward",
		"key", key, "hops", hops,
		"status", fwdCtx.Response.StatusCode())
}

// initReplayCtx builds the scratch RequestCtx the owner replays the
// original request through (D1/D3): fresh ctx on a fake conn, control
// headers stripped, original request-target restored. A missing or
// oversized forward-URI header is an error — the replay must never
// silently run against the endpoint path.
func (h *PeerForwardHandler) initReplayCtx(src *fasthttp.RequestCtx, clientHTTPS bool) (*fasthttp.RequestCtx, error) {
	conn := net.Conn(&shieldConn{})
	if clientHTTPS {
		// The cache key embeds the client scheme, but the forward rode
		// the admin plane — mark the fake conn TLS so ctx.IsTLS() answers
		// the client's scheme.
		conn = &shieldTLSConn{shieldConn{}}
	}
	fwdCtx := &fasthttp.RequestCtx{}
	fwdCtx.Init2(conn, observability.NewFastHTTPClientLogger(h.logger, "shield"), false)
	src.Request.CopyTo(&fwdCtx.Request)
	// Strip the shield control headers before replay: hop metadata for
	// THIS RPC, not part of the client's request — leaving them in would
	// leak shield plumbing into Vary computation and origin fetches.
	// The scheme is re-derived via the URI restore below (SetRequestURIBytes
	// clears parsedURI), after the header strip.
	fwdCtx.Request.Header.Del(header.XBouineDeadline)
	fwdCtx.Request.Header.Del(header.XBouineShieldKey)
	fwdCtx.Request.Header.Del(header.XBouineScheme)
	fwdCtx.Request.Header.Del(BouineHopHeader)
	fwdCtx.Request.Header.Del(ClusterVersionHeader)
	if orig := src.Request.Header.Peek(header.XBouineForwardURI); len(orig) > 0 && len(orig) <= 8192 {
		fwdCtx.Request.SetRequestURIBytes(orig)
	} else {
		return nil, errors.New("missing or invalid forward URI")
	}
	fwdCtx.Request.Header.Del(header.XBouineForwardURI)
	if clientHTTPS {
		fwdCtx.Request.URI().SetScheme("https")
	}
	return fwdCtx, nil
}

// isHTTPSScheme: only "https" marks a client https request; the
// requester sets the header unconditionally.
func isHTTPSScheme(s string) bool { return s == "https" }

// shieldConn is the fake conn Init2 needs; the replay never touches
// the network (the data plane dials origin through its own pool).
type shieldConn struct{}

var shieldZeroTCPAddr = &net.TCPAddr{}

func (shieldConn) Read([]byte) (int, error)         { return 0, net.ErrClosed }
func (shieldConn) Write([]byte) (int, error)        { return 0, net.ErrClosed }
func (shieldConn) Close() error                     { return nil }
func (shieldConn) LocalAddr() net.Addr              { return shieldZeroTCPAddr }
func (shieldConn) RemoteAddr() net.Addr             { return shieldZeroTCPAddr }
func (shieldConn) SetDeadline(time.Time) error      { return nil }
func (shieldConn) SetReadDeadline(time.Time) error  { return nil }
func (shieldConn) SetWriteDeadline(time.Time) error { return nil }

// shieldTLSConn marks the replay as TLS (fasthttp's IsTLS asserts
// tls.Conn) so an https client request produces an https key on the
// owner.
type shieldTLSConn struct {
	shieldConn
}

func (*shieldTLSConn) Handshake() error                     { return nil }
func (*shieldTLSConn) ConnectionState() tls.ConnectionState { return tls.ConnectionState{} }

// ShieldForward forwards the original client request to the owner's
// PeerForwardPath endpoint (requester side, §4.2). Any transport
// error, refusal, or deadline miss returns an error — the caller
// falls back to its own origin fetch. Rides a dedicated pipeline
// lane: a forward takes origin-scale time and lookups must never
// queue behind it.
func (f *PeerFetcher) ShieldForward(ctx context.Context, peer api.PeerInfo, req *fasthttp.Request, key api.Key, clientTLS bool, deadline time.Time) (*fasthttp.Response, error) {
	addr := peerAddr(peer)
	if !f.breakerAllowed(addr) {
		return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, ErrPeerBlacklisted)
	}
	if err := f.acquireSlot(ctx, f.shieldSem); err != nil {
		return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, err)
	}
	defer func() { <-f.shieldSem }()

	// Control headers: the hex key lets the owner gate on ownership;
	// the scheme lets it rebuild the CLIENT's key; the original
	// request-target travels in a header because the wire request line
	// must address the endpoint.
	origURI := req.RequestURI()
	req.Header.Set(header.XBouineDeadline, strconv.FormatInt(deadline.UnixNano(), 10))
	req.Header.Set(header.XBouineShieldKey, key.Hex())
	scheme := "http"
	if clientTLS {
		scheme = "https"
	}
	req.Header.Set(header.XBouineScheme, scheme)
	req.Header.SetBytesV(header.XBouineForwardURI, origURI)
	req.Header.Set(BouineHopHeader, "1")
	req.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)
	req.SetRequestURI(PeerForwardPath)
	// The Host header MUST stay the client's: the owner's key embeds it,
	// and overwriting it made the owner derive a foreign key, fail its
	// ownership gate, and forward onward — a self-sustaining loop.
	// UseHostHeader keeps the header; the pipeline client dials its
	// configured Addr regardless of the URI host.
	req.UseHostHeader = true

	resp := fasthttp.AcquireResponse()
	client := f.getShieldClient(addr)
	if client == nil {
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("shield forward %s: fetcher closed during shutdown", peer.Addr)
	}
	// The owner clamps against its own budget too, so the effective
	// wait never exceeds what the requester can still use.
	if time.Until(deadline) > shieldForwardTimeout {
		if err := client.DoTimeout(req, resp, shieldForwardTimeout); err != nil {
			if ctx.Err() == nil {
				f.recordPeerFailure(addr)
			}
			fasthttp.ReleaseResponse(resp)
			return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, err)
		}
	} else if err := client.DoDeadline(req, resp, deadline); err != nil {
		if ctx.Err() == nil {
			f.recordPeerFailure(addr)
		}
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, err)
	}
	f.recordPeerSuccess(addr)

	// The result marker separates endpoint refusals (and unmarked
	// replies from an endpoint-less old build) from origin answers
	// relayed through the replay — refusals and origin answers can
	// share a status (404), and only the marker disambiguates. Body
	// size is bounded by the client's MaxResponseBodySize.
	if result := resp.Header.Peek(header.XBouineShieldResult); string(result) != shieldResultServed {
		status := resp.StatusCode()
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("shield forward %s: refused (status %d): %w", peer.Addr, status, ErrShieldForward)
	}
	return resp, nil
}

// getShieldClient returns the shield lane's HTTP client for addr. A
// plain pooled client, not the key lane's PipelineClient: a forward
// holds its slot for origin-scale time, so pipelining buys nothing,
// and fasthttp's PipelineClient cannot bound the response body — the
// client's MaxResponseBodySize aborts the read mid-body instead of
// allocating first and checking after.
func (f *PeerFetcher) getShieldClient(addr string) *fasthttp.HostClient {
	clients := f.pipelineClients.Load()
	if clients == nil {
		return nil // closed during shutdown
	}
	key := addr + shieldLaneKeySuffix
	if _, retired := f.retiredAddrs.Load(addr); retired {
		clients.Delete(key)
	}
	if v, ok := clients.Load(key); ok {
		return v.(*fasthttp.HostClient)
	}
	client := &fasthttp.HostClient{
		Addr:                addr,
		MaxConns:            f.maxConnsPerHost,
		MaxIdleConnDuration: f.maxIdleConnDuration,
		ReadTimeout:         shieldForwardTimeout,
		WriteTimeout:        5 * time.Minute,
		MaxResponseBodySize: int(shieldMaxBodyBytes),
		IsTLS:               f.useTLS,
		TLSConfig:           f.tlsConfig,
		// Parity with the origin client: a 4 KiB default caps the
		// parseable response-header block below what origins emit.
		ReadBufferSize:                64 << 10,
		DisableHeaderNamesNormalizing: true,
		Dial: func(addr string) (net.Conn, error) {
			// Unlike the key lane's PipelineClient there is no worker
			// to park: a dial error just fails this request.
			if _, retired := f.retiredAddrs.Load(addr); retired {
				return nil, errPeerAddrRetired
			}
			return (&net.Dialer{
				Timeout:   peerDialTimeout,
				KeepAlive: 30 * time.Second,
			}).Dial("tcp", addr)
		},
	}
	actual, _ := clients.LoadOrStore(key, client)
	return actual.(*fasthttp.HostClient)
}
