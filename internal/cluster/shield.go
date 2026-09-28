package cluster

import (
	"context"
	"crypto/tls"
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

// PeerForwardPath is the HTTP path for shield forwards
// (cluster.origin_shield, ADR-0052): the non-owner sends the original
// client request here and the ring owner runs it through its standard
// data-plane miss path, proxying the response bytes back.
const PeerForwardPath = "/v1/peer/forward"

// shieldForwardTimeout bounds one shield forward on both sides: the
// requester's RPC wait and, carried in the X-Bouine-Deadline header, the
// owner's clamped fetch budget. config.Validate rejects explicit route
// fetch_timeouts at or below this while the shield is on.
const shieldForwardTimeout = api.ShieldForwardTimeout

// shieldMaxBodyBytes caps the response body a shield forward may carry
// through the admin plane's body-limit middleware. The admin default
// (admin.max_body_bytes, 1 MiB) applies below this value.
const shieldMaxBodyBytes int64 = 64 << 20

// ErrShieldForward is returned by ShieldForward when the owner refused
// or could not serve the forward. Callers treat it like any other peer
// error: fall back to the local origin fetch.
var ErrShieldForward = errors.New("shield forward failed")

// parseShieldDeadline reads the absolute unix-nano deadline the
// requester carried. ok=false maps to a 400 — the deadline is mandatory
// (D4): an owner without a bound could outlive the requester's
// patience and burn a wasted origin fetch.
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

// parseShieldKey reads the requester's hex cache key (X-Bouine-Shield-Key)
// and the hop header. ok=false maps to a 400: both are mandatory — the
// key feeds the ownership gate (D5), the hop count is the loop bound.
func parseShieldKey(b []byte) (api.Key, bool) {
	if len(b) != 2*len(api.Key{}) {
		return api.Key{}, false
	}
	var key api.Key
	for i := range key {
		hi, okHi := unhexDigit(b[2*i])
		lo, okLo := unhexDigit(b[2*i+1])
		if !okHi || !okLo {
			return api.Key{}, false
		}
		key[i] = hi<<4 | lo
	}
	return key, true
}

func unhexDigit(c byte) (byte, bool) {
	switch {
	case c >= '0' && c <= '9':
		return c - '0', true
	case c >= 'a' && c <= 'f':
		return c - 'a' + 10, true
	case c >= 'A' && c <= 'F':
		return c - 'A' + 10, true
	default:
		return 0, false
	}
}

// PeerForwardHandler is the fasthttp.RequestHandler for shield
// forwards. Mount on PeerForwardPath (admin plane). The handler checks
// ring ownership (D5) and the hop limit, replays the forwarded request
// through the node's data-plane router (the standard miss path: Vary,
// HEAD, streaming, size caps, negative caching and request rewrites all
// apply by construction, D3), and writes the response bytes back on the
// same connection (D6).
type PeerForwardHandler struct {
	// DataPlane routes the replayed request. Required.
	DataPlane fasthttp.RequestHandler
	// OwnsKey reports whether THIS node currently owns the shield key.
	// A non-owner answers 404 so the requester falls back to origin —
	// under ring disagreement this is the loop prevention (D5). Nil
	// disables the gate (unit tests).
	OwnsKey func(key api.Key) bool
	logger  observability.Logger
	metrics *Metrics
	// FetchBudget is the owner's own fetch budget; the forwarded
	// deadline is clamped to min(its own, the carried one) (D4).
	// Zero applies api.ShieldForwardTimeout.
	FetchBudget time.Duration
	// HopLimit caps the hop count; 0 applies MaxHops.
	HopLimit int
	// Enabled mirrors cluster.origin_shield: a flag-off node answers
	// 404 (never serves forwards, D9).
	Enabled bool
}

// NewPeerForwardHandler creates a shield-forward handler. metrics may
// be nil (then the shield counters are skipped); logger may be nil.
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
		// D9: a flag-off node neither forwards nor serves forwards.
		// 404 (not 501) matches how an endpoint-less old build answers,
		// so mixed-version fleets behave identically during rolling
		// deploys.
		ctx.Error("shield disabled", fasthttp.StatusNotFound)
		return
	}
	method := string(ctx.Method())
	if method != fasthttp.MethodGet && method != fasthttp.MethodHead {
		// D8: GET/HEAD only — matches cacheable-method semantics and
		// keeps request-body streaming out of scope.
		ctx.Error("method not allowed", fasthttp.StatusMethodNotAllowed)
		return
	}
	key, _, hops, ok := h.checkForwardGate(ctx)
	if !ok {
		return
	}

	// D1/D3: replay the ORIGINAL request bytes through the standard
	// data plane. The route's own rewrites (strip_prefix, path_rewrite,
	// header_set) apply here, exactly once, on the owner — which is why
	// the forward must carry the un-rewritten request. UserValues are
	// fresh (no fast-path hints), so the router sees a plain client
	// request; its middleware attributes the hop like any other.
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

// checkForwardGate runs the per-forward admission checks (hop limit,
// deadline, key, ownership, expiry, SSE scope) and answers the error
// itself, returning ok=false when the forward must not proceed.
//
//nolint:gocyclo // 9: gate branches are a checklist by design (D3–D8)
func (h *PeerForwardHandler) checkForwardGate(ctx *fasthttp.RequestCtx) (api.Key, time.Time, int, bool) {
	// Hop limit (D5): a forward is one hop past the key-only
	// peer-fetch the requester already ran, so the counter starts at 1.
	hopBytes := ctx.Request.Header.Peek(BouineHopHeader)
	hops := 1
	if len(hopBytes) > 0 {
		if parsed, err := strconv.Atoi(string(hopBytes)); err == nil && parsed > 0 {
			hops = parsed
		}
	}
	if hops >= h.HopLimit {
		ctx.Error("hop limit", fasthttp.StatusLoopDetected)
		return api.Key{}, time.Time{}, 0, false
	}

	deadline, ok := parseShieldDeadline(ctx.Request.Header.Peek(header.XBouineDeadline))
	if !ok {
		ctx.Error("missing or invalid deadline", fasthttp.StatusBadRequest)
		return api.Key{}, time.Time{}, 0, false
	}
	// D4: clamp the carried deadline against this node's own fetch
	// budget. Identical-config pods make this a no-op in steady state;
	// under config skew the shorter budget wins, so the owner can never
	// be mid-fetch when the requester gives up.
	ownDeadline := time.Now().Add(h.FetchBudget)
	if ownDeadline.Before(deadline) {
		deadline = ownDeadline
	}

	key, ok := parseShieldKey(ctx.Request.Header.Peek(header.XBouineShieldKey))
	if !ok {
		ctx.Error("missing or invalid shield key", fasthttp.StatusBadRequest)
		return api.Key{}, time.Time{}, 0, false
	}
	if h.OwnsKey != nil && !h.OwnsKey(key) {
		// D5: only the ring owner serves forwards. A requester with a
		// stale ring view addressed a non-owner; answering 404 sends it
		// back to its own origin fetch (today's behavior) instead of
		// running a second cluster-wide fetch for the cold key.
		h.logger.Info("refused shield forward: not the ring owner",
			"key", key, "hops", hops)
		ctx.Error("not owner", fasthttp.StatusNotFound)
		return api.Key{}, time.Time{}, 0, false
	}
	if time.Now().After(deadline) {
		// The requester's budget is already spent — a fetch would be
		// wasted (it cannot reach the client in time). Cheap and early.
		ctx.Error("deadline exceeded", fasthttp.StatusGatewayTimeout)
		return api.Key{}, time.Time{}, 0, false
	}

	// D8-adjacent scope: SSE is explicitly out of the shield — a live
	// event stream cannot be replayed into a second connection (the
	// same contract as the foreground singleflight unshareable path,
	// ADR-0042). The requester's plain SSE handling runs locally.
	if header.AcceptsEventStream(ctx.Request.Header.Peek(header.Accept)) {
		ctx.Error("sse not forwardable", fasthttp.StatusNotFound)
		return api.Key{}, time.Time{}, 0, false
	}
	return key, deadline, hops, true
}

// serveForwardResponse proxies the data plane's answer back to the
// requester (D6).
func (h *PeerForwardHandler) serveForwardResponse(ctx *fasthttp.RequestCtx, fwdCtx *fasthttp.RequestCtx, key api.Key, hops int, start time.Time) {
	// D6: proxy the response bytes verbatim — no re-encoding, no
	// object envelope. Status and headers are whatever the standard
	// miss path produced (a MISS origin fill, a 503 shed, an error
	// mapping). The scratch ctx is fully synchronous — the data plane
	// never wrote to a network conn — so by the time it returns, the
	// only body form the cache paths leave open is a tee writer set
	// via SetBodyStreamWriter (the streaming cacheable fill). Reading
	// Body() runs that writer to completion HERE, on the owner: it
	// completes the client copy AND the cache-store tee the fill was
	// promised (streamMissTee buffers for storage inside the writer).
	// Forwarding the stream itself would tie the store to the
	// requester's connection (Reset below would close it mid-write).
	// The copy is one extra body-buffer memcpy per shield fill — the
	// same cost a singleflight follower already pays.
	_ = fwdCtx.Response.Body()
	fwdCtx.Response.CopyTo(&ctx.Response)
	// Fix the framing: a tee'd fill carries Transfer-Encoding: chunked
	// (Content-Length -1) from the stream writer, but the body is now
	// buffered — leaving the chunked header would misframe this
	// response AND every pipelined response after it on the same
	// connection, hanging the rest of a cold-key wave's forwards.
	// SetContentLength with n >= 0 also deletes the Transfer-Encoding header.
	ctx.Response.Header.SetContentLength(len(ctx.Response.Body()))
	if h.metrics != nil {
		h.metrics.IncShield("owner")
		h.metrics.ObserveShieldDuration(time.Since(start))
	}
	h.logger.Debug("served shield forward",
		"key", key, "hops", hops,
		"status", fwdCtx.Response.StatusCode())
}

// initReplayCtx builds the scratch RequestCtx the owner replays the
// original client request through (D1/D3): a fresh ctx on a fake
// conn, control headers stripped, and the client's original
// request-target restored. A missing or oversized forward-URI header
// is an error — the replay must never silently run against the
// endpoint path.
func (h *PeerForwardHandler) initReplayCtx(src *fasthttp.RequestCtx, clientHTTPS bool) (*fasthttp.RequestCtx, error) {
	conn := net.Conn(&shieldConn{})
	if clientHTTPS {
		// The cache key embeds the client scheme (IsTLS feeds
		// BuildKeyFast) but the forward rode the admin plane, whose
		// connection is not the client's — mark the fake conn as TLS
		// so ctx.IsTLS() answers the client's scheme.
		conn = &shieldTLSConn{shieldConn{}}
	}
	fwdCtx := &fasthttp.RequestCtx{}
	fwdCtx.Init2(conn, observability.NewFastHTTPClientLogger(h.logger, "shield"), false)
	src.Request.CopyTo(&fwdCtx.Request)
	// CopyTo preserves the source's parsed URI (computed against the
	// admin conn's scheme). req.isTLS is unexported, so the URI scheme
	// cannot be re-derived via re-parse — it is set explicitly AFTER
	// the URI restore below (SetRequestURIBytes clears parsedURI and a
	// re-parse would consult the stale isTLS), and the cache key reads
	// ctx.IsTLS() (the TLS-marked fake conn) plus the URI forms.
	// Strip the shield control headers before replay: they are hop
	// metadata for THIS RPC, not part of the client's request, and
	// leaving them in would leak the shield's plumbing into Vary
	// computation and origin fetches.
	fwdCtx.Request.Header.Del(header.XBouineDeadline)
	fwdCtx.Request.Header.Del(header.XBouineShieldKey)
	fwdCtx.Request.Header.Del(header.XBouineScheme)
	fwdCtx.Request.Header.Del(BouineHopHeader)
	fwdCtx.Request.Header.Del(ClusterVersionHeader)
	// The wire request line addressed /v1/peer/forward; restore the
	// client's original request-target (path + query) before the data
	// plane sees it.
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

// isHTTPSScheme reports whether the XBouineScheme header value marks
// a client https request. Anything else (including absent — the
// requester-side header is set unconditionally) keeps plain http.
func isHTTPSScheme(s string) bool { return s == "https" }

// shieldConn is the fake conn Init2 needs; the replay never touches
// the network (the data-plane handler dials origin through its own
// pool), so it never reads or writes. RemoteAddr/LocalAddr answer a
// zero TCP address, matching fasthttp's own fakeAddrer behavior.
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

// shieldTLSConn marks the replayed request as having arrived over
// TLS (fasthttp's RequestCtx.IsTLS type-asserts tls.Conn): the shield
// forward rides the admin plane, but the owner's cache key embeds the
// CLIENT's scheme, so an https client request must produce an https
// key on the owner.
type shieldTLSConn struct {
	shieldConn
}

func (*shieldTLSConn) Handshake() error                     { return nil }
func (*shieldTLSConn) ConnectionState() tls.ConnectionState { return tls.ConnectionState{} }

// ShieldForward forwards the original client request to the owner's
// /v1/peer/forward endpoint (requester side of the shield, §4.2). The
// request carries the shield control headers (deadline, hex key, hop,
// cluster version, client scheme); the owner's response bytes are
// returned as-is. Any transport error, refusal, or deadline miss
// returns an error — the caller falls back to its own origin fetch.
//
// The forward rides a dedicated pipeline lane (not the 500ms key-only
// lane): a shield forward legitimately takes origin-scale time, and
// lookups must never queue behind an origin round-trip.
func (f *PeerFetcher) ShieldForward(ctx context.Context, peer api.PeerInfo, req *fasthttp.Request, key api.Key, clientTLS bool, deadline time.Time) (*fasthttp.Response, error) {
	addr := peerAddr(peer)
	if !f.breakerAllowed(addr) {
		return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, ErrPeerBlacklisted)
	}
	if err := f.acquireSlot(ctx, f.shieldSem); err != nil {
		return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, err)
	}
	defer func() { <-f.shieldSem }()

	// Control headers (D4/D5): the deadline is absolute unix-nano; the
	// key hex lets the owner gate on ownership without recomputation;
	// the scheme lets the owner rebuild the CLIENT's cache key (the
	// forward rides the admin plane); the original request-target
	// travels in a header because the wire request line must address
	// /v1/peer/forward; the hop counter starts at 1 (the key-only
	// peer-fetch already ran).
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
	// The wire request line addresses the endpoint, but the Host header
	// MUST stay the client's: the owner's cache key embeds the Host
	// (BuildKeyFast) — overwriting it here (the plain SetHost path) made
	// the owner derive a foreign key, fail the ownership gate, and
	// forward the request onward: a self-sustaining forward loop that
	// burned the whole 30s budget. UseHostHeader keeps the header as
	// the client sent it; the pipeline client dials its configured Addr
	// regardless of the URI host.
	req.UseHostHeader = true

	resp := fasthttp.AcquireResponse()
	pc := f.getShieldPipelineClient(addr)
	if pc == nil {
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("shield forward %s: fetcher closed during shutdown", peer.Addr)
	}
	// The RPC budget is the carried deadline clamped to the lane bound:
	// the owner clamps again against its own fetch budget (D4), so the
	// effective wait never exceeds what the requester can still use.
	if time.Until(deadline) > shieldForwardTimeout {
		if err := pc.DoTimeout(req, resp, shieldForwardTimeout); err != nil {
			if ctx.Err() == nil {
				f.recordPeerFailure(addr)
			}
			fasthttp.ReleaseResponse(resp)
			return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, err)
		}
	} else if err := pc.DoDeadline(req, resp, deadline); err != nil {
		if ctx.Err() == nil {
			f.recordPeerFailure(addr)
		}
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("shield forward %s: %w", peer.Addr, err)
	}
	f.recordPeerSuccess(addr)

	if resp.StatusCode() != fasthttp.StatusOK {
		// 404/410: the owner refused (flag off, not owner, no route) —
		// the plain origin fallback applies, and a 404 is NOT a peer
		// health failure. 503: the owner shed at its fetch semaphore —
		// fall back rather than retry (the same bargem the owner's own
		// clients accept).
		// Read the status BEFORE the release: ReleaseResponse resets
		// the response, so a post-release StatusCode is always 0.
		status := resp.StatusCode()
		fasthttp.ReleaseResponse(resp)
		if status == fasthttp.StatusNotFound {
			return nil, fmt.Errorf("shield forward %s: refused: %w", peer.Addr, ErrShieldForward)
		}
		return nil, fmt.Errorf("shield forward %s: status %d: %w", peer.Addr, status, ErrShieldForward)
	}
	if int64(len(resp.Body())) > shieldMaxBodyBytes {
		fasthttp.ReleaseResponse(resp)
		return nil, fmt.Errorf("shield forward %s: response too large: %w", peer.Addr, ErrShieldForward)
	}
	return resp, nil
}

// getShieldPipelineClient returns the shield-lane PipelineClient for
// addr, creating one on first use. The lane is separate from the
// key-only fetch lane: its ReadTimeout accommodates origin-scale
// latency (a key-only fetch must never queue behind an origin
// round-trip and vice versa). Retired addresses park like the fetch
// lane (see getPipelineClient).
func (f *PeerFetcher) getShieldPipelineClient(addr string) *fasthttp.PipelineClient {
	clients := f.pipelineClients.Load()
	if clients == nil {
		return nil // closed during shutdown
	}
	key := addr + "\x00shield"
	if _, retired := f.retiredAddrs.Load(addr); retired {
		clients.Delete(key)
	}
	if v, ok := clients.Load(key); ok {
		return v.(*fasthttp.PipelineClient)
	}
	pc := &fasthttp.PipelineClient{
		Addr:                          addr,
		MaxConns:                      f.maxConnsPerHost,
		MaxPendingRequests:            peerMaxPendingRequests,
		MaxIdleConnDuration:           f.maxIdleConnDuration,
		ReadTimeout:                   shieldForwardTimeout,
		WriteTimeout:                  5 * time.Minute,
		IsTLS:                         f.useTLS,
		TLSConfig:                     f.tlsConfig,
		DisableHeaderNamesNormalizing: true,
		Logger:                        observability.NewFastHTTPClientLogger(f.logger, "cluster"),
		Dial: func(addr string) (net.Conn, error) {
			if _, retired := f.retiredAddrs.Load(addr); retired {
				<-f.done
				return nil, errPeerAddrRetired
			}
			return (&net.Dialer{
				Timeout:   peerDialTimeout,
				KeepAlive: 30 * time.Second,
			}).Dial("tcp", addr)
		},
	}
	actual, _ := clients.LoadOrStore(key, pc)
	return actual.(*fasthttp.PipelineClient)
}
