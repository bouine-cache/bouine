package cluster

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/transport"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// broadcastTimeout is the per-call timeout for HTTP fan-out. Enforced
// via context.WithTimeout so the shared client has no global Timeout
// (which would cancel in-flight requests across all goroutines).
const broadcastTimeout = 2 * time.Second

// countPeers returns the number of live peers excluding the local node.
func countPeers(members []api.PeerInfo) int {
	n := len(members)
	if n > 0 {
		n--
	}
	return n
}

// Broadcaster fans out purge and ban events to all cluster peers.
// In strong mode it uses HTTP fan-out for invalidations; in eventual
// mode it uses gossip only.
//
// Stable.
type Broadcaster struct {
	cluster *Cluster
	fetcher *PeerFetcher
	client  *transport.Client // shared across all postBinary calls for connection reuse
	seq     atomic.Uint64
	logger  observability.Logger
	token   string
	mode    string // ClusterModeStrong | ClusterModeEventual
	metrics *Metrics
}

// NewBroadcaster creates a broadcaster for the given cluster.
// token is the admin bearer token used when posting to peer admin APIs.
func NewBroadcaster(c *Cluster, fetcher *PeerFetcher, token ...string) *Broadcaster {
	logger := c.logger
	tok := ""
	if len(token) > 0 {
		tok = token[0]
	}
	// Broadcast is fire-and-forget (one request per peer), so it uses
	// a standalone non-pipelined client. The PeerFetcher's PipelineClient
	// is per-peer-address and optimized for request collapsing, not
	// one-shot fan-out. TLS is inherited from the fetcher when present.
	var tlsCfg *tls.Config
	if fetcher != nil && fetcher.useTLS {
		tlsCfg = fetcher.tlsConfig
	}
	fc := &fasthttp.Client{
		MaxConnsPerHost:     256,
		MaxIdleConnDuration: 90 * time.Second,
		ReadTimeout:         broadcastTimeout,
		WriteTimeout:        5 * time.Minute,
		TLSConfig:           tlsCfg,
		Dial: func(addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).Dial("tcp", addr)
		},
	}
	client := transport.NewClient(fc)
	return &Broadcaster{
		cluster: c,
		fetcher: fetcher,
		client:  client,
		logger:  logger,
		token:   tok,
		mode:    c.Mode(),
		metrics: c.metrics,
	}
}

// BroadcastPurge sends a purge event for key to all live peers.
// In strong mode it posts to each peer's admin API and also enqueues
// via gossip for redundant delivery. In eventual mode it sends via
// gossip only (no HTTP fan-out).
func (b *Broadcaster) BroadcastPurge(ctx context.Context, key api.Key, varyKey string) {
	// Detach from the caller's context so peer fan-out is bounded only
	// by broadcastTimeout, not by the engine lifecycle or per-request
	// cancellation. This ensures final purges during shutdown reach all
	// peers. The caller's ctx still governs local store operations.
	fanoutCtx := context.WithoutCancel(ctx)

	evt := api.PurgeEvent{
		Key:      key,
		VaryKey:  varyKey,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}

	if b.mode == config.ClusterModeStrong {
		peers := b.cluster.Members()
		var wg sync.WaitGroup
		for _, p := range peers {
			if p.Name == b.cluster.cfg.NodeName {
				continue
			}
			wg.Add(1)
			go func(peer api.PeerInfo) {
				defer wg.Done()
				defer func() {
					if v := recover(); v != nil {
						b.logger.Error("purge broadcast panicked",
							"peer", peer.Name,
							"panic", v)
					}
				}()
				if err := b.sendPurge(fanoutCtx, peer, evt); err != nil {
					b.logger.Warn("purge broadcast failed",
						"peer", peer.Name,
						"key", evt.Key,
						"error", err)
					b.metrics.IncBroadcastFailure("purge", broadcastFailureReason(err))
				} else {
					b.metrics.IncHTTPInvalidation("purge")
				}
			}(p)
		}
		wg.Wait()
	}

	// All modes: enqueue via gossip. In strong mode this is redundant
	// delivery (peer admin may be temporarily unreachable). In eventual
	// mode this is the sole delivery path for invalidations.
	if body, err := EncodePurgeGossip(evt); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	peerCount := countPeers(b.cluster.Members())
	b.logger.Info("gossiped purge to peers",
		"key", evt.Key,
		"issuer", evt.Issuer,
		"seq", evt.Seq,
		"peers", peerCount,
	)
}

// BroadcastBan sends a ban predicate to all live peers.
// In strong mode it posts to each peer's admin API. In eventual
// mode it sends via gossip only.
func (b *Broadcaster) BroadcastBan(ctx context.Context, expr api.BanExpr) {
	// Detach from the caller's context so peer fan-out is bounded only
	// by broadcastTimeout, not by the engine lifecycle or per-request
	// cancellation. Same rationale as BroadcastPurge.
	fanoutCtx := context.WithoutCancel(ctx)

	evt := api.BanEvent{
		Predicate: expr,
		Issuer:    b.cluster.cfg.NodeName,
		IssuedAt:  time.Now(),
		Seq:       b.seq.Add(1),
	}

	if b.mode == config.ClusterModeStrong {
		peers := b.cluster.Members()
		var wg sync.WaitGroup
		for _, p := range peers {
			if p.Name == b.cluster.cfg.NodeName {
				continue
			}
			wg.Add(1)
			go func(peer api.PeerInfo) {
				defer wg.Done()
				defer func() {
					if v := recover(); v != nil {
						b.logger.Error("ban broadcast panicked",
							"peer", peer.Name,
							"panic", v)
					}
				}()
				if err := b.sendBan(fanoutCtx, peer, evt); err != nil {
					b.logger.Warn("ban broadcast failed",
						"peer", peer.Name,
						"error", err)
					b.metrics.IncBroadcastFailure("ban", broadcastFailureReason(err))
				} else {
					b.metrics.IncHTTPInvalidation("ban")
				}
			}(p)
		}
		wg.Wait()
	}

	// All modes: enqueue via gossip.
	if body, err := EncodeBanGossip(evt); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	peerCount := countPeers(b.cluster.Members())
	b.logger.Info("gossiped ban to peers",
		"issuer", evt.Issuer,
		"seq", evt.Seq,
		"peers", peerCount,
	)
}

// BroadcastRefresh sends a soft-purge (refresh) event for key to all
// live peers. In strong mode it posts to each peer's admin API and also
// enqueues via gossip for redundant delivery. In eventual mode it sends
// via gossip only.
func (b *Broadcaster) BroadcastRefresh(ctx context.Context, key api.Key) {
	fanoutCtx := context.WithoutCancel(ctx)

	evt := api.RefreshEvent{
		Key:      key,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}

	if b.mode == config.ClusterModeStrong {
		peers := b.cluster.Members()
		var wg sync.WaitGroup
		for _, p := range peers {
			if p.Name == b.cluster.cfg.NodeName {
				continue
			}
			wg.Add(1)
			go func(peer api.PeerInfo) {
				defer wg.Done()
				defer func() {
					if v := recover(); v != nil {
						b.logger.Error("refresh broadcast panicked",
							"peer", peer.Name,
							"panic", v)
					}
				}()
				if err := b.sendRefresh(fanoutCtx, peer, evt); err != nil {
					b.logger.Warn("refresh broadcast failed",
						"peer", peer.Name,
						"key", evt.Key,
						"error", err)
					b.metrics.IncBroadcastFailure("refresh", broadcastFailureReason(err))
				} else {
					b.metrics.IncHTTPInvalidation("refresh")
				}
			}(p)
		}
		wg.Wait()
	}

	if body, err := EncodeRefreshGossip(evt); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	peerCount := countPeers(b.cluster.Members())
	b.logger.Info("gossiped refresh to peers",
		"key", evt.Key,
		"issuer", evt.Issuer,
		"seq", evt.Seq,
		"peers", peerCount,
	)
}

func (b *Broadcaster) sendPurge(ctx context.Context, peer api.PeerInfo, evt api.PurgeEvent) error {
	body, err := EncodePurgeHTTP(evt)
	if err != nil {
		return fmt.Errorf("broadcast purge encode: %w", err)
	}
	return b.postBinary(ctx, peer.AdminAddr, "/v1/peer/purge", body)
}

func (b *Broadcaster) sendBan(ctx context.Context, peer api.PeerInfo, evt api.BanEvent) error {
	body, err := EncodeBanHTTP(evt)
	if err != nil {
		return fmt.Errorf("broadcast ban encode: %w", err)
	}
	return b.postBinary(ctx, peer.AdminAddr, "/v1/peer/ban", body)
}

func (b *Broadcaster) sendRefresh(ctx context.Context, peer api.PeerInfo, evt api.RefreshEvent) error {
	body, err := EncodeRefreshHTTP(evt)
	if err != nil {
		return fmt.Errorf("broadcast refresh encode: %w", err)
	}
	return b.postBinary(ctx, peer.AdminAddr, "/v1/peer/refresh", body)
}

// broadcastFailureReason maps a broadcast error to a short label
// for use as a Prometheus dimension.
func broadcastFailureReason(err error) string {
	// fasthttp errors are plain errors (not *url.Error), so check
	// for common patterns in the error message.
	errStr := err.Error()
	if strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline") {
		return "timeout"
	}
	if strings.Contains(errStr, "connection refused") || strings.Contains(errStr, "dial") || strings.Contains(errStr, "no such host") {
		return "dial"
	}
	return "5xx"
}

func (b *Broadcaster) postBinary(ctx context.Context, addr, path string, body []byte) error {
	ctx, cancel := context.WithTimeout(ctx, broadcastTimeout)
	defer cancel()

	scheme := "http"
	if b.fetcher != nil && b.fetcher.useTLS {
		scheme = "https"
	}
	uri := scheme + "://" + addr + path

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)

	req.Header.SetMethod(fasthttp.MethodPost)
	req.SetRequestURI(uri)
	req.SetBodyRaw(body)
	req.Header.Set(header.ContentType, "application/octet-stream")
	if b.token != "" {
		req.Header.Set(header.Authorization, "Bearer "+b.token)
	}

	if err := b.client.Do(ctx, req, resp); err != nil {
		return fmt.Errorf("broadcast %s%s: %w", addr, path, err)
	}
	if resp.StatusCode() >= 500 {
		return fmt.Errorf("broadcast %s%s: status %d", addr, path, resp.StatusCode())
	}
	return nil
}
