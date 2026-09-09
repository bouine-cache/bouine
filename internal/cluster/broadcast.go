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

// broadcastMaxConnsPerHost caps persistent connections per peer for the
// broadcast fan-out client. Broadcast sends one request per peer per
// event; 128 is a generous ceiling for large clusters and burst purges.
const broadcastMaxConnsPerHost = 128

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
	metrics *Metrics
	// batcher coalesces purge/refresh events into batch frames per
	// ADR-0044. nil in tests that construct Broadcaster directly; the
	// enqueue helpers treat nil as "send unbatched".
	batcher *invalidationBatcher
	logger  observability.Logger
	token   string
	mode    string // ClusterModeStrong | ClusterModeEventual
	seq     atomic.Uint64
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
		MaxConnsPerHost:     broadcastMaxConnsPerHost,
		MaxIdleConnDuration: 90 * time.Second,
		ReadTimeout:         broadcastTimeout,
		WriteTimeout:        5 * time.Minute,
		TLSConfig:           tlsCfg,
		Dial: func(addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 2 * time.Second, KeepAlive: 30 * time.Second}).Dial("tcp", addr)
		},
	}
	client := transport.NewClient(fc)
	b := &Broadcaster{
		cluster: c,
		fetcher: fetcher,
		client:  client,
		logger:  logger,
		token:   tok,
		mode:    c.Mode(),
		metrics: c.metrics,
	}
	b.batcher = newInvalidationBatcher(
		logger,
		c.metrics,
		b.flushPurgeBatch,
		b.flushRefreshBatch,
		func() { c.metrics.IncBroadcastOverflow() },
	)
	return b
}

// Close stops the batcher's flush loop and flushes pending events.
// Called during engine shutdown so final purges reach all peers.
func (b *Broadcaster) Close() {
	if b.batcher != nil {
		b.batcher.close()
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
	_ = context.WithoutCancel(ctx)

	evt := api.PurgeEvent{
		Key:      key,
		VaryKey:  varyKey,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}

	if b.batcher != nil {
		if batch, deliver := b.batcher.enqueuePurge(evt); deliver {
			// Idle queue or overflow: synchronous delivery preserves
			// the guarantee that a returned purge has already fanned
			// out (the admin API relies on it). Storms coalesce behind
			// the flush window.
			b.flushPurgeBatch(batch)
			b.batcher.donePurgeFlush()
		}
		return
	}
	b.flushPurgeBatch([]api.PurgeEvent{evt})
}

// BroadcastPurges sends one purge event per key in a single batch
// frame (ADR-0044). It is the fan-out counterpart of the admin
// /v1/purge/batch endpoint: N keys produce one POST per peer instead
// of N. Delivery is synchronous, like BroadcastPurge on an idle
// queue, so a returned call has already fanned out.
func (b *Broadcaster) BroadcastPurges(ctx context.Context, keys []api.Key) {
	// Detached like BroadcastPurge: bounded by broadcastTimeout, not
	// the engine lifecycle or per-request cancellation.
	_ = context.WithoutCancel(ctx)

	if len(keys) == 0 {
		return
	}
	now := time.Now()
	evts := make([]api.PurgeEvent, len(keys))
	for i, key := range keys {
		evts[i] = api.PurgeEvent{
			Key:      key,
			Issuer:   b.cluster.cfg.NodeName,
			IssuedAt: now,
			Seq:      b.seq.Add(1),
		}
	}
	b.flushPurgeBatch(evts)
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
			go func(peer api.PeerInfo) { //nolint:contextcheck // fanoutCtx is a detached context created in this scope
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
	_ = context.WithoutCancel(ctx)

	evt := api.RefreshEvent{
		Key:      key,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}

	if b.batcher != nil {
		if batch, deliver := b.batcher.enqueueRefresh(evt); deliver {
			b.flushRefreshBatch(batch)
			b.batcher.doneRefreshFlush()
		}
		return
	}
	b.flushRefreshBatch([]api.RefreshEvent{evt})
}

// flushPurgeBatch delivers one batch of purge events: a single batch
// frame posted to each live peer (strong mode) plus one gossip batch
// frame. Encoding happens once and the body is shared across peers.
//
//nolint:contextcheck // fan-out deliberately detached from request contexts
func (b *Broadcaster) flushPurgeBatch(evts []api.PurgeEvent) {
	if len(evts) == 0 {
		return
	}
	// Fan-out is detached from any request context by design: batch
	// events may originate from many request goroutines and must
	// survive their cancellation. postBinary bounds the call by
	// broadcastTimeout internally.
	fanoutCtx := context.WithoutCancel(context.Background())
	if b.mode == config.ClusterModeStrong {
		body, err := EncodePurgeBatchHTTP(evts)
		if err != nil {
			b.logger.Warn("purge batch encode failed", "error", err, "events", len(evts))
			b.metrics.IncBroadcastFailure("purge_batch", "marshal")
		} else {
			peers := b.cluster.Members()
			var wg sync.WaitGroup
			for _, p := range peers {
				if p.Name == b.cluster.cfg.NodeName {
					continue
				}
				wg.Add(1)
				go func(peer api.PeerInfo) { //nolint:contextcheck // fanoutCtx is a detached context created in this scope
					defer wg.Done()
					defer func() {
						if v := recover(); v != nil {
							b.logger.Error("purge batch broadcast panicked",
								"peer", peer.Name,
								"panic", v)
						}
					}()
					if err := b.postBinary(fanoutCtx, peer.AdminAddr, "/v1/peer/purge/batch", body); err != nil {
						b.logger.Warn("purge batch broadcast failed",
							"peer", peer.Name,
							"error", err)
						b.metrics.IncBroadcastFailure("purge_batch", broadcastFailureReason(err))
					} else {
						b.metrics.IncHTTPInvalidation("purge_batch")
					}
				}(p)
			}
			wg.Wait()
		}
	}

	// All modes: enqueue via gossip. In strong mode this is redundant
	// delivery (peer admin may be temporarily unreachable). In eventual
	// mode this is the sole delivery path for invalidations.
	if body, err := EncodePurgeBatchGossip(evts); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	b.logger.Info("gossiped purge batch to peers",
		"events", len(evts),
		"issuer", evts[0].Issuer,
		"peers", countPeers(b.cluster.Members()),
	)
}

// flushRefreshBatch delivers one batch of refresh events, mirroring
// flushPurgeBatch.
//
//nolint:contextcheck // fan-out deliberately detached from request contexts
func (b *Broadcaster) flushRefreshBatch(evts []api.RefreshEvent) {
	if len(evts) == 0 {
		return
	}
	// Detached fan-out context; see flushPurgeBatch.
	fanoutCtx := context.WithoutCancel(context.Background())
	if b.mode == config.ClusterModeStrong {
		body, err := EncodeRefreshBatchHTTP(evts)
		if err != nil {
			b.logger.Warn("refresh batch encode failed", "error", err, "events", len(evts))
			b.metrics.IncBroadcastFailure("refresh_batch", "marshal")
		} else {
			peers := b.cluster.Members()
			var wg sync.WaitGroup
			for _, p := range peers {
				if p.Name == b.cluster.cfg.NodeName {
					continue
				}
				wg.Add(1)
				go func(peer api.PeerInfo) { //nolint:contextcheck // fanoutCtx is a detached context created in this scope
					defer wg.Done()
					defer func() {
						if v := recover(); v != nil {
							b.logger.Error("refresh batch broadcast panicked",
								"peer", peer.Name,
								"panic", v)
						}
					}()
					if err := b.postBinary(fanoutCtx, peer.AdminAddr, "/v1/peer/refresh/batch", body); err != nil {
						b.logger.Warn("refresh batch broadcast failed",
							"peer", peer.Name,
							"error", err)
						b.metrics.IncBroadcastFailure("refresh_batch", broadcastFailureReason(err))
					} else {
						b.metrics.IncHTTPInvalidation("refresh_batch")
					}
				}(p)
			}
			wg.Wait()
		}
	}

	if body, err := EncodeRefreshBatchGossip(evts); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	b.logger.Info("gossiped refresh batch to peers",
		"events", len(evts),
		"issuer", evts[0].Issuer,
		"peers", countPeers(b.cluster.Members()),
	)
}

func (b *Broadcaster) sendBan(ctx context.Context, peer api.PeerInfo, evt api.BanEvent) error {
	body, err := EncodeBanHTTP(evt)
	if err != nil {
		return fmt.Errorf("broadcast ban encode: %w", err)
	}
	return b.postBinary(ctx, peer.AdminAddr, "/v1/peer/ban", body)
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
