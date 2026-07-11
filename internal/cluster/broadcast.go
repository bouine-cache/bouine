package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bouine-cache/bouine/internal/config"
	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

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
	return &Broadcaster{
		cluster: c,
		fetcher: fetcher,
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
	evt := api.PurgeEvent{
		Type:     api.GossipTypePurge,
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
				if err := b.sendPurge(ctx, peer, evt); err != nil {
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
	evt := api.BanEvent{
		Type:      api.GossipTypeBan,
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
				if err := b.sendBan(ctx, peer, evt); err != nil {
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

// broadcastFailureReason maps a broadcast HTTP error to a short label
// for use as a Prometheus dimension.
func broadcastFailureReason(err error) string {
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return "timeout"
		}
		return "dial"
	}
	return "5xx"
}

func (b *Broadcaster) postBinary(ctx context.Context, addr, path string, body []byte) error {
	url := "http://" + addr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
		bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("broadcast request: %w", err)
	}
	req.Header.Set(header.ContentType, "application/octet-stream")
	if b.token != "" {
		req.Header.Set(header.Authorization, "Bearer "+b.token)
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("broadcast %s%s: %w", addr, path, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 500 {
		return fmt.Errorf("broadcast %s%s: status %d", addr, path, resp.StatusCode)
	}
	return nil
}
