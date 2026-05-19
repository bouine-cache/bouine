package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thylong/bouine/pkg/api"
)

// Broadcaster fans out purge and ban events to all cluster peers.
//
// Stable.
type Broadcaster struct {
	cluster *Cluster
	fetcher *PeerFetcher
	seq     atomic.Uint64
	logger  *slog.Logger
}

// NewBroadcaster creates a broadcaster for the given cluster.
func NewBroadcaster(c *Cluster, fetcher *PeerFetcher) *Broadcaster {
	return &Broadcaster{
		cluster: c,
		fetcher: fetcher,
		logger:  c.logger,
	}
}

// BroadcastPurge sends a purge event for key to all live peers.
// It is fire-and-forget for each peer (failures are logged, not
// returned). Purges to unreachable peers are tolerated — the
// anti-entropy reconciler will catch up.
func (b *Broadcaster) BroadcastPurge(ctx context.Context, key api.Key, varyKey string) {
	evt := api.PurgeEvent{
		Key:      key,
		VaryKey:  varyKey,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}

	peers := b.cluster.Members()
	var wg sync.WaitGroup
	for _, p := range peers {
		if p.Name == b.cluster.cfg.NodeName {
			continue
		}
		wg.Add(1)
		go func(peer api.PeerInfo) {
			defer wg.Done()
			if err := b.sendPurge(ctx, peer, evt); err != nil {
				b.logger.Warn("purge broadcast failed",
					"peer", peer.Name,
					"key", evt.Key,
					"error", err)
			}
		}(p)
	}
	wg.Wait()
}

// BroadcastBan sends a ban predicate to all live peers.
func (b *Broadcaster) BroadcastBan(ctx context.Context, expr api.BanExpr) {
	evt := api.BanEvent{
		Predicate: expr,
		Issuer:    b.cluster.cfg.NodeName,
		IssuedAt:  time.Now(),
		Seq:       b.seq.Add(1),
	}

	peers := b.cluster.Members()
	var wg sync.WaitGroup
	for _, p := range peers {
		if p.Name == b.cluster.cfg.NodeName {
			continue
		}
		wg.Add(1)
		go func(peer api.PeerInfo) {
			defer wg.Done()
			if err := b.sendBan(ctx, peer, evt); err != nil {
				b.logger.Warn("ban broadcast failed",
					"peer", peer.Name,
					"error", err)
			}
		}(p)
	}
	wg.Wait()
}

func (b *Broadcaster) sendPurge(ctx context.Context, peer api.PeerInfo, evt api.PurgeEvent) error {
	return b.post(ctx, peer.AdminAddr, "/v1/peer/purge", evt)
}

func (b *Broadcaster) sendBan(ctx context.Context, peer api.PeerInfo, evt api.BanEvent) error {
	return b.post(ctx, peer.AdminAddr, "/v1/peer/ban", evt)
}

func (b *Broadcaster) post(ctx context.Context, addr, path string, body any) error {
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	_ = addr
	_ = path
	_ = data
	_ = ctx
	// Full HTTP post implementation wired in phase 4 when the admin
	// server mounts the /v1/peer/* endpoints.
	return nil
}
