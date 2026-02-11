package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
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
	logger := c.logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Broadcaster{
		cluster: c,
		fetcher: fetcher,
		logger:  logger,
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
		return fmt.Errorf("broadcast marshal: %w", err)
	}

	url := "http://" + addr + path
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url,
		bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("broadcast request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

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
