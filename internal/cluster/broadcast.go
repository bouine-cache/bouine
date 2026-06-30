package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thylong/bouine/internal/config"
	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// Broadcaster fans out purge, ban, and replication events to all
// cluster peers. In strong mode it uses HTTP fan-out for
// invalidations; in eventual and full modes it uses gossip only.
//
// Stable.
type Broadcaster struct {
	cluster *Cluster
	fetcher *PeerFetcher
	seq     atomic.Uint64
	logger  observability.Logger
	token   string
	mode    string // ClusterModeStrong | ClusterModeEventual | ClusterModeFull
	metrics *Metrics
}

// NewBroadcaster creates a broadcaster for the given cluster.
// token is the admin bearer token used when posting to peer admin APIs.
func NewBroadcaster(c *Cluster, fetcher *PeerFetcher, token ...string) *Broadcaster {
	logger := c.logger
	if logger == nil {
		logger = observability.NewSampledLogger(nil, observability.DefaultKeySampleRate)
	}
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
// via gossip for redundant delivery. In eventual and full modes it
// sends via gossip only (no HTTP fan-out).
func (b *Broadcaster) BroadcastPurge(ctx context.Context, key api.Key, varyKey string) {
	evt := api.PurgeEvent{
		Type:     api.GossipTypePurge,
		Key:      key,
		VaryKey:  varyKey,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}

	if b.mode == config.ClusterModeStrong || b.mode == config.ClusterModeFull {
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
	// and full modes this is the sole delivery path for invalidations.
	if body, err := json.Marshal(evt); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	b.logger.Info("gossiped purge to peers",
		"key", evt.Key,
		"issuer", evt.Issuer,
		"seq", evt.Seq,
		"peers", len(b.cluster.Members())-1,
	)
}

// BroadcastBan sends a ban predicate to all live peers.
// In strong mode it posts to each peer's admin API. In eventual and
// full modes it sends via gossip only.
func (b *Broadcaster) BroadcastBan(ctx context.Context, expr api.BanExpr) {
	evt := api.BanEvent{
		Type:      api.GossipTypeBan,
		Predicate: expr,
		Issuer:    b.cluster.cfg.NodeName,
		IssuedAt:  time.Now(),
		Seq:       b.seq.Add(1),
	}

	if b.mode == config.ClusterModeStrong || b.mode == config.ClusterModeFull {
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
	if body, err := json.Marshal(evt); err == nil {
		b.cluster.QueueBroadcast(body)
	}
	b.logger.Info("gossiped ban to peers",
		"issuer", evt.Issuer,
		"seq", evt.Seq,
		"peers", len(b.cluster.Members())-1,
	)
}

// BroadcastReplicate sends a full cached object to all peers via gossip.
// Only used in full mode. In other modes this is a no-op.
//
// The caller may pass an Object whose Body aliases a sync.Pool buffer
// (e.g. the recorder pool used by the cache handler). To keep the
// marshaling free of lifetime hazards regardless of the caller's
// ownership story, the body, header, and surrogate keys are copied
// into storage owned by this function before json.Marshal reads it.
// The body copy is right-sized so the gossip payload does not carry
// the pool's over-allocation.
func (b *Broadcaster) BroadcastReplicate(_ context.Context, obj *api.Object) {
	if b.mode != config.ClusterModeFull {
		return
	}
	// Defensive copy so the marshaled payload cannot observe a slice
	// or map that the caller may return to a pool or mutate before
	// memberlist's gossip round reads the queued bytes.
	replicated := *obj
	if len(obj.Body) > 0 {
		bodyCopy := make([]byte, len(obj.Body))
		copy(bodyCopy, obj.Body)
		replicated.Body = bodyCopy
	}
	if len(obj.SurrogateKeys) > 0 {
		keysCopy := make([]string, len(obj.SurrogateKeys))
		copy(keysCopy, obj.SurrogateKeys)
		replicated.SurrogateKeys = keysCopy
	}
	replicated.Header = obj.Header.Clone()
	evt := api.ReplicationEvent{
		Type:     api.GossipTypeReplication,
		Method:   "GET",
		Object:   &replicated,
		Issuer:   b.cluster.cfg.NodeName,
		IssuedAt: time.Now(),
		Seq:      b.seq.Add(1),
	}
	if body, err := json.Marshal(evt); err == nil {
		b.cluster.QueueBroadcast(body)
		b.metrics.IncReplicationSent()
		b.metrics.AddReplicationBytes("sent", float64(len(body)))
	}
	b.logger.Info("gossiped replication to peers",
		"key", obj.Key,
		"issuer", evt.Issuer,
		"seq", evt.Seq,
		"peers", len(b.cluster.Members())-1,
	)
}

func (b *Broadcaster) sendPurge(ctx context.Context, peer api.PeerInfo, evt api.PurgeEvent) error {
	return b.post(ctx, peer.AdminAddr, "/v1/peer/purge", evt)
}

func (b *Broadcaster) sendBan(ctx context.Context, peer api.PeerInfo, evt api.BanEvent) error {
	return b.post(ctx, peer.AdminAddr, "/v1/peer/ban", evt)
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
	if b.token != "" {
		req.Header.Set("Authorization", "Bearer "+b.token)
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
