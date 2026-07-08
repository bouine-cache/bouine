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
	"github.com/bouine-cache/bouine/internal/storage"
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
	// replSem bounds concurrent async replication goroutines (full mode).
	// Acquired non-blockingly; when full, replications are dropped.
	replSem    chan struct{}
	replClient *http.Client
	// replWG tracks in-flight replication goroutines for testing.
	replWG sync.WaitGroup
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
		cluster:    c,
		fetcher:    fetcher,
		logger:     logger,
		token:      tok,
		mode:       c.Mode(),
		metrics:    c.metrics,
		replSem:    make(chan struct{}, 64),
		replClient: &http.Client{Timeout: 5 * time.Second},
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

// BroadcastReplicate sends a full cached object to all peers via async
// HTTP POST. Only used in full mode. In other modes this is a no-op.
//
// The caller may pass an Object whose Body aliases a sync.Pool buffer
// (e.g. the recorder pool used by the cache handler). To keep the
// encoding free of lifetime hazards regardless of the caller's ownership
// story, the body, header, and surrogate keys are copied into storage
// owned by this function before storage.EncodeObject reads them. The
// body copy is right-sized so the HTTP payload does not carry the pool's
// over-allocation.
//
// Replication is fire-and-forget: each peer POST runs in its own goroutine
// bounded by replSem (size 64). This must not block the data path —
// BroadcastReplicate is called from storeAndReplicate on every cache fill.
// When the semaphore is full, the replication is dropped (anti-entropy
// heals any gaps).
func (b *Broadcaster) BroadcastReplicate(_ context.Context, obj *api.Object) {
	if b.mode != config.ClusterModeFull {
		return
	}

	// Defensive copy so the encoded payload cannot observe a slice
	// or map that the caller may return to a pool or mutate before the
	// async goroutine reads it.
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

	// Encode once; the same blob is sent to every peer.
	encoded := storage.EncodeObject(&replicated)

	issuer := b.cluster.cfg.NodeName
	seq := b.seq.Add(1)
	now := time.Now().UTC().Format(time.RFC3339)

	peers := b.cluster.Members()
	peerCount := 0
	for _, p := range peers {
		if p.Name == b.cluster.cfg.NodeName {
			continue
		}
		peerCount++

		// Non-blocking semaphore acquire — never block the data path.
		select {
		case b.replSem <- struct{}{}:
		default:
			b.metrics.IncReplicationDropped()
			b.logger.Warn("replication dropped, semaphore full",
				"key", obj.Key,
				"peer", p.Name,
			)
			continue
		}

		b.replWG.Add(1)
		go func(peer api.PeerInfo, body []byte) { //nolint:gosec,contextcheck // G118: detached context is intentional; parent ctx is the request ctx which may be cancelled
			// Intentionally use context.Background(): the request that triggered
			// this replication may have already returned to the client, so its
			// context is cancelled. The 5s timeout inside sendReplicate bounds
			// the lifetime.
			ctx := context.Background()
			defer b.replWG.Done()
			defer func() { <-b.replSem }()
			defer func() {
				if v := recover(); v != nil {
					b.logger.Error("replication goroutine panicked",
						"peer", peer.Name,
						"panic", v,
					)
				}
			}()

			if err := b.sendReplicate(ctx, peer, body, issuer, seq, now); err != nil {
				b.metrics.IncReplicationDropped()
				b.logger.Warn("replication POST failed",
					"peer", peer.Name,
					"key", obj.Key,
					"error", err,
				)
			}
		}(p, encoded)
	}

	b.metrics.IncReplicationSent()
	b.metrics.AddReplicationBytes("sent", float64(len(encoded)))
	b.logger.Info("replicated to peers via HTTP",
		"key", obj.Key,
		"issuer", issuer,
		"seq", seq,
		"peers", peerCount,
		"bytes", len(encoded),
	)
}

func (b *Broadcaster) sendReplicate(parentCtx context.Context, peer api.PeerInfo, body []byte, issuer string, seq uint64, issuedAt string) error {
	ctx, cancel := context.WithTimeout(parentCtx, 5*time.Second)
	defer cancel()

	url := "http://" + peer.AdminAddr + "/v1/peer/replicate"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("replication request: %w", err)
	}
	req.Header.Set(header.ContentType, "application/octet-stream")
	req.Header.Set(header.BouineIssuer, issuer)
	req.Header.Set(header.BouineSeq, fmt.Sprintf("%d", seq))
	req.Header.Set(header.BouineIssuedAt, issuedAt)
	req.Header.Set(header.BouineMethod, "GET")
	if b.token != "" {
		req.Header.Set(header.Authorization, "Bearer "+b.token)
	}

	resp, err := b.replClient.Do(req)
	if err != nil {
		return fmt.Errorf("replication POST %s: %w", peer.AdminAddr, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("replication POST %s: status %d", peer.AdminAddr, resp.StatusCode)
	}
	return nil
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
