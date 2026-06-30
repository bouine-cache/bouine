package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// Aggregator fetches metrics from all cluster peers and merges them.
// It caches the last successful summary per peer so that stale entries
// still contribute data when a peer is temporarily unreachable.
type Aggregator struct {
	rings    *observability.Rings
	peersFn  func() []api.PeerInfo
	selfAddr string
	token    string
	timeout  time.Duration
	logger   observability.Logger

	mu        sync.Mutex
	lastKnown map[string]observability.MetricsSummary // peer name → last successful summary
}

// NewAggregator creates an Aggregator.
func NewAggregator(rings *observability.Rings, peersFn func() []api.PeerInfo, selfAddr, token string, logger observability.Logger) *Aggregator {
	if logger == nil {
		logger = observability.NewSampledLogger(nil, observability.DefaultKeySampleRate)
	}
	return &Aggregator{
		rings:     rings,
		peersFn:   peersFn,
		selfAddr:  selfAddr,
		token:     token,
		timeout:   200 * time.Millisecond,
		logger:    logger,
		lastKnown: make(map[string]observability.MetricsSummary),
	}
}

// PeerResult holds the result of a single peer fetch.
type PeerResult struct {
	NodeName string
	Summary  observability.MetricsSummary
	Stale    bool // true if the peer was unreachable (last-known used)
	Err      error
}

// Collect fans out to all live peers and returns the merged summary.
// Unreachable peers are marked stale; their last-known summary is used
// if available.
//
// SCALE: migrate to gossip push aggregation beyond ~5 pods — see docs/architecture.md §2
func (a *Aggregator) Collect(ctx context.Context) (observability.MetricsSummary, []PeerResult) {
	peers := []api.PeerInfo{}
	if a.peersFn != nil {
		peers = a.peersFn()
	}

	// Members() includes self, so we separate self from remote peers.
	// We always fan-out to non-self peers and inject the local rings
	// summary directly (no network call for self).
	nonSelf := 0
	for _, p := range peers {
		if p.AdminAddr != a.selfAddr && p.Name != a.rings.NodeName {
			nonSelf++
		}
	}
	total := nonSelf + 1 // non-self peers + local rings

	ch := make(chan fetchResult, total)
	ch <- fetchResult{summary: a.rings.Summary()}

	for _, p := range peers {
		if p.AdminAddr == a.selfAddr || p.Name == a.rings.NodeName {
			continue // handled via local rings above
		}
		go func(peer api.PeerInfo) {
			sum, err := a.fetchPeer(ctx, peer)
			ch <- fetchResult{peer: peer, summary: sum, err: err}
		}(p)
	}

	summaries := make([]observability.MetricsSummary, 0, total)
	peerResults := make([]PeerResult, 0, total)

	for collected := 0; collected < total; collected++ {
		select {
		case r := <-ch:
			sum, stale := a.resolveResult(r)
			summaries = append(summaries, sum)
			nodeName := sum.NodeName
			if nodeName == "" {
				nodeName = r.peer.Name
			}
			peerResults = append(peerResults, PeerResult{
				NodeName: nodeName,
				Summary:  sum,
				Stale:    stale,
				Err:      r.err,
			})
		case <-ctx.Done():
			goto done
		}
	}
done:
	a.recordPeerHealth(peerResults)
	sort.Slice(peerResults, func(i, j int) bool {
		return peerResults[i].NodeName < peerResults[j].NodeName
	})
	return observability.MergeSummaries(summaries), peerResults
}

// recordPeerHealth samples each remote peer result into the PeerRing.
func (a *Aggregator) recordPeerHealth(results []PeerResult) {
	if a.rings == nil {
		return
	}
	for _, pr := range results {
		if pr.NodeName != "" && pr.NodeName != a.rings.NodeName {
			a.rings.Peer.Record(pr.NodeName, !pr.Stale)
		}
	}
}

type fetchResult struct {
	peer    api.PeerInfo
	summary observability.MetricsSummary
	err     error
}

// resolveResult returns the summary to use and whether the peer is stale.
// On error it falls back to the last-known cached summary.
// On success it updates the last-known cache.
func (a *Aggregator) resolveResult(r fetchResult) (observability.MetricsSummary, bool) {
	if r.err == nil {
		if r.peer.Name != "" {
			a.mu.Lock()
			a.lastKnown[r.peer.Name] = r.summary
			a.mu.Unlock()
		}
		return r.summary, false
	}
	if r.peer.Name != "" {
		a.mu.Lock()
		lk, ok := a.lastKnown[r.peer.Name]
		a.mu.Unlock()
		if ok {
			return lk, true
		}
	}
	return r.summary, true
}

func (a *Aggregator) fetchPeer(ctx context.Context, peer api.PeerInfo) (observability.MetricsSummary, error) {
	ctx, cancel := context.WithTimeout(ctx, a.timeout)
	defer cancel()

	url := fmt.Sprintf("http://%s/v1/peer/metrics", peer.AdminAddr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return observability.MetricsSummary{NodeName: peer.Name}, err
	}
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return observability.MetricsSummary{NodeName: peer.Name}, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return observability.MetricsSummary{NodeName: peer.Name},
			fmt.Errorf("peer %s: status %d", peer.Name, resp.StatusCode)
	}

	var sum observability.MetricsSummary
	if err := json.NewDecoder(resp.Body).Decode(&sum); err != nil {
		return observability.MetricsSummary{NodeName: peer.Name}, err
	}
	return sum, nil
}

// PeerMetricsHandler returns the local ring summary as JSON.
// Mounted at GET /v1/peer/metrics on the admin server.
func PeerMetricsHandler(rings *observability.Rings) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		sum := rings.Summary()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(sum)
	})
}

// EncodeJSON is a helper used by tests.
func EncodeJSON(v any) ([]byte, error) {
	var buf bytes.Buffer
	err := json.NewEncoder(&buf).Encode(v)
	return buf.Bytes(), err
}
