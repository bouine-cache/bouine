package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"time"

	"github.com/thylong/bouine/internal/observability"
	"github.com/thylong/bouine/pkg/api"
)

// Aggregator fetches metrics from all cluster peers and merges them.
type Aggregator struct {
	rings    *observability.Rings
	peersFn  func() []api.PeerInfo
	selfAddr string
	token    string
	timeout  time.Duration
	logger   *slog.Logger
}

// NewAggregator creates an Aggregator.
func NewAggregator(rings *observability.Rings, peersFn func() []api.PeerInfo, selfAddr, token string, logger *slog.Logger) *Aggregator {
	if logger == nil {
		logger = slog.Default()
	}
	return &Aggregator{
		rings:    rings,
		peersFn:  peersFn,
		selfAddr: selfAddr,
		token:    token,
		timeout:  200 * time.Millisecond,
		logger:   logger,
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
// SCALE: migrate to gossip push aggregation beyond ~5 pods — see PLAN.md §6.4
func (a *Aggregator) Collect(ctx context.Context) (observability.MetricsSummary, []PeerResult) {
	peers := []api.PeerInfo{}
	if a.peersFn != nil {
		peers = a.peersFn()
	}

	type result struct {
		peer    api.PeerInfo
		summary observability.MetricsSummary
		err     error
	}

	ch := make(chan result, len(peers)+1)

	// Always include self.
	selfSum := a.rings.Summary()
	ch <- result{summary: selfSum}

	for _, p := range peers {
		if p.AdminAddr == a.selfAddr || p.Name == a.rings.NodeName {
			continue
		}
		go func(peer api.PeerInfo) {
			sum, err := a.fetchPeer(ctx, peer)
			ch <- result{peer: peer, summary: sum, err: err}
		}(p)
	}

	// Count distinct non-self peers actually dispatched to.
	nonSelf := 0
	for _, p := range peers {
		if p.AdminAddr != a.selfAddr && p.Name != a.rings.NodeName {
			nonSelf++
		}
	}
	total := nonSelf + 1 // self + non-self peers

	summaries := make([]observability.MetricsSummary, 0, total)
	peerResults := make([]PeerResult, 0, total)

	deadline := time.After(a.timeout)
	collected := 0
	for collected < total {
		select {
		case r := <-ch:
			summaries = append(summaries, r.summary)
			stale := r.err != nil
			peerResults = append(peerResults, PeerResult{
				NodeName: r.summary.NodeName,
				Summary:  r.summary,
				Stale:    stale,
				Err:      r.err,
			})
			collected++
		case <-deadline:
			// Timeout — proceed with what we have.
			goto done
		case <-ctx.Done():
			goto done
		}
	}
done:
	sort.Slice(peerResults, func(i, j int) bool {
		return peerResults[i].NodeName < peerResults[j].NodeName
	})
	return observability.MergeSummaries(summaries), peerResults
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
