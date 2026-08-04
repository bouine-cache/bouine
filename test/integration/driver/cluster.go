//go:build integration

// Package driver boots bouine nodes in-process for integration tests.
// No Docker required — each node is a goroutine running engine.run().
package driver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/cmd/bouine/cmd"
	"github.com/bouine-cache/bouine/internal/cluster"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
	"github.com/bouine-cache/bouine/pkg/api"
)

const (
	IntegrationToken = "inttest-token"
	CrossNodeHost    = "testhost:8080"

	// GossipConvergence is the max time for gossip to propagate
	// invalidations across the in-process cluster.
	GossipConvergence = 15 * time.Second
)

// ClusterNode describes one bouine node.
type ClusterNode struct {
	Name      string
	HTTPAddr  string
	AdminAddr string
	Token     string
	cfgPath   string // path to config YAML (for RestartNode)
}

// ClusterStack holds a live in-process 3-node cluster + origin.
type ClusterStack struct {
	Mode      string
	Nodes     [3]ClusterNode
	OriginURL string

	origin    *httptest.Server
	originCtl *originControl
	cancels   [3]context.CancelFunc
	errChs    [3]chan error
	paused    [3]atomic.Bool // per-node application-level pause gate
	configDir string
}

// ClusterOptions configures BootCluster.
type ClusterOptions struct {
	Mode          string
	NoAutoCleanup bool
}

// freePort returns an available TCP port on localhost.
func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port
}

// BootCluster starts a 3-node in-process bouine cluster with an httptest origin.
func BootCluster(t *testing.T, opts ClusterOptions) *ClusterStack {
	t.Helper()
	if opts.Mode == "" {
		opts.Mode = "strong"
	}

	origin, originCtl := startOriginWithControl()

	// Allocate ports for all nodes upfront so gossip seeds are known.
	type nodePorts struct {
		http, admin, gossip int
	}
	ports := [3]nodePorts{}
	for i := range ports {
		ports[i] = nodePorts{
			http:   freePort(t),
			admin:  freePort(t),
			gossip: freePort(t),
		}
	}

	// Build gossip seed list.
	var seeds []string
	for _, p := range ports {
		seeds = append(seeds, fmt.Sprintf("127.0.0.1:%d", p.gossip))
	}
	seedList := `["` + strings.Join(seeds, `","`) + `"]`

	s := &ClusterStack{
		Mode:      opts.Mode,
		OriginURL: origin.URL,
		origin:    origin,
		originCtl: originCtl,
		configDir: t.TempDir(),
	}

	// Write configs and start each node.
	configDir := s.configDir
	for i := range 3 {
		p := ports[i]
		name := fmt.Sprintf("bouine-%d", i+1)

		cfg := fmt.Sprintf(`listen:
  http: "127.0.0.1:%d"
  admin: "127.0.0.1:%d"
  cluster: "127.0.0.1:%d"
admin:
  token: %s
storage:
  hot_max_bytes: 128MiB
cluster:
  enabled: true
  node_name: %s
  mode: %s
  join: %s
  hop_limit: 2
upstream_pools:
  - name: origin
    targets: [%q]
routes:
  - match: {}
    pool: origin
    cache:
      ttl_default: 60s
`, p.http, p.admin, p.gossip, IntegrationToken, name, opts.Mode, seedList,
			origin.Listener.Addr().String())

		cfgPath := filepath.Join(configDir, name+".yaml")
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("write config %s: %v", name, err)
		}

		s.Nodes[i] = ClusterNode{
			Name:      name,
			HTTPAddr:  fmt.Sprintf("http://127.0.0.1:%d", p.http),
			AdminAddr: fmt.Sprintf("http://127.0.0.1:%d", p.admin),
			Token:     IntegrationToken,
			cfgPath:   cfgPath,
		}

		ctx, cancel := context.WithCancel(context.Background())
		s.cancels[i] = cancel
		s.errChs[i] = make(chan error, 1)

		root := cmd.Root()
		root.SetArgs([]string{"serve", "--config", cfgPath, "--log-level", "warn"})
		go func(ch chan error) {
			ch <- root.ExecuteContext(ctx)
		}(s.errChs[i])
	}

	if !opts.NoAutoCleanup {
		t.Cleanup(func() { s.Down() })
	}

	// Wait for all nodes to be healthy.
	s.waitHealthy(t, 30*time.Second)
	s.waitMembership(t, 30*time.Second, 3)

	t.Logf("cluster: %s stack ready — %s %s %s (origin: %s)",
		opts.Mode, s.Nodes[0].HTTPAddr, s.Nodes[1].HTTPAddr, s.Nodes[2].HTTPAddr, origin.URL)
	return s
}

func (s *ClusterStack) waitHealthy(t *testing.T, timeout time.Duration) {
	t.Helper()
	for _, node := range s.Nodes {
		ep := node.AdminAddr + "/healthz"
		poll.Eventually(t, timeout, 50*time.Millisecond, func() bool {
			resp, err := http.Get(ep) //nolint:noctx
			if err != nil {
				return false
			}
			ok := resp.StatusCode == 200
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			return ok
		})
	}
}

func (s *ClusterStack) waitMembership(t *testing.T, timeout time.Duration, expected int) {
	t.Helper()
	poll.Eventually(t, timeout, 200*time.Millisecond, func() bool {
		for i := range s.Nodes {
			peers := s.Peers(t, i)
			if len(peers) < expected {
				return false
			}
		}
		return true
	})
}

// Down stops all nodes and the origin.
func (s *ClusterStack) Down() {
	for i, cancel := range s.cancels {
		if cancel != nil {
			cancel()
			select {
			case <-s.errChs[i]:
			case <-time.After(5 * time.Second):
			}
			s.cancels[i] = nil
		}
	}
	if s.origin != nil {
		s.origin.Close()
		s.origin = nil
	}
}

// ConfigDir returns an empty string (no temp config dir to expose).
func (s *ClusterStack) ConfigDir() string { return "" }

// Dump is a no-op for in-process nodes (logs go to stderr).
func (s *ClusterStack) Dump(_ *testing.T) {}

// RestartNode re-boots a previously killed node with fresh ports to
// avoid bind conflicts from lingering sockets.
func (s *ClusterStack) RestartNode(t *testing.T, n int) {
	t.Helper()
	if s.cancels[n] != nil {
		return
	}

	// Allocate fresh ports — the old ones may still be in TIME_WAIT.
	httpPort := freePort(t)
	adminPort := freePort(t)
	gossipPort := freePort(t)

	name := s.Nodes[n].Name
	seedList := s.gossipSeeds()

	cfg := fmt.Sprintf(`listen:
  http: "127.0.0.1:%d"
  admin: "127.0.0.1:%d"
  cluster: "127.0.0.1:%d"
admin:
  token: %s
storage:
  hot_max_bytes: 128MiB
cluster:
  enabled: true
  node_name: %s
  mode: %s
  join: %s
  hop_limit: 2
upstream_pools:
  - name: origin
    targets: [%q]
routes:
  - match: {}
    pool: origin
    cache:
      ttl_default: 60s
`, httpPort, adminPort, gossipPort, IntegrationToken, name, s.Mode, seedList,
		s.origin.Listener.Addr().String())

	cfgPath := filepath.Join(s.configDir, name+"-restart.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write restart config: %v", err)
	}

	s.Nodes[n].HTTPAddr = fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	s.Nodes[n].AdminAddr = fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	s.Nodes[n].cfgPath = cfgPath

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels[n] = cancel
	s.errChs[n] = make(chan error, 1)

	root := cmd.Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-level", "warn"})
	go func(ch chan error) {
		ch <- root.ExecuteContext(ctx)
	}(s.errChs[n])

	// Wait for health.
	poll.Eventually(t, 30*time.Second, 50*time.Millisecond, func() bool {
		resp, err := http.Get(s.Nodes[n].AdminAddr + "/healthz") //nolint:noctx
		if err != nil {
			return false
		}
		ok := resp.StatusCode == 200
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if ok {
			t.Logf("cluster: restarted %s on %s", name, s.Nodes[n].HTTPAddr)
		}
		return ok
	})
}

// gossipSeeds returns the gossip join list from live nodes.
func (s *ClusterStack) gossipSeeds() string {
	var seeds []string
	for i := range s.Nodes {
		if s.cancels[i] != nil {
			// Extract gossip port from config — it's the cluster listen port.
			// Parse from the admin addr as a proxy (gossip port is adjacent).
			seeds = append(seeds, "127.0.0.1:"+s.extractGossipPort(i))
		}
	}
	if len(seeds) == 0 {
		return `[]`
	}
	return `["` + strings.Join(seeds, `","`) + `"]`
}

func (s *ClusterStack) extractGossipPort(n int) string {
	// Read the config file to find the cluster listen port.
	data, err := os.ReadFile(s.Nodes[n].cfgPath)
	if err != nil {
		return "7000"
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "cluster:") {
			continue
		}
		// Look for the listen.cluster line: `cluster: "127.0.0.1:PORT"`
		if strings.Contains(line, "cluster:") && strings.Contains(line, "127.0.0.1:") {
			parts := strings.Split(line, ":")
			if len(parts) >= 3 {
				port := strings.Trim(parts[len(parts)-1], `" `)
				return port
			}
		}
	}
	return "7000"
}

// PauseNode sets a per-node gate that blocks the origin from responding,
// simulating application-level partition.
func (s *ClusterStack) PauseNode(_ *testing.T, n int) {
	s.paused[n].Store(true)
}

// UnpauseNode clears the pause gate.
func (s *ClusterStack) UnpauseNode(_ *testing.T, n int) {
	s.paused[n].Store(false)
}

// FlapOrigin toggles origin errors n times with a pause between each flap.
func (s *ClusterStack) FlapOrigin(t *testing.T, n int, pause time.Duration) {
	t.Helper()
	for i := range n {
		s.originCtl.forceErr.Store(true)
		time.Sleep(pause)
		s.originCtl.forceErr.Store(false)
		t.Logf("origin flap %d/%d: toggled error→ok", i+1, n)
		time.Sleep(pause)
	}
}

// ScaleOriginLatency injects ms of latency into every origin response.
// Pass 0 to disable.
func (s *ClusterStack) ScaleOriginLatency(ms int) error {
	s.originCtl.latencyMs.Store(int64(ms))
	return nil
}

// SetOriginError forces the origin to return 503 for all requests.
func (s *ClusterStack) SetOriginError(on bool) {
	s.originCtl.forceErr.Store(on)
}

// KillNode cancels the context of node n, stopping it.
func (s *ClusterStack) KillNode(t *testing.T, n int) {
	t.Helper()
	if s.cancels[n] != nil {
		s.cancels[n]()
		select {
		case <-s.errChs[n]:
		case <-time.After(5 * time.Second):
		}
		s.cancels[n] = nil
		t.Logf("cluster: killed %s", s.Nodes[n].Name)
	}
}

// Get performs a GET request against node n.
func (s *ClusterStack) Get(t *testing.T, n int, path string) *http.Response {
	t.Helper()
	return s.GetWithHost(t, n, path, "")
}

// GetWithHost performs a GET with a specific Host header.
func (s *ClusterStack) GetWithHost(t *testing.T, n int, path string, host string) *http.Response {
	t.Helper()
	url := s.Nodes[n].HTTPAddr + path
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	if host != "" {
		req.Host = host
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp
}

// GetBody performs a GET and returns both the response and body.
func (s *ClusterStack) GetBody(t *testing.T, n int, path string) (*http.Response, string) {
	t.Helper()
	url := s.Nodes[n].HTTPAddr + path
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp, string(body)
}

// Purge sends POST /v1/purge to node n.
func (s *ClusterStack) Purge(t *testing.T, n int, targetURL string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"url": targetURL})
	s.adminPost(t, n, "/v1/purge", body)
}

// Ban sends POST /v1/ban to node n.
func (s *ClusterStack) Ban(t *testing.T, n int, hostRegex, pathRegex string) {
	t.Helper()
	payload := map[string]string{}
	if hostRegex != "" {
		payload["host_regex"] = hostRegex
	}
	if pathRegex != "" {
		payload["path_regex"] = pathRegex
	}
	body, _ := json.Marshal(payload)
	s.adminPost(t, n, "/v1/ban", body)
}

// PeerPurge sends a local-only purge to node n via /v1/peer/purge.
// Unlike Purge, this does not broadcast to other peers.
func (s *ClusterStack) PeerPurge(t *testing.T, n int, evt api.PurgeEvent) {
	t.Helper()
	body, _ := cluster.EncodePurgeHTTP(evt)
	s.peerPost(t, n, "/v1/peer/purge", body)
}

func (s *ClusterStack) peerPost(t *testing.T, n int, path string, body []byte) {
	t.Helper()
	url := s.Nodes[n].AdminAddr + path
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: status %d body: %s", url, resp.StatusCode, string(b))
	}
}

func (s *ClusterStack) adminPost(t *testing.T, n int, path string, body []byte) {
	t.Helper()
	url := s.Nodes[n].AdminAddr + path
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode >= 300 {
		t.Fatalf("POST %s: status %d body: %s", url, resp.StatusCode, string(b))
	}
}

// Peers returns the cluster peers from node n's admin API.
func (s *ClusterStack) Peers(t *testing.T, n int) []map[string]any {
	t.Helper()
	url := s.Nodes[n].AdminAddr + "/v1/cluster/peers"
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("peers GET: %v", err)
	}
	defer resp.Body.Close()
	var peers []map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&peers)
	return peers
}

// MetricValue reads a Prometheus metric value from node n.
func (s *ClusterStack) MetricValue(t *testing.T, n int, metric string) float64 {
	t.Helper()
	url := s.Nodes[n].AdminAddr + "/metrics"
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("metrics GET: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var total float64
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, metric) {
			continue
		}
		rest := line[len(metric):]
		if rest == "" || rest[0] == '{' || rest[0] == ' ' {
			parts := strings.Fields(line)
			if len(parts) >= 2 {
				v, _ := strconv.ParseFloat(parts[len(parts)-1], 64)
				total += v
			}
		}
	}
	return total
}

// RetryUntil polls f until it returns true or deadline expires.
func RetryUntil(t *testing.T, deadline time.Duration, interval time.Duration, f func() bool) {
	t.Helper()
	poll.Eventually(t, deadline, interval, f)
}

// XCache returns the X-Cache header value.
func XCache(resp *http.Response) string {
	return resp.Header.Get("X-Cache")
}
