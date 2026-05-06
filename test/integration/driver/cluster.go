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
	"testing"
	"time"

	"github.com/thylong/bouine/cmd/bouine/cmd"
)

const (
	IntegrationToken = "inttest-token"
	CrossNodeHost    = "testhost:8080"

	// GossipConvergence is the max time for gossip to propagate
	// invalidations across the in-process cluster.
	GossipConvergence   = 15 * time.Second
	ReplicationDeadline = 10 * time.Second
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

	origin  *httptest.Server
	cancels [3]context.CancelFunc
	errChs  [3]chan error
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

	origin := startOrigin()

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
	}

	// Write configs and start each node.
	configDir := t.TempDir()
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
	deadline := time.Now().Add(timeout)
	for _, node := range s.Nodes {
		ep := node.AdminAddr + "/healthz"
		for time.Now().Before(deadline) {
			resp, err := http.Get(ep) //nolint:noctx
			if err == nil && resp.StatusCode == 200 {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				break
			}
			if resp != nil {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
			time.Sleep(50 * time.Millisecond)
		}
		if time.Now().After(deadline) {
			t.Fatalf("node %s did not become healthy within %s", node.Name, timeout)
		}
	}
}

func (s *ClusterStack) waitMembership(t *testing.T, timeout time.Duration, expected int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := range s.Nodes {
			peers := s.Peers(t, i)
			if len(peers) < expected {
				ok = false
				break
			}
		}
		if ok {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("cluster did not reach %d members within %s", expected, timeout)
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

// RestartNode re-boots a previously killed node with fresh config.
func (s *ClusterStack) RestartNode(t *testing.T, n int) {
	t.Helper()
	if s.cancels[n] != nil {
		return
	}

	cfgPath := s.Nodes[n].cfgPath
	if cfgPath == "" {
		t.Fatalf("no config path for node %d", n)
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels[n] = cancel
	s.errChs[n] = make(chan error, 1)

	root := cmd.Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-level", "warn"})
	go func(ch chan error) {
		ch <- root.ExecuteContext(ctx)
	}(s.errChs[n])

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.Nodes[n].AdminAddr + "/healthz") //nolint:noctx
		if err == nil && resp.StatusCode == 200 {
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			t.Logf("cluster: restarted %s", s.Nodes[n].Name)
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("node %s did not become healthy after restart", s.Nodes[n].Name)
}

// PauseNode is not supported in in-process mode (no SIGSTOP for goroutines).
func (s *ClusterStack) PauseNode(t *testing.T, _ int) {
	t.Helper()
	t.Skip("PauseNode not supported in in-process mode")
}

// UnpauseNode is not supported in in-process mode.
func (s *ClusterStack) UnpauseNode(t *testing.T, _ int) {
	t.Helper()
	t.Skip("UnpauseNode not supported in in-process mode")
}

// FlapOrigin drops all active origin connections n times with a pause between.
func (s *ClusterStack) FlapOrigin(t *testing.T, n int, pause time.Duration) {
	t.Helper()
	for i := range n {
		s.origin.CloseClientConnections()
		t.Logf("origin flap %d/%d: connections dropped", i+1, n)
		time.Sleep(pause)
	}
}

// ScaleOriginLatency is not supported in in-process mode.
func (s *ClusterStack) ScaleOriginLatency(_ int) error {
	return fmt.Errorf("ScaleOriginLatency not supported in in-process mode")
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
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(interval)
	}
	t.Fatalf("condition not met within %s", deadline)
}

// XCache returns the X-Cache header value.
func XCache(resp *http.Response) string {
	return resp.Header.Get("X-Cache")
}
