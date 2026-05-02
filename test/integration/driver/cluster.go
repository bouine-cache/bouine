//go:build integration

package driver

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	// IntegrationToken is the admin bearer token used by all cluster
	// integration tests. It is baked into the per-node config files.
	IntegrationToken = "inttest-token"

	// CrossNodeHost is the Host header value used for cache-key-consistent
	// requests across nodes. Must match the port in purge URLs for gossip
	// invalidation to target the same keys on all nodes.
	CrossNodeHost = "testhost:8080"

	// External ports reachable from the test host.
	// Layout per node: HTTP=1808N, Admin=1900N  (N = 1..3)
	// Origin: 18080
	OriginExtPort  = "18080"
	Node1HTTPPort  = "18081"
	Node2HTTPPort  = "18082"
	Node3HTTPPort  = "18083"
	Node1AdminPort = "19001"
	Node2AdminPort = "19002"
	Node3AdminPort = "19003"

	// GossipConvergence is the max time to wait for gossip to propagate
	// purge/ban events across the cluster. PushPullInterval is set to 5s
	// (down from the memberlist default of 30s), so two push/pull rounds
	// complete in < 15 s, leaving margin for processing.
	GossipConvergence = 35 * time.Second
	// ReplicationDeadline is the max time to wait for a full-mode
	// replication event to be stored on a peer node.
	ReplicationDeadline = 15 * time.Second
)

// ClusterNode describes one bouine node reachable from the test host.
//
// Stable.
type ClusterNode struct {
	// Name is the container service name ("bouine-1", "bouine-2", "bouine-3").
	Name string
	// HTTPAddr is the data-plane base URL ("http://127.0.0.1:18081").
	HTTPAddr string
	// AdminAddr is the admin API base URL ("http://127.0.0.1:19001").
	AdminAddr string
	// Token is the admin bearer token.
	Token string
}

// ClusterStack holds a live 3-node bouine cluster plus origin.
//
// Stable.
type ClusterStack struct {
	// Mode is the cluster consistency mode ("strong", "eventual", "full").
	Mode string
	// Nodes are the three bouine nodes; Nodes[0] = bouine-1, etc.
	Nodes [3]ClusterNode
	// OriginURL is the direct origin base URL ("http://127.0.0.1:18080").
	OriginURL string

	t          *testing.T
	configDir  string // tmpdir holding per-node config YAML files
	projectDir string // path to test/integration/ (compose file location)
	project    string // docker compose project name
}

// ClusterOptions configures BootCluster.
type ClusterOptions struct {
	// Mode is the cluster consistency mode. One of "strong", "eventual", "full".
	// Defaults to "strong" if empty.
	Mode string
	// NoAutoCleanup disables automatic teardown registration on t. The caller
	// is responsible for calling stack.Down(). Used by TestMain-level setup.
	NoAutoCleanup bool
}

// BootCluster starts a 3-node bouine cluster via Docker Compose and
// returns a live ClusterStack. The caller must not call t.Cleanup manually;
// BootCluster registers teardown automatically.
//
// If Docker Compose is not available the test is skipped.
func BootCluster(t *testing.T, opts ClusterOptions) *ClusterStack {
	t.Helper()
	if opts.Mode == "" {
		opts.Mode = "strong"
	}

	// Skip if docker compose is not available on this machine.
	if err := exec.Command("docker", "compose", "version").Run(); err != nil {
		t.Skip("docker compose not available — skipping cluster integration test")
	}

	// Resolve the test/integration directory (docker-compose file lives there).
	_, thisFile, _, _ := runtime.Caller(0)
	integrationDir := filepath.Join(filepath.Dir(thisFile), "..")

	// Write per-node config files to a temp directory.
	configDir, err := os.MkdirTemp("", "bouine-inttest-*")
	if err != nil {
		t.Fatalf("create config tmpdir: %v", err)
	}

	for i := 1; i <= 3; i++ {
		name := fmt.Sprintf("bouine-%d", i)
		cfg := nodeConfig(opts.Mode)
		path := filepath.Join(configDir, name+".yaml")
		if err := os.WriteFile(path, []byte(cfg), 0o600); err != nil {
			t.Fatalf("write node config %s: %v", name, err)
		}
	}

	project := func() string {
		b := make([]byte, 4)
		_, _ = rand.Read(b)
		return fmt.Sprintf("bouine-inttest-%s-%s", opts.Mode, hex.EncodeToString(b))
	}()

	s := &ClusterStack{
		Mode: opts.Mode,
		Nodes: [3]ClusterNode{
			{Name: "bouine-1", HTTPAddr: "http://127.0.0.1:" + Node1HTTPPort, AdminAddr: "http://127.0.0.1:" + Node1AdminPort, Token: IntegrationToken},
			{Name: "bouine-2", HTTPAddr: "http://127.0.0.1:" + Node2HTTPPort, AdminAddr: "http://127.0.0.1:" + Node2AdminPort, Token: IntegrationToken},
			{Name: "bouine-3", HTTPAddr: "http://127.0.0.1:" + Node3HTTPPort, AdminAddr: "http://127.0.0.1:" + Node3AdminPort, Token: IntegrationToken},
		},
		OriginURL:  "http://127.0.0.1:" + OriginExtPort,
		t:          t,
		configDir:  configDir,
		projectDir: integrationDir,
		project:    project,
	}

	// Tear down any leftover stack from a previous interrupted run, then start fresh.
	// Kill any leftover containers from previous test runs by scanning for
	// any container that has "bouine-inttest" or "bouine-test" in its name.
	// Use raw docker commands since we don't know the compose project names.
	killCmd := exec.Command("sh", "-c",
		"docker ps -q --filter name=bouine-inttest --filter name=bouine-test 2>/dev/null | xargs -r docker rm -f 2>/dev/null")
	_ = killCmd.Run()

	if !opts.NoAutoCleanup {
		t.Cleanup(func() {
			s.Dump(t)
			s.composeDown(true)
			_ = os.RemoveAll(configDir)
		})
	}

	t.Logf("cluster: starting %s mode stack (project=%s)", opts.Mode, project)
	upCmd := s.compose("up", "--build", "--detach")
	if out, err := upCmd.CombinedOutput(); err != nil {
		t.Logf("docker compose up output:\n%s", string(out))
		// Try to get logs before fataling.
		s.Dump(t)
		t.Fatalf("cluster: docker compose up failed: %v", err)
	}

	// Poll health endpoints directly (belt-and-suspenders on top of compose --wait).
	s.waitHealthy(t, 90*time.Second)
	// In clustered mode, wait additionally for gossip membership to converge.
	s.waitMembership(t, 30*time.Second, 3)

	t.Logf("cluster: %s stack ready — nodes: %s %s %s",
		opts.Mode,
		s.Nodes[0].HTTPAddr, s.Nodes[1].HTTPAddr, s.Nodes[2].HTTPAddr)
	return s
}

// nodeConfig renders the bouine YAML config for a cluster node.
// All three nodes share the same config; POD_IP is injected via Docker
// environment variable so each container advertises the right address.
func nodeConfig(mode string) string {
	return fmt.Sprintf(`listen:
  http: ":8080"
  admin: ":9000"
  cluster: ":7000"
admin:
  token: %s
storage:
  hot_max_bytes: 128MiB
cluster:
  enabled: true
  mode: %s
  join:
    - "bouine-1:7000"
    - "bouine-2:7000"
    - "bouine-3:7000"
  hop_limit: 2
upstream_pools:
  - name: origin
    targets: ["origin:8080"]
routes:
  - match: {}
    pool: origin
    cache:
      ttl_default: 60s
`, IntegrationToken, mode)
}

// ---- ClusterStack helpers ------------------------------------------------

// Get performs a GET request against the data-plane of node n with
// an optional Host header override via query parameter (when requesting
// from multiple nodes with a consistent key for cross-node cache tests).
// It returns the response for inspection; the body is fully read and closed.
func (s *ClusterStack) Get(t *testing.T, n int, path string) *http.Response {
	t.Helper()
	return s.GetWithHost(t, n, path, "")
}

// GetWithHost performs a GET with a specific Host header value. Use
// this when you need identical cache keys across nodes (e.g. purge
// propagation tests). Pass host="" to use the default (node's addr).
func (s *ClusterStack) GetWithHost(t *testing.T, n int, path string, host string) *http.Response {
	t.Helper()
	url := s.Nodes[n].HTTPAddr + path
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("GET %s: new request: %v", url, err)
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

// GetBody performs a GET request and returns both the response and body string.
func (s *ClusterStack) GetBody(t *testing.T, n int, path string) (*http.Response, string) {
	t.Helper()
	url := s.Nodes[n].HTTPAddr + path
	resp, err := http.Get(url) //nolint:noctx
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body %s: %v", url, err)
	}
	return resp, string(body)
}

// XCache returns the X-Cache header value from the last request to node n.
func XCache(resp *http.Response) string {
	return resp.Header.Get("X-Cache")
}

// Purge sends POST /v1/purge to node n for the given data-plane URL.
func (s *ClusterStack) Purge(t *testing.T, n int, targetURL string) {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"url": targetURL})
	s.adminPost(t, n, "/v1/purge", body)
}

// Ban sends POST /v1/ban to node n with the given host/path regex predicates.
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
	// Use a fresh client per request to avoid connection-reuse issues with
	// shared connections that may be in a degraded state.
	client := &http.Client{Timeout: 30 * time.Second}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 500 * time.Millisecond)
		}
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			t.Fatalf("POST %s: new request: %v", url, err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			t.Logf("POST %s attempt %d: %v (retrying)", url, attempt+1, err)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode >= 300 {
			t.Fatalf("POST %s: status %d body: %s", url, resp.StatusCode, string(b))
		}
		return
	}
	t.Fatalf("POST %s: %v", url, lastErr)
}

// Peers returns the PeerInfo JSON array from node n's admin API.
func (s *ClusterStack) Peers(t *testing.T, n int) []map[string]any {
	t.Helper()
	url := s.Nodes[n].AdminAddr + "/v1/cluster/peers"
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("peers: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("peers GET: %v", err)
	}
	defer resp.Body.Close()
	var peers []map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil {
		t.Fatalf("peers decode: %v", err)
	}
	return peers
}

// MetricValue reads the Prometheus counter or gauge named metric from node n.
// It returns the sum of all label combinations that match the metric name prefix.
// Returns 0 if the metric is not found or has no value.
func (s *ClusterStack) MetricValue(t *testing.T, n int, metric string) float64 {
	t.Helper()
	url := s.Nodes[n].AdminAddr + "/metrics"
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	resp, err := http.DefaultClient.Do(req)
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
		// Match lines that start with the metric name (with or without labels).
		if !strings.HasPrefix(line, metric) {
			continue
		}
		// Ensure it's the right metric (not a metric with a longer name).
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

// RetryUntil polls f repeatedly until it returns true or the deadline passes.
// It calls t.Fatal if the deadline expires before f returns true.
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

// ---- internal helpers ------------------------------------------------

func (s *ClusterStack) compose(args ...string) *exec.Cmd {
	composeFile := filepath.Join(s.projectDir, "docker-compose.cluster.yaml")
	cmdArgs := append([]string{
		"compose",
		"-p", s.project,
		"-f", composeFile,
	}, args...)
	cmd := exec.Command("docker", cmdArgs...)
	cmd.Dir = s.projectDir
	cmd.Env = append(os.Environ(), "BOUINE_CONFIG_DIR="+s.configDir)
	return cmd
}

func (s *ClusterStack) composeDown(fatal bool) {
	cmd := s.compose("down", "--volumes", "--remove-orphans", "--timeout", "15")
	if out, err := cmd.CombinedOutput(); err != nil && fatal {
		s.t.Logf("docker compose down: %v\n%s", err, string(out))
	}
}

func (s *ClusterStack) waitHealthy(t *testing.T, timeout time.Duration) {
	t.Helper()
	endpoints := []string{
		"http://127.0.0.1:" + Node1AdminPort + "/healthz",
		"http://127.0.0.1:" + Node2AdminPort + "/healthz",
		"http://127.0.0.1:" + Node3AdminPort + "/healthz",
	}
	deadline := time.Now().Add(timeout)
	for _, ep := range endpoints {
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
			time.Sleep(500 * time.Millisecond)
		}
		if time.Now().After(deadline) {
			t.Fatalf("cluster: node at %s did not become healthy within %s", ep, timeout)
		}
	}
}

func (s *ClusterStack) waitMembership(t *testing.T, timeout time.Duration, expectedPeers int) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok := true
		for i := range s.Nodes {
			func() {
				url := s.Nodes[i].AdminAddr + "/v1/cluster/peers"
				req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
				if err != nil {
					ok = false
					return
				}
				req.Header.Set("Authorization", "Bearer "+s.Nodes[i].Token)
				resp, err := http.DefaultClient.Do(req)
				if err != nil {
					ok = false
					return
				}
				defer resp.Body.Close()
				var peers []map[string]any
				if err := json.NewDecoder(resp.Body).Decode(&peers); err != nil || len(peers) < expectedPeers {
					ok = false
				}
			}()
		}
		if ok {
			return
		}
		time.Sleep(1 * time.Second)
	}
	t.Fatalf("cluster: did not reach %d members within %s", expectedPeers, timeout)
}

// Dump prints container logs for all cluster services to t.Log.
func (s *ClusterStack) Dump(t *testing.T) {
	t.Helper()
	for _, svc := range []string{"origin", "bouine-1", "bouine-2", "bouine-3"} {
		cmd := s.compose("logs", "--no-color", "--tail=50", svc)
		out, _ := cmd.CombinedOutput()
		if len(out) > 0 {
			t.Logf("=== logs: %s ===\n%s", svc, string(out))
		}
	}
}

// KillNode stops a single bouine container to simulate a hard node failure.
// The node is not removed from compose; call RestartNode to bring it back.
func (s *ClusterStack) KillNode(t *testing.T, n int) {
	t.Helper()
	cmd := s.compose("stop", s.Nodes[n].Name)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("stop %s: %v\n%s", s.Nodes[n].Name, err, string(out))
	}
	t.Logf("cluster: stopped %s", s.Nodes[n].Name)
}

// RestartNode brings a previously stopped node back up and waits for it to
// pass its health check. Useful after KillNode or PauseNode.
func (s *ClusterStack) RestartNode(t *testing.T, n int) {
	t.Helper()
	cmd := s.compose("start", s.Nodes[n].Name)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("start %s: %v\n%s", s.Nodes[n].Name, err, string(out))
	}
	// Wait for the node to pass health.
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(s.Nodes[n].HTTPAddr + "/healthz")
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			t.Logf("cluster: %s healthy after restart", s.Nodes[n].Name)
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	t.Logf("cluster: %s did not become healthy within 30s — continuing anyway", s.Nodes[n].Name)
}

// PauseNode suspends a bouine container's processes (SIGSTOP via Docker)
// to simulate a partial network partition. The node is still reachable by
// Docker networking but stops processing requests.
// Call UnpauseNode to resume.
func (s *ClusterStack) PauseNode(t *testing.T, n int) {
	t.Helper()
	cmd := s.compose("pause", s.Nodes[n].Name)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("pause %s: %v\n%s", s.Nodes[n].Name, err, string(out))
	}
	t.Logf("cluster: paused %s (partition simulation)", s.Nodes[n].Name)
}

// UnpauseNode resumes a paused bouine container (SIGCONT via Docker).
func (s *ClusterStack) UnpauseNode(t *testing.T, n int) {
	t.Helper()
	cmd := s.compose("unpause", s.Nodes[n].Name)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("unpause %s: %v\n%s", s.Nodes[n].Name, err, string(out))
	}
	t.Logf("cluster: unpaused %s", s.Nodes[n].Name)
}

// FlapOrigin stops and restarts the origin container repeatedly to simulate
// an origin flap. It performs n cycles of (stop, wait, start) with the
// given interval between cycles.
func (s *ClusterStack) FlapOrigin(t *testing.T, cycles int, interval time.Duration) {
	t.Helper()
	for i := range cycles {
		cmd := s.compose("stop", "origin")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("origin flap cycle %d stop: %v\n%s", i, err, string(out))
		}
		time.Sleep(interval)
		cmd = s.compose("start", "origin")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Logf("origin flap cycle %d start: %v\n%s", i, err, string(out))
		}
		t.Logf("cluster: origin flap cycle %d/%d complete", i+1, cycles)
	}
}

// ScaleOriginLatency injects artificial latency into the origin container
// using tc-netem (requires Linux with iproute2 in the container).
// Latency is in milliseconds. Pass 0 to clear the rule.
// Returns an error (rather than fatalling) because tc may be unavailable;
// callers should skip the test if this returns a non-nil error.
func (s *ClusterStack) ScaleOriginLatency(latencyMs int) error {
	var args []string
	if latencyMs > 0 {
		args = []string{
			"exec", "origin",
			"tc", "qdisc", "add", "dev", "eth0", "root",
			"netem", "delay", fmt.Sprintf("%dms", latencyMs),
		}
	} else {
		args = []string{
			"exec", "origin",
			"tc", "qdisc", "del", "dev", "eth0", "root",
		}
	}
	cmd := s.compose(args...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("tc netem: %w\n%s", err, string(out))
	}
	return nil
}

// ConfigDir returns the path of the temporary directory containing per-node
// config files. The caller is responsible for removing it.
func (s *ClusterStack) ConfigDir() string { return s.configDir }

// Down tears down the cluster (docker compose down) without requiring a T.
// Used by TestMain-level cleanup managed via NoAutoCleanup.
func (s *ClusterStack) Down() {
	cmd := s.compose("down", "--volumes", "--remove-orphans", "--timeout", "15")
	_, _ = cmd.CombinedOutput()
}
