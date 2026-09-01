//go:build integration

// Package driver boots bouine nodes in-process for integration tests.
// No Docker required — each node is a goroutine running engine.run().
package driver

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valyala/fasthttp"

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

// HeaderMap is a map of response headers with a Get method compatible
// with the http.Header API used by test callers.
type HeaderMap map[string]string

func (h HeaderMap) Get(key string) string { return h[key] }

// Response is a minimal response type returned by driver HTTP methods,
// replacing *http.Response. It carries the status code, headers, and body.
type Response struct {
	StatusCode int
	Header     HeaderMap
	Body       []byte
}

// ClusterNode describes one bouine node.
type ClusterNode struct {
	Name       string
	HTTPAddr   string
	HTTPSAddr  string // empty when TLS is not configured
	AdminAddr  string
	GossipAddr string // host:port for cluster gossip
	Token      string
	cfgPath    string // path to config YAML (for RestartNode)
}

// ClusterStack holds a live in-process 3-node cluster + origin.
type ClusterStack struct {
	Mode      string
	Nodes     [3]ClusterNode
	OriginURL string

	origin    *fasthttpTestServer
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
	TLS           TLSOptions
}

// TLSOptions configures data-plane TLS for the cluster. When Enabled is
// true, each node gets a listen.https address and the provided cert/key
// pair is referenced in the tls.certs config section. ExtraCerts adds
// additional cert entries for SNI-based selection.
type TLSOptions struct {
	Enabled    bool
	CertFile   string
	KeyFile    string
	SNI        []string // optional SNI matches; empty = match all
	MinVersion string   // optional: "1.2" (default) or "1.3"
	ExtraCerts []TLSCertEntry
}

// TLSCertEntry is an additional cert/key pair for SNI selection.
type TLSCertEntry struct {
	CertFile string
	KeyFile  string
	SNI      []string
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

// formatCertEntry renders a single tls.certs entry in YAML.
func formatCertEntry(certFile, keyFile string, sni []string) string {
	entry := fmt.Sprintf("    - cert_file: %q\n      key_file: %q", certFile, keyFile)
	if len(sni) > 0 {
		quoted := make([]string, len(sni))
		for i, s := range sni {
			quoted[i] = fmt.Sprintf("%q", s)
		}
		entry += "\n      sni: [" + strings.Join(quoted, ", ") + "]"
	}
	return entry + "\n"
}

// nodeConfigParams holds the parameters for generating a bouine node's
// YAML config file. It is shared by BootCluster, RestartNode, and
// RestartNodeWithTLS to keep the config template in one place.
type nodeConfigParams struct {
	name       string
	mode       string
	httpPort   int
	httpsPort  int // 0 when TLS is not configured
	adminPort  int
	gossipPort int
	seedList   string
	originAddr string
	tls        *TLSOptions // nil when TLS is not configured
}

// buildNodeConfig renders the YAML config for a single bouine node.
// When tls is non-nil, the https listener and tls section are included.
func buildNodeConfig(p nodeConfigParams) string {
	var b strings.Builder
	fmt.Fprintf(&b, `listen:
  http: "127.0.0.1:%d"
`, p.httpPort)
	if p.tls != nil {
		fmt.Fprintf(&b, "  https: \"127.0.0.1:%d\"\n", p.httpsPort)
	}
	fmt.Fprintf(&b, `  admin: "127.0.0.1:%d"
  cluster: "127.0.0.1:%d"
admin:
  token: %s
storage:
  hot_max_bytes: 128MiB
cluster:
  node_name: %s
  mode: %s
  join: %s
  hop_limit: 2
upstream_pools:
  - name: origin
    targets: [%q]
routes:
  - match:
      path_prefix: /api/v1/
    pool: origin
    cache:
      ttl_default: 60s
    request:
      strip_prefix: /api/v1
  - match: {}
    pool: origin
    cache:
      ttl_default: 60s
`,
		p.adminPort, p.gossipPort, IntegrationToken, p.name, p.mode, p.seedList,
		p.originAddr)

	if p.tls != nil {
		minVer := p.tls.MinVersion
		if minVer == "" {
			minVer = "1.2"
		}
		b.WriteString("tls:\n  min_version: " + fmt.Sprintf("%q", minVer) + "\n  certs:\n")
		b.WriteString(formatCertEntry(p.tls.CertFile, p.tls.KeyFile, p.tls.SNI))
		for _, ec := range p.tls.ExtraCerts {
			b.WriteString(formatCertEntry(ec.CertFile, ec.KeyFile, ec.SNI))
		}
	}
	return b.String()
}

// BootCluster starts a 3-node in-process bouine cluster with a fasthttp origin.
func BootCluster(t *testing.T, opts ClusterOptions) *ClusterStack {
	t.Helper()
	if opts.Mode == "" {
		opts.Mode = "strong"
	}

	origin, originCtl := startOriginWithControl()

	// Allocate ports for all nodes upfront so gossip seeds are known.
	type nodePorts struct {
		http, https, admin, gossip int
	}
	ports := [3]nodePorts{}
	for i := range ports {
		ports[i] = nodePorts{
			http:   freePort(t),
			admin:  freePort(t),
			gossip: freePort(t),
		}
		if opts.TLS.Enabled {
			ports[i].https = freePort(t)
		}
	}

	// Build gossip seed list.
	var seeds []string
	for _, p := range ports {
		seeds = append(seeds, fmt.Sprintf("127.0.0.1:%d", p.gossip))
	}
	seedList := `["` + strings.Join(seeds, `","`) + `"]`

	configDir, err := os.MkdirTemp("", "bouine-integration-*")
	if err != nil {
		t.Fatalf("create config dir: %v", err)
	}
	s := &ClusterStack{
		Mode:      opts.Mode,
		OriginURL: origin.url,
		origin:    origin,
		originCtl: originCtl,
		configDir: configDir,
	}

	// Write configs and start each node.
	for i := range 3 {
		p := ports[i]
		name := fmt.Sprintf("bouine-%d", i+1)

		var tlsOpts *TLSOptions
		if opts.TLS.Enabled {
			tlsOpts = &opts.TLS
		}
		cfg := buildNodeConfig(nodeConfigParams{
			name:       name,
			mode:       opts.Mode,
			httpPort:   p.http,
			httpsPort:  p.https,
			adminPort:  p.admin,
			gossipPort: p.gossip,
			seedList:   seedList,
			originAddr: origin.addr,
			tls:        tlsOpts,
		})

		cfgPath := filepath.Join(s.configDir, name+".yaml")
		if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
			t.Fatalf("write config %s: %v", name, err)
		}

		s.Nodes[i] = ClusterNode{
			Name:       name,
			HTTPAddr:   fmt.Sprintf("http://127.0.0.1:%d", p.http),
			AdminAddr:  fmt.Sprintf("http://127.0.0.1:%d", p.admin),
			GossipAddr: fmt.Sprintf("127.0.0.1:%d", p.gossip),
			Token:      IntegrationToken,
			cfgPath:    cfgPath,
		}
		if opts.TLS.Enabled {
			s.Nodes[i].HTTPSAddr = fmt.Sprintf("https://127.0.0.1:%d", p.https)
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
		opts.Mode, s.Nodes[0].HTTPAddr, s.Nodes[1].HTTPAddr, s.Nodes[2].HTTPAddr, origin.url)
	return s
}

func (s *ClusterStack) waitHealthy(t *testing.T, timeout time.Duration) {
	t.Helper()
	for _, node := range s.Nodes {
		ep := node.AdminAddr + "/readyz"
		poll.Eventually(t, timeout, 50*time.Millisecond, func() bool {
			req := fasthttp.AcquireRequest()
			resp := fasthttp.AcquireResponse()
			defer fasthttp.ReleaseRequest(req)
			defer fasthttp.ReleaseResponse(resp)
			req.SetRequestURI(ep)
			if err := fasthttp.Do(req, resp); err != nil {
				return false
			}
			return resp.StatusCode() == 200
		})
	}
}

// IsAlive reports whether node n is currently running.
func (s *ClusterStack) IsAlive(n int) bool {
	return s.cancels[n] != nil
}

// AliveNodes returns the indices of nodes that are currently running.
func (s *ClusterStack) AliveNodes() []int {
	var out []int
	for i := range s.Nodes {
		if s.IsAlive(i) {
			out = append(out, i)
		}
	}
	return out
}

func (s *ClusterStack) waitMembership(t *testing.T, timeout time.Duration, expected int) {
	t.Helper()
	poll.Eventually(t, timeout, 200*time.Millisecond, func() bool {
		for _, i := range s.AliveNodes() {
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
	if s.configDir != "" {
		_ = os.RemoveAll(s.configDir)
		s.configDir = ""
	}
}

// Dump is a no-op for in-process nodes (logs go to stderr).
func (s *ClusterStack) Dump(_ *testing.T) {}

// RestartNode re-boots a previously killed node with fresh ports to
// avoid bind conflicts from lingering sockets.
func (s *ClusterStack) RestartNode(t *testing.T, n int) {
	s.restartNode(t, n, nil)
}

// RestartNodeWithTLS re-boots a killed node with fresh ports and a new
// TLS cert/key pair. This is used for certificate-rotation tests.
func (s *ClusterStack) RestartNodeWithTLS(t *testing.T, n int, tlsOpts TLSOptions) {
	s.restartNode(t, n, &tlsOpts)
}

// restartNode is the shared implementation for RestartNode and
// RestartNodeWithTLS. When tlsOpts is non-nil, the node is configured
// with an HTTPS listener and the provided cert/key pair.
func (s *ClusterStack) restartNode(t *testing.T, n int, tlsOpts *TLSOptions) {
	t.Helper()
	if s.cancels[n] != nil {
		return
	}

	httpPort := freePort(t)
	adminPort := freePort(t)
	gossipPort := freePort(t)

	name := s.Nodes[n].Name
	seedList := s.gossipSeeds()

	var httpsPort int
	if tlsOpts != nil {
		httpsPort = freePort(t)
	}

	cfg := buildNodeConfig(nodeConfigParams{
		name:       name,
		mode:       s.Mode,
		httpPort:   httpPort,
		httpsPort:  httpsPort,
		adminPort:  adminPort,
		gossipPort: gossipPort,
		seedList:   seedList,
		originAddr: s.origin.addr,
		tls:        tlsOpts,
	})

	suffix := "-restart"
	if tlsOpts != nil {
		suffix = "-restart-tls"
	}
	cfgPath := filepath.Join(s.configDir, name+suffix+".yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatalf("write restart config: %v", err)
	}

	s.Nodes[n].HTTPAddr = fmt.Sprintf("http://127.0.0.1:%d", httpPort)
	s.Nodes[n].HTTPSAddr = ""
	if tlsOpts != nil {
		s.Nodes[n].HTTPSAddr = fmt.Sprintf("https://127.0.0.1:%d", httpsPort)
	}
	s.Nodes[n].AdminAddr = fmt.Sprintf("http://127.0.0.1:%d", adminPort)
	s.Nodes[n].GossipAddr = fmt.Sprintf("127.0.0.1:%d", gossipPort)
	s.Nodes[n].cfgPath = cfgPath

	ctx, cancel := context.WithCancel(context.Background())
	s.cancels[n] = cancel
	s.errChs[n] = make(chan error, 1)

	root := cmd.Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-level", "warn"})
	go func(ch chan error) {
		ch <- root.ExecuteContext(ctx)
	}(s.errChs[n])

	poll.Eventually(t, 30*time.Second, 50*time.Millisecond, func() bool {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		defer fasthttp.ReleaseRequest(req)
		defer fasthttp.ReleaseResponse(resp)
		req.SetRequestURI(s.Nodes[n].AdminAddr + "/readyz")
		if err := fasthttp.Do(req, resp); err != nil {
			return false
		}
		ok := resp.StatusCode() == 200
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
		if s.cancels[i] != nil && s.Nodes[i].GossipAddr != "" {
			seeds = append(seeds, s.Nodes[i].GossipAddr)
		}
	}
	if len(seeds) == 0 {
		return `[]`
	}
	return `["` + strings.Join(seeds, `","`) + `"]`
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

// doGet performs a GET request and returns a Response.
func doGet(url, host string) (*Response, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if host != "" {
		// UseHostHeader keeps the dial target from the URL while the
		// Host header is overridden. The header must be set via
		// req.Header.SetHost — req.SetHost overwrites the URI host and
		// the client would dial the override value (e.g. "testhost",
		// which resolves to nothing).
		req.UseHostHeader = true
		req.Header.SetHost(host)
	}
	if err := fasthttp.Do(req, resp); err != nil {
		return nil, err
	}
	return responseFromFastHTTP(resp), nil
}

// doGetWithClient performs a GET using a pre-allocated client.
func doGetWithClient(client *fasthttp.Client, url, host string) (*Response, error) {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if host != "" {
		// Same as doGet: override the Host header, not the dial target.
		req.UseHostHeader = true
		req.Header.SetHost(host)
	}
	if err := client.Do(req, resp); err != nil {
		return nil, err
	}
	return responseFromFastHTTP(resp), nil
}

// responseFromFastHTTP copies relevant fields from a fasthttp.Response
// into a Response. The caller must not use the fasthttp.Response after
// this call (it may have been released).
func responseFromFastHTTP(resp *fasthttp.Response) *Response {
	hdr := HeaderMap{}
	resp.Header.VisitAll(func(k, v []byte) {
		hdr[string(k)] = string(v)
	})
	return &Response{
		StatusCode: resp.StatusCode(),
		Header:     hdr,
		Body:       append([]byte(nil), resp.Body()...),
	}
}

// Get performs a GET request against node n.
func (s *ClusterStack) Get(t *testing.T, n int, path string) *Response {
	t.Helper()
	return s.GetWithHost(t, n, path, "")
}

// GetWithHost performs a GET with a specific Host header.
func (s *ClusterStack) GetWithHost(t *testing.T, n int, path string, host string) *Response {
	t.Helper()
	url := s.Nodes[n].HTTPAddr + path
	resp, err := doGet(url, host)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// GetBody performs a GET and returns both the response and body.
func (s *ClusterStack) GetBody(t *testing.T, n int, path string) (*Response, string) {
	t.Helper()
	url := s.Nodes[n].HTTPAddr + path
	resp, err := doGet(url, "")
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp, string(resp.Body)
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
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(body)
	req.Header.SetContentType("application/octet-stream")
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	if err := fasthttp.Do(req, resp); err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	if resp.StatusCode() >= 300 {
		t.Fatalf("POST %s: status %d body: %s", url, resp.StatusCode(), string(resp.Body()))
	}
}

func (s *ClusterStack) adminPost(t *testing.T, n int, path string, body []byte) {
	t.Helper()
	url := s.Nodes[n].AdminAddr + path
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod("POST")
	req.SetBody(body)
	req.Header.SetContentType("application/json")
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	if err := fasthttp.Do(req, resp); err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	if resp.StatusCode() >= 300 {
		t.Fatalf("POST %s: status %d body: %s", url, resp.StatusCode(), string(resp.Body()))
	}
}

// Peers returns the cluster peers from node n's admin API.
func (s *ClusterStack) Peers(t *testing.T, n int) []map[string]any {
	t.Helper()
	url := s.Nodes[n].AdminAddr + "/v1/cluster/peers"
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.Set("Authorization", "Bearer "+s.Nodes[n].Token)
	if err := fasthttp.Do(req, resp); err != nil {
		t.Fatalf("peers GET: %v", err)
	}
	var peers []map[string]any
	_ = json.Unmarshal(resp.Body(), &peers)
	return peers
}

// MetricValue reads a Prometheus metric value from node n.
func (s *ClusterStack) MetricValue(t *testing.T, n int, metric string) float64 {
	t.Helper()
	url := s.Nodes[n].AdminAddr + "/metrics"
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if err := fasthttp.Do(req, resp); err != nil {
		t.Fatalf("metrics GET: %v", err)
	}
	raw := resp.Body()
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
func XCache(resp *Response) string {
	return resp.Header.Get("X-Cache")
}

// httpsClient returns a fasthttp client configured for HTTPS with
// InsecureSkipVerify (test certs are self-signed).
func (s *ClusterStack) httpsClient() *fasthttp.Client {
	return &fasthttp.Client{
		TLSConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec // test
	}
}

// GetTLS performs a GET request over HTTPS against node n.
func (s *ClusterStack) GetTLS(t *testing.T, n int, path string) *Response {
	t.Helper()
	r, err := s.GetTLSWithHost(t, n, path, "")
	if err != nil {
		t.Fatalf("GET %s%s: %v", s.Nodes[n].HTTPSAddr, path, err)
	}
	return r
}

// GetTLSWithHost performs a GET over HTTPS with a specific Host header
// (also used as SNI for the TLS handshake).
func (s *ClusterStack) GetTLSWithHost(t *testing.T, n int, path string, host string) (*Response, error) {
	t.Helper()
	url := s.Nodes[n].HTTPSAddr + path
	return doGetWithClient(s.httpsClient(), url, host)
}

// GetTLSResponse performs a GET over HTTPS and returns the raw response
// without draining the body. The caller is responsible for closing the body.
func (s *ClusterStack) GetTLSResponse(t *testing.T, n int, path string) *Response {
	t.Helper()
	url := s.Nodes[n].HTTPSAddr + path
	resp, err := doGetWithClient(s.httpsClient(), url, "")
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	return resp
}

// TLSServerCerts returns the peer certificates from a TLS connection to
// node n. This is used to verify SNI-based cert selection.
func (s *ClusterStack) TLSServerCerts(t *testing.T, n int, serverName string) []*x509.Certificate {
	t.Helper()
	hostPort := strings.TrimPrefix(s.Nodes[n].HTTPSAddr, "https://")
	conf := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test
		ServerName:         serverName,
		NextProtos:         []string{"h2", "http/1.1"},
	}
	// Retry the TLS dial a few times. The admin /readyz check confirms
	// the node process is up, but the TLS listener on the data port may
	// not be accepting connections yet under CI load.
	var conn *tls.Conn
	var err error
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	for range 5 {
		conn, err = tls.DialWithDialer(dialer, "tcp", hostPort, conf)
		if err == nil {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("TLS dial %s (SNI=%s): %v", hostPort, serverName, err)
	}
	defer conn.Close()
	return conn.ConnectionState().PeerCertificates
}
