package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/cmd/bouine/cmd"
)

// tier boots a 3-node strong cluster in-process: one cobra entrypoint per
// node over a generated YAML config, real listeners, real gossip join — the
// same technique test/integration/driver uses, minus the testing.T plumbing.
type tierNode struct {
	cancel    context.CancelFunc
	errCh     chan error
	name      string
	dataAddr  string
	adminAddr string
}

type tier struct {
	cfgDir string
	nodes  []tierNode
}

// nodePorts names one node's http/admin/gossip port triple.
type nodePorts struct {
	http   int
	admin  int
	gossip int
}

func bootTier(originAddr string, tr tracingSpec) (*tier, error) {
	np := [3]nodePorts{}
	for i := range np {
		p, err := reserveNodePorts()
		if err != nil {
			return nil, err
		}
		np[i] = p
	}
	seeds := fmt.Sprintf(`"127.0.0.1:%d","127.0.0.1:%d","127.0.0.1:%d"`, np[0].gossip, np[1].gossip, np[2].gossip)

	cfgDir, err := os.MkdirTemp("", "bouine-topology-*")
	if err != nil {
		return nil, err
	}
	t := &tier{cfgDir: cfgDir}

	for i := range 3 {
		node, err := t.bootNode(i, np[i], seeds, originAddr, tr)
		if err != nil {
			t.down()
			return nil, err
		}
		t.nodes = append(t.nodes, node)
	}

	if err := t.waitReady(60 * time.Second); err != nil {
		t.down()
		return nil, err
	}
	return t, nil
}

// bootNode writes one node's YAML config and starts its cobra entrypoint.
func (t *tier) bootNode(i int, p nodePorts, seeds, originAddr string, tr tracingSpec) (tierNode, error) {
	name := "topo-" + strconv.Itoa(i+1)
	cfg := fmt.Sprintf(`listen:
  http: "127.0.0.1:%d"
  admin: "127.0.0.1:%d"
  cluster: "127.0.0.1:%d"
admin:
  token: %q
storage:
  hot_max_bytes: 512MiB
cluster:
  node_name: %s
  mode: strong
  join: [%s]
  hop_limit: 2
%supstream_pools:
  - name: ssr
    targets: ["%s"]
routes:
  - match:
      path_prefix: /ssr/
    pool: ssr
    cache:
      ttl_default: 600s
  - match:
      path_prefix: /nostore/
    pool: ssr
    cache:
      ttl_default: 600s
  - match: {}
    pool: ssr
    cache:
      ttl_default: 600s
`,
		p.http, p.admin, p.gossip, harnessToken, name, seeds, tr.yaml(), originAddr)

	cfgPath := filepath.Join(t.cfgDir, name+".yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o600); err != nil {
		return tierNode{}, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	root := cmd.Root()
	root.SetArgs([]string{"serve", "--config", cfgPath, "--log-level", "error"})
	go func() { errCh <- root.ExecuteContext(ctx) }()

	return tierNode{
		name:      name,
		dataAddr:  fmt.Sprintf("http://127.0.0.1:%d", p.http),
		adminAddr: fmt.Sprintf("http://127.0.0.1:%d", p.admin),
		cancel:    cancel,
		errCh:     errCh,
	}, nil
}

// reserveNodePorts reserves one node's http/admin/gossip ports; gossip
// binds UDP, so the UDP number is probed too.
func reserveNodePorts() (nodePorts, error) {
	var out nodePorts
	http, err := freePort()
	if err != nil {
		return out, err
	}
	admin, err := freePort()
	if err != nil {
		return out, err
	}
	gossip, err := freePort()
	if err != nil {
		return out, err
	}
	return nodePorts{http: http, admin: admin, gossip: gossip}, nil
}

func (t *tier) dataAddrs() []string {
	out := make([]string, len(t.nodes))
	for i, n := range t.nodes {
		out[i] = n.dataAddr
	}
	return out
}

// peerFetchCounters is one node's peer-fetch telemetry scraped from its
// admin /v1/status readout.
type peerFetchCounters struct {
	hits   int64
	misses int64
}

func (t *tier) peerFetchCounters() [nodesPerTier]peerFetchCounters {
	var out [nodesPerTier]peerFetchCounters
	for i, n := range t.nodes {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		req.SetRequestURI(n.adminAddr + "/v1/status")
		req.Header.Set("Authorization", "Bearer "+harnessToken)
		if err := fasthttp.Do(req, resp); err == nil && resp.StatusCode() == fasthttp.StatusOK {
			var doc struct {
				Cluster struct {
					PeerFetch struct {
						Hits   int64 `json:"hits"`
						Misses int64 `json:"misses"`
					} `json:"peer_fetch"`
				} `json:"cluster"`
			}
			if json.Unmarshal(resp.Body(), &doc) == nil {
				out[i] = peerFetchCounters{hits: doc.Cluster.PeerFetch.Hits, misses: doc.Cluster.PeerFetch.Misses}
			}
		}
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}
	return out
}

func (t *tier) down() {
	for i := range t.nodes {
		t.nodes[i].cancel()
		select {
		case err := <-t.nodes[i].errCh:
			_ = err // a non-nil exit only means cobra returned; teardown is cancel-driven
		case <-time.After(10 * time.Second):
		}
	}
	_ = os.RemoveAll(t.cfgDir)
}
func (t *tier) waitReady(timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ready := true
		for _, n := range t.nodes {
			if !adminProbe(n.adminAddr + "/readyz") {
				ready = false
				break
			}
		}
		if ready && t.membershipConverged() {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("cluster not ready within %s", timeout)
}

func (t *tier) membershipConverged() bool {
	for _, n := range t.nodes {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		req.SetRequestURI(n.adminAddr + "/v1/cluster/peers")
		err := fasthttp.Do(req, resp)
		body := append([]byte(nil), resp.Body()...)
		status := resp.StatusCode()
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
		if err != nil || status != fasthttp.StatusOK {
			return false
		}
		var peers []struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(body, &peers) != nil || len(peers) < 3 {
			return false
		}
	}
	return true
}

func adminProbe(url string) bool {
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	if err := fasthttp.Do(req, resp); err != nil {
		return false
	}
	return resp.StatusCode() == fasthttp.StatusOK
}

// listenLocal reserves a TCP port on loopback.
func listenLocal() (net.Listener, error) {
	var lc net.ListenConfig
	return lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
}

// freePort reserves an advisory-free port; gossip binds UDP, so the UDP
// number is probed too (mirrors test/integration/driver.freePort).
func freePort() (int, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	_ = ln.Close()
	udpln, err := lc.ListenPacket(context.Background(), "udp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return freePort()
	}
	_ = udpln.Close()
	return port, nil
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1) //nolint:forbidigo // bench CLI abort — no deferred cleanup can run past this point
}
