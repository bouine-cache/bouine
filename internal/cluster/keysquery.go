package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// Keys-query channel (issue #816): `bouine keys find --peer` asks the
// ring owner for one key's state. Unlike the invalidation fan-outs
// (POST + broadcast), this is a point-to-point GET between admin
// servers: the querying node has already computed the key with its
// own key policy; the owner answers from its own tiers. The payload
// is api.KeyState JSON — small, bounded, never body bytes.

const (
	// PeerKeysFindPath is mounted on each peer's admin server. Token
	// is not required: like every /v1/peer/* route the traffic is
	// intra-cluster and the admin port is network-isolated in
	// production (see AGENTS.md §6).
	PeerKeysFindPath = "/v1/peer/keys/find"
	// maxKeysQueryBytes caps the peer key-state response. The state
	// block is a few hundred bytes for typical objects — surrogate
	// keys are the only unbounded field — so 64 KiB leaves generous
	// headroom while keeping a corrupt peer from pinning memory.
	maxKeysQueryBytes = 64 << 10
)

// NewPeerKeysFindHandler returns a fasthttp.RequestHandler that
// answers GET /v1/peer/keys/find?key=<hex> with the local state of
// the key, delegating to fn (the node's storage probe). Absent keys
// answer 200 with present=false — absence is data, not an error.
func NewPeerKeysFindHandler(fn func(key api.Key) (api.KeyState, bool)) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		raw := string(ctx.QueryArgs().Peek("key"))
		if raw == "" {
			ctx.Error("bad request: missing key query parameter", fasthttp.StatusBadRequest)
			return
		}
		key, err := api.ParseKeyHex(raw)
		if err != nil {
			ctx.Error("bad request: "+err.Error(), fasthttp.StatusBadRequest)
			return
		}
		st, _ := fn(key)
		ctx.Response.Header.Set(header.ContentType, "application/json")
		ctx.SetStatusCode(fasthttp.StatusOK)
		_ = json.NewEncoder(ctx).Encode(st)
	}
}

// QueryKeyState asks peer for the local state of key over the peer
// admin channel. It reuses the fetcher's pipelined client pool, the
// per-address breaker, and the PeerFetchTimeout RPC budget, but not
// a fetch semaphore slot: the query is an admin-path call
// (bouine keys find), bounded by its own request — never a
// data-plane miss path — so it must not shed under fetch pressure.
func (f *PeerFetcher) QueryKeyState(ctx context.Context, peer api.PeerInfo, key api.Key) (api.KeyState, error) {
	addr := peerAddr(peer)
	if !f.breakerAllowed(addr) {
		return api.KeyState{}, fmt.Errorf("cluster: keys query: peer %s in breaker cooldown", addr)
	}
	c := f.getPipelineClient(addr)
	if c == nil {
		return api.KeyState{}, fmt.Errorf("cluster: keys query: fetcher closed")
	}
	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseRequest(req)
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod(fasthttp.MethodGet)
	req.SetRequestURI(fmt.Sprintf("%s?key=%s", PeerKeysFindPath, key.Hex()))
	req.SetHost(addr)
	req.Header.Set(ClusterVersionHeader, ClusterProtocolVersion)
	if err := f.pipelineDo(ctx, c, req, resp); err != nil {
		f.recordPeerFailure(addr)
		return api.KeyState{}, fmt.Errorf("cluster: keys query: %w", err)
	}
	f.recordPeerSuccess(addr)
	if resp.StatusCode() != fasthttp.StatusOK {
		return api.KeyState{}, fmt.Errorf("cluster: keys query: status %d from %s", resp.StatusCode(), addr)
	}
	body := resp.Body()
	if len(body) > maxKeysQueryBytes {
		return api.KeyState{}, fmt.Errorf("cluster: keys query: response exceeds %d bytes", maxKeysQueryBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	var st api.KeyState
	if err := dec.Decode(&st); err != nil {
		return api.KeyState{}, fmt.Errorf("cluster: keys query: %w", err)
	}
	// The stream must end exactly at the object: a second value or
	// non-JSON junk after it is trailing data, matching the strictness
	// of the binary frames.
	if err := dec.Decode(new(json.RawMessage)); !errors.Is(err, io.EOF) {
		return api.KeyState{}, fmt.Errorf("cluster: keys query: trailing data")
	}
	return st, nil
}
