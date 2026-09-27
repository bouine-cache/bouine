package cluster

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

// encodeCoalescePeerFetchRequest encodes a v3 (coalescing) request body
// the way buildPeerRequest emits it. Test fixtures stay far below the
// 64 KiB wire caps, so an encode failure is a bug in the fixture.
func encodeCoalescePeerFetchRequest(req api.PeerFetchRequest) []byte {
	body, ok := encodePeerFetchBody(nil, req)
	if !ok {
		panic("test fixture exceeds the 64 KiB wire cap")
	}
	return body
}

type stubOriginFetcher struct {
	calls  atomic.Int64
	obj    *api.Object
	err    error
	reqURI atomic.Value // string
}

func (s *stubOriginFetcher) FetchOrigin(_ context.Context, key api.Key, originReq *fasthttp.Request) (*api.Object, error) {
	s.calls.Add(1)
	s.reqURI.Store(string(originReq.RequestURI()))
	if s.err != nil {
		return nil, s.err
	}
	return s.obj, nil
}

func coalesceReq(key api.Key) api.PeerFetchRequest {
	return api.PeerFetchRequest{
		Key:      key,
		Coalesce: true,
		Route:    "assets",
		OriginRequest: api.OriginRequest{
			Method: fasthttp.MethodGet,
			URI:    "/shielded?x=1",
			Host:   "origin.example",
			Headers: []api.PeerHeader{
				{Name: header.AcceptEncoding, Value: "br, gzip"},
			},
		},
	}
}

func coalesceObject(key api.Key) *api.Object {
	return &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("shielded body"),
		BodySize:   13,
		TTL:        60_000_000_000,
		StoredAt:   time.Unix(0, 0),
	}
}

// decodeCoalesceResponse decodes the encoded object a coalesced (or
// plain hit) peer-fetch response carries.
func decodeCoalesceResponse(body []byte) (*api.Object, error) {
	return storage.DecodeObject(body)
}

// v3 wire format: round-trips the coalescing extension through the
// parser, including envelope headers.
func TestParsePeerFetchBody_V3RoundTrip(t *testing.T) {
	t.Parallel()
	req := coalesceReq(testkey.Key(7))
	body := encodeCoalescePeerFetchRequest(req)
	got, ok := parsePeerFetchBody(body)
	require.True(t, ok)
	assert.True(t, got.Coalesce)
	assert.Equal(t, "assets", got.Route)
	assert.Equal(t, req.Key, got.Key)
	assert.Equal(t, fasthttp.MethodGet, got.OriginRequest.Method)
	assert.Equal(t, "/shielded?x=1", got.OriginRequest.URI)
	assert.Equal(t, "origin.example", got.OriginRequest.Host)
	require.Len(t, got.OriginRequest.Headers, 1)
	assert.Equal(t, header.AcceptEncoding, got.OriginRequest.Headers[0].Name)
	assert.Equal(t, "br, gzip", got.OriginRequest.Headers[0].Value)
}

// v2 bodies parse unchanged (format compat on the receiving side), and
// a v3 body is rejected by the v2 grammar — the rolling-deploy path.
func TestParsePeerFetchBody_V2Compat(t *testing.T) {
	t.Parallel()
	key := testkey.Key(9)
	v2 := encodePeerFetchRequest(api.PeerFetchRequest{Key: key, VaryKey: "frhash"})
	got, ok := parsePeerFetchBody(v2)
	require.True(t, ok)
	assert.False(t, got.Coalesce)
	assert.Equal(t, "frhash", got.VaryKey)

	v3 := encodeCoalescePeerFetchRequest(coalesceReq(key))
	_, ok = parsePeerFetchBody(v2) // sanity: v2 parses as v2
	require.True(t, ok)
	got3, ok := parsePeerFetchBody(v3)
	require.True(t, ok)
	assert.True(t, got3.Coalesce)

	// Truncated v3 extension → reject, not panic.
	_, ok = parsePeerFetchBody(v3[:len(v3)-3])
	assert.False(t, ok)
	// Unknown version → reject.
	bad := append([]byte(nil), v2...)
	bad[0] = 9
	_, ok = parsePeerFetchBody(bad)
	assert.False(t, ok)
}

// Coalesced miss: the owner's OriginFetcher answers and the object is
// returned encoded; a non-registered route answers 404 as today.
func TestPeerFetchHandler_Coalesce(t *testing.T) {
	t.Parallel()
	key := testkey.Key(11)
	fetcher := &stubOriginFetcher{obj: coalesceObject(key)}
	h := NewPeerFetchHandler(&stubStore{}, 0)
	h.SetOriginFetchers(map[string]OriginFetcher{"assets": fetcher})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI(PeerFetchPath)
	ctx.Request.SetBody(encodeCoalescePeerFetchRequest(coalesceReq(key)))
	h.Handle(ctx)

	require.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), string(ctx.Response.Body()))
	obj, err := decodeCoalesceResponse(ctx.Response.Body())
	require.NoError(t, err)
	assert.Equal(t, 200, obj.StatusCode)
	assert.Equal(t, []byte("shielded body"), obj.Body)
	assert.EqualValues(t, 1, fetcher.calls.Load())
	assert.Equal(t, "/shielded?x=1", fetcher.reqURI.Load())

	// Unknown route → 404, fetcher untouched.
	unknown := coalesceReq(key)
	unknown.Route = "nope"
	ctx2 := &fasthttp.RequestCtx{}
	ctx2.Request.Header.SetMethod("POST")
	ctx2.Request.SetRequestURI(PeerFetchPath)
	ctx2.Request.SetBody(encodeCoalescePeerFetchRequest(unknown))
	h.Handle(ctx2)
	assert.Equal(t, fasthttp.StatusNotFound, ctx2.Response.StatusCode())
	assert.EqualValues(t, 1, fetcher.calls.Load())
}

// Coalesced fetch failure: the flight error maps to a dedicated error
// status (never a 404, which would mean "not coalesced").
func TestPeerFetchHandler_CoalesceFailure(t *testing.T) {
	t.Parallel()
	key := testkey.Key(12)
	fetcher := &stubOriginFetcher{err: errors.New("origin down")}
	h := NewPeerFetchHandler(&stubStore{}, 0)
	h.SetOriginFetchers(map[string]OriginFetcher{"assets": fetcher})

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI(PeerFetchPath)
	ctx.Request.SetBody(encodeCoalescePeerFetchRequest(coalesceReq(key)))
	h.Handle(ctx)
	assert.Equal(t, fasthttp.StatusBadGateway, ctx.Response.StatusCode())
}

// A coalesce=true request with a non-GET/HEAD envelope method is
// refused: the requester fetches origin directly.
func TestPeerFetchHandler_CoalesceRejectsNonGet(t *testing.T) {
	t.Parallel()
	key := testkey.Key(13)
	fetcher := &stubOriginFetcher{obj: coalesceObject(key)}
	h := NewPeerFetchHandler(&stubStore{}, 0)
	h.SetOriginFetchers(map[string]OriginFetcher{"assets": fetcher})

	req := coalesceReq(key)
	req.OriginRequest.Method = fasthttp.MethodPost
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("POST")
	ctx.Request.SetRequestURI(PeerFetchPath)
	ctx.Request.SetBody(encodeCoalescePeerFetchRequest(req))
	h.Handle(ctx)
	assert.Equal(t, fasthttp.StatusNotFound, ctx.Response.StatusCode())
	assert.EqualValues(t, 0, fetcher.calls.Load())
}

// A lane at capacity sheds with a dedicated status instead of parking
// the RPC behind a slow origin. Runs against a real server: the shed
// path reads the request context's done channel, which only a served
// RequestCtx has. The shed answer is 503 + Retry-After (the origin was
// never contacted), written exactly once by handleCoalesce — never the
// 502 of a failed origin flight.
func TestPeerFetchHandler_CoalesceShed(t *testing.T) {
	t.Parallel()
	key := testkey.Key(14)
	release := make(chan struct{})
	defer close(release)
	blocking := &blockingFetcher{release: release}
	h := NewPeerFetchHandler(&stubStore{}, 0)
	for range defaultCoalesceConcurrency {
		h.coalesceSem <- struct{}{}
	}
	h.SetOriginFetchers(map[string]OriginFetcher{"assets": blocking})

	srv := fasthttptest.NewServer(t, h.Handle)
	defer srv.Close()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("POST")
	req.SetRequestURI("http://" + srv.Addr + PeerFetchPath)
	req.SetBody(encodeCoalescePeerFetchRequest(coalesceReq(key)))
	require.NoError(t, fasthttp.Do(req, resp))
	assert.Equal(t, fasthttp.StatusServiceUnavailable, resp.StatusCode(),
		"a shed coalesced fetch must answer 503, not the 502 of a failed flight")
	assert.Equal(t, "1", string(resp.Header.Peek("Retry-After")))
}

type blockingFetcher struct {
	release chan struct{}
	calls   atomic.Int64
}

func (b *blockingFetcher) FetchOrigin(_ context.Context, _ api.Key, _ *fasthttp.Request) (*api.Object, error) {
	b.calls.Add(1)
	<-b.release
	return nil, errors.New("blocked")
}

// The owner's fetcher is invoked once per RPC even under concurrent
// coalesced requests (lane concurrency allows it; the collapse happens
// in the route handler's singleflight — pinned there, not here).
func TestPeerFetchHandler_CoalesceConcurrent(t *testing.T) {
	t.Parallel()
	key := testkey.Key(15)
	fetcher := &stubOriginFetcher{obj: coalesceObject(key)}
	h := NewPeerFetchHandler(&stubStore{}, 0)
	h.SetOriginFetchers(map[string]OriginFetcher{"assets": fetcher})

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx := &fasthttp.RequestCtx{}
			ctx.Request.Header.SetMethod("POST")
			ctx.Request.SetRequestURI(PeerFetchPath)
			ctx.Request.SetBody(encodeCoalescePeerFetchRequest(coalesceReq(key)))
			h.Handle(ctx)
			assert.Equal(t, fasthttp.StatusOK, ctx.Response.StatusCode(), string(ctx.Response.Body()))
		}()
	}
	wg.Wait()
	assert.EqualValues(t, 8, fetcher.calls.Load())
}

// An envelope field above the 64 KiB wire cap rejects the request
// construction instead of truncating: a silently shortened URI would
// make the owner fetch — and cache — the wrong origin resource.
func TestEncodePeerFetchBody_RejectsOversized(t *testing.T) {
	t.Parallel()
	key := testkey.Key(17)
	req := coalesceReq(key)
	req.OriginRequest.URI = strings.Repeat("a", maxStringLen+1)
	_, ok := encodePeerFetchBody(nil, req)
	assert.False(t, ok, "an over-cap envelope URI must be rejected, not truncated")

	// The caller surfaces it as a fetch error, not a silent short URI.
	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	defer f.Close(context.Background())
	_, err := f.Fetch(context.Background(), api.PeerInfo{Addr: "127.0.0.1:1"}, req)
	require.Error(t, err, "an over-cap envelope must fail request construction, not send a truncated URI")
}
