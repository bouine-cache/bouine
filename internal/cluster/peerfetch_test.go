package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

type stubStore struct {
	objects map[api.Key]*api.Object
}

func (s *stubStore) Get(_ context.Context, key api.Key) (*api.Object, api.Source, error) {
	return s.objects[key], "", nil
}

func (s *stubStore) Put(_ context.Context, key api.Key, obj *api.Object) error {
	if s.objects == nil {
		s.objects = make(map[api.Key]*api.Object)
	}
	s.objects[key] = obj
	return nil
}

// fastHTTPToHTTP adapts a fasthttp.RequestHandler to an http.Handler
// for use with httptest.NewServer.
func fastHTTPToHTTP(h fasthttp.RequestHandler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := &fasthttp.RequestCtx{}
		ctx.Request.Header.SetMethod(r.Method)
		ctx.Request.SetRequestURI(r.URL.String())
		ctx.Request.Header.SetHost(r.Host)
		for k, vs := range r.Header {
			for _, v := range vs {
				ctx.Request.Header.Add(k, v)
			}
		}
		if r.Body != nil {
			body, _ := io.ReadAll(r.Body)
			ctx.Request.SetBody(body)
		}
		h(ctx)
		w.WriteHeader(ctx.Response.StatusCode())
		for k, v := range ctx.Response.Header.All() {
			w.Header().Add(string(k), string(v))
		}
		_, _ = w.Write(ctx.Response.Body())
	})
}

// peerFetchServerAdapter wraps a PeerFetchHandler as an http.Handler
// for httptest.NewServer compatibility.
func peerFetchServerAdapter(h *PeerFetchHandler) http.Handler {
	return fastHTTPToHTTP(h.Handle)
}

func peerPutServerAdapter(h *PeerPutHandler) http.Handler {
	return fastHTTPToHTTP(h.Handle)
}

func postFetch(t *testing.T, h *PeerFetchHandler, req api.PeerFetchRequest, hop int) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err, "marshal")
	r, _ := http.NewRequestWithContext(context.Background(), "POST", PeerFetchPath, bytes.NewReader(body))
	r.Header.Set(header.ContentType, "application/json")
	if hop > 0 {
		r.Header.Set(BouineHopHeader, fmt.Sprintf("%d", hop))
	}
	rr := httptest.NewRecorder()
	peerFetchServerAdapter(h).ServeHTTP(rr, r)
	return rr
}

func TestPeerFetchHandler_Hit(t *testing.T) {
	t.Parallel()
	key := testkey.Key(42)
	store := &stubStore{objects: map[api.Key]*api.Object{
		key: {Key: key, StatusCode: 200, Body: []byte("cached")},
	}}
	h := NewPeerFetchHandler(store, 0)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: key}, 0)
	require.Equal(t, 200, rr.Code)
	obj, err := storage.DecodeObject(rr.Body.Bytes())
	require.NoError(t, err, "binary decode")
	if obj.Key != key || obj.StatusCode != 200 {
		t.Fatalf("decoded mismatch: key=%d status=%d", obj.Key, obj.StatusCode)
	}
}

func TestPeerFetchHandler_BinaryWireProtocol(t *testing.T) {
	t.Parallel()
	key := testkey.Key(42)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("cached-body"),
		ETag:       `"abc"`,
	}
	obj.Header = header.NewMap(1)
	obj.Header.AppendEntry("Content-Type", "text/plain")
	store := &stubStore{objects: map[api.Key]*api.Object{key: obj}}
	h := NewPeerFetchHandler(store, 0)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: key}, 0)

	require.Equal(t, 200, rr.Code)
	ct := rr.Header().Get(header.ContentType)
	require.Equal(t, "application/octet-stream", ct)

	decoded, err := storage.DecodeObject(rr.Body.Bytes())
	require.NoError(t, err, "binary decode")
	if decoded.Key != obj.Key || decoded.StatusCode != obj.StatusCode {
		t.Fatalf("decoded mismatch: key=%d status=%d", decoded.Key, decoded.StatusCode)
	}
	require.Equal(t, "cached-body", string(decoded.Body))
	require.Equal(t, obj.ETag, decoded.ETag)
	require.Equal(t, 1, decoded.Header.Len())
}

func TestPeerFetcher_BinaryRoundTrip(t *testing.T) {
	t.Parallel()
	key := testkey.Key(7)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("roundtrip"),
	}
	obj.Header = header.NewMap(1)
	obj.Header.AppendEntry("X-Custom", "value")

	srv := httptest.NewServer(peerFetchServerAdapter(NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{
		key: obj,
	}}, 0)))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	got, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: key})
	require.NoError(t, err, "fetch")
	require.NotNil(t, got)
	if got.Key != key || string(got.Body) != "roundtrip" {
		t.Fatalf("got key=%d body=%q", got.Key, got.Body)
	}
	require.Equal(t, 1, got.Header.Len())
}

func TestPeerFetchHandler_Miss(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{}}, 0)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: testkey.Key(999)}, 0)
	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPeerFetchHandler_HopLimit(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{}, 0)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: testkey.Key(1)}, MaxHops)
	require.Equal(t, http.StatusLoopDetected, rr.Code)
}

func TestPeerFetchHandler_CustomHopLimit(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{}, 1)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: testkey.Key(1)}, 1)
	require.Equal(t, http.StatusLoopDetected, rr.Code)
	h3 := NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{}}, 3)
	rr3 := postFetch(t, h3, api.PeerFetchRequest{Key: testkey.Key(1)}, 2)
	require.NotEqual(t, http.StatusLoopDetected, rr3.Code)
}

func TestPeerFetchHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{}, 0)
	rr := httptest.NewRecorder()
	r, _ := http.NewRequestWithContext(context.Background(), "GET", PeerFetchPath, nil)
	peerFetchServerAdapter(h).ServeHTTP(rr, r)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestPeerPutHandler_Stores(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	h := NewPeerPutHandler(store, nil)
	obj := &api.Object{
		Key:        testkey.Key(42),
		StatusCode: 200,
		Body:       []byte("from-non-owner"),
		BodySize:   15,
		TTL:        60 * time.Second,
		StoredAt:   time.Now(),
	}
	encoded := storage.EncodeObject(obj)
	r, _ := http.NewRequestWithContext(context.Background(), "POST", PeerPutPath, bytes.NewReader(encoded))
	r.Header.Set(header.ContentType, "application/octet-stream")
	rr := httptest.NewRecorder()
	peerPutServerAdapter(h).ServeHTTP(rr, r)
	require.Equal(t, http.StatusOK, rr.Code)
	stored, _, err := store.Get(context.Background(), obj.Key)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, obj.Body, stored.Body)
}

func TestPeerPutHandler_OnStoreCallback(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	h := NewPeerPutHandler(store, nil)
	var called atomic.Int32
	h.SetOnStore(func(_ context.Context, obj *api.Object) {
		called.Add(1)
		assert.Equal(t, testkey.Key(42), obj.Key)
	})
	encoded := storage.EncodeObject(&api.Object{
		Key:        testkey.Key(42),
		StatusCode: 200,
		Body:       []byte("x"),
		BodySize:   1,
		TTL:        60 * time.Second,
		StoredAt:   time.Now(),
	})
	r, _ := http.NewRequestWithContext(context.Background(), "POST", PeerPutPath, bytes.NewReader(encoded))
	r.Header.Set(header.ContentType, "application/octet-stream")
	rr := httptest.NewRecorder()
	peerPutServerAdapter(h).ServeHTTP(rr, r)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, int32(1), called.Load())
}

func TestPeerPutHandler_BadBody(t *testing.T) {
	t.Parallel()
	h := NewPeerPutHandler(&stubStore{}, nil)
	r, _ := http.NewRequestWithContext(context.Background(), "POST", PeerPutPath, bytes.NewReader([]byte{0xFF, 0x00}))
	r.Header.Set(header.ContentType, "application/octet-stream")
	rr := httptest.NewRecorder()
	peerPutServerAdapter(h).ServeHTTP(rr, r)
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

func TestPeerPutHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := NewPeerPutHandler(&stubStore{}, nil)
	r, _ := http.NewRequestWithContext(context.Background(), "GET", PeerPutPath, nil)
	rr := httptest.NewRecorder()
	peerPutServerAdapter(h).ServeHTTP(rr, r)
	assert.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestPeerFetcher_Put_RoundTrip(t *testing.T) {
	t.Parallel()
	store := &stubStore{}
	srv := httptest.NewServer(peerPutServerAdapter(NewPeerPutHandler(store, nil)))
	defer srv.Close()
	f := NewPeerFetcher(nil, nil, 0)
	obj := &api.Object{
		Key:        testkey.Key(7),
		StatusCode: 200,
		Body:       []byte("payload"),
		BodySize:   7,
		TTL:        60 * time.Second,
		StoredAt:   time.Now(),
	}
	peer := api.PeerInfo{Addr: srv.Listener.Addr().String()}
	err := f.Put(context.Background(), peer, obj)
	require.NoError(t, err)
	stored, _, err := store.Get(context.Background(), obj.Key)
	require.NoError(t, err)
	require.NotNil(t, stored)
	assert.Equal(t, obj.Body, stored.Body)
}

func TestPeerFetcher_RecordsRoundTripLatency(t *testing.T) {
	t.Parallel()
	const delay = 5 * time.Millisecond
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		w.Header().Set(header.ContentType, "application/octet-stream")
		_, _ = w.Write(storage.EncodeObject(&api.Object{Key: testkey.Key(1), StatusCode: 200, Body: []byte("cached")}))
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: testkey.Key(1)})
	require.NoError(t, err, "fetch")
	require.NotNil(t, obj)

	hits, _, _, latN, latSumMs := f.PeerFetchStats()
	if hits != 1 || latN != 1 {
		t.Fatalf("hits=%d latN=%d, want 1,1", hits, latN)
	}
	if latSumMs <= 0 {
		t.Fatalf("latSumMs=%d, want >0", latSumMs)
	}
}

func TestPeerFetcher_BinaryRoundTrip_TimeFields(t *testing.T) {
	t.Parallel()
	key := testkey.Key(9)
	storedAt := time.Date(2026, 7, 6, 12, 0, 0, 0, time.UTC)
	lastMod := time.Date(2026, 7, 5, 8, 30, 0, 0, time.UTC)
	obj := &api.Object{
		Key:                  key,
		StatusCode:           200,
		Body:                 []byte("timebody"),
		TTL:                  30 * time.Second,
		StaleWhileRevalidate: 10 * time.Second,
		StaleIfError:         60 * time.Second,
		StoredAt:             storedAt,
		LastModified:         lastMod,
	}

	srv := httptest.NewServer(peerFetchServerAdapter(NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{
		key: obj,
	}}, 0)))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	got, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: key})
	require.NoError(t, err, "fetch")
	require.NotNil(t, got)
	require.Equal(t, obj.TTL, got.TTL)
	require.Equal(t, obj.StaleWhileRevalidate, got.StaleWhileRevalidate)
	require.Equal(t, obj.StaleIfError, got.StaleIfError)
	require.True(t, got.StoredAt.Equal(obj.StoredAt))
	require.True(t, got.LastModified.Equal(obj.LastModified))
}

func TestPeerFetcher_MissIncrementsCounter(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: testkey.Key(1)})
	require.NoError(t, err, "fetch")
	require.Nil(t, obj)
	hits, misses, _, _, _ := f.PeerFetchStats()
	require.Equal(t, int64(0), hits)
	require.Equal(t, int64(1), misses)
}

func TestPeerFetcher_HopLimitReached(t *testing.T) {
	t.Parallel()
	f := NewPeerFetcher(nil, nil, 0)
	obj, err := f.Fetch(context.Background(), api.PeerInfo{Addr: "unused:0"},
		api.PeerFetchRequest{Key: testkey.Key(1), Hops: MaxHops})
	require.NoError(t, err, "hop limit should return nil,nil")
	require.Nil(t, obj)
}

func TestPeerFetcher_OversizedResponseReturnsError(t *testing.T) {
	t.Parallel()
	validResp := storage.EncodeObject(&api.Object{
		Key:        testkey.Key(1),
		StatusCode: 200,
		Body:       bytes.Repeat([]byte("A"), 4096),
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/octet-stream")
		_, _ = w.Write(validResp)
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	f.maxBodyBytes = int64(len(validResp) - 1)

	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: testkey.Key(1)})
	require.Error(t, err)
	require.Nil(t, obj)
}

func TestPeerFetcher_ConcurrencySemaphoreBoundsFetches(t *testing.T) {
	t.Parallel()
	var inFlight, maxInFlight atomic.Int32

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		cur := inFlight.Add(1)
		for {
			old := maxInFlight.Load()
			if cur <= old || maxInFlight.CompareAndSwap(old, cur) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		inFlight.Add(-1)
		w.Header().Set(header.ContentType, "application/octet-stream")
		_, _ = w.Write(storage.EncodeObject(&api.Object{Key: testkey.Key(1), StatusCode: 200, Body: []byte("x")}))
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)

	var wg sync.WaitGroup
	for i := 0; i < defaultPeerFetchConcurrency*3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.Fetch(context.Background(),
				api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
				api.PeerFetchRequest{Key: testkey.Key(1)})
		}()
	}
	wg.Wait()

	if got := maxInFlight.Load(); got > int32(defaultPeerFetchConcurrency) {
		t.Fatalf("max concurrent peer-fetches = %d, want <= %d", got, defaultPeerFetchConcurrency)
	}
}

func TestPeerFetcher_ContextCancelWhileWaitingForSemaphore(t *testing.T) {
	t.Parallel()
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
		w.Header().Set(header.ContentType, "application/octet-stream")
		_, _ = w.Write(storage.EncodeObject(&api.Object{Key: testkey.Key(1)}))
	}))
	defer srv.Close()
	defer close(block)

	f := NewPeerFetcher(nil, nil, 0)

	var wg sync.WaitGroup
	for i := 0; i < defaultPeerFetchConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = f.Fetch(context.Background(),
				api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
				api.PeerFetchRequest{Key: testkey.Key(1)})
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := f.Fetch(ctx,
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: testkey.Key(2)})
	require.Error(t, err)

	wg.Wait()
}

func BenchmarkPeerFetchHandler_ServeHTTP(b *testing.B) {
	key := testkey.Key(1)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       bytes.Repeat([]byte("A"), 4096),
		ETag:       `"benchmark-etag"`,
	}
	obj.Header = header.NewMap(10)
	for i := range 10 {
		obj.Header.AppendEntry(
			"X-Benchmark-Header-"+strconv.Itoa(i),
			"value-"+strconv.Itoa(i),
		)
	}
	store := &stubStore{objects: map[api.Key]*api.Object{key: obj}}
	h := NewPeerFetchHandler(store, 0)
	adapter := peerFetchServerAdapter(h)

	reqBody, _ := json.Marshal(api.PeerFetchRequest{Key: key})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rr := httptest.NewRecorder()
		r, _ := http.NewRequestWithContext(context.Background(),
			http.MethodPost, PeerFetchPath, bytes.NewReader(reqBody))
		r.Header.Set(header.ContentType, "application/json")
		adapter.ServeHTTP(rr, r)
		if rr.Code != 200 {
			b.Fatalf("status=%d", rr.Code)
		}
	}
}

func BenchmarkPeerFetcher_Fetch(b *testing.B) {
	key := testkey.Key(1)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       bytes.Repeat([]byte("A"), 4096),
		ETag:       `"benchmark-etag"`,
	}
	obj.Header = header.NewMap(10)
	for i := range 10 {
		obj.Header.AppendEntry(
			"X-Benchmark-Header-"+strconv.Itoa(i),
			"value-"+strconv.Itoa(i),
		)
	}
	encoded := storage.EncodeObject(obj)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/octet-stream")
		_, _ = w.Write(encoded)
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	peer := api.PeerInfo{AdminAddr: srv.Listener.Addr().String()}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		got, err := f.Fetch(context.Background(), peer, api.PeerFetchRequest{Key: key})
		if err != nil {
			b.Fatalf("fetch: %v", err)
		}
		if got == nil || got.Key != key {
			b.Fatalf("unexpected result: %+v", got)
		}
	}
}
