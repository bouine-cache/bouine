package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

type stubStore struct {
	objects map[api.Key]*api.Object
}

func (s *stubStore) Get(_ context.Context, key api.Key) (*api.Object, api.Source, error) {
	return s.objects[key], "", nil
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
	h.ServeHTTP(rr, r)
	return rr
}

func TestPeerFetchHandler_Hit(t *testing.T) {
	t.Parallel()
	key := api.KeyFromPrimary(42)
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

// TestPeerFetchHandler_BinaryWireProtocol pins that the handler
// responds with the binary object codec (application/octet-stream),
// not JSON. This is the single biggest allocation win from issue #187:
// the JSON path base64-encoded the body and allocated a
// map[string][]string per header on every peer fetch.
func TestPeerFetchHandler_BinaryWireProtocol(t *testing.T) {
	t.Parallel()
	key := api.KeyFromPrimary(42)
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

// TestPeerFetcher_BinaryRoundTrip verifies the full client→server
// round-trip uses the binary codec with zero JSON on the response path.
func TestPeerFetcher_BinaryRoundTrip(t *testing.T) {
	t.Parallel()
	key := api.KeyFromPrimary(7)
	obj := &api.Object{
		Key:        key,
		StatusCode: 200,
		Body:       []byte("roundtrip"),
	}
	obj.Header = header.NewMap(1)
	obj.Header.AppendEntry("X-Custom", "value")

	srv := httptest.NewServer(NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{
		key: obj,
	}}, 0))
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
	rr := postFetch(t, h, api.PeerFetchRequest{Key: api.KeyFromPrimary(999)}, 0)
	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestPeerFetchHandler_HopLimit(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{}, 0)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: api.KeyFromPrimary(1)}, MaxHops)
	require.Equal(t, http.StatusLoopDetected, rr.Code)
}

func TestPeerFetchHandler_CustomHopLimit(t *testing.T) {
	t.Parallel()
	// hopLimit=1: a request at hop 1 should be rejected.
	h := NewPeerFetchHandler(&stubStore{}, 1)
	rr := postFetch(t, h, api.PeerFetchRequest{Key: api.KeyFromPrimary(1)}, 1)
	require.Equal(t, http.StatusLoopDetected, rr.Code)
	// hopLimit=3: a request at hop 2 should pass through (not rejected).
	h3 := NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{}}, 3)
	rr3 := postFetch(t, h3, api.PeerFetchRequest{Key: api.KeyFromPrimary(1)}, 2)
	require.NotEqual(t, http.StatusLoopDetected, rr3.Code)
}

func TestPeerFetchHandler_WrongMethod(t *testing.T) {
	t.Parallel()
	h := NewPeerFetchHandler(&stubStore{}, 0)
	rr := httptest.NewRecorder()
	r, _ := http.NewRequestWithContext(context.Background(), "GET", PeerFetchPath, nil)
	h.ServeHTTP(rr, r)
	require.Equal(t, http.StatusMethodNotAllowed, rr.Code)
}

func TestPeerFetcher_RecordsRoundTripLatency(t *testing.T) {
	t.Parallel()
	const delay = 5 * time.Millisecond // > 1ms so it survives Milliseconds() truncation
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(delay)
		w.Header().Set(header.ContentType, "application/octet-stream")
		_, _ = w.Write(storage.EncodeObject(&api.Object{Key: api.KeyFromPrimary(1), StatusCode: 200, Body: []byte("cached")}))
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: api.KeyFromPrimary(1)})
	require.NoError(t, err, "fetch")
	require.NotNil(t, obj)

	hits, _, _, latN, latSumMs := f.PeerFetchStats()
	if hits != 1 || latN != 1 {
		t.Fatalf("hits=%d latN=%d, want 1,1", hits, latN)
	}
	if latSumMs <= 0 {
		t.Fatalf("latSumMs=%d, want >0 (latency must be measured around the RPC)", latSumMs)
	}
}

// TestPeerFetcher_BinaryRoundTrip_TimeFields pins that the binary codec
// round-trips the time.Duration and time.Time fields that ADR-0015 flags
// as a risk (the time.Time zero-value edge case). The storage codec's own
// tests cover the codec in isolation; this test pins the wire-format
// contract that peer-fetch depends on.
func TestPeerFetcher_BinaryRoundTrip_TimeFields(t *testing.T) {
	t.Parallel()
	key := api.KeyFromPrimary(9)
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

	srv := httptest.NewServer(NewPeerFetchHandler(&stubStore{objects: map[api.Key]*api.Object{
		key: obj,
	}}, 0))
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

// TestPeerFetcher_MissIncrementsCounter pins that a 404 response
// increments the misses counter. The pre-#187 code had a dead increment
// (the !fetchResp.Hit branch was unreachable because the handler always
// sent 404 for misses); the binary cutover moved the increment into the
// 404 branch. This test prevents regressions of that fix.
func TestPeerFetcher_MissIncrementsCounter(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	f := NewPeerFetcher(nil, nil, 0)
	obj, err := f.Fetch(context.Background(),
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: api.KeyFromPrimary(1)})
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
		api.PeerFetchRequest{Key: api.KeyFromPrimary(1), Hops: MaxHops})
	require.NoError(t, err, "hop limit should return nil,nil")
	require.Nil(t, obj)
}

func TestPeerFetcher_OversizedResponseReturnsError(t *testing.T) {
	t.Parallel()
	validResp := storage.EncodeObject(&api.Object{
		Key:        api.KeyFromPrimary(1),
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
		api.PeerFetchRequest{Key: api.KeyFromPrimary(1)})
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
		_, _ = w.Write(storage.EncodeObject(&api.Object{Key: api.KeyFromPrimary(1), StatusCode: 200, Body: []byte("x")}))
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
				api.PeerFetchRequest{Key: api.KeyFromPrimary(1)})
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
		_, _ = w.Write(storage.EncodeObject(&api.Object{Key: api.KeyFromPrimary(1)}))
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
				api.PeerFetchRequest{Key: api.KeyFromPrimary(1)})
		}()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err := f.Fetch(ctx,
		api.PeerInfo{AdminAddr: srv.Listener.Addr().String()},
		api.PeerFetchRequest{Key: api.KeyFromPrimary(2)})
	require.Error(t, err)

	wg.Wait()
}

// BenchmarkPeerFetchHandler_ServeHTTP measures the handler encode path:
// store.Get → storage.EncodeObject → ResponseWriter.Write. This is the
// hot path that issue #187 targeted. The body is 4 KiB with 10 headers,
// matching the PR's throwaway benchmark setup so results are comparable.
func BenchmarkPeerFetchHandler_ServeHTTP(b *testing.B) {
	key := api.KeyFromPrimary(1)
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

	reqBody, _ := json.Marshal(api.PeerFetchRequest{Key: key})
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		rr := httptest.NewRecorder()
		r, _ := http.NewRequestWithContext(context.Background(),
			http.MethodPost, PeerFetchPath, bytes.NewReader(reqBody))
		r.Header.Set(header.ContentType, "application/json")
		h.ServeHTTP(rr, r)
		if rr.Code != 200 {
			b.Fatalf("status=%d", rr.Code)
		}
	}
}

// BenchmarkPeerFetcher_Fetch measures the full client→server round-trip:
// HTTP request → handler encode → HTTP transport → client read →
// storage.DecodeObject. This is the end-to-end path that the binary
// codec replaces the JSON tower on.
func BenchmarkPeerFetcher_Fetch(b *testing.B) {
	key := api.KeyFromPrimary(1)
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
