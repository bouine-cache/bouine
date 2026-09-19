package server

import (
	"sync"
	"testing"
	"time"

	"github.com/bouine-cache/bouine/pkg/api"
)

// benchStatusLine is the fixture's shared status line: a single
// package-level backing slice reused by every pooled response, so the
// fixture introduces no per-op allocations.
var benchStatusLine = []byte("HTTP/1.1 200 OK\r\n\r\n")

// benchRespPool mirrors cache.FastPathHandler's pooled-response
// lifecycle (fastPathRespPool): TryHit acquires and Release recycles,
// so the delegated cycle is allocation-free and the benchmark
// isolates the wrapper's route-resolution cost.
var benchRespPool = sync.Pool{
	New: func() any {
		return &api.FastPathResponse{}
	},
}

// pooledBenchFP is a pool-backed FastPathHandler fixture.
type pooledBenchFP struct {
	pool string
}

func (f *pooledBenchFP) TryHit(_ *api.RawRequest, _ time.Time) (*api.FastPathResponse, bool) {
	resp := benchRespPool.Get().(*api.FastPathResponse)
	resp.CacheResult = "HIT"
	resp.Pool = f.pool
	resp.StatusCode = 200
	resp.BuffersArr[0] = benchStatusLine
	resp.Buffers = resp.BuffersArr[:1]
	return resp, true
}

func (*pooledBenchFP) Release(resp *api.FastPathResponse) {
	resp.Buffers = nil
	resp.Pool = ""
	resp.CacheResult = ""
	resp.StatusCode = 0
	resp.BuffersArr = [3][]byte{}
	benchRespPool.Put(resp)
}

// BenchmarkGate_RoutedFastPath_Hit gates the routed fast-path
// wrapper's hit path (issue #696): route resolution (host strip +
// first-match-wins walk) plus TryHit delegation. The wrapper runs
// before every production fast-path hit, so it must stay zero-alloc —
// the route table is walked with index loops over pre-built strings,
// no maps, no per-request state, and the pooled fixture makes the
// delegated TryHit/Release cycle allocation-free.
func BenchmarkGate_RoutedFastPath_Hit(b *testing.B) {
	rt := NewRouter(RouterConfig{})
	a := &pooledBenchFP{pool: "api-pool"}
	miss := &pooledBenchFP{pool: "miss"}
	rt.AddRoute("", "/static/", "static", "", nil, ok200("static"), nil)
	rt.AddRoute("api.example.com", "/v1/", "api", "api-pool", nil, ok200("api"), a)
	rt.AddRoute("", "/", "root", "root-pool", nil, ok200("root"), miss)

	rfp := NewRoutedFastPath(rt, a)

	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/v1/x",
		Host:        "api.example.com:443",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		resp, ok := rfp.TryHit(req, now)
		if !ok {
			b.Fatal("TryHit returned false")
		}
		rfp.Release(resp)
	}
}

// BenchmarkGate_RoutedFastPath_Hit_TrafficClass gates the routed
// fast-path wrapper with the traffic-class classifier active
// (ADR-0047): Classify runs per hit inside TryHit, after route
// resolution. Must stay zero-alloc — patterns are lowercased once at
// compile time and matching uses length-bounded EqualFold, never
// strings.ToLower on the request Host.
func BenchmarkGate_RoutedFastPath_Hit_TrafficClass(b *testing.B) {
	tc := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "csr", Hosts: []string{"www.backmarket.fr", "www.backmarket.de", "www.example.com"}},
		{Name: "ssr", Hosts: []string{"*.svc.cluster.local", "api.example.*"}},
	})
	rt := NewRouter(RouterConfig{TrafficClassify: tc})
	a := &pooledBenchFP{pool: "api-pool"}
	miss := &pooledBenchFP{pool: "miss"}
	rt.AddRoute("", "/static/", "static", "", nil, ok200("static"), nil)
	rt.AddRoute("api.example.com", "/v1/", "api", "api-pool", nil, ok200("api"), a)
	rt.AddRoute("", "/", "root", "root-pool", nil, ok200("root"), miss)

	rfp := NewRoutedFastPath(rt, a)
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/v1/x",
		Host:        "api.example.com:443",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		resp, ok := rfp.TryHit(req, now)
		if !ok {
			b.Fatal("TryHit returned false")
		}
		if resp.TrafficClass != "ssr" {
			b.Fatalf("expected class ssr, got %q", resp.TrafficClass)
		}
		rfp.Release(resp)
	}
}

// BenchmarkGate_RoutedFastPath_Hit_TrafficClass_MixedCase is the
// allocation-hazard variant (ADR-0047 §3): a ToLower-based classifier
// would allocate only when any rune changes case, so the all-lowercase
// benchmark above would hide it. The adversarial mixed-case Host must
// classify without allocating.
func BenchmarkGate_RoutedFastPath_Hit_TrafficClass_MixedCase(b *testing.B) {
	tc := NewTrafficClassifier([]TrafficClassSpec{
		{Name: "csr", Hosts: []string{"www.backmarket.fr", "www.backmarket.de", "www.example.com"}},
		{Name: "ssr", Hosts: []string{"*.svc.cluster.local", "api.example.*"}},
	})
	rt := NewRouter(RouterConfig{TrafficClassify: tc})
	a := &pooledBenchFP{pool: "api-pool"}
	rt.AddRoute("", "/", "root", "root-pool", nil, ok200("root"), a)

	rfp := NewRoutedFastPath(rt, a)
	req := &api.RawRequest{
		Method:      "GET",
		Path:        "/v1/x",
		Host:        "aPi.ExAmPle.COM:443",
		Scheme:      "http",
		HTTPVersion: "HTTP/1.1",
	}
	now := time.Now()

	b.ResetTimer()
	b.ReportAllocs()
	for range b.N {
		resp, ok := rfp.TryHit(req, now)
		if !ok {
			b.Fatal("TryHit returned false")
		}
		if resp.TrafficClass != "ssr" {
			b.Fatalf("expected class ssr, got %q", resp.TrafficClass)
		}
		rfp.Release(resp)
	}
}
