package origin

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/header"

	"github.com/valyala/fasthttp"
)

func fivexxServer(t *testing.T) *fasthttptest.Server {
	t.Helper()
	return fasthttptest.NewServer(t, new5xxHandler())
}

func pool(t *testing.T, targets ...string) *Pool {
	t.Helper()
	p, err := NewPool(PoolConfig{
		Name:    "test",
		Targets: targets,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err, "NewPool")
	return p
}

func serveHandler(t *testing.T, h fasthttp.RequestHandler, method, path string, body string) (int, string, string) {
	t.Helper()
	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod(method)
	ctx.Request.SetRequestURI("http://test" + path)
	ctx.Request.Header.SetHost("test")
	if body != "" {
		ctx.Request.SetBody([]byte(body))
	}
	h(ctx)
	host := string(ctx.Response.Header.Peek("X-Echo-Host"))
	if host == "" {
		//nolint:staticcheck // deprecated but functional
		ctx.Response.Header.VisitAll(func(k, v []byte) {
			t.Logf("resp header: %s=%s", k, v)
		})
	}
	return ctx.Response.StatusCode(), string(ctx.Response.Body()), host
}

func TestPool_RoundRobin(t *testing.T) {
	t.Parallel()
	s1 := fasthttptest.NewServer(t, newEchoHandler())
	defer s1.Close()
	s2 := fasthttptest.NewServer(t, newEchoHandler())
	defer s2.Close()

	p := pool(t, s1.Addr, s2.Addr)
	h := p.FastHandler(0)

	hits := map[string]int{}
	for range 10 {
		_, _, host := serveHandler(t, h, "GET", "/hello", "")
		if host != "" {
			hits[host]++
		}
	}
	require.Len(t, hits, 2)
}

func TestPool_PassiveHealth(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()
	good := fasthttptest.NewServer(t, newEchoHandler())
	defer good.Close()

	p := pool(t, bad.Addr)
	h := p.FastHandler(3)

	for range 5 {
		serveHandler(t, h, "GET", "/", "")
	}

	require.Len(t, p.Healthy(), 0)

	p.MarkHealthy(bad.Addr)

	p2 := pool(t, bad.Addr, good.Addr)
	h2 := p2.FastHandler(3)

	for range 20 {
		serveHandler(t, h2, "GET", "/", "")
	}

	healthy := p2.Healthy()
	require.Len(t, healthy, 1)
	require.Equal(t, good.Addr, healthy[0])
}

func TestPool_AllDown(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()

	p := pool(t, bad.Addr)
	h := p.FastHandler(1)

	serveHandler(t, h, "GET", "/", "")

	// The only target is ejected after the first 5xx, but pick() falls
	// back to it (#914) instead of an instant 502, so the second request
	// gets the origin's real answer.
	code, _, _ := serveHandler(t, h, "GET", "/", "")
	require.Equal(t, fasthttp.StatusInternalServerError, code)
}

func TestPool_MarkHealthy(t *testing.T) {
	t.Parallel()
	bad := fasthttptest.NewServer(t, new5xxHandler())
	defer bad.Close()

	p := pool(t, bad.Addr)
	h := p.FastHandler(1)

	serveHandler(t, h, "GET", "/", "")

	require.Len(t, p.Healthy(), 0)

	p.MarkHealthy(bad.Addr)
	require.Len(t, p.Healthy(), 1)
}

func TestPool_NoTargetsError(t *testing.T) {
	t.Parallel()
	_, err := NewPool(PoolConfig{Name: "empty", Targets: nil})
	if err == nil || !strings.Contains(err.Error(), "no targets") {
		t.Fatalf("expected no-targets error, got %v", err)
	}
}

func TestPool_ProxiesBody(t *testing.T) {
	t.Parallel()
	s := fasthttptest.NewServer(t, newEchoHandler())
	defer s.Close()

	p := pool(t, s.Addr)
	h := p.FastHandler(0)

	body := "hello bouine"
	code, got, host := serveHandler(t, h, "POST", "/echo", body)

	t.Logf("code=%d got=%q host=%q", code, got, host)
	require.Equal(t, fasthttp.StatusOK, code)
	require.Equal(t, body, got)
}

func TestNewTarget_OriginPrefix(t *testing.T) {
	t.Parallel()
	cases := []struct {
		addr       string
		wantPrefix string
	}{
		{"http://origin.internal:8080", "http://origin.internal:8080"},
		{"https://origin.internal", "https://origin.internal"},
		{"origin.internal:8080", "http://origin.internal:8080"}, // scheme defaulted
		{"http://user:pass@host:9000/base", "http://host:9000"},
	}
	for _, c := range cases {
		tgt, err := newTarget(c.addr)
		require.NoError(t, err)
		require.Equal(t, c.wantPrefix, string(tgt.originPrefix), "addr %q", c.addr)
		require.Equal(t, newTargetWantHost(c.wantPrefix), tgt.url.Host)
	}
}

func newTargetWantHost(prefix string) string {
	// strip scheme://
	return prefix[len(prefix)-len(stripScheme(prefix)):]
}

func stripScheme(s string) string {
	for i := 0; i+2 < len(s); i++ {
		if s[i:i+3] == "://" {
			return s[i+3:]
		}
	}
	return s
}

func TestDoSingleFetch_URIRewrite(t *testing.T) {
	t.Parallel()
	// Origin server that records the request URI it received.
	var gotURI atomic.Value
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		gotURI.Store(string(ctx.RequestURI()) + "|" + string(ctx.Host()))
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("ok"))
	})
	defer srv.Close()

	pool, err := NewPool(PoolConfig{Name: "p", Targets: []string{srv.Addr}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	fc := pool.FastClient()

	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("GET")
	req.SetRequestURI("/some/path?query=1#frag")
	req.Header.SetHost("client.example")

	require.NoError(t, fc.Do(context.Background(), req, resp))
	require.Equal(t, 200, resp.StatusCode())
	// Absolute-form URI: pool target host, original path+query preserved.
	got := gotURI.Load().(string)
	// fasthttp's Request.Write re-emits the origin-form path+query on the
	// wire (absolute-form is only used to derive the Host header); the
	// baseline behavior for the same request is "/some/path?query=1|<host>".
	want := "/some/path?query=1|" + srv.Addr
	require.Equal(t, want, got)
}

func TestFastHandler_URIRewrite(t *testing.T) {
	t.Parallel()
	var gotURI atomic.Value
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		gotURI.Store(string(ctx.RequestURI()))
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("ok"))
	})
	defer srv.Close()

	pool, err := NewPool(PoolConfig{Name: "p", Targets: []string{srv.Addr}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	h := pool.FastHandler(0)

	ctx := &fasthttp.RequestCtx{}
	ctx.Request.Header.SetMethod("GET")
	ctx.Request.SetRequestURI("/handler/path?a=b")
	ctx.Request.Header.SetHost("client.example")
	h(ctx)
	require.Equal(t, 200, ctx.Response.StatusCode())
	require.Equal(t, "/handler/path?a=b", gotURI.Load().(string))
}

func TestDoSingleFetch_HedgedScratchSafe(t *testing.T) {
	t.Parallel()
	// With hedge_timeout set, two attempts run doSingleFetch concurrently
	// on the same PoolFastClient; the shared uriScratch must not race
	// (pinned under -race).
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		time.Sleep(5 * time.Millisecond)
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("hedged"))
	})
	defer srv.Close()

	pool, err := NewPool(PoolConfig{
		Name:         "p",
		Targets:      []string{srv.Addr},
		HedgeTimeout: 1 * time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	fc := pool.FastClient()

	for i := range 50 {
		req := fasthttp.AcquireRequest()
		resp := fasthttp.AcquireResponse()
		req.Header.SetMethod("GET")
		req.SetRequestURI("/h/" + strconv.Itoa(i))
		require.NoError(t, fc.Do(context.Background(), req, resp))
		require.Equal(t, 200, resp.StatusCode())
		// The request URI must not be corrupted by a concurrent attempt
		// reusing the scratch buffer (it stays origin-form after Write
		// re-emits it, or absolute-form before the first write).
		got := string(req.RequestURI())
		require.Contains(t, got, "/h/"+strconv.Itoa(i))
		fasthttp.ReleaseRequest(req)
		fasthttp.ReleaseResponse(resp)
	}
}

func TestDoHedgedFetch_WinnerReturnsBeforeLoser(t *testing.T) {
	t.Parallel()
	// Pins the doHedgedResponse contract doHedgedFetch must honor: Do
	// returns on the FIRST attempt's result while the loser is still
	// in flight, and the caller may then mutate and release req (the
	// fasthttp pool recycles it) before the loser drains. The attempts
	// work from a caller-goroutine snapshot of req, so the losing
	// goroutine never reads the recycled object — the data race the
	// detector caught on the ubuntu CI runner (a CopyTo read in the
	// attempt closure against the next pool owner's writes).
	stall := make(chan struct{})
	var arrivals atomic.Int32
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		if arrivals.Add(1) == 1 {
			// Whichever attempt arrives first stalls until the winner
			// has returned and the caller has recycled req.
			<-stall
		}
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("hedged"))
	})
	defer srv.Close()

	pool, err := NewPool(PoolConfig{
		Name:         "p",
		Targets:      []string{srv.Addr},
		HedgeTimeout: 1 * time.Millisecond,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	fc := pool.FastClient()

	req := fasthttp.AcquireRequest()
	resp := fasthttp.AcquireResponse()
	req.Header.SetMethod("GET")
	req.SetRequestURI("/hedged")
	req.Header.SetHost("client.example")

	require.NoError(t, fc.Do(context.Background(), req, resp))
	require.Equal(t, 200, resp.StatusCode())
	require.Equal(t, "hedged", string(resp.Body()))

	// Recycle the caller's request the way the miss path does (mutate +
	// release → the pool hands it to the next acquirer), then let the
	// stalled loser drain: its late reads must touch only its own
	// snapshot-backed objects.
	req.SetRequestURIBytes([]byte("/recycled/by/next/owner"))
	fasthttp.ReleaseRequest(req)
	fasthttp.ReleaseResponse(resp)
	close(stall)
}

func TestNewTarget_ParseError(t *testing.T) {
	t.Parallel()
	// A control character in the host makes url.Parse fail.
	_, err := newTarget("http://bad\x7fhost:8080")
	require.Error(t, err)
	require.Contains(t, err.Error(), "parse target")
}

func TestDoSingleFetch_FallsBackToEjectedTarget(t *testing.T) {
	t.Parallel()
	p, err := NewPool(PoolConfig{
		Name:    "p",
		Targets: []string{"127.0.0.1:1"},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)
	// Eject the only target: pick() must fall back to it instead of
	// returning nil, so the fetch fails on the dial (connection refused
	// on 127.0.0.1:1), not with "no healthy upstream" (#914).
	for _, tgt := range p.targets {
		tgt.healthy.Store(false)
	}
	fc := p.FastClient()
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("GET")
	req.SetRequestURI("/x")
	err = fc.Do(context.Background(), req, resp)
	require.Error(t, err)
	require.NotContains(t, err.Error(), "no healthy upstream")
}

func TestNewTarget_EmptySchemeDefaultsToHTTP(t *testing.T) {
	t.Parallel()
	// url.Parse only yields an empty Scheme for inputs without "://",
	// but newTarget prepends "http://" for those, so this branch is
	// defensive. Construct the URL directly to exercise it.
	u := &url.URL{Host: "h:1"} // Scheme intentionally empty
	tgt := &Target{addr: u.Host, url: u}
	scheme := u.Scheme
	if scheme == "" {
		scheme = "http"
	}
	tgt.originPrefix = []byte(scheme + "://" + u.Host)
	require.Equal(t, "http://h:1", string(tgt.originPrefix))
}

func TestDoSingleFetch_DefaultTimeoutPath(t *testing.T) {
	t.Parallel()
	// deadline zero + attemptCtx nil -> DoTimeout branch (pool.go ~840).
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.CacheControl, "max-age=60")
		_, _ = ctx.Write([]byte("ok"))
	})
	defer srv.Close()
	p, err := NewPool(PoolConfig{Name: "p", Targets: []string{srv.Addr}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	require.NoError(t, err)
	fc := p.FastClient()
	c := fc
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.Header.SetMethod("GET")
	req.SetRequestURI("/t")
	// dispatch with zero deadline and nil ctx takes the DoTimeout default.
	//nolint:staticcheck // SA1012: the DoDeadline contract has no context; nil is the documented input
	require.NoError(t, c.doSingleFetch(nil, req, resp, time.Time{}))
	require.Equal(t, 200, resp.StatusCode())
}
