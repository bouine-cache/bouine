package origin

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/valyala/fasthttp"
)

func echoServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(newEchoHandler())
}

func fivexxServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(new5xxHandler())
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
	s1 := httptest.NewServer(newEchoHandler())
	defer s1.Close()
	s2 := httptest.NewServer(newEchoHandler())
	defer s2.Close()

	p := pool(t, s1.Listener.Addr().String(), s2.Listener.Addr().String())
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
	bad := httptest.NewServer(new5xxHandler())
	defer bad.Close()
	good := httptest.NewServer(newEchoHandler())
	defer good.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.FastHandler(3)

	for range 5 {
		serveHandler(t, h, "GET", "/", "")
	}

	require.Len(t, p.Healthy(), 0)

	p.MarkHealthy(bad.Listener.Addr().String())

	p2 := pool(t, bad.Listener.Addr().String(), good.Listener.Addr().String())
	h2 := p2.FastHandler(3)

	for range 20 {
		serveHandler(t, h2, "GET", "/", "")
	}

	healthy := p2.Healthy()
	require.Len(t, healthy, 1)
	require.Equal(t, good.Listener.Addr().String(), healthy[0])
}

func TestPool_AllDown(t *testing.T) {
	t.Parallel()
	bad := httptest.NewServer(new5xxHandler())
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.FastHandler(1)

	serveHandler(t, h, "GET", "/", "")

	code, _, _ := serveHandler(t, h, "GET", "/", "")
	require.Equal(t, fasthttp.StatusBadGateway, code)
}

func TestPool_MarkHealthy(t *testing.T) {
	t.Parallel()
	bad := httptest.NewServer(new5xxHandler())
	defer bad.Close()

	p := pool(t, bad.Listener.Addr().String())
	h := p.FastHandler(1)

	serveHandler(t, h, "GET", "/", "")

	require.Len(t, p.Healthy(), 0)

	p.MarkHealthy(bad.Listener.Addr().String())
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
	s := httptest.NewServer(newEchoHandler())
	defer s.Close()

	p := pool(t, s.Listener.Addr().String())
	h := p.FastHandler(0)

	body := "hello bouine"
	code, got, host := serveHandler(t, h, "POST", "/echo", body)

	t.Logf("code=%d got=%q host=%q", code, got, host)
	require.Equal(t, fasthttp.StatusOK, code)
	require.Equal(t, body, got)
}
