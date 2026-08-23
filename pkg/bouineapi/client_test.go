package bouineapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/internal/testutil/fasthttptest"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func writeJSON(ctx *fasthttp.RequestCtx, v any) {
	ctx.Response.Header.Set(header.ContentType, "application/json")
	data, _ := json.Marshal(v)
	_, _ = ctx.Write(data)
}

func TestClient_Healthz(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, api.HealthStatus{Status: "ok"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Healthz(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ok", got.Status)
}

func TestClient_Version(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, api.VersionInfo{Version: "1.0.0", Commit: "abc", Date: "today"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", got.Version)
	assert.Equal(t, "abc", got.Commit)
}

func TestClient_Purge(t *testing.T) {
	t.Parallel()
	var receivedURL string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		var body struct {
			URL string `json:"url"`
		}
		_ = json.Unmarshal(ctx.PostBody(), &body)
		receivedURL = body.URL
		writeJSON(ctx, PurgeResult{Status: "purged"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Purge(context.Background(), "https://example.com/page")
	require.NoError(t, err)
	assert.Equal(t, "purged", got.Status)
	assert.Equal(t, "https://example.com/page", receivedURL)
}

func TestClient_Ban(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, BanResult{Status: "banned", Count: 5})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Ban(context.Background(), api.BanExpr{HostRegex: "example.com"})
	require.NoError(t, err)
	assert.Equal(t, "banned", got.Status)
	assert.Equal(t, 5, got.Count)
}

func TestClient_Refresh(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, RefreshResult{Status: "refreshed"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Refresh(context.Background(), "https://example.com/page")
	require.NoError(t, err)
	assert.Equal(t, "refreshed", got.Status)
}

func TestClient_WithToken(t *testing.T) {
	t.Parallel()
	var gotAuth string
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		gotAuth = string(ctx.Request.Header.Peek(header.Authorization))
		writeJSON(ctx, api.HealthStatus{Status: "ok"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr).WithToken("secret")
	_, err := c.Healthz(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer secret", gotAuth)
}

func TestClient_ErrorResponse(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("forbidden", fasthttp.StatusForbidden)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "403")
}

func TestClient_ErrorBodyCapped(t *testing.T) {
	t.Parallel()
	big := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusBadGateway)
		_, _ = ctx.Write(big)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
	if want := maxErrorBody + 512; len(err.Error()) > want {
		t.Errorf("error string length = %d, want <= %d (body must be capped)", len(err.Error()), want)
	}
	assert.Contains(t, err.Error(), "truncated")
}

func TestClient_ErrorBodySanitized(t *testing.T) {
	t.Parallel()
	payload := "line1\nfake-log-line INJECTED\r\n\x00\x1b[31m"
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.SetStatusCode(fasthttp.StatusBadGateway)
		_, _ = ctx.Write([]byte(payload))
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
	assert.False(t, strings.Contains(err.Error(), "\n"))
	assert.False(t, strings.Contains(err.Error(), "\r"))
	assert.False(t, strings.Contains(err.Error(), "\x00"))
	assert.False(t, strings.Contains(err.Error(), "\x1b"))
}

func TestClient_SuccessBodyNotCapped(t *testing.T) {
	t.Parallel()
	var peers []api.PeerInfo
	for range 50 {
		peers = append(peers, api.PeerInfo{
			Name:      strings.Repeat("n", 40),
			Addr:      strings.Repeat("a", 40),
			AdminAddr: strings.Repeat("d", 40),
			DataAddr:  strings.Repeat("e", 40),
			Version:   "v1.2.3",
			Weight:    1.0,
			JoinedAt:  time.Now(),
		})
	}
	body, err := json.Marshal(peers)
	require.NoError(t, err)
	if len(body) < 4096 {
		t.Fatalf("test body too small: %d bytes (need >4096)", len(body))
	}

	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/json")
		_, _ = ctx.Write(body)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Peers(context.Background())
	require.NoError(t, err, "Peers failed on large success body")
	assert.Len(t, got, 50)
}

func TestClient_DefaultTimeout(t *testing.T) {
	t.Parallel()
	c := New("http://127.0.0.1:0")
	require.NotNil(t, c.HTTPClient)
	if c.HTTPClient.ReadTimeout <= 0 {
		t.Errorf("default HTTPClient.ReadTimeout = %v, want > 0", c.HTTPClient.ReadTimeout)
	}
}

func TestClient_DefaultTimeoutEnforced(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn
			time.Sleep(60 * time.Second)
			_ = conn.Close()
		}
	}()

	c := New("http://" + ln.Addr().String())
	start := time.Now()
	_, err = c.Healthz(context.Background())
	elapsed := time.Since(start)
	require.Error(t, err)
	if elapsed >= 30*time.Second {
		t.Errorf("call took %v, expected to return near the default timeout (~10s)", elapsed)
	}
}

func TestClient_HTTPClientOverrideWins(t *testing.T) {
	t.Parallel()
	override := &fasthttp.Client{}
	c := New("http://127.0.0.1:0")
	c.HTTPClient = override
	assert.Equal(t, override, c.httpClient())
}

func TestClient_HTTPClientFallback(t *testing.T) {
	t.Parallel()
	c := &Client{BaseURL: "http://127.0.0.1:0"}
	require.NotNil(t, c.httpClient())
}

func TestClient_Readyz(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, api.HealthStatus{Status: "ready"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Readyz(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ready", got.Status)
}

func TestClient_Stats(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, api.Stats{HotEntries: 7, WarmEntries: 42})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.Stats(context.Background())
	require.NoError(t, err)
	assert.Equal(t, int64(7), got.HotEntries)
	assert.Equal(t, int64(42), got.WarmEntries)
}

func TestClient_BatchPurge(t *testing.T) {
	t.Parallel()
	var receivedCount int
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		var body struct {
			URLs []string `json:"urls"`
		}
		_ = json.Unmarshal(ctx.PostBody(), &body)
		receivedCount = len(body.URLs)
		writeJSON(ctx, BatchPurgeResult{Status: "ok", Count: len(body.URLs)})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	got, err := c.BatchPurge(context.Background(), []string{"https://a.com", "https://b.com"})
	require.NoError(t, err)
	assert.Equal(t, 2, got.Count)
	assert.Equal(t, 2, receivedCount)
}

func TestClient_AuthCheck(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		writeJSON(ctx, map[string]string{"status": "ok"})
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	require.NoError(t, c.AuthCheck(context.Background()))
}

func TestClient_Peers_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("internal", fasthttp.StatusInternalServerError)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Peers(context.Background())
	require.Error(t, err)
}

func TestClient_Version_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusBadRequest)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Version(context.Background())
	require.Error(t, err)
}

func TestClient_Purge_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusBadRequest)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Purge(context.Background(), "https://x.com")
	require.Error(t, err)
}

func TestClient_Ban_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusBadRequest)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Ban(context.Background(), api.BanExpr{HostRegex: "x.com"})
	require.Error(t, err)
}

func TestClient_Refresh_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusBadRequest)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Refresh(context.Background(), "https://x.com")
	require.Error(t, err)
}

func TestClient_Readyz_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusServiceUnavailable)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Readyz(context.Background())
	require.Error(t, err)
}

func TestClient_Stats_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusInternalServerError)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.Stats(context.Background())
	require.Error(t, err)
}

func TestClient_BatchPurge_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("nope", fasthttp.StatusBadRequest)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	_, err := c.BatchPurge(context.Background(), []string{"https://a.com"})
	require.Error(t, err)
}

func TestClient_AuthCheck_Error(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Error("forbidden", fasthttp.StatusForbidden)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	require.Error(t, c.AuthCheck(context.Background()))
}

func TestClient_Get_InvalidURL(t *testing.T) {
	t.Parallel()
	c := New("http://[::1]:named") // invalid URL
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
}

func TestClient_Post_InvalidURL(t *testing.T) {
	t.Parallel()
	c := New("http://[::1]:named") // invalid URL
	_, err := c.Purge(context.Background(), "https://x.com")
	require.Error(t, err)
}

func TestClient_Post_MarshalError(t *testing.T) {
	t.Parallel()
	c := &Client{BaseURL: "http://127.0.0.1:0"}
	err := c.post(context.Background(), "/v1/purge", make(chan int), nil)
	require.Error(t, err)
}

func TestClient_EmptyResponseBody(t *testing.T) {
	t.Parallel()
	srv := fasthttptest.NewServer(t, func(ctx *fasthttp.RequestCtx) {
		ctx.Response.Header.Set(header.ContentType, "application/json")
		ctx.SetStatusCode(fasthttp.StatusNoContent)
	})
	defer srv.Close()

	c := New("http://" + srv.Addr)
	require.NoError(t, c.AuthCheck(context.Background()))
}
