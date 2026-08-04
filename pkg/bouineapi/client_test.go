package bouineapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestClient_Healthz(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(api.HealthStatus{Status: "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Healthz(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "ok", got.Status)
}

func TestClient_Version(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(api.VersionInfo{Version: "1.0.0", Commit: "abc", Date: "today"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Version(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "1.0.0", got.Version)
}

func TestClient_Purge(t *testing.T) {
	t.Parallel()
	var receivedURL string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			URL string `json:"url"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		receivedURL = body.URL
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(PurgeResult{Status: "purged"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Purge(context.Background(), "https://example.com/foo")
	require.NoError(t, err)
	assert.Equal(t, "purged", got.Status)
	assert.Equal(t, "https://example.com/foo", receivedURL)
}

func TestClient_Ban(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(BanResult{Status: "banned", Count: 5})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Ban(context.Background(), api.BanExpr{HostRegex: "example.com"})
	require.NoError(t, err)
	assert.Equal(t, 5, got.Count)
}

func TestClient_Refresh(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(RefreshResult{Status: "refreshed"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Refresh(context.Background(), "https://example.com/bar")
	require.NoError(t, err)
	assert.Equal(t, "refreshed", got.Status)
}

func TestClient_WithToken(t *testing.T) {
	t.Parallel()
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get(header.Authorization)
		w.Header().Set(header.ContentType, "application/json")
		_ = json.NewEncoder(w).Encode(api.HealthStatus{Status: "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL).WithToken("secret")
	_, err := c.Healthz(context.Background())
	require.NoError(t, err)
	assert.Equal(t, "Bearer secret", gotAuth)
}

func TestClient_ErrorResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	c := New(srv.URL)
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
}

func TestClient_ErrorBodyCapped(t *testing.T) {
	t.Parallel()
	// A misconfigured proxy returning a full HTML 502 must not load
	// megabytes into the error string.
	big := bytes.Repeat([]byte("x"), 1<<20) // 1 MiB
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	c := New(srv.URL)
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
	// Prefix is ~60 bytes; truncation marker is ~14. 512 bytes of slack
	// is plenty while still catching a regression that doubles the cap.
	if want := maxErrorBody + 512; len(err.Error()) > want {
		t.Errorf("error string length = %d, want <= %d (body must be capped)", len(err.Error()), want)
	}
	assert.Contains(t, err.Error(), "truncated")
}

func TestClient_ErrorBodySanitized(t *testing.T) {
	t.Parallel()
	// Log-injection: a body containing newlines/control chars must not
	// be embedded raw into the error string — otherwise a malicious or
	// buggy upstream can forge fake log lines in the caller's log output.
	payload := "line1\nfake-log-line INJECTED\r\n\x00\x1b[31m"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(payload))
	}))
	defer srv.Close()

	c := New(srv.URL)
	_, err := c.Healthz(context.Background())
	require.Error(t, err)
	// The injection vector is the raw newline/carriage-return that
	// lets "fake-log-line INJECTED" look like a separate log entry.
	// Sanitization must strip those control chars; the literal text
	// can remain on a single line.
	assert.False(t, strings.Contains(err.Error(), "\n"))
	assert.False(t, strings.Contains(err.Error(), "\r"))
	assert.False(t, strings.Contains(err.Error(), "\x00"))
	assert.False(t, strings.Contains(err.Error(), "\x1b"))
}

func TestClient_SuccessBodyNotCapped(t *testing.T) {
	t.Parallel()
	// Large successful responses (e.g. a big Peers list) must still
	// unmarshal correctly — the cap only applies to error bodies.
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

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set(header.ContentType, "application/json")
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Peers(context.Background())
	require.NoError(t, err, "Peers failed on large success body")
	assert.Len(t, got, 50)
}

func TestClient_DefaultTimeout(t *testing.T) {
	t.Parallel()
	// New must construct a client with a non-zero default timeout so a
	// hung admin endpoint cannot hang the SDK caller forever.
	c := New("http://127.0.0.1:0")
	require.NotNil(t, c.HTTPClient)
	if c.HTTPClient.Timeout <= 0 {
		t.Errorf("default HTTPClient.Timeout = %v, want > 0", c.HTTPClient.Timeout)
	}
}

func TestClient_DefaultTimeoutEnforced(t *testing.T) {
	t.Parallel()
	// A server that hangs past the default timeout must cause the call
	// to return an error (context-deadline / client-timeout), not block
	// forever. Use a listener on a free port that accepts but never
	// responds.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			// Hold the connection open without responding. The kernel
			// TCP buffer will fill but we never read; the client's
			// Timeout will fire first.
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
	// If the caller sets HTTPClient, it must override the default —
	// including a zero-timeout client (the caller owns that decision).
	override := &http.Client{Timeout: 0}
	c := New("http://127.0.0.1:0")
	c.HTTPClient = override
	assert.Equal(t, override, c.httpClient())
}
