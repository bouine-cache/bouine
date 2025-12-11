package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
)

func newTestServer(t *testing.T, ready func() bool) *Server {
	t.Helper()
	return New(Config{
		Logger:  slog.New(slog.NewJSONHandler(io.Discard, nil)),
		ReadyFn: ready,
	})
}

func get(t *testing.T, s *Server, path string) (status int, body []byte) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), "GET", path, nil)
	resp, err := s.App().Test(req)
	if err != nil {
		t.Fatalf("Test(%s): %v", path, err)
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil {
			t.Errorf("close body: %v", cerr)
		}
	}()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return resp.StatusCode, b
}

func TestHealthz(t *testing.T) {
	s := newTestServer(t, nil)

	status, body := get(t, s, "/healthz")
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	var got map[string]string
	if err := json.Unmarshal(body, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got["status"] != "ok" {
		t.Fatalf("status field = %q, want ok", got["status"])
	}
}

func TestReadyz_Ready(t *testing.T) {
	s := newTestServer(t, func() bool { return true })
	status, _ := get(t, s, "/readyz")
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
}

func TestReadyz_NotReady(t *testing.T) {
	s := newTestServer(t, func() bool { return false })
	status, _ := get(t, s, "/readyz")
	if status != 503 {
		t.Fatalf("status = %d, want 503", status)
	}
}

func TestVersion(t *testing.T) {
	s := newTestServer(t, nil)
	status, body := get(t, s, "/version")
	if status != 200 {
		t.Fatalf("status = %d, want 200", status)
	}
	if !bytes.Contains(body, []byte("version")) {
		t.Fatalf("missing version field: %s", body)
	}
}
