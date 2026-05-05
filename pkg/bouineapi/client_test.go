package bouineapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/thylong/bouine/pkg/api"
)

func TestClient_Healthz(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.HealthStatus{Status: "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Healthz(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "ok" {
		t.Errorf("status = %q, want ok", got.Status)
	}
}

func TestClient_Version(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.VersionInfo{Version: "1.0.0", Commit: "abc", Date: "today"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Version(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != "1.0.0" {
		t.Errorf("version = %q, want 1.0.0", got.Version)
	}
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
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(PurgeResult{Status: "purged"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Purge(context.Background(), "https://example.com/foo")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "purged" {
		t.Errorf("status = %q, want purged", got.Status)
	}
	if receivedURL != "https://example.com/foo" {
		t.Errorf("url = %q, want https://example.com/foo", receivedURL)
	}
}

func TestClient_Ban(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(BanResult{Status: "banned", Count: 5})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Ban(context.Background(), api.BanExpr{HostRegex: "example.com"})
	if err != nil {
		t.Fatal(err)
	}
	if got.Count != 5 {
		t.Errorf("count = %d, want 5", got.Count)
	}
}

func TestClient_Refresh(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RefreshResult{Status: "refreshed"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Refresh(context.Background(), "https://example.com/bar")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "refreshed" {
		t.Errorf("status = %q, want refreshed", got.Status)
	}
}

func TestClient_Reload(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(ReloadResult{Status: "reload-requested"})
	}))
	defer srv.Close()

	c := New(srv.URL)
	got, err := c.Reload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "reload-requested" {
		t.Errorf("status = %q, want reload-requested", got.Status)
	}
}

func TestClient_WithToken(t *testing.T) {
	t.Parallel()
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(api.HealthStatus{Status: "ok"})
	}))
	defer srv.Close()

	c := New(srv.URL).WithToken("secret")
	_, err := c.Healthz(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer secret" {
		t.Errorf("auth = %q, want Bearer secret", gotAuth)
	}
}

func TestClient_ErrorResponse(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "forbidden", http.StatusForbidden)
	}))
	defer srv.Close()

	c := New(srv.URL)
	_, err := c.Healthz(context.Background())
	if err == nil {
		t.Fatal("expected error for 403 response")
	}
}
