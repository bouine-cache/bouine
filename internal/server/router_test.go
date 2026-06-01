package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func ok200(body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, body)
	})
}

func TestRouter_FirstMatchWins(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/a", "", ok200("first"))
	rt.AddRoute("", "/a", "", ok200("second"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/a/b", nil))
	if rr.Body.String() != "first" {
		t.Fatalf("expected first match, got %q", rr.Body.String())
	}
}

func TestRouter_HostMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "", "", ok200("api"))
	rt.AddRoute("", "", "", ok200("default"))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "api.example.com"
	rt.ServeHTTP(rr, req)
	if rr.Body.String() != "api" {
		t.Fatalf("expected api, got %q", rr.Body.String())
	}
}

func TestRouter_HostWithPort(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "", "", ok200("api"))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "api.example.com:443"
	rt.ServeHTTP(rr, req)
	if rr.Body.String() != "api" {
		t.Fatalf("expected api, got %q", rr.Body.String())
	}
}

func TestRouter_NoRoute(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("only.com", "", "", ok200("x"))

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/", nil)
	req.Host = "other.com"
	rt.ServeHTTP(rr, req)
	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestRouter_PathPrefix(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "", ok200("api"))
	rt.AddRoute("", "/", "", ok200("root"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/foo", nil))
	if rr.Body.String() != "api" {
		t.Fatalf("expected api, got %q", rr.Body.String())
	}

	rr2 := httptest.NewRecorder()
	rt.ServeHTTP(rr2, httptest.NewRequest("GET", "/other", nil))
	if rr2.Body.String() != "root" {
		t.Fatalf("expected root, got %q", rr2.Body.String())
	}
}

func TestRouter_CatchAll(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "", "", ok200("all"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/anything", nil))
	if rr.Body.String() != "all" {
		t.Fatalf("expected all, got %q", rr.Body.String())
	}
}
