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
	rt.AddRoute("", "/a", "", nil, ok200("first"))
	rt.AddRoute("", "/a", "", nil, ok200("second"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/a/b", nil))
	if rr.Body.String() != "first" {
		t.Fatalf("expected first match, got %q", rr.Body.String())
	}
}

func TestRouter_HostMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("api.example.com", "", "", nil, ok200("api"))
	rt.AddRoute("", "", "", nil, ok200("default"))

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
	rt.AddRoute("api.example.com", "", "", nil, ok200("api"))

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
	rt.AddRoute("only.com", "", "", nil, ok200("x"))

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
	rt.AddRoute("", "/api/", "", nil, ok200("api"))
	rt.AddRoute("", "/", "", nil, ok200("root"))

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
	rt.AddRoute("", "", "", nil, ok200("all"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/anything", nil))
	if rr.Body.String() != "all" {
		t.Fatalf("expected all, got %q", rr.Body.String())
	}
}

func TestRouter_MethodMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "", []string{"GET", "HEAD"}, ok200("read"))
	rt.AddRoute("", "/api/", "", []string{"POST", "PUT"}, ok200("write"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/api/v1/foo", nil))
	if rr.Body.String() != "read" {
		t.Fatalf("GET expected read, got %q", rr.Body.String())
	}

	rr2 := httptest.NewRecorder()
	rt.ServeHTTP(rr2, httptest.NewRequest("POST", "/api/v1/foo", nil))
	if rr2.Body.String() != "write" {
		t.Fatalf("POST expected write, got %q", rr2.Body.String())
	}
}

func TestRouter_MethodNoMatch(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/api/", "", []string{"GET"}, ok200("get-only"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("DELETE", "/api/v1/foo", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("DELETE should not match GET-only route, got %d", rr.Code)
	}
}

func TestRouter_MethodFallthrough(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/", "", []string{"GET", "HEAD"}, ok200("cached"))
	rt.AddRoute("", "/", "", nil, ok200("passthrough"))

	rr := httptest.NewRecorder()
	rt.ServeHTTP(rr, httptest.NewRequest("GET", "/page", nil))
	if rr.Body.String() != "cached" {
		t.Fatalf("GET expected cached, got %q", rr.Body.String())
	}

	rr2 := httptest.NewRecorder()
	rt.ServeHTTP(rr2, httptest.NewRequest("POST", "/page", nil))
	if rr2.Body.String() != "passthrough" {
		t.Fatalf("POST should fall through to catch-all, got %q", rr2.Body.String())
	}
}

func TestRouter_NilMethodsMatchAll(t *testing.T) {
	t.Parallel()
	rt := NewRouter(RouterConfig{})
	rt.AddRoute("", "/", "", nil, ok200("any"))

	for _, m := range []string{"GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"} {
		rr := httptest.NewRecorder()
		rt.ServeHTTP(rr, httptest.NewRequest(m, "/x", nil))
		if rr.Body.String() != "any" {
			t.Errorf("%s expected any, got %q", m, rr.Body.String())
		}
	}
}
