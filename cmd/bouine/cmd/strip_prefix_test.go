package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStripPrefixHandler_Strips(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	h := stripPrefixHandler("/api/v1", origin)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/users/123", nil))
	if gotPath != "/users/123" {
		t.Errorf("expected /users/123, got %q", gotPath)
	}
}

func TestStripPrefixHandler_PreservesLeadingSlash(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	h := stripPrefixHandler("/api/v1/", origin)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/", nil))
	if gotPath != "/" {
		t.Errorf("expected /, got %q", gotPath)
	}
}

func TestStripPrefixHandler_NoMatchPassthrough(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	h := stripPrefixHandler("/api/v1", origin)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/other/path", nil))
	if gotPath != "/other/path" {
		t.Errorf("expected /other/path (no stripping), got %q", gotPath)
	}
}

func TestStripPrefixHandler_OriginalRequestUnchanged(t *testing.T) {
	t.Parallel()
	origin := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	h := stripPrefixHandler("/api", origin)

	req := httptest.NewRequest("GET", "/api/foo", nil)
	originalPath := req.URL.Path

	h.ServeHTTP(httptest.NewRecorder(), req)
	if req.URL.Path != originalPath {
		t.Errorf("original request mutated: %q, want %q", req.URL.Path, originalPath)
	}
}
