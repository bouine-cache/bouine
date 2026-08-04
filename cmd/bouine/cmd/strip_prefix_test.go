package cmd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStripPrefixHandler_Strips(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	h := stripPrefixHandler("/api/v1", origin)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/users/123", nil))
	assert.Equal(t, "/users/123", gotPath)
}

func TestStripPrefixHandler_PreservesLeadingSlash(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	h := stripPrefixHandler("/api/v1/", origin)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/api/v1/", nil))
	assert.Equal(t, "/", gotPath)
}

func TestStripPrefixHandler_NoMatchPassthrough(t *testing.T) {
	t.Parallel()
	var gotPath string
	origin := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
	})
	h := stripPrefixHandler("/api/v1", origin)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", "/other/path", nil))
	assert.Equal(t, "/other/path", gotPath)
}

func TestStripPrefixHandler_OriginalRequestUnchanged(t *testing.T) {
	t.Parallel()
	origin := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {})
	h := stripPrefixHandler("/api", origin)

	req := httptest.NewRequest("GET", "/api/foo", nil)
	originalPath := req.URL.Path

	h.ServeHTTP(httptest.NewRecorder(), req)
	assert.Equal(t, originalPath, req.URL.Path)
}
