// Package dashboard embeds static assets served by the operator dashboard.
package dashboard

import (
	"embed"
	"net/http"
)

//go:embed favicon logo.png logo-white.png
var staticFS embed.FS

// FaviconHandler returns an http.Handler that serves all favicon assets.
// Mount it at /favicon/ on the admin mux.
func FaviconHandler() http.Handler {
	return http.FileServer(http.FS(staticFS))
}
