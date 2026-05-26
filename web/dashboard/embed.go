// Package dashboard embeds static assets served by the operator dashboard.
package dashboard

import (
	"embed"
	"net/http"
)

//go:embed favicon
var faviconFS embed.FS

// FaviconHandler returns an http.Handler that serves all favicon assets.
// Mount it at /favicon/ on the admin mux.
func FaviconHandler() http.Handler {
	return http.FileServer(http.FS(faviconFS))
}
