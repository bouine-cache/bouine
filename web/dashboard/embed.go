// Package dashboard embeds static assets served by the operator dashboard.
package dashboard

import (
	"embed"
	"io/fs"
	"path"
	"strings"

	"github.com/valyala/fasthttp"
)

//go:embed favicon logo.png logo-white.png
var staticFS embed.FS

// FaviconHandler returns a fasthttp.RequestHandler that serves all
// favicon and logo assets from the embedded filesystem.
// Mount it at /favicon/ on the admin server.
func FaviconHandler() fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		p := string(ctx.Path())
		// Strip the /favicon/ prefix to get the asset path within the
		// embedded FS. For top-level assets (logo.png, logo-white.png)
		// the path is used as-is.
		relPath := strings.TrimPrefix(p, "/favicon/")
		if relPath == "" || relPath == "/" {
			ctx.Error("not found", fasthttp.StatusNotFound)
			return
		}
		// Clean the path to prevent traversal.
		relPath = path.Clean("/" + relPath)[1:]
		data, err := staticFS.ReadFile(relPath)
		if err != nil {
			ctx.Error("not found", fasthttp.StatusNotFound)
			return
		}
		ctx.SetContentType(contentTypeForPath(relPath))
		ctx.SetBody(data)
	}
}

// contentTypeForPath returns a MIME type based on the file extension.
func contentTypeForPath(p string) string {
	ext := strings.ToLower(path.Ext(p))
	switch ext {
	case ".png":
		return "image/png"
	case ".ico":
		return "image/x-icon"
	case ".webmanifest":
		return "application/manifest+json"
	case ".svg":
		return "image/svg+xml"
	default:
		return "application/octet-stream"
	}
}

// Ensure fs is used (embed.FS implements fs.FS).
var _ fs.FS = staticFS
