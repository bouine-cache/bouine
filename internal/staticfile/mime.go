package staticfile

import "mime"

// bundledMIMEs is a curated set of web MIME types registered at package
// load time. This ensures consistent Content-Type across all nodes
// regardless of host OS MIME database differences (/etc/mime.types on
// Linux, registry on Windows). Unknown extensions fall back to
// application/octet-stream.
var bundledMIMEs = map[string]string{
	".html":  "text/html; charset=utf-8",
	".htm":   "text/html; charset=utf-8",
	".css":   "text/css; charset=utf-8",
	".js":    "application/javascript",
	".mjs":   "application/javascript",
	".json":  "application/json",
	".xml":   "application/xml",
	".png":   "image/png",
	".jpg":   "image/jpeg",
	".jpeg":  "image/jpeg",
	".gif":   "image/gif",
	".svg":   "image/svg+xml",
	".ico":   "image/x-icon",
	".webp":  "image/webp",
	".woff":  "font/woff",
	".woff2": "font/woff2",
	".ttf":   "font/ttf",
	".otf":   "font/otf",
	".wasm":  "application/wasm",
	".txt":   "text/plain; charset=utf-8",
	".pdf":   "application/pdf",
	".map":   "application/json",
}

// init registers bundled MIME types with the stdlib mime package so that
// mime.TypeByExtension returns consistent values across all platforms.
// This only calls mime.AddExtensionType (registering encoders with the
// stdlib registry) — no I/O or goroutines, per AGENTS.md §4.
func init() {
	for ext, typ := range bundledMIMEs {
		_ = mime.AddExtensionType(ext, typ)
	}
}
