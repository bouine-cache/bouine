package staticfile

import "strings"

// bundledMIMEs is a curated set of web MIME types looked up directly by the
// handler. This ensures consistent Content-Type across all nodes regardless
// of host OS MIME database differences (/etc/mime.types on Linux, registry
// on Windows). Unknown extensions fall back to application/octet-stream.
//
// Per ADR-0017 §6, Content-Type is set from this bundled map, not the host
// OS MIME database. The handler lowercases the extension before lookup to
// preserve the case-insensitive matching that mime.TypeByExtension provided.
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

// contentTypeByExtension returns the MIME type for a file extension from the
// bundled map. The extension is lowercased to preserve case-insensitive
// matching. Returns "application/octet-stream" for unknown extensions.
func contentTypeByExtension(ext string) string {
	if ct, ok := bundledMIMEs[strings.ToLower(ext)]; ok {
		return ct
	}
	return "application/octet-stream"
}
