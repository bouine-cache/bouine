// Package staticfile serves files from a local directory. It is designed
// to be wired into the L1 router as an alternative to an upstream pool,
// and optionally wrapped by the L3 cache handler as its upstream.
//
// Layer: L4 (origin). A local file source is an origin — it replaces the
// upstream pool. The cache handler (L3) treats the static handler as its
// "upstream", exactly like a remote HTTP origin.
//
// The handler does NOT support directory listing. Path traversal is
// prevented by path.Clean + filepath.Rel containment check. Symlinks in
// the root are resolved once at startup; per-request symlink evaluation
// is not performed.
package staticfile
