// Package buildinfo exposes version metadata stamped into the binary at
// link time. Values default to "dev" when the binary is built without
// the recommended -ldflags.
//
// The Makefile sets these via:
//
//	-X github.com/bouine-cache/bouine/internal/buildinfo.Version=...
//	-X github.com/bouine-cache/bouine/internal/buildinfo.Commit=...
//	-X github.com/bouine-cache/bouine/internal/buildinfo.Date=...
package buildinfo

// Stable.
var (
	// Version is the semantic version (or "dev" when not stamped).
	Version = "dev"
	// Commit is the short Git SHA at build time.
	Commit = "unknown"
	// Date is the UTC build timestamp in RFC 3339 form.
	Date = "unknown"
)
