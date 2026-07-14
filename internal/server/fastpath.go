package server

import (
	"github.com/bouine-cache/bouine/pkg/api"
)

// This file re-exports the fast-path types from pkg/api as type aliases
// so that code within internal/server (L1) can reference them without
// changing imports. The canonical definitions live in pkg/api/fastpath.go
// because L3 (internal/cache) must implement the FastPathHandler interface
// and cannot import L1 (internal/server) per the depguard layer rules.

// RawRequest is a parsed HTTP/1.1 request. See pkg/api/fastpath.go.
//
// Unstable.
type RawRequest = api.RawRequest

// RawHeader is a single parsed header key-value pair. See pkg/api/fastpath.go.
type RawHeader = api.RawHeader

// MaxRawHeaders caps the number of headers the h1parser can store inline.
const MaxRawHeaders = api.MaxRawHeaders

// FastPathHandler is implemented by the cache layer (L3). See pkg/api/fastpath.go.
//
// Unstable.
type FastPathHandler = api.FastPathHandler

// FastPathResponse is the pre-serialized response for a cache hit.
// See pkg/api/fastpath.go.
//
// Unstable.
type FastPathResponse = api.FastPathResponse

// EqualFold compares two ASCII strings case-insensitively without allocating.
var EqualFold = api.EqualFold
