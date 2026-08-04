// Package bouineapi is the official Go SDK for the bouine admin API.
// It provides typed methods for every admin endpoint so callers don't
// need to construct HTTP requests manually.
//
// The SDK and the Cobra CLI share the same wire types (pkg/api) and
// the same auth model (bearer token or mTLS).
//
// Stable — v2.0 surface.
//
// v2.0 removes the Client.Reload method and ReloadResult type. Config
// changes are applied by rolling the pod (standard Kubernetes rolling
// update); there is no live config-reload endpoint.
package bouineapi
