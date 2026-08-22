// Package transport provides fasthttp client and server wrappers with
// context-aware cancellation, logging integration, and connection
// pooling tuned for bouine's workload.
//
// It is the single place that bridges context.Context and fasthttp.Client.
// All outbound HTTP callers (origin, peer, broadcast, health, dashboard
// aggregator) use [Client.Do] instead of fasthttp.Client.Do directly.
//
// Unstable.
package transport
