// Package origin is the L5 upstream layer. It manages connection pools
// to origin servers, selects targets via round-robin (ADR-0005),
// performs passive health checking (consecutive-5xx ejection), active
// health probes, hedged requests, and exposes a fasthttp.RequestHandler
// that forwards requests to the chosen target.
package origin
