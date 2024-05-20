package middlewares

import "errors"

// ErrUpstreamUnhealthy is returned by the healthcheck middleware when upstream is unhealthy.
// This mechanism prevents bouine instances to swarm upstream when they're slow or unresponsive.
var ErrUpstreamUnhealthy = errors.New("upstream is unhealthy")
