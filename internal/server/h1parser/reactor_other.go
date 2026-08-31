//go:build !linux

package h1parser

// reactor_other.go — non-Linux platforms have no epoll transport, so
// the reactor is unavailable and the listener uses the blocking
// per-connection parser path. The portable state machine in reactor.go
// still compiles (and is unit-tested) on all platforms via injected
// I/O functions; only the readiness transport is Linux-specific.

import "net"

// reactorEpoll is the platform reactor type. On non-Linux it is a
// marker that can never be constructed with ok=true.
type reactorEpoll struct{}

// newReactorLoop reports the reactor unavailable on this platform.
func newReactorLoop(_ *Parser, _ net.Listener) (r *reactorEpoll, ok bool) {
	return nil, false
}

// ReactorLoop is the platform reactor handle returned by
// NewReactorLoop. On non-Linux platforms the constructor reports it
// unavailable; the methods below are unreachable then.
type ReactorLoop = reactorEpoll

// NewReactorLoop is the exported constructor used by the listener. It
// returns ok=false when the platform has no reactor transport, in
// which case the caller falls back to the blocking parser path.
func NewReactorLoop(p *Parser, ln net.Listener) (r *ReactorLoop, ok bool) {
	return newReactorLoop(p, ln)
}

// Run drives the reactor until the listener is closed. Blocks.
func (r *reactorEpoll) Run() {}

// The portable state machine's handoff path and the connection cap are
// Linux-only at runtime but part of the shared contract — reference
// them here so the non-Linux build compiles with the same symbols.
var (
	_ = (*reactorConn).handoff
	_ = reactorMaxConns
)
