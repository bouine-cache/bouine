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
// Unreachable on non-Linux: NewReactorLoop never returns ok=true here.
func (r *reactorEpoll) Run() {}

// Close stops the loop and drains in-flight handoffs. Unreachable on
// non-Linux (see Run); exists so the listener wiring compiles against
// the same contract on every platform.
func (r *reactorEpoll) Close() {}

// The portable state machine's handoff path, the connection cap, and
// the shutdown-drain symbols are Linux-only at runtime but part of the
// shared contract — reference them here so the non-Linux build (and
// the unused linter) sees the same symbols as Linux.
var (
	_ = (*reactorConn).handoff
	_ = reactorMaxConns
	_ = handoffDrainGrace
	_ = (*handoffTracker).drainForceClose
	// epollInterest is the Linux transport's mod()-skip mask tracker;
	// reference the field so the non-Linux build keeps the shared
	// struct definition identical.
	_ = reactorConn{}.epollInterest
)
