//go:build integration

// Package driver is the shared scaffold for bouine integration tests.
// Phase 1+ test files import it to build the binary, materialize TLS
// cert files, boot the daemon on random ports, and tear everything
// down on cleanup.
//
// Phase 0 ships the structure; real implementations land alongside the
// listener PRs in phase 1.
package driver

import "testing"

// Stack is the running test stack (origin + bouine + driver). It is
// returned from Boot.
type Stack struct {
	// AdminAddr is the URL of the admin listener.
	AdminAddr string
	// HTTPAddr is the URL of the plaintext HTTP/1.1 + h2c listener
	// (empty if disabled).
	HTTPAddr string
	// HTTPSAddr is the URL of the TLS HTTP/1.1 + H2 listener (empty if
	// disabled).
	HTTPSAddr string
	// OriginAddr is the URL of the echo origin.
	OriginAddr string
}

// Options configures Boot. The zero value uses sensible defaults for
// phase-1 listener tests.
type Options struct {
	// EnableHTTP, EnableHTTPS toggle individual listeners.
	EnableHTTP  bool
	EnableHTTPS bool
	// ConfigOverlay is YAML that is merged on top of the default test
	// config before the daemon is started. Empty is fine.
	ConfigOverlay string
}

// Boot brings up an origin + bouine and returns the live stack. Real
// implementation lands in phase 1; this skeleton causes tests to skip
// loudly so they are listed in CI but do not produce confusing
// "missing binary" failures.
func Boot(t *testing.T, _ Options) *Stack {
	t.Helper()
	t.Skip("integration driver not implemented yet — phase 1 deliverable")
	return nil
}

// Dump appends the bouine and origin logs to the test output. Called
// automatically from t.Cleanup, but exported so tests can dump
// mid-scenario.
func (s *Stack) Dump(t *testing.T) {
	t.Helper()
}
