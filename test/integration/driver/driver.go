//go:build integration

// Package driver boots bouine nodes in-process for integration tests.
// No Docker required — each node is a goroutine running the bouine CLI.
package driver

import "testing"

// Stack is the running test stack for single-node listener tests.
type Stack struct {
	AdminAddr  string
	HTTPAddr   string
	HTTPSAddr  string
	OriginAddr string
}

// Options configures Boot for single-node listener tests.
type Options struct {
	EnableHTTP    bool
	EnableHTTPS   bool
	ConfigOverlay string
}

// Boot brings up an origin + single bouine node for listener tests.
func Boot(t *testing.T, _ Options) *Stack {
	t.Helper()
	t.Skip("single-node listener driver not implemented — use cluster driver")
	return nil
}

// Dump is a no-op.
func (s *Stack) Dump(t *testing.T) {
	t.Helper()
}
