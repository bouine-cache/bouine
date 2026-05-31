//go:build integration

package integration_test

import (
	"testing"

	"github.com/thylong/bouine/test/integration/driver"
)

func TestHTTP1_ProxyParity(t *testing.T) {
	s := driver.Boot(t, driver.Options{EnableHTTP: true})
	_ = s
	// Phase 1 fills in:
	//   - GET / HEAD / POST through bouine.HTTPAddr
	//   - Identical request directly to s.OriginAddr
	//   - Assert headers (modulo hop-by-hop), body, status match.
}

func TestHTTP2_ProxyParity(t *testing.T) {
	s := driver.Boot(t, driver.Options{EnableHTTPS: true})
	_ = s
	// Phase 1: same as HTTP/1.1 but over TLS with ALPN h2.
}
