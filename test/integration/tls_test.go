//go:build integration

package integration

import (
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/tlsutil"
	"github.com/bouine-cache/bouine/test/integration/driver"
)

// bootTLSCluster boots a 3-node cluster with data-plane TLS enabled.
// The cert files are written to a temp directory and shared across all
// nodes.
func bootTLSCluster(t *testing.T, hosts ...string) *driver.ClusterStack {
	t.Helper()
	dir := t.TempDir()
	certPath, keyPath := tlsutil.WriteCertFiles(t, dir, hosts...)
	return driver.BootCluster(t, driver.ClusterOptions{
		Mode: "strong",
		TLS: driver.TLSOptions{
			Enabled:  true,
			CertFile: certPath,
			KeyFile:  keyPath,
		},
	})
}

func TestTLS_TerminationAndH2(t *testing.T) {
	stack := bootTLSCluster(t)

	// Prime the cache with the first request (MISS), then verify the
	// second request is a HIT and negotiated HTTP/2 via ALPN.
	resp := stack.GetTLS(t, 0, "/hit")
	require.Equal(t, 200, resp.StatusCode, "HTTPS request should succeed")
	assert.Equal(t, "MISS", driver.XCache(resp), "first request should be a MISS")

	resp2 := stack.GetTLSResponse(t, 0, "/hit")
	defer resp2.Body.Close()
	require.Equal(t, 200, resp2.StatusCode)
	assert.Equal(t, "HTTP/2.0", resp2.Proto,
		"HTTPS listener should negotiate HTTP/2 via ALPN")
	assert.Equal(t, "HIT", driver.XCache(resp2),
		"second request should be a cache HIT")
}

func TestTLS_SNICertSelection(t *testing.T) {
	// Each cert must be in its own directory because WriteCertFiles
	// always writes tls.crt and tls.key.
	dir1 := t.TempDir()
	dir2 := t.TempDir()
	cert1, key1 := tlsutil.WriteCertFiles(t, dir1, "localhost")
	cert2, key2 := tlsutil.WriteCertFiles(t, dir2, "sni-test.local")

	// Cert selection in bouine is SAN-based: Go's tls.Config matches the
	// SNI hostname against the leaf certificate's DNS SANs. The config
	// `sni` field is not wired to a GetCertificate handler, so we rely
	// on the SANs in the certs themselves to drive selection.
	stack := driver.BootCluster(t, driver.ClusterOptions{
		Mode: "strong",
		TLS: driver.TLSOptions{
			Enabled:  true,
			CertFile: cert1,
			KeyFile:  key1,
			ExtraCerts: []driver.TLSCertEntry{
				{CertFile: cert2, KeyFile: key2},
			},
		},
	})

	// Connect with SNI=localhost — cert1 has "localhost" in its SANs.
	certs1 := stack.TLSServerCerts(t, 0, "localhost")
	require.NotEmpty(t, certs1, "server must present a certificate for SNI=localhost")
	assert.Contains(t, certs1[0].DNSNames, "localhost",
		"served cert for SNI=localhost must contain 'localhost' in DNS SANs")

	// Connect with SNI=sni-test.local — cert2 has "sni-test.local" in its SANs.
	certs2 := stack.TLSServerCerts(t, 0, "sni-test.local")
	require.NotEmpty(t, certs2, "server must present a certificate for SNI=sni-test.local")
	assert.Contains(t, certs2[0].DNSNames, "sni-test.local",
		"served cert for SNI=sni-test.local must contain 'sni-test.local' in DNS SANs")

	// Verify the two certs are actually different (different serials).
	assert.NotEqual(t, certs1[0].SerialNumber, certs2[0].SerialNumber,
		"SNI must select different certs for different hostnames")
}

func TestTLS_CertRotation(t *testing.T) {
	dir1 := t.TempDir()
	cert1, key1 := tlsutil.WriteCertFiles(t, dir1, "localhost")

	stack := driver.BootCluster(t, driver.ClusterOptions{
		Mode: "strong",
		TLS: driver.TLSOptions{
			Enabled:  true,
			CertFile: cert1,
			KeyFile:  key1,
		},
	})

	// Verify initial cert works.
	resp := stack.GetTLS(t, 0, "/hit")
	require.Equal(t, 200, resp.StatusCode, "initial cert should work")

	// Capture the initial certificate's serial number.
	initialCerts := stack.TLSServerCerts(t, 0, "localhost")
	require.NotEmpty(t, initialCerts)
	initialSerial := initialCerts[0].SerialNumber

	// Kill node 0 and restart with a fresh cert.
	stack.KillNode(t, 0)

	dir2 := t.TempDir()
	cert2, key2 := tlsutil.WriteCertFiles(t, dir2, "localhost")

	stack.RestartNodeWithTLS(t, 0, driver.TLSOptions{
		Enabled:  true,
		CertFile: cert2,
		KeyFile:  key2,
	})

	// Verify the new cert is different and the node still serves traffic.
	rotatedCerts := stack.TLSServerCerts(t, 0, "localhost")
	require.NotEmpty(t, rotatedCerts)
	assert.NotEqual(t, initialSerial, rotatedCerts[0].SerialNumber,
		"rotated cert must have a different serial number")

	resp2 := stack.GetTLS(t, 0, "/hit")
	require.Equal(t, 200, resp2.StatusCode,
		"node should serve traffic after cert rotation")
}

func TestTLS_MinVersionEnforced(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := tlsutil.WriteCertFiles(t, dir, "localhost")

	stack := driver.BootCluster(t, driver.ClusterOptions{
		Mode: "strong",
		TLS: driver.TLSOptions{
			Enabled:    true,
			CertFile:   certPath,
			KeyFile:    keyPath,
			MinVersion: "1.2",
		},
	})

	// A TLS 1.2 connection should succeed.
	resp := stack.GetTLS(t, 1, "/hit")
	require.Equal(t, 200, resp.StatusCode, "TLS 1.2 connection should succeed")

	// Attempt a TLS 1.0 handshake — must be rejected because the
	// server enforces min_version 1.2.
	hostPort := strings.TrimPrefix(stack.Nodes[1].HTTPSAddr, "https://")
	conf := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // test
		ServerName:         "localhost",
		MinVersion:         tls.VersionTLS10,
		MaxVersion:         tls.VersionTLS10,
	}
	dialer := &net.Dialer{Timeout: 5 * time.Second}
	conn, err := tls.DialWithDialer(dialer, "tcp", hostPort, conf)
	if err == nil {
		conn.Close()
		t.Fatal("TLS 1.0 handshake should be rejected by server with min_version=1.2")
	}
}
