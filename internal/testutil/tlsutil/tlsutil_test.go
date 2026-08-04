package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCert_HasSANs(t *testing.T) {
	t.Parallel()
	c := Cert(t, "api.test")
	require.NotNil(t, c.Leaf)
	want := map[string]bool{
		"localhost": false,
		"api.test":  false,
	}
	for _, n := range c.Leaf.DNSNames {
		want[n] = true
	}
	for n, ok := range want {
		assert.Truef(t, ok, "missing SAN: %s", n)
	}
}

func TestServerConfig_DefaultsSane(t *testing.T) {
	t.Parallel()
	cfg := ServerConfig(t)
	if cfg.MinVersion < tls.VersionTLS12 {
		t.Fatalf("MinVersion = %d, want >= TLS 1.2", cfg.MinVersion)
	}
	require.NotEqual(t, 0, len(cfg.NextProtos))
}

func TestWriteCertFiles_RoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	certPath, keyPath := WriteCertFiles(t, dir, "rt.test")

	assert.Equal(t, dir, filepath.Dir(certPath))

	// Cert PEM parses.
	pemBytes, err := os.ReadFile(certPath) //nolint:gosec // test file
	require.NoErrorf(t, err, "read cert: %v", err)
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatal("invalid cert PEM")
	}
	_, err = x509.ParseCertificate(block.Bytes)
	require.NoErrorf(t, err, "parse cert: %v", err)

	// Cert file is 0600.
	cst, err := os.Stat(certPath)
	require.NoErrorf(t, err, "stat cert: %v", err)
	require.Equal(t, os.FileMode(0o600), cst.Mode().Perm())

	// Key file is 0600.
	st, err := os.Stat(keyPath)
	require.NoErrorf(t, err, "stat key: %v", err)
	require.Equal(t, os.FileMode(0o600), st.Mode().Perm())

	// Round-trip with tls.LoadX509KeyPair.
	_, err = tls.LoadX509KeyPair(certPath, keyPath)
	require.NoErrorf(t, err, "LoadX509KeyPair: %v", err)
}
