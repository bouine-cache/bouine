package tlsutil

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
)

func TestCert_HasSANs(t *testing.T) {
	c := Cert(t, "api.test")
	if c.Leaf == nil {
		t.Fatal("Leaf should be populated")
	}
	want := map[string]bool{
		"localhost": false,
		"api.test":  false,
	}
	for _, n := range c.Leaf.DNSNames {
		want[n] = true
	}
	for n, ok := range want {
		if !ok {
			t.Errorf("missing SAN: %s", n)
		}
	}
}

func TestServerConfig_DefaultsSane(t *testing.T) {
	cfg := ServerConfig(t)
	if cfg.MinVersion < tls.VersionTLS12 {
		t.Fatalf("MinVersion = %d, want >= TLS 1.2", cfg.MinVersion)
	}
	if len(cfg.NextProtos) == 0 {
		t.Fatal("NextProtos should be non-empty")
	}
}

func TestWriteCertFiles_RoundTrip(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := WriteCertFiles(t, dir, "rt.test")

	if filepath.Dir(certPath) != dir {
		t.Errorf("cert path %s not in %s", certPath, dir)
	}

	// Cert PEM parses.
	pemBytes, err := os.ReadFile(certPath) //nolint:gosec // test file
	if err != nil {
		t.Fatalf("read cert: %v", err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil || block.Type != "CERTIFICATE" {
		t.Fatal("invalid cert PEM")
	}
	if _, err := x509.ParseCertificate(block.Bytes); err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	// Cert file is 0600.
	cst, err := os.Stat(certPath)
	if err != nil {
		t.Fatalf("stat cert: %v", err)
	}
	if cst.Mode().Perm() != 0o600 {
		t.Fatalf("cert perms = %o, want 600", cst.Mode().Perm())
	}

	// Key file is 0600.
	st, err := os.Stat(keyPath)
	if err != nil {
		t.Fatalf("stat key: %v", err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("key perms = %o, want 600", st.Mode().Perm())
	}

	// Round-trip with tls.LoadX509KeyPair.
	if _, err := tls.LoadX509KeyPair(certPath, keyPath); err != nil {
		t.Fatalf("LoadX509KeyPair: %v", err)
	}
}
