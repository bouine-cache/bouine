// Package tlsutil generates ephemeral TLS material for tests. It MUST
// NOT be used from production code paths; the package is sealed to
// _test.go files via the depguard rules.
//
// Two flavours of helper:
//
//   - In-process: Cert and ServerConfig return live tls structures
//     ready to plug into httptest.NewUnstartedServer or quic-go.
//   - On-disk: WriteCertFiles writes PEM files to a directory, used
//     when an integration test execs the bouine binary which loads
//     certs from disk.
package tlsutil

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Cert generates a short-lived self-signed ECDSA P-256 leaf certificate.
// Hosts may contain DNS names or IP literals; "localhost", "127.0.0.1",
// and "::1" are always included.
//
// The returned tls.Certificate has Leaf populated so callers can read
// it without re-parsing.
func Cert(t *testing.T, hosts ...string) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("tlsutil: generate key: %v", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		t.Fatalf("tlsutil: serial: %v", err)
	}

	tpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "bouine-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	hosts = append(hosts, "localhost", "127.0.0.1", "::1")
	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			tpl.IPAddresses = append(tpl.IPAddresses, ip)
		} else {
			tpl.DNSNames = append(tpl.DNSNames, h)
		}
	}

	der, err := x509.CreateCertificate(rand.Reader, &tpl, &tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("tlsutil: create cert: %v", err)
	}

	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("tlsutil: parse cert: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{der},
		PrivateKey:  key,
		Leaf:        leaf,
	}
}

// ServerConfig returns a *tls.Config suitable for an httptest TLS
// server or a quic-go listener.
func ServerConfig(t *testing.T, hosts ...string) *tls.Config {
	t.Helper()
	cert := Cert(t, hosts...)
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h3", "h2", "http/1.1"},
	}
}

// WriteCertFiles writes a self-signed cert + key pair under dir as
// "tls.crt" and "tls.key" (PEM). It returns the two file paths so the
// caller can plug them into a bouine config file.
//
// Permissions are 0o600 on the key, 0o644 on the cert.
func WriteCertFiles(t *testing.T, dir string, hosts ...string) (certPath, keyPath string) {
	t.Helper()
	if dir == "" {
		t.Fatalf("tlsutil: WriteCertFiles requires a non-empty dir")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatalf("tlsutil: mkdir %s: %v", dir, err)
	}

	cert := Cert(t, hosts...)
	certPath = filepath.Join(dir, "tls.crt")
	keyPath = filepath.Join(dir, "tls.key")

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Certificate[0]})
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("tlsutil: write cert: %v", err)
	}

	key, ok := cert.PrivateKey.(*ecdsa.PrivateKey)
	if !ok {
		t.Fatalf("tlsutil: unexpected key type %T", cert.PrivateKey)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("tlsutil: marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("tlsutil: write key: %v", err)
	}

	return certPath, keyPath
}

// ErrUnsupportedKey is reported when a helper expects an ECDSA key but
// gets something else. Exported for tests of the helper itself.
var ErrUnsupportedKey = errors.New("tlsutil: unsupported private-key type")
