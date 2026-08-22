package cmd

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"

	"github.com/bouine-cache/bouine/internal/config"
)

// buildTLSConfig translates the config.TLS section into a *tls.Config.
func buildTLSConfig(cfg *config.Config) (*tls.Config, error) {
	if len(cfg.TLS.Certs) == 0 {
		return nil, fmt.Errorf("tls: no certs configured; HTTPS requires at least one cert+key")
	}

	var certs []tls.Certificate
	for _, c := range cfg.TLS.Certs {
		pair, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls: load %s / %s: %w", c.CertFile, c.KeyFile, err)
		}
		certs = append(certs, pair)
	}

	var minVer uint16
	switch cfg.TLS.MinVersion {
	case "1.3":
		minVer = tls.VersionTLS13
	case "1.2", "":
		minVer = tls.VersionTLS12
	default:
		return nil, fmt.Errorf("tls: unsupported min_version %q", cfg.TLS.MinVersion)
	}

	return &tls.Config{ //nolint:gosec // G402: MinVersion is enforced >= 1.2 by the switch above; gosec can't trace through the variable
		Certificates: certs,
		MinVersion:   minVer,
		NextProtos:   []string{"http/1.1"},
	}, nil
}

// buildClusterTLSConfig builds a *tls.Config from ClusterTLS settings for
// mTLS between bouine peers.
func buildClusterTLSConfig(cfg config.ClusterTLS) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load cluster client cert: %w", err)
	}
	pool, err := loadCertPool(cfg.CABundle)
	if err != nil {
		return nil, fmt.Errorf("load cluster CA: %w", err)
	}
	return &tls.Config{
		Certificates:       []tls.Certificate{cert},
		RootCAs:            pool,
		ClientCAs:          pool,
		ClientAuth:         tls.RequireAndVerifyClientCert,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: false,
	}, nil
}

// loadCertPool reads PEM-encoded CA certificates from path and returns a
// *x509.CertPool.
func loadCertPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path) //nolint:gosec // configured path
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("no valid PEM certificates found in %s", path)
	}
	return pool, nil
}
