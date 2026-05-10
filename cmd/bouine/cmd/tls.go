package cmd

import (
	"crypto/tls"
	"fmt"

	"github.com/thylong/bouine/internal/config"
)

// buildTLSConfig translates the config.TLS section into a *tls.Config.
// Phase 1 supports a single cert; multi-SNI cert selection (PLAN.md §9)
// lands in phase 4.
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

	var minVer int
	switch cfg.TLS.MinVersion {
	case "1.3":
		minVer = tls.VersionTLS13
	case "1.2", "":
		minVer = tls.VersionTLS12
	default:
		return nil, fmt.Errorf("tls: unsupported min_version %q", cfg.TLS.MinVersion)
	}

	return &tls.Config{
		Certificates: certs,
		MinVersion:   uint16(minVer),
		NextProtos:   cfg.TLS.ALPN,
	}, nil
}
