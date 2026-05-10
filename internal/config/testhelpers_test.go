package config

import (
	"testing"

	"gopkg.in/yaml.v3"
)

// yamlUnmarshal is a small test helper to exercise YAML decoding of
// types in isolation.
func yamlUnmarshal(t *testing.T, b []byte, dst any) error {
	t.Helper()
	return yaml.Unmarshal(b, dst)
}
