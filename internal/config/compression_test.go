package config

import (
	"strings"
	"testing"
)

func compressionBaseConfig(cc CompressionConfig) Config {
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	return Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{pool},
		Routes:        []Route{{Pool: "app", Cache: RouteCache{Compression: cc}}},
	}
}

func TestValidate_Compression_DisabledAllowsAnything(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero compression block must be valid, got %v", err)
	}
}

func TestValidate_Compression_DisabledWithFieldsRejected(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{ContentTypes: []string{"text/html"}})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("expected compression dead-config error, got %v", err)
	}
}

func TestValidate_Compression_EnabledRequiresContentTypes(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{Enabled: true})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "content_types") {
		t.Fatalf("expected content_types required error, got %v", err)
	}
}

func TestValidate_Compression_EnabledValid(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{
		Enabled:      true,
		Encodings:    []string{"gzip", "zstd"},
		MinSize:      512,
		MaxSize:      1 << 20,
		ContentTypes: []string{"text/html", "text/*", "application/json"},
	})
	if err := cfg.Validate(); err != nil {
		t.Fatalf("expected valid compression block, got %v", err)
	}
}

func TestValidate_Compression_UnknownEncodingRejected(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{
		Enabled:      true,
		Encodings:    []string{"snappy"},
		ContentTypes: []string{"text/html"},
	})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "encodings") {
		t.Fatalf("expected encodings error, got %v", err)
	}
}

func TestValidate_Compression_MaxSizeHardCap(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{
		Enabled:      true,
		MaxSize:      (1 << 20) + 1,
		ContentTypes: []string{"text/html"},
	})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "max_size") {
		t.Fatalf("expected max_size cap error, got %v", err)
	}
}

func TestValidate_Compression_MinGeqMaxRejected(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{
		Enabled:      true,
		MinSize:      2048,
		MaxSize:      1024,
		ContentTypes: []string{"text/html"},
	})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "min_size") {
		t.Fatalf("expected min_size < max_size error, got %v", err)
	}
}

func TestValidate_Compression_DuplicateContentType(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{
		Enabled:      true,
		ContentTypes: []string{"text/html", "TEXT/HTML"},
	})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate content_types error, got %v", err)
	}
}

func TestValidate_Compression_BadContentType(t *testing.T) {
	t.Parallel()
	cfg := compressionBaseConfig(CompressionConfig{
		Enabled:      true,
		ContentTypes: []string{"html"},
	})
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "content_types") {
		t.Fatalf("expected content_types media-type error, got %v", err)
	}
}

func TestValidate_Compression_DeadOnDisabledRoute(t *testing.T) {
	t.Parallel()
	off := false
	pool := UpstreamPool{Name: "app", Targets: []string{"a:1"}}
	cfg := Config{
		Listen:        Listen{Admin: ":9000"},
		UpstreamPools: []UpstreamPool{pool},
		Routes: []Route{{Pool: "app", Cache: RouteCache{
			Enabled:     &off,
			Compression: CompressionConfig{Enabled: true, ContentTypes: []string{"text/html"}},
		}}},
	}
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "compression") {
		t.Fatalf("expected compression dead-config error on disabled route, got %v", err)
	}
}
