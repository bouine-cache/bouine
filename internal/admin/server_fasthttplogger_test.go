package admin

import (
	"io"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/bouine-cache/bouine/internal/observability"
)

func TestServer_FastHTTPLoggerWired(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))

	s := New(Config{Logger: logger, ReadyFn: func() bool { return true }})
	assert.Equal(t, observability.NewFastHTTPLogger(logger, "admin"), s.inner.Logger)

	ms := NewMinimal("", nil, nil, nil, logger)
	assert.Equal(t, observability.NewFastHTTPLogger(logger, "admin"), ms.inner.Logger)
}
