package server

import (
	"bytes"
	"context"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/testutil/poll"
)

// bufferLogger adapts a mutex-guarded bytes.Buffer into an
// observability.Logger so tests can assert on the records fasthttp
// emits through the adapter. The mutex is required: fasthttp emits
// from its worker-pool goroutines while the test reads the buffer.
type bufferLogger struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	logger *slog.Logger
}

func newBufferLogger() *bufferLogger {
	b := &bufferLogger{}
	b.logger = slog.New(slog.NewTextHandler(b, nil))
	return b
}

func (b *bufferLogger) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *bufferLogger) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func (b *bufferLogger) Info(msg string, args ...any)  { b.logger.Info(msg, args...) }
func (b *bufferLogger) Warn(msg string, args ...any)  { b.logger.Warn(msg, args...) }
func (b *bufferLogger) Error(msg string, args ...any) { b.logger.Error(msg, args...) }
func (b *bufferLogger) Debug(msg string, args ...any) { b.logger.Debug(msg, args...) }

func TestListener_FastHTTPLoggerWired(t *testing.T) {
	t.Parallel()
	log := newBufferLogger()

	http := NewHTTP(ListenerConfig{Addr: "127.0.0.1:0", Handler: echo200(), Logger: log})
	https := NewHTTPS(ListenerConfig{Addr: "127.0.0.1:0", Handler: echo200(), Logger: log})

	assert.Equal(t, observability.NewFastHTTPLogger(log, "http"), http.inner.Logger)
	assert.Equal(t, observability.NewFastHTTPLogger(log, "https"), https.inner.Logger)
}

// TestListener_FastHTTPErrorsLogAtWarn proves end-to-end that fasthttp's
// internal diagnostics surface as structured WARN records instead of raw
// stderr lines. A malformed request line ("garbage" with no whitespace)
// fails request parsing with an error fasthttp does not classify as
// benign, so its worker pool forwards it to the configured logger.
func TestListener_FastHTTPErrorsLogAtWarn(t *testing.T) {
	t.Parallel()
	log := newBufferLogger()
	srv := NewHTTP(ListenerConfig{
		Addr:    "127.0.0.1:0",
		Handler: echo200(),
		Logger:  log,
	})

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Serve(ctx) }()
	defer func() {
		cancel()
		require.NoError(t, <-errCh)
	}()

	waitForAddr(t, srv)

	conn, err := net.DialTimeout("tcp", srv.Addr(), time.Second)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Write([]byte("garbage\r\n\r\n"))
	require.NoError(t, err)

	poll.Eventually(t, 3*time.Second, 10*time.Millisecond, func() bool {
		return bytes.Contains([]byte(log.String()), []byte("level=WARN"))
	})

	out := log.String()
	assert.Contains(t, out, "error when serving connection")
	assert.Contains(t, out, "cannot find http request method")
	assert.Contains(t, out, "component=http")
}
