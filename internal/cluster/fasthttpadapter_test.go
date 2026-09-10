package cluster

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/valyala/fasthttp"

	"github.com/bouine-cache/bouine/pkg/api"
)

// workerFormat mirrors fasthttp@v1.74.0 client.go:3038, the only call
// site of the PipelineClient logger. Anchored by pipelineWorkerFmt in
// fasthttpadapter.go.
const workerFormat = pipelineWorkerFmt

func TestFasthttpLogger_Classification(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		calls func(l *fasthttpLogger)
		want  string
	}{
		{
			name: "dial connection refused is WARN",
			calls: func(l *fasthttpLogger) {
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeRefused)
			},
			want: "WARN",
		},
		{
			name: "dial timeout is WARN",
			calls: func(l *fasthttpLogger) {
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeDialTimeout)
			},
			want: "WARN",
		},
		{
			name: "no such host is WARN",
			calls: func(l *fasthttpLogger) {
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeNoSuchHost)
			},
			want: "WARN",
		},
		{
			name: "unexpected error is ERROR",
			calls: func(l *fasthttpLogger) {
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeTLS)
			},
			want: "ERROR",
		},
		{
			name: "closed conn during shutdown is DEBUG",
			calls: func(l *fasthttpLogger) {
				l.markClosing()
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeClosedConn)
			},
			want: "DEBUG",
		},
		{
			name: "closed conn before shutdown is ERROR",
			calls: func(l *fasthttpLogger) {
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeClosedConn)
			},
			want: "ERROR",
		},
		{
			name: "refused during shutdown stays WARN",
			calls: func(l *fasthttpLogger) {
				l.markClosing()
				l.Printf(workerFormat, "10.0.0.5:9000", errFakeRefused)
			},
			want: "WARN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			logger, mu, buf := captureLogger(t)
			l := newFasthttpLogger(logger)
			tc.calls(l)

			records := parseAdapterRecords(t, mu, buf)
			require.Len(t, records, 1)
			assert.Equal(t, tc.want, records[0]["level"])
			assert.Equal(t, "fasthttp", records[0]["component"])
			assert.Contains(t, records[0]["msg"], "error in PipelineClient")
			assert.Contains(t, records[0]["msg"], "10.0.0.5:9000")
		})
	}
}

var (
	errFakeRefused     = errFake{msg: "dial tcp 10.0.0.5:9000: connect: connection refused"}
	errFakeDialTimeout = errFake{msg: "dial tcp 10.0.0.5:9000: connect: i/o timeout", timeout: true}
	errFakeNoSuchHost  = errFake{msg: "dial tcp: lookup bouine-3 on 1.2.3.4: no such host"}
	errFakeTLS         = errFake{msg: "tls: failed to verify certificate: x509: unknown authority"}
	errFakeClosedConn  = errFake{msg: "write tcp 10.0.0.1:1234->10.0.0.5:9000: use of closed network connection"}
)

type errFake struct {
	msg     string
	timeout bool
}

func (e errFake) Error() string { return e.msg }

func (e errFake) Timeout() bool { return e.timeout }

func (e errFake) Temporary() bool { return false }

func TestFasthttpLogger_SetOnPipelineClients(t *testing.T) {
	t.Parallel()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	peer := api.PeerInfo{Addr: "10.0.0.5:9000"}

	pc := f.getPipelineClient(peerAddr(peer))
	require.NotNil(t, pc)
	assert.Same(t, f.httpLog, pc.Logger,
		"pipeline clients must log through the slog adapter, not fasthttp's default stderr logger")
}

func TestFasthttpLogger_RealWorkerErrorSurfacesAsWarn(t *testing.T) {
	t.Parallel()

	// End to end through fasthttp: dial a closed port so the pipeline
	// worker fails and logs via the adapter, then check the record.
	logger, mu, buf := captureLogger(t)
	pc := &fasthttp.PipelineClient{
		Addr:   "127.0.0.1:1",
		Logger: newFasthttpLogger(logger),
		Dial: func(addr string) (net.Conn, error) {
			return nil, errFakeRefused
		},
	}
	req := fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	resp := fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	// DoTimeout with a deadline that never succeeds: the worker's dial
	// error is logged asynchronously; the request itself times out.
	_ = pc.DoTimeout(req, resp, testWorkerLogWait)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := buf.Len()
		mu.Unlock()
		if n > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	records := parseAdapterRecords(t, mu, buf)
	require.NotEmpty(t, records, "fasthttp worker error must be captured by the adapter")
	assert.Equal(t, "WARN", records[0]["level"])
	assert.Contains(t, records[0]["msg"], "error in PipelineClient")
}

const testWorkerLogWait = 50 * time.Millisecond

func TestPeerFetcherClose_MarksAdapterClosing(t *testing.T) {
	t.Parallel()

	f := NewPeerFetcherWithConfig(PeerFetcherConfig{}, nil, nil)
	require.NotNil(t, f.httpLog)
	assert.False(t, f.httpLog.closing.Load())
	require.NoError(t, f.Close(context.Background()))
	assert.True(t, f.httpLog.closing.Load())
}

func TestFasthttpLogger_ImplementsInterface(t *testing.T) {
	t.Parallel()

	var _ fasthttp.Logger = (*fasthttpLogger)(nil)
}
