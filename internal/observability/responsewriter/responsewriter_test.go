package responsewriter

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

type fullResponseWriter struct {
	http.ResponseWriter
	flushed  bool
	hijacked bool
	readFrom bool
}

func (f *fullResponseWriter) Flush() { f.flushed = true }
func (f *fullResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	f.hijacked = true
	return nil, nil, nil
}
func (f *fullResponseWriter) ReadFrom(src io.Reader) (int64, error) {
	f.readFrom = true
	return io.Copy(struct{ io.Writer }{f}, src)
}

type plainResponseWriter struct {
	http.ResponseWriter
}

func TestAcquireResetsFields(t *testing.T) {
	w := httptest.NewRecorder()
	rw := Acquire(w)
	rw.Status = 500
	rw.Bytes = 999
	Release(rw)

	rw2 := Acquire(w)
	require.Equal(t, 200, rw2.Status)
	require.Equal(t, int64(0), rw2.Bytes)
	Release(rw2)
}

func TestWriteHeaderAndWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	rw.WriteHeader(206)
	require.Equal(t, 206, rw.Status)
	require.Equal(t, 206, rec.Code)

	n, err := rw.Write([]byte("hello"))
	require.NoErrorf(t, err, "Write error: %v", err)
	require.Equal(t, 5, n)
	require.Equal(t, int64(5), rw.Bytes)
	Release(rw)
}

func TestWriteHeaderRecordsOnlyFirstCall(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	defer Release(rw)

	rw.WriteHeader(http.StatusInternalServerError)
	rw.WriteHeader(http.StatusOK)

	require.Equal(t, http.StatusInternalServerError, rw.Status)
	require.Equal(t, http.StatusInternalServerError, rec.Code)
}

func TestWriteBeforeWriteHeaderKeepsDefaultStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	defer Release(rw)

	_, _ = rw.Write([]byte("hello"))
	rw.WriteHeader(http.StatusInternalServerError)

	require.Equal(t, http.StatusOK, rw.Status)
	require.Equal(t, int64(5), rw.Bytes)
}

func TestAcquireResetsHeaderWritten(t *testing.T) {
	rec1 := httptest.NewRecorder()
	rw := Acquire(rec1)
	rw.WriteHeader(http.StatusTeapot)
	Release(rw)

	rec2 := httptest.NewRecorder()
	rw = Acquire(rec2)
	defer Release(rw)

	require.False(t, rw.headerWritten)
	rw.WriteHeader(http.StatusCreated)
	require.Equal(t, http.StatusCreated, rw.Status)
}

func TestFlushDelegates(t *testing.T) {
	frw := &fullResponseWriter{ResponseWriter: httptest.NewRecorder()}
	rw := Acquire(frw)
	rw.Flush()
	require.True(t, frw.flushed)
	Release(rw)
}

func TestFlushNoOpWhenUnsupported(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	rw.Flush()
	Release(rw)
}

func TestHijackDelegates(t *testing.T) {
	frw := &fullResponseWriter{ResponseWriter: httptest.NewRecorder()}
	rw := Acquire(frw)
	_, _, err := rw.Hijack()
	require.NoErrorf(t, err, "Hijack error: %v", err)
	require.True(t, frw.hijacked)
	Release(rw)
}

func TestHijackErrNotSupported(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	_, _, err := rw.Hijack()
	require.True(t, errors.Is(err, ErrNotSupported))
	Release(rw)
}

func TestReadFromDelegates(t *testing.T) {
	frw := &fullResponseWriter{ResponseWriter: httptest.NewRecorder()}
	rw := Acquire(frw)
	n, err := rw.ReadFrom(bytes.NewReader([]byte("streamed")))
	require.NoErrorf(t, err, "ReadFrom error: %v", err)
	require.Equal(t, int64(8), n)
	require.True(t, frw.readFrom)
	require.Equal(t, int64(8), rw.Bytes)
	Release(rw)
}

func TestReadFromFallbackCopy(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	n, err := rw.ReadFrom(bytes.NewReader([]byte("fallback")))
	require.NoErrorf(t, err, "ReadFrom error: %v", err)
	require.Equal(t, int64(8), n)
	require.Equal(t, int64(8), rw.Bytes)
	Release(rw)
}

func TestInterfaceGuards(t *testing.T) {
	rw := Acquire(httptest.NewRecorder())
	defer Release(rw)
	_, ok := any(rw).(http.Flusher)
	require.True(t, ok)
	_, ok = any(rw).(http.Hijacker)
	require.True(t, ok)
	_, ok = any(rw).(io.ReaderFrom)
	require.True(t, ok)
}

func BenchmarkResponseWriterPool_AcquireRelease(b *testing.B) {
	w := httptest.NewRecorder()
	b.ReportAllocs()
	for b.Loop() {
		rw := Acquire(w)
		Release(rw)
	}
}
