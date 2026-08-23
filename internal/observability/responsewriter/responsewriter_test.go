package responsewriter

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
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
	require.NoError(t, err, "Write error")
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
	require.NoError(t, err, "Hijack error")
	require.True(t, frw.hijacked)
	Release(rw)
}

func TestHijackErrNotSupported(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	_, _, err := rw.Hijack()
	require.Equal(t, ErrNotSupported, err)
	Release(rw)
}

func TestReadFromDelegates(t *testing.T) {
	frw := &fullResponseWriter{ResponseWriter: httptest.NewRecorder()}
	rw := Acquire(frw)
	n, err := rw.ReadFrom(bytes.NewReader([]byte("streamed")))
	require.NoError(t, err, "ReadFrom error")
	require.Equal(t, int64(8), n)
	require.True(t, frw.readFrom)
	require.Equal(t, int64(8), rw.Bytes)
	Release(rw)
}

func TestReadFromFallbackCopy(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	n, err := rw.ReadFrom(bytes.NewReader([]byte("fallback")))
	require.NoError(t, err, "ReadFrom error")
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

func TestNewDefaultsTo200(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := New(rec)
	require.Equal(t, 200, rw.Status)
	require.Equal(t, int64(0), rw.Bytes)
	rw.WriteHeader(http.StatusNotFound)
	require.Equal(t, http.StatusNotFound, rw.Status)
}

func TestSetCacheKeySingle(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	defer Release(rw)

	key := api.Key{0xde, 0xad, 0xbe, 0xef}
	rw.SetCacheKey(key)
	require.Equal(t, key, rw.Key)
}

func TestSetCacheKeyNested(t *testing.T) {
	inner := Acquire(httptest.NewRecorder())
	defer Release(inner)

	outer := Acquire(inner)
	defer Release(outer)

	key := api.Key{0xca, 0xfe, 0xba, 0xbe}
	outer.SetCacheKey(key)

	require.Equal(t, key, outer.Key)
	require.Equal(t, key, inner.Key)
}

func TestSetCacheKeyStopsAtNonWrapper(t *testing.T) {
	inner := Acquire(httptest.NewRecorder())
	defer Release(inner)

	outer := Acquire(inner)
	defer Release(outer)

	key := api.Key{0x12, 0x34}
	outer.SetCacheKey(key)
	require.Equal(t, key, outer.Key)
	require.Equal(t, key, inner.Key)
}

func BenchmarkResponseWriterPool_AcquireRelease(b *testing.B) {
	w := httptest.NewRecorder()
	b.ReportAllocs()
	for b.Loop() {
		rw := Acquire(w)
		Release(rw)
	}
}
