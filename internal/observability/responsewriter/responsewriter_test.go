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
	if rw2.Status != 200 {
		t.Fatalf("Status = %d, want 200", rw2.Status)
	}
	if rw2.Bytes != 0 {
		t.Fatalf("Bytes = %d, want 0", rw2.Bytes)
	}
	Release(rw2)
}

func TestWriteHeaderAndWrite(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	rw.WriteHeader(206)
	if rw.Status != 206 {
		t.Fatalf("Status = %d, want 206", rw.Status)
	}
	if rec.Code != 206 {
		t.Fatalf("underlying Code = %d, want 206", rec.Code)
	}

	n, err := rw.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Write returned %d, want 5", n)
	}
	if rw.Bytes != 5 {
		t.Fatalf("Bytes = %d, want 5", rw.Bytes)
	}
	Release(rw)
}

func TestWriteHeaderRecordsOnlyFirstCall(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	defer Release(rw)

	rw.WriteHeader(http.StatusInternalServerError)
	rw.WriteHeader(http.StatusOK)

	if rw.Status != http.StatusInternalServerError {
		t.Fatalf("Status = %d, want %d (first call must win)", rw.Status, http.StatusInternalServerError)
	}
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("wire Code = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestWriteBeforeWriteHeaderKeepsDefaultStatus(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := Acquire(rec)
	defer Release(rw)

	_, _ = rw.Write([]byte("hello"))
	rw.WriteHeader(http.StatusInternalServerError)

	if rw.Status != http.StatusOK {
		t.Fatalf("Status = %d, want %d (implicit 200 from Write must win)", rw.Status, http.StatusOK)
	}
	if rw.Bytes != 5 {
		t.Fatalf("Bytes = %d, want 5", rw.Bytes)
	}
}

func TestAcquireResetsHeaderWritten(t *testing.T) {
	rec1 := httptest.NewRecorder()
	rw := Acquire(rec1)
	rw.WriteHeader(http.StatusTeapot)
	Release(rw)

	rec2 := httptest.NewRecorder()
	rw = Acquire(rec2)
	defer Release(rw)

	if rw.headerWritten {
		t.Fatal("headerWritten not reset after Acquire")
	}
	rw.WriteHeader(http.StatusCreated)
	if rw.Status != http.StatusCreated {
		t.Fatalf("Status = %d, want %d", rw.Status, http.StatusCreated)
	}
}

func TestFlushDelegates(t *testing.T) {
	frw := &fullResponseWriter{ResponseWriter: httptest.NewRecorder()}
	rw := Acquire(frw)
	rw.Flush()
	if !frw.flushed {
		t.Fatal("Flush did not delegate to underlying writer")
	}
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
	if err != nil {
		t.Fatalf("Hijack error: %v", err)
	}
	if !frw.hijacked {
		t.Fatal("Hijack did not delegate to underlying writer")
	}
	Release(rw)
}

func TestHijackErrNotSupported(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	_, _, err := rw.Hijack()
	if !errors.Is(err, ErrNotSupported) {
		t.Fatalf("Hijack error = %v, want ErrNotSupported", err)
	}
	Release(rw)
}

func TestReadFromDelegates(t *testing.T) {
	frw := &fullResponseWriter{ResponseWriter: httptest.NewRecorder()}
	rw := Acquire(frw)
	n, err := rw.ReadFrom(bytes.NewReader([]byte("streamed")))
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if n != 8 {
		t.Fatalf("ReadFrom returned %d, want 8", n)
	}
	if !frw.readFrom {
		t.Fatal("ReadFrom did not delegate to underlying writer")
	}
	if rw.Bytes != 8 {
		t.Fatalf("Bytes = %d, want 8", rw.Bytes)
	}
	Release(rw)
}

func TestReadFromFallbackCopy(t *testing.T) {
	rw := Acquire(plainResponseWriter{ResponseWriter: httptest.NewRecorder()})
	n, err := rw.ReadFrom(bytes.NewReader([]byte("fallback")))
	if err != nil {
		t.Fatalf("ReadFrom error: %v", err)
	}
	if n != 8 {
		t.Fatalf("ReadFrom returned %d, want 8", n)
	}
	if rw.Bytes != 8 {
		t.Fatalf("Bytes = %d, want 8", rw.Bytes)
	}
	Release(rw)
}

func TestInterfaceGuards(t *testing.T) {
	rw := Acquire(httptest.NewRecorder())
	defer Release(rw)
	if _, ok := any(rw).(http.Flusher); !ok {
		t.Fatal("ResponseWriter does not satisfy http.Flusher")
	}
	if _, ok := any(rw).(http.Hijacker); !ok {
		t.Fatal("ResponseWriter does not satisfy http.Hijacker")
	}
	if _, ok := any(rw).(io.ReaderFrom); !ok {
		t.Fatal("ResponseWriter does not satisfy io.ReaderFrom")
	}
}

func BenchmarkResponseWriterPool_AcquireRelease(b *testing.B) {
	w := httptest.NewRecorder()
	b.ReportAllocs()
	for b.Loop() {
		rw := Acquire(w)
		Release(rw)
	}
}
