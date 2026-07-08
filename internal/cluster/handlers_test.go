package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bouine-cache/bouine/internal/observability"
	"github.com/bouine-cache/bouine/internal/storage"
	"github.com/bouine-cache/bouine/pkg/api"
	"github.com/bouine-cache/bouine/pkg/header"
)

func TestPeerReplicateHandler_DecodesAndStores(t *testing.T) {
	t.Parallel()

	var storedObj *api.Object
	handler := NewPeerReplicateHandler(
		func(_ context.Context, obj *api.Object) error {
			storedObj = obj
			return nil
		},
		&Metrics{},
		observability.NoopLogger{},
	)

	obj := &api.Object{
		Key:        api.Key(99),
		StatusCode: 200,
		Body:       []byte("test body"),
		Header:     header.FromHTTP(http.Header{"X-Custom": []string{"val"}}),
	}
	encoded := storage.EncodeObject(obj)

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/replicate", bytesReader(encoded))
	req.Header.Set(header.ContentType, "application/octet-stream")
	req.Header.Set(header.BouineIssuer, "node-sender")
	req.Header.Set(header.BouineSeq, "42")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if storedObj == nil {
		t.Fatal("object was not stored")
	}
	if storedObj.Key != api.Key(99) {
		t.Errorf("stored key = %d, want 99", storedObj.Key)
	}
	if string(storedObj.Body) != "test body" {
		t.Errorf("stored body = %q, want %q", storedObj.Body, "test body")
	}
	if storedObj.Header.Get("X-Custom") != "val" {
		t.Errorf("stored header X-Custom = %q, want val", storedObj.Header.Get("X-Custom"))
	}
}

func TestPeerReplicateHandler_BadBody(t *testing.T) {
	t.Parallel()

	handler := NewPeerReplicateHandler(
		func(_ context.Context, _ *api.Object) error {
			t.Fatal("should not be called on bad body")
			return nil
		},
		&Metrics{},
		observability.NoopLogger{},
	)

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/replicate", bytesReader([]byte("not a valid object")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestPeerReplicateHandler_StoreError(t *testing.T) {
	t.Parallel()

	handler := NewPeerReplicateHandler(
		func(_ context.Context, _ *api.Object) error {
			return context.DeadlineExceeded
		},
		&Metrics{},
		observability.NoopLogger{},
	)

	obj := &api.Object{Key: api.Key(1), StatusCode: 200, Body: []byte("x")}
	encoded := storage.EncodeObject(obj)

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/replicate", bytesReader(encoded))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

// bytesReader wraps a []byte as an io.Reader for httptest.NewRequest.
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func bytesReader(data []byte) io.Reader {
	return &byteReader{data: data}
}
