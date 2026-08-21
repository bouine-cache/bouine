package cluster

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

func TestPeerPurgeHandler_DecodesAndCalls(t *testing.T) {
	t.Parallel()
	var received api.PurgeEvent
	handler := NewPeerPurgeHandler(func(evt api.PurgeEvent) error {
		received = evt
		return nil
	})

	evt := api.PurgeEvent{Key: testkey.Key(42), Issuer: "node-0"}
	body, _ := EncodePurgeHTTP(evt)

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/purge", bytesReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testkey.Key(42), received.Key)
}

func TestPeerPurgeHandler_BadBody(t *testing.T) {
	t.Parallel()
	handler := NewPeerPurgeHandler(func(api.PurgeEvent) error {
		t.Fatal("should not be called")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/purge", bytesReader([]byte("bad")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestPeerBanHandler_DecodesAndCalls(t *testing.T) {
	t.Parallel()
	var received api.BanEvent
	handler := NewPeerBanHandler(func(evt api.BanEvent) error {
		received = evt
		return nil
	})

	evt := api.BanEvent{Issuer: "node-0", Predicate: api.BanExpr{HostRegex: "example.com"}}
	body, _ := EncodeBanHTTP(evt)

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/ban", bytesReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "example.com", received.Predicate.HostRegex)
}

func TestPeerRefreshHandler_DecodesAndCalls(t *testing.T) {
	t.Parallel()
	var received api.RefreshEvent
	handler := NewPeerRefreshHandler(func(evt api.RefreshEvent) error {
		received = evt
		return nil
	})

	evt := api.RefreshEvent{Key: testkey.Key(42), Issuer: "node-0"}
	body, _ := EncodeRefreshHTTP(evt)

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/refresh", bytesReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, testkey.Key(42), received.Key)
	assert.Equal(t, "node-0", received.Issuer)
}

func TestPeerRefreshHandler_BadBody(t *testing.T) {
	t.Parallel()
	handler := NewPeerRefreshHandler(func(api.RefreshEvent) error {
		t.Fatal("should not be called")
		return nil
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/peer/refresh", bytesReader([]byte("bad")))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

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
