package accesslog

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMiddleware_LogsAccess(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	inner := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(201)
		_, _ = io.WriteString(w, "ok")
	})

	h := Middleware(logger, inner)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/api/test", nil)
	req.Host = "example.com"
	h.ServeHTTP(rr, req)

	if rr.Code != 201 {
		t.Fatalf("status = %d", rr.Code)
	}

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rec["method"] != "POST" {
		t.Errorf("method = %v", rec["method"])
	}
	if rec["status"] != float64(201) {
		t.Errorf("status = %v", rec["status"])
	}
	if rec["path"] != "/api/test" {
		t.Errorf("path = %v", rec["path"])
	}
	if rec["host"] != "example.com" {
		t.Errorf("host = %v", rec["host"])
	}
	if rec["bytes_out"] != float64(2) {
		t.Errorf("bytes_out = %v", rec["bytes_out"])
	}
}
