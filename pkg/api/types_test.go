package api

import (
	"encoding/json"
	"testing"
)

func TestRequestContext_JSONRoundtrip(t *testing.T) {
	t.Parallel()
	rc := RequestContext{
		RequestID:    "abc",
		Method:       "GET",
		Host:         "example.com",
		URL:          "/foo",
		CacheResult:  CacheResultHit,
		Status:       200,
		Route:        "api",
		UpstreamPool: "app",
	}
	b, err := json.Marshal(rc)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back RequestContext
	if err := json.Unmarshal(b, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if back.CacheResult != CacheResultHit {
		t.Fatalf("cache_result lost in roundtrip: %v", back.CacheResult)
	}
}

func TestCacheResult_UnknownTolerated(t *testing.T) {
	t.Parallel()
	var rc RequestContext
	if err := json.Unmarshal([]byte(`{"cache_result":"future_value"}`), &rc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rc.CacheResult != "future_value" {
		t.Fatalf("expected open enum, got %v", rc.CacheResult)
	}
}
