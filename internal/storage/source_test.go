package storage

import (
	"context"
	"testing"
)

func TestTieredStore_Get_Miss(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := KeyHash([]byte("tiered-miss"))

	got, src, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got != nil {
		t.Fatal("expected nil on miss")
	}
	if src != "" {
		t.Fatalf("source = %q, want empty", src)
	}
}

func TestHotStore_Get_DelegatesToGet(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("delegate"))
	o := obj(k, 100)

	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, _, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit via Get")
	}
}
