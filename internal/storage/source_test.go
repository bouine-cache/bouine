package storage

import (
	"context"
	"testing"

	"github.com/bouine-cache/bouine/pkg/api"
)

func TestHotStore_Get_Hot(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("source-hot"))
	o := obj(k, 100)

	if err := s.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, src, err := s.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit")
	}
	if src != api.SourceHot {
		t.Fatalf("source = %q, want %q", src, api.SourceHot)
	}
}

func TestHotStore_Get_Miss(t *testing.T) {
	t.Parallel()
	s := NewHotStore(HotConfig{MaxBytes: 1 << 20, NumShards: 4})
	k := KeyHash([]byte("source-miss"))

	got, src, err := s.Get(context.Background(), k)
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

func TestTieredStore_Get_Hot(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, false)
	k := KeyHash([]byte("tiered-hot"))
	o := bigObj(k, 100) // below threshold, hot only

	if err := ts.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, src, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected hit")
	}
	if src != api.SourceHot {
		t.Fatalf("source = %q, want %q", src, api.SourceHot)
	}
}

func TestTieredStore_Get_Warm(t *testing.T) {
	t.Parallel()
	ts := tieredStore(t, true)
	k := KeyHash([]byte("tiered-warm"))
	o := bigObj(k, 8192) // above 1024 threshold → written to warm

	if err := ts.Put(context.Background(), k, o); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Evict from hot tier so the next Get falls through to warm.
	if err := ts.hot.Delete(context.Background(), k); err != nil {
		t.Fatalf("delete from hot: %v", err)
	}

	got, src, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("expected warm hit")
	}
	if src != api.SourceWarm {
		t.Fatalf("source = %q, want %q", src, api.SourceWarm)
	}

	// After warm hit, object is promoted to hot — second Get should
	// report SourceHot.
	got2, src2, err := ts.Get(context.Background(), k)
	if err != nil {
		t.Fatalf("second Get: %v", err)
	}
	if got2 == nil {
		t.Fatal("expected hot hit after promotion")
	}
	if src2 != api.SourceHot {
		t.Fatalf("source after promotion = %q, want %q", src2, api.SourceHot)
	}
}

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
