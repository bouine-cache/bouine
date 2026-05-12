package storage

import (
	"context"
	"testing"

	"github.com/thylong/bouine/pkg/api"
)

func BenchmarkHotStore_Get_Hit(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})
	k := KeyHash([]byte("bench-hit"))
	_ = s.Put(context.Background(), k, obj(k, 1024))

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = s.Get(context.Background(), k)
	}
}

func BenchmarkHotStore_Get_Miss(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = s.Get(context.Background(), 0xDEADBEEF)
	}
}

func BenchmarkHotStore_Put(b *testing.B) {
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 16})

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		k := api.Key(i)
		_ = s.Put(context.Background(), k, obj(k, 1024))
	}
}

func BenchmarkHotStore_Put_Eviction(b *testing.B) {
	// Tight budget forces eviction on every put after warmup.
	s := NewHotStore(HotConfig{MaxBytes: 8192, NumShards: 4})
	// Fill up.
	for i := range 100 {
		k := api.Key(i)
		_ = s.Put(context.Background(), k, obj(k, 512))
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := range b.N {
		k := api.Key(i + 10000)
		_ = s.Put(context.Background(), k, obj(k, 512))
	}
}

func BenchmarkSIEVE_Access(b *testing.B) {
	// Benchmark the SIEVE access path in isolation.
	s := NewHotStore(HotConfig{MaxBytes: 256 << 20, NumShards: 1})
	k := KeyHash([]byte("sieve-bench"))
	_ = s.Put(context.Background(), k, obj(k, 64))

	b.ResetTimer()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = s.Get(context.Background(), k)
	}
}
