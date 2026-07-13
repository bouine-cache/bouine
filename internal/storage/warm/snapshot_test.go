package warm

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	keys := []uint64{1, 42, 100, 999, 12345}
	for _, k := range keys {
		body := []byte("value-for-key")
		if _, _, err := s.Put(k, body); err != nil {
			t.Fatalf("put %d: %v", k, err)
		}
	}

	if err := s.WriteSnapshot(); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}

	snapPath := s.SnapshotPath()
	if snapPath == "" {
		t.Fatal("empty snapshot path")
	}
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("snapshot file not created: %v", err)
	}

	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}

	if got := s2.IndexLen(); got != len(keys) {
		t.Fatalf("index len = %d, want %d", got, len(keys))
	}

	for _, k := range keys {
		body, gErr := s2.Get(k)
		if gErr != nil {
			t.Fatalf("get %d: %v", k, gErr)
		}
		if body == nil {
			t.Fatalf("key %d not found after snapshot load", k)
		}
		if string(body) != "value-for-key" {
			t.Fatalf("key %d body = %q", k, string(body))
		}
	}

	entries, bytes := s2.Stats()
	if entries != int64(len(keys)) {
		t.Fatalf("stats entries = %d, want %d", entries, len(keys))
	}
	if bytes <= 0 {
		t.Fatalf("stats bytes = %d, want > 0", bytes)
	}
}

func TestSnapshotCorruptHeaderMagic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, _, err := s.Put(1, []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.WriteSnapshot(); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()
	_ = s.Close()

	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	binary.LittleEndian.PutUint32(data[0:4], 0xDEADBEEF)
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("write corrupt snapshot: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err == nil {
		t.Fatal("expected error for corrupt header magic")
	}
}

func TestSnapshotCorruptFooterCRC(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, _, err := s.Put(1, []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.WriteSnapshot(); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()
	_ = s.Close()

	data, err := os.ReadFile(snapPath)
	if err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	footerOff := len(data) - snapFooterLen
	binary.LittleEndian.PutUint32(data[footerOff:footerOff+4], 0xDEADBEEF)
	if err := os.WriteFile(snapPath, data, 0o600); err != nil {
		t.Fatalf("write corrupt snapshot: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err == nil {
		t.Fatal("expected error for corrupt footer CRC")
	}
}

func TestSnapshotMissingSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, _, err := s.Put(1, []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.WriteSnapshot(); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()

	s.mu.RLock()
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()
	_ = s.Close()

	for _, seg := range segs {
		_ = os.Remove(seg.Path)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err == nil {
		t.Fatal("expected error for missing segment")
	}
}

func TestSnapshotSegmentSizeMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, _, err := s.Put(1, []byte("data")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := s.WriteSnapshot(); err != nil {
		t.Fatalf("writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()

	s.mu.RLock()
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()
	_ = s.Close()

	for _, seg := range segs {
		if err := os.Truncate(seg.Path, 10); err != nil {
			t.Logf("truncate %s: %v", seg.Path, err)
		}
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err == nil {
		t.Fatal("expected error for segment size mismatch")
	}
}

func TestSnapshotCloseWritesSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	for i := range 10 {
		if _, _, err := s.Put(uint64(i+1), []byte("val")); err != nil {
			t.Fatalf("put %d: %v", i+1, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	snapPath := filepath.Join(dir, snapFile)
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("snapshot not written on close: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err != nil {
		t.Fatalf("loadSnapshot: %v", err)
	}
	if got := s2.IndexLen(); got != 10 {
		t.Fatalf("index len = %d, want 10", got)
	}
}

func TestSnapshotEmptyIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	if err := s.WriteSnapshot(); err != nil {
		t.Fatalf("writeSnapshot empty: %v", err)
	}

	snapPath := s.SnapshotPath()
	_ = s.Close()

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer func() { _ = s2.Close() }()

	if err := s2.LoadSnapshot(snapPath); err != nil {
		t.Fatalf("loadSnapshot empty: %v", err)
	}
	if got := s2.IndexLen(); got != 0 {
		t.Fatalf("index len = %d, want 0", got)
	}
}

func TestSegmentCloseOnUnopened(t *testing.T) {
	t.Parallel()
	seg := &Segment{
		ID:   0,
		Path: "/nonexistent/path",
	}
	if err := seg.Close(); err != nil {
		t.Fatalf("Close on unopened segment should return nil, got %v", err)
	}
}
