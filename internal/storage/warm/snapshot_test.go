package warm

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")

	keys := []uint64{1, 42, 100, 999, 12345}
	for _, k := range keys {
		body := []byte("value-for-key")
		_, _, err := s.Put(testkey.From(k), body)
		require.NoErrorf(t, err, "put %d", k)
	}

	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot")

	snapPath := s.SnapshotPath()
	require.NotEqual(t, "", snapPath)
	_, err = os.Stat(snapPath)
	require.NoError(t, err, "snapshot file not created")

	err = s.Close()
	require.NoError(t, err, "close")

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.NoError(t, err, "loadSnapshot")

	got := s2.IndexLen()
	require.Len(t, keys, got)

	for _, k := range keys {
		body, gErr := s2.Get(testkey.From(k))
		require.Nil(t, gErr)
		require.NotNil(t, body)
		require.Equal(t, "value-for-key", string(body))
	}

	entries, bytes := s2.Stats()
	require.Equal(t, int64(len(keys)), entries)
	if bytes <= 0 {
		t.Fatalf("stats bytes = %d, want > 0", bytes)
	}
}

func TestSnapshotCorruptHeaderMagic(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")
	_, _, err = s.Put(testkey.From(1), []byte("data"))
	require.NoError(t, err, "put")
	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot")
	snapPath := s.SnapshotPath()
	_ = s.Close()

	data, err := os.ReadFile(snapPath)
	require.NoError(t, err, "read snapshot")
	binary.LittleEndian.PutUint32(data[0:4], 0xDEADBEEF)
	err = os.WriteFile(snapPath, data, 0o600)
	require.NoError(t, err, "write corrupt snapshot")

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.Error(t, err)
}

func TestSnapshotCorruptFooterCRC(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")
	_, _, err = s.Put(testkey.From(1), []byte("data"))
	require.NoError(t, err, "put")
	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot")
	snapPath := s.SnapshotPath()
	_ = s.Close()

	data, err := os.ReadFile(snapPath)
	require.NoError(t, err, "read snapshot")
	footerOff := len(data) - snapFooterLen
	binary.LittleEndian.PutUint32(data[footerOff:footerOff+4], 0xDEADBEEF)
	err = os.WriteFile(snapPath, data, 0o600)
	require.NoError(t, err, "write corrupt snapshot")

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.Error(t, err)
}

func TestSnapshotMissingSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")
	_, _, err = s.Put(testkey.From(1), []byte("data"))
	require.NoError(t, err, "put")
	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot")
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
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.Error(t, err)
}

func TestSnapshotSegmentSizeMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")
	_, _, err = s.Put(testkey.From(1), []byte("data"))
	require.NoError(t, err, "put")
	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot")
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
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.Error(t, err)
}

func TestSnapshotSegmentGrewAfterCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")
	_, _, err = s.Put(testkey.From(1), []byte("initial"))
	require.NoError(t, err, "put")
	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot")
	snapPath := s.SnapshotPath()

	// Simulate writes that happen after the checkpoint: append more
	// data to the same segment so its on-disk size grows beyond the
	// snapshot size. The WAL would normally capture these writes.
	_, _, err = s.Put(testkey.From(2), []byte("appended-data"))
	require.NoError(t, err, "put after snapshot")

	// Simulate a hard kill (SIGKILL) — close the file handles without
	// writing a new snapshot. The on-disk snapshot still reflects
	// only key 1, but the segment files contain both keys.
	s.mu.RLock()
	segs := make([]*Segment, len(s.segs))
	copy(segs, s.segs)
	s.mu.RUnlock()
	for _, seg := range segs {
		_ = seg.Close()
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	// The snapshot should load successfully despite segments being
	// larger on disk — the WAL replay applies the delta on top.
	err = s2.LoadSnapshot(snapPath)
	require.NoError(t, err, "loadSnapshot should succeed when segments grew")
	got := s2.IndexLen()
	require.Equal(t, 1, got)
}

func TestSnapshotCloseWritesSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")
	for i := range 10 {
		_, _, err := s.Put(testkey.From(uint64(i+1)), []byte("val"))
		require.NoErrorf(t, err, "put %d", i+1)
	}
	err = s.Close()
	require.NoError(t, err, "close")

	snapPath := filepath.Join(dir, snapFile)
	_, err = os.Stat(snapPath)
	require.NoError(t, err, "snapshot not written on close")

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.NoError(t, err, "loadSnapshot")
	got := s2.IndexLen()
	require.Equal(t, 10, got)
}

func TestSnapshotEmptyIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "new")

	err = s.WriteSnapshot()
	require.NoError(t, err, "writeSnapshot empty")

	snapPath := s.SnapshotPath()
	_ = s.Close()

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	err = s2.LoadSnapshot(snapPath)
	require.NoError(t, err, "loadSnapshot empty")
	got := s2.IndexLen()
	require.Equal(t, 0, got)
}

func TestSegmentCloseOnUnopened(t *testing.T) {
	t.Parallel()
	seg := &Segment{
		ID:   0,
		Path: "/nonexistent/path",
	}
	err := seg.Close()
	require.NoError(t, err, "Close on unopened segment should return nil,")
}

// TestSnapshotConcurrentWithCompact reproduces the data race between
// WriteSnapshotFromCopy (iterating segByID after releasing s.mu.RLock)
// and rebuildSegByID (mutating segByID under s.mu.Lock). Under
// -race, the pre-fix code fatals with "concurrent map read and map
// write".
func TestSnapshotConcurrentWithCompact(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 4 << 10})
	require.NoError(t, err, "new")
	defer func() { _ = s.Close() }()

	for i := range 200 {
		_, _, err := s.Put(testkey.From(uint64(i)), make([]byte, 1024))
		require.NoErrorf(t, err, "put %d", i)
	}

	// rebuildSegByID mutates segByID under s.mu.Lock (clear + insert).
	// WriteSnapshotFromCopy reads segByID under s.mu.RLock then iterates
	// the map after releasing the lock. Calling rebuildSegByID in a
	// tight loop (no I/O) maximises the race window against the
	// snapshot's lock-free iteration phase.
	var wg sync.WaitGroup
	const snapGoroutines = 2
	const snapIters = 50
	const rebuildIters = 500

	for range snapGoroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range snapIters {
				_ = s.WriteSnapshot()
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range rebuildIters {
			s.mu.Lock()
			s.rebuildSegByID()
			s.mu.Unlock()
		}
	}()
	wg.Wait()
}
