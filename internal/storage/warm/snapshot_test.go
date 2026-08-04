package warm

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSnapshotRoundTrip(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)

	keys := []uint64{1, 42, 100, 999, 12345}
	for _, k := range keys {
		body := []byte("value-for-key")
		{
			_, _, err := s.Put(k, body)
			require.NoErrorf(t, err, "put %d: %v", k, err)
		}
	}

	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot: %v", err)
	}

	snapPath := s.SnapshotPath()
	require.NotEqual(t, "", snapPath)
	{
		_, err := os.Stat(snapPath)
		require.NoErrorf(t, err, "snapshot file not created: %v", err)
	}

	{
		err := s.Close()
		require.NoErrorf(t, err, "close: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.NoErrorf(t, err, "loadSnapshot: %v", err)
	}

	{
		got := s2.IndexLen()
		require.Len(t, keys, got)
	}

	for _, k := range keys {
		body, gErr := s2.Get(k)
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
	require.NoErrorf(t, err, "new: %v", err)
	{
		_, _, err := s.Put(1, []byte("data"))
		require.NoErrorf(t, err, "put: %v", err)
	}
	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()
	_ = s.Close()

	data, err := os.ReadFile(snapPath)
	require.NoErrorf(t, err, "read snapshot: %v", err)
	binary.LittleEndian.PutUint32(data[0:4], 0xDEADBEEF)
	{
		err := os.WriteFile(snapPath, data, 0o600)
		require.NoErrorf(t, err, "write corrupt snapshot: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.Error(t, err)
	}
}

func TestSnapshotCorruptFooterCRC(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)
	{
		_, _, err := s.Put(1, []byte("data"))
		require.NoErrorf(t, err, "put: %v", err)
	}
	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()
	_ = s.Close()

	data, err := os.ReadFile(snapPath)
	require.NoErrorf(t, err, "read snapshot: %v", err)
	footerOff := len(data) - snapFooterLen
	binary.LittleEndian.PutUint32(data[footerOff:footerOff+4], 0xDEADBEEF)
	{
		err := os.WriteFile(snapPath, data, 0o600)
		require.NoErrorf(t, err, "write corrupt snapshot: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.Error(t, err)
	}
}

func TestSnapshotMissingSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)
	{
		_, _, err := s.Put(1, []byte("data"))
		require.NoErrorf(t, err, "put: %v", err)
	}
	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot: %v", err)
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
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.Error(t, err)
	}
}

func TestSnapshotSegmentSizeMismatch(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)
	{
		_, _, err := s.Put(1, []byte("data"))
		require.NoErrorf(t, err, "put: %v", err)
	}
	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot: %v", err)
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
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.Error(t, err)
	}
}

func TestSnapshotSegmentGrewAfterCheckpoint(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)
	{
		_, _, err := s.Put(1, []byte("initial"))
		require.NoErrorf(t, err, "put: %v", err)
	}
	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot: %v", err)
	}
	snapPath := s.SnapshotPath()

	// Simulate writes that happen after the checkpoint: append more
	// data to the same segment so its on-disk size grows beyond the
	// snapshot size. The WAL would normally capture these writes.
	{
		_, _, err := s.Put(2, []byte("appended-data"))
		require.NoErrorf(t, err, "put after snapshot: %v", err)
	}

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
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	// The snapshot should load successfully despite segments being
	// larger on disk — the WAL replay applies the delta on top.
	{
		err := s2.LoadSnapshot(snapPath)
		require.NoErrorf(t, err, "loadSnapshot should succeed when segments grew: %v", err)
	}
	{
		got := s2.IndexLen()
		require.Equal(t, 1, got)
	}
}

func TestSnapshotCloseWritesSnapshot(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)
	for i := range 10 {
		{
			_, _, err := s.Put(uint64(i+1), []byte("val"))
			require.NoErrorf(t, err, "put %d: %v", i+1, err)
		}
	}
	{
		err := s.Close()
		require.NoErrorf(t, err, "close: %v", err)
	}

	snapPath := filepath.Join(dir, snapFile)
	{
		_, err := os.Stat(snapPath)
		require.NoErrorf(t, err, "snapshot not written on close: %v", err)
	}

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.NoErrorf(t, err, "loadSnapshot: %v", err)
	}
	{
		got := s2.IndexLen()
		require.Equal(t, 10, got)
	}
}

func TestSnapshotEmptyIndex(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "new: %v", err)

	{
		err := s.WriteSnapshot()
		require.NoErrorf(t, err, "writeSnapshot empty: %v", err)
	}

	snapPath := s.SnapshotPath()
	_ = s.Close()

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoErrorf(t, err, "reopen: %v", err)
	defer func() { _ = s2.Close() }()

	{
		err := s2.LoadSnapshot(snapPath)
		require.NoErrorf(t, err, "loadSnapshot empty: %v", err)
	}
	{
		got := s2.IndexLen()
		require.Equal(t, 0, got)
	}
}

func TestSegmentCloseOnUnopened(t *testing.T) {
	t.Parallel()
	seg := &Segment{
		ID:   0,
		Path: "/nonexistent/path",
	}
	{
		err := seg.Close()
		require.NoErrorf(t, err, "Close on unopened segment should return nil, got %v", err)
	}
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
	require.NoErrorf(t, err, "new: %v", err)
	defer func() { _ = s.Close() }()

	for i := range 200 {
		{
			_, _, err := s.Put(uint64(i), make([]byte, 1024))
			require.NoErrorf(t, err, "put %d: %v", i, err)
		}
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
