package warm

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// TestCorruption_RecoveryAfterBodyCorruption writes 5 records, corrupts
// the body of record 2, closes and reopens the store, and verifies
// that Scan returns records 0 and 1 (before the corrupt record) and
// stops at record 2 (CRC mismatch). The corrupt record must not appear
// in the scan results.
func TestCorruption_RecoveryAfterBodyCorruption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")

	for i := range 5 {
		_, _, err := s.Put(testkey.Key(uint64(i)), []byte("payload"))
		require.NoErrorf(t, err, "put %d", i)
	}

	// Corrupt the body of the record at key 2.
	// Record 2 starts at offset = 2 * (HeaderLen + 7 + FooterLen) = 2 * 35 = 70.
	// Body starts at offset + HeaderLen = 70 + 24 = 94.
	s.mu.RLock()
	seg := s.segs[0]
	s.mu.RUnlock()
	seg.mu.Lock()
	bodyOff := int64(2 * (HeaderLen + len("payload") + FooterLen))
	_, err = seg.f.WriteAt([]byte{0xFF}, bodyOff+HeaderLen)
	seg.mu.Unlock()
	require.NoError(t, err, "corrupt body")

	err = s.Close()
	require.NoError(t, err, "close after corruption")

	// Reopen and scan.
	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	var goodKeys []api.Key
	scanErr := s2.Scan(func(r Record) error {
		if !r.IsTomb {
			goodKeys = append(goodKeys, r.Key)
		}
		return nil
	})

	// scanSegment returns a CRC error on the corrupt record, stopping
	// the scan. Records 0 and 1 (before the corrupt record) are passed
	// to the callback; records 2-4 are not reached.
	require.Error(t, scanErr, "scan should error on corrupt record")
	require.Len(t, goodKeys, 2, "only records before the corrupt one should be scanned")

	// Verify exactly which records survived.
	expected := []api.Key{testkey.Key(0), testkey.Key(1)}
	for i, k := range goodKeys {
		assert.Equal(t, expected[i], k, "record %d should be key %d", i, i)
	}

	// The corrupt record (key 2) must not appear.
	for _, k := range goodKeys {
		if k == testkey.Key(2) {
			t.Fatal("corrupt record key 2 should not be in scan results")
		}
	}
}

// TestCorruption_MagicFlipToTombstone flips the magic field from
// magicLive (BOUI) to magicDead (DEAD) without updating the CRC.
// Get must return a CRC error, not silently treat the record as a
// tombstone.
func TestCorruption_MagicFlipToTombstone(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	body := []byte("integrity check")
	segID, off, err := s.Put(testkey.Key(77), body)
	require.NoError(t, err, "put")

	// Flip the magic bytes from BOUI to DEAD without updating CRC.
	s.mu.RLock()
	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == segID {
			seg = ss
			break
		}
	}
	s.mu.RUnlock()
	require.NotNil(t, seg)
	seg.mu.Lock()
	_, err = seg.f.WriteAt([]byte{0x44, 0x45, 0x41, 0x44}, off) // "DEAD"
	seg.mu.Unlock()
	require.NoError(t, err, "flip magic")

	// Get must detect the CRC mismatch, not silently return a tombstone.
	got, err := s.Get(testkey.Key(77))
	require.Error(t, err, "Get should return CRC error, not silent tombstone")
	require.Nil(t, got, "Get should return nil body on CRC error")
}

// TestCorruption_BodyLenOverflow corrupts the body_len field to a very
// large value. ReadRecord (which uses readRecordAtLegacy with size=0)
// tries to read a huge body, ReadAt fails with EOF, and returns
// (nil, nil) — treating the corrupt record as a torn trailing record.
// Get (which uses readRecordAtSingle with the known size from the
// index) allocates a buffer of the original size, reads it, detects
// the corrupt body_len via the bounds check, and also returns
// (nil, nil). The corruption is detected in both paths, and the store
// doesn't panic.
func TestCorruption_BodyLenOverflow(t *testing.T) {
	t.Parallel()
	s := tmpStore(t)

	body := []byte("short")
	segID, off, err := s.Put(testkey.Key(88), body)
	require.NoError(t, err, "put")

	// Corrupt body_len to a huge value.
	s.mu.RLock()
	var seg *Segment
	for _, ss := range s.segs {
		if ss.ID == segID {
			seg = ss
			break
		}
	}
	s.mu.RUnlock()
	require.NotNil(t, seg)
	seg.mu.Lock()
	// body_len is at offset + 20 (after magic 4 + key 16).
	hugeLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(hugeLen, 0xFFFFFFFF)
	_, err = seg.f.WriteAt(hugeLen, off+20)
	seg.mu.Unlock()
	require.NoError(t, err, "corrupt body_len")

	// ReadRecord must not panic. It detects the corruption (ReadAt
	// fails with EOF on the huge body) and returns (nil, nil) —
	// treating it as a torn record, not returning corrupt data.
	rec, err := s.ReadRecord(segID, off)
	assert.Nil(t, rec, "ReadRecord should return nil record on corruption")
	assert.NoError(t, err, "ReadRecord should not return error (torn record → nil,nil")

	// Get must also not return corrupt data. The index entry still
	// points at the corrupt record; Get reads it, hits the same
	// torn-record path, and returns nil.
	got, err := s.Get(testkey.Key(88))
	assert.NoError(t, err, "Get should not surface error on torn record")
	assert.Nil(t, got, "Get should return nil body on corrupt record")

	// The self-heal should have removed the stale index entry so
	// future Get calls don't re-read the corrupt record.
	s.idxMu.RLock()
	_, ok := s.index[testkey.Key(88)]
	s.idxMu.RUnlock()
	assert.False(t, ok, "corrupt record should be self-healed from index")
}

// TestCorruption_ReopenWithCorruptSegment writes 5 records, corrupts
// a byte in record 1 (key 20), closes, reopens, and verifies that
// Scan returns only record 0 (key 10) before hitting the CRC error.
func TestCorruption_ReopenWithCorruptSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")

	keys := []uint64{10, 20, 30, 40, 50}
	for _, k := range keys {
		_, _, err := s.Put(testkey.Key(k), []byte("data"))
		require.NoErrorf(t, err, "put %d", k)
	}

	// Corrupt a byte in the second record (record 1, key 20).
	// Each record is 32 bytes: 24 header + 4 body + 4 footer.
	// Record 1 starts at offset 32. Corrupt the body at offset 32+24=56.
	s.mu.RLock()
	seg := s.segs[0]
	s.mu.RUnlock()
	seg.mu.Lock()
	_, err = seg.f.WriteAt([]byte{0xFF}, 56)
	seg.mu.Unlock()
	require.NoError(t, err, "corrupt byte")

	err = s.Close()
	require.NoError(t, err, "close")

	// Reopen must not panic.
	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen")
	defer func() { _ = s2.Close() }()

	// Scan should return only record 0 (key 10) before hitting the
	// CRC error on record 1.
	var scannedKeys []api.Key
	scanErr := s2.Scan(func(r Record) error {
		if !r.IsTomb {
			scannedKeys = append(scannedKeys, r.Key)
		}
		return nil
	})

	require.Error(t, scanErr, "scan should error on corrupt record 1")
	require.Len(t, scannedKeys, 1, "only record 0 should be scanned before corruption")
	assert.Equal(t, testkey.Key(10), scannedKeys[0], "first record should be key 10")
}

// TestOpen_EmptySegmentFile creates a zero-length segment file
// alongside valid segments and verifies the store opens without error
// and Scan finds the valid record. An empty segment is a valid state
// (e.g. preallocated but never written to) and should not cause errors.
func TestOpen_EmptySegmentFile(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "NewStore")

	_, _, err = s.Put(testkey.Key(1), []byte("real"))
	require.NoError(t, err, "put")

	err = s.Close()
	require.NoError(t, err, "close")

	// Create an empty .seg file alongside the valid one.
	emptyPath := filepath.Join(dir, "999999"+segExt)
	f, err := os.Create(emptyPath)
	require.NoError(t, err, "create empty segment")
	_ = f.Close()

	s2, err := NewStore(Config{Dir: dir, MaxBytes: 100 << 20, SegMax: 1 << 20})
	require.NoError(t, err, "reopen with empty segment")
	defer func() { _ = s2.Close() }()

	// Scan should find the valid record and skip the empty segment.
	var found int
	err = s2.Scan(func(r Record) error {
		if !r.IsTomb {
			found++
		}
		return nil
	})
	require.NoError(t, err, "scan with empty segment")
	require.Equal(t, 1, found, "should find the one valid record")
}
