package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/testutil/testkey"
)

// TestReplay_MidRecordCorruption writes three entries, corrupts a byte
// in the middle of the second entry, and verifies that replay returns
// the first entry and stops at the corrupt one (CRC mismatch → return
// nil). This pins the existing behavior: a corrupt WAL record means
// the rest of the file is untrustworthy and replay stops.
func TestReplay_MidRecordCorruption(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)

	entries := []Entry{
		PutEntry(testkey.Key(1), 0, 0),
		PutEntry(testkey.Key(2), 0, 100),
		PutEntry(testkey.Key(3), 0, 200),
	}
	for _, e := range entries {
		err := l.Append(e)
		require.NoError(t, err, "append")
	}
	_ = l.Close()

	// Corrupt a byte in the middle of the second record (offset = 33 + 10).
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	_, err = f.WriteAt([]byte{0xFF}, int64(recLen+10))
	_ = f.Close()
	require.NoError(t, err, "corrupt byte")

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay should not return error on corruption")
	// Only the first entry (before the corrupt one) should be replayed.
	require.Len(t, replayed, 1, "only the first entry should replay")
	assert.Equal(t, testkey.Key(1), replayed[0].Key)
}

// TestReplay_V2TruncatedToV1Length writes a v2 (+size) entry, truncates
// the file to v1 length (33 bytes), and verifies replay reads it as a
// v1 entry without crashing. The op byte check (opPutV2 vs opPut)
// determines record width; a truncated v2 record reads as v1 because
// the op byte is intact and the remaining 8 bytes (size field) are gone.
func TestReplay_V2TruncatedToV1Length(t *testing.T) {
	t.Parallel()
	l, path := tmpWAL(t)

	// Write a v2 (+size) entry (41 bytes).
	err := l.Append(PutEntryWithSize(testkey.Key(42), 5, 1000, 512))
	require.NoError(t, err, "append v2")
	_ = l.Close()

	// Truncate to v1 length (33 bytes) — chop off the 8-byte size field
	// and the 4-byte CRC, leaving the op byte + key + segID + offset.
	// The op byte is opPutV2 (3), but only 33 bytes are available.
	// Replay reads 33 bytes, sees op=3, tries to read 8 more bytes,
	// gets EOF, and returns nil (torn record).
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	require.NoError(t, err)
	err = f.Truncate(int64(recLen))
	_ = f.Close()
	require.NoError(t, err, "truncate to v1 length")

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay should not error on truncated v2")
	// The v2 record is truncated, so replay skips it (torn tail).
	require.Empty(t, replayed, "truncated v2 record should not be replayed")
}

// TestReplay_CorruptCRCStopsReplay writes two valid entries, then
// appends a third entry with a corrupted CRC, and verifies replay
// returns the first two entries and stops at the corrupt one.
func TestReplay_CorruptCRCStopsReplay(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "corrupt_crc.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syncFlag, 0o600)
	require.NoError(t, err, "open")

	// Write two valid entries manually.
	for _, e := range []Entry{
		PutEntry(testkey.Key(1), 0, 0),
		PutEntry(testkey.Key(2), 0, 100),
	} {
		buf := encodeEntry(e)
		_, err := f.Write(buf)
		entryBufPool.Put(&buf)
		require.NoError(t, err, "write entry")
	}

	// Write a third entry with a corrupted CRC.
	e3 := PutEntry(testkey.Key(3), 0, 200)
	buf := encodeEntry(e3)
	// Corrupt the CRC bytes (last 4 bytes of the 33-byte record).
	binary.LittleEndian.PutUint32(buf[29:33], 0xDEADBEEF)
	_, err = f.Write(buf)
	entryBufPool.Put(&buf)
	require.NoError(t, err, "write corrupt entry")
	_ = f.Close()

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay should not error on corrupt CRC")
	// First two entries are valid; the third has a corrupt CRC and
	// replay stops (returns nil).
	require.Len(t, replayed, 2, "only valid entries should replay")
	assert.Equal(t, testkey.Key(1), replayed[0].Key)
	assert.Equal(t, testkey.Key(2), replayed[1].Key)
}

// TestReplay_PartialV2Entry writes a valid v1 entry followed by a
// partial v2 entry (only the first few bytes of the v2 record), and
// verifies replay returns the v1 entry and skips the partial v2 tail.
func TestReplay_PartialV2Entry(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "partial_v2.wal")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND|syncFlag, 0o600)
	require.NoError(t, err, "open")

	// Write a valid v1 entry.
	buf := encodeEntry(PutEntry(testkey.Key(1), 0, 0))
	_, err = f.Write(buf)
	entryBufPool.Put(&buf)
	require.NoError(t, err, "write v1 entry")

	// Write a partial v2 entry: op byte + key (17 bytes of 41).
	_, err = f.Write([]byte{opPutV2, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00})
	require.NoError(t, err, "write partial v2")
	_ = f.Close()

	var replayed []Entry
	err = Replay(path, func(e Entry) error {
		replayed = append(replayed, e)
		return nil
	})
	require.NoError(t, err, "replay should not error on partial v2")
	require.Len(t, replayed, 1, "only the valid v1 entry should replay")
	assert.Equal(t, testkey.Key(1), replayed[0].Key)
}
