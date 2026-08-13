package warm

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/internal/platform"
	"github.com/bouine-cache/bouine/internal/testutil/testkey"
	"github.com/bouine-cache/bouine/pkg/api"
)

// withPwritevFn temporarily replaces pwritevFn for the duration of the
// test, restoring the original (platform.Pwritev) on cleanup. This lets
// tests simulate short writes and I/O errors without a real pwritev.
func withPwritevFn(t *testing.T, fn func(fd int, buffers [][]byte, offset int64) (int, error)) {
	t.Helper()
	orig := pwritevFn
	pwritevFn = fn
	t.Cleanup(func() { pwritevFn = orig })
}

// errSimulatedIO is a sentinel I/O error used by short-write tests to
// simulate a failing device without depending on platform-specific
// errno values.
var errSimulatedIO = errors.New("simulated I/O error")

// verifyRecord reads the full record from f at offset and asserts
// that every field (magic, key, bodyLen, body, CRC) is correct and
// that the record is fully on disk. Shared by all writeRecordAt tests.
func verifyRecord(t *testing.T, f *os.File, offset int64, wantMagic uint32, wantKey api.Key, wantBody []byte) {
	t.Helper()
	totalSize := int64(HeaderLen + len(wantBody) + FooterLen)
	buf := make([]byte, totalSize)
	n, err := f.ReadAt(buf, offset)
	require.NoError(t, err, "read back")
	require.Equal(t, int(totalSize), n, "expected full record on disk")

	magic := binary.LittleEndian.Uint32(buf[0:4])
	assert.Equal(t, wantMagic, magic, "magic")
	var gotKey api.Key
	copy(gotKey[:], buf[4:20])
	assert.Equal(t, wantKey, gotKey, "key")
	bodyLen := binary.LittleEndian.Uint32(buf[20:24])
	assert.Equal(t, uint32(len(wantBody)), bodyLen, "body length")
	if len(wantBody) > 0 {
		assert.Equal(t, wantBody, buf[HeaderLen:HeaderLen+len(wantBody)], "body")
	}

	storedCRC := binary.LittleEndian.Uint32(buf[totalSize-FooterLen:])
	expectedCRC := crc32.Checksum(buf[:totalSize-FooterLen], crcTable)
	assert.Equal(t, expectedCRC, storedCRC, "CRC")
}

// TestWriteRecordAt_HappyPath verifies the normal full-write path:
// pwritev writes all bytes, writeRecordAt returns nil immediately.
func TestWriteRecordAt_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seg.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	// Use the real pwritev (default) — on macOS this returns
	// ErrPwritevUnsupported and falls back to sequential WriteAt.
	key := testkey.Key(7)
	body := []byte("happy path body")
	err = writeRecordAt(f, 0, magicLive, key, body)
	require.NoError(t, err, "writeRecordAt happy path")
	verifyRecord(t, f, 0, magicLive, key, body)
}

// TestWriteRecordAt_ShortWriteCompletes simulates pwritev writing only
// a prefix of the iovec (n=5, nil). writeRecordAt must complete the
// remaining bytes via sequential WriteAt and return nil — the record
// must be fully on disk with a valid CRC.
func TestWriteRecordAt_ShortWriteCompletes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "short.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	key := testkey.Key(42)
	body := []byte("short-write completion body")

	// Simulate pwritev writing 5 bytes (part of the header) then
	// returning nil. writeRecordAt must write the remaining bytes.
	withPwritevFn(t, func(fd int, buffers [][]byte, offset int64) (int, error) {
		// Write the first 5 bytes to the file to simulate the kernel
		// committing them, then return n=5.
		_, werr := f.WriteAt(buffers[0][:5], offset)
		require.NoError(t, werr)
		return 5, nil
	})

	err = writeRecordAt(f, 0, magicLive, key, body)
	require.NoError(t, err, "writeRecordAt should complete the full record after short write")
	verifyRecord(t, f, 0, magicLive, key, body)
}

// TestWriteRecordAt_ShortWriteWithErrorCompletes simulates pwritev
// writing a prefix then returning an I/O error. writeRecordAt must
// still complete the record (data integrity: no torn record) and
// return nil — the caller must advance seg.size. Returning an error
// after a completed write would cause seg.size desync.
func TestWriteRecordAt_ShortWriteWithErrorCompletes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "short_err.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	key := testkey.Key(99)
	body := []byte("body despite pwritev error")

	// Simulate pwritev writing 10 bytes then returning EIO.
	withPwritevFn(t, func(fd int, buffers [][]byte, offset int64) (int, error) {
		_, werr := f.WriteAt(buffers[0][:10], offset)
		require.NoError(t, werr)
		return 10, errSimulatedIO
	})

	err = writeRecordAt(f, 0, magicLive, key, body)
	require.NoError(t, err, "record must be completed despite pwritev error; caller must advance seg.size")
	verifyRecord(t, f, 0, magicLive, key, body)
}

// TestWriteRecordAt_NonLinuxFallback verifies the ErrPwritevUnsupported
// path: pwritev returns (0, ErrPwritevUnsupported), writeRecordAt
// writes the full record sequentially and returns nil.
func TestWriteRecordAt_NonLinuxFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "fallback.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	key := testkey.Key(1)
	body := []byte("non-linux fallback")

	// Simulate the non-Linux stub: always returns ErrPwritevUnsupported.
	withPwritevFn(t, func(fd int, buffers [][]byte, offset int64) (int, error) {
		return 0, platform.ErrPwritevUnsupported
	})

	err = writeRecordAt(f, 0, magicLive, key, body)
	require.NoError(t, err, "sequential fallback should succeed")
	verifyRecord(t, f, 0, magicLive, key, body)
}

// TestWriteRecordAt_EmptyBody verifies the short-write completion path
// handles records with no body (tombstone-style writes).
func TestWriteRecordAt_EmptyBody(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	key := testkey.Key(42)
	err = writeRecordAt(f, 0, magicDead, key, nil)
	require.NoError(t, err)
	verifyRecord(t, f, 0, magicDead, key, nil)
}

// TestWriteRecordAt_NonZeroOffset verifies writeRecordAt and verifyRecord
// handle a non-zero offset — the real segment append path writes at
// seg.size, not 0. Guards against hardcoded-offset regressions in
// verifyRecord.
func TestWriteRecordAt_NonZeroOffset(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "offset.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	// Pre-fill the first 64 bytes so the record does not start at 0.
	prefix := make([]byte, 64)
	_, err = f.WriteAt(prefix, 0)
	require.NoError(t, err)

	offset := int64(64)
	key := testkey.Key(13)
	body := []byte("non-zero offset body")
	err = writeRecordAt(f, offset, magicLive, key, body)
	require.NoError(t, err, "writeRecordAt at non-zero offset")
	verifyRecord(t, f, offset, magicLive, key, body)
}

// TestWriteRemaining_FullFallback verifies writeRemaining writes all
// buffers when alreadyWritten is 0 (the non-Linux / zero-progress path).
func TestWriteRemaining_FullFallback(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "full.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	hdr := []byte("HEADER")
	body := []byte("BODY-DATA")
	foot := []byte("FOOT")
	iov := [][]byte{hdr, body, foot}
	err = writeRemaining(f.WriteAt, 0, iov, 0)
	require.NoError(t, err)

	total := len(hdr) + len(body) + len(foot)
	buf := make([]byte, total)
	n, err := f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, total, n, "all bytes written")
	assert.Equal(t, "HEADERBODY-DATAFOOT", string(buf), "buffer order preserved")
}

// TestWriteRemaining_PartialFirstBuffer verifies writeRemaining correctly
// skips the first buffer when it was fully written and writes the rest.
func TestWriteRemaining_PartialFirstBuffer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "partial.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	hdr := []byte("HEADER") // 6 bytes
	body := []byte("BODY")  // 4 bytes
	foot := []byte("FOOT")  // 4 bytes
	iov := [][]byte{hdr, body, foot}

	// Simulate Pwritev wrote 8 bytes (all of hdr + first 2 of body).
	alreadyWritten := 8
	err = writeRemaining(f.WriteAt, int64(alreadyWritten), iov, alreadyWritten)
	require.NoError(t, err)

	total := len(hdr) + len(body) + len(foot)
	buf := make([]byte, total)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 0, 0, 0, 0, 0}, buf[0:8], "prefix untouched by writeRemaining")
	assert.Equal(t, "DYFOOT", string(buf[8:]), "remaining body tail + foot written sequentially")
}

// TestWriteRemaining_PartialSecondBuffer simulates Pwritev writing part
// of the body buffer (first buffer fully written, body partially written).
func TestWriteRemaining_PartialSecondBuffer(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "partial2.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	hdr := []byte("HDR")       // 3 bytes
	body := []byte("BODYDATA") // 8 bytes
	foot := []byte("FOOT")     // 4 bytes
	iov := [][]byte{hdr, body, foot}

	// Pwritev wrote 5 bytes: all of hdr (3) + first 2 of body ("BO").
	alreadyWritten := 5
	err = writeRemaining(f.WriteAt, int64(alreadyWritten), iov, alreadyWritten)
	require.NoError(t, err)

	total := len(hdr) + len(body) + len(foot)
	buf := make([]byte, total)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, []byte{0, 0, 0, 0, 0}, buf[0:5], "Pwritev prefix untouched")
	assert.Equal(t, "DYDATAFOOT", string(buf[5:]), "remaining body + foot written sequentially")
}

// TestWriteRemaining_WriteAtError verifies the WriteAt error path: when
// writeAt returns an error mid-completion, writeRemaining returns it
// immediately without retrying. Covers the werr != nil branch.
func TestWriteRemaining_WriteAtError(t *testing.T) {
	t.Parallel()
	iov := [][]byte{[]byte("HDR"), []byte("BODY"), []byte("FOOT")}

	writeAt := func([]byte, int64) (int, error) {
		return 0, errSimulatedIO
	}
	err := writeRemaining(writeAt, 0, iov, 0)
	require.ErrorIs(t, err, errSimulatedIO, "writeRemaining must surface the WriteAt error")
}

// TestWriteRemaining_WriteAtZeroNoError verifies the (0, nil) guard: if
// writeAt returns zero bytes without an error, writeRemaining must fail
// rather than loop forever. Covers the nw == 0 branch.
func TestWriteRemaining_WriteAtZeroNoError(t *testing.T) {
	t.Parallel()
	iov := [][]byte{[]byte("HDR"), []byte("BODY"), []byte("FOOT")}

	writeAt := func([]byte, int64) (int, error) {
		return 0, nil
	}
	err := writeRemaining(writeAt, 0, iov, 0)
	require.Error(t, err, "writeRemaining must fail on a (0, nil) writeAt return")
	assert.Contains(t, err.Error(), "returned 0 bytes without error")
}

// TestWriteRemaining_WriteAtShortWrite verifies the short-write retry
// loop: writeAt returns partial writes, and writeRemaining must keep
// retrying until the full buffer is on disk.
func TestWriteRemaining_WriteAtShortWrite(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "shortwriteat.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	hdr := []byte("HDR")
	body := []byte("BODYDATA")
	foot := []byte("FOOT")
	iov := [][]byte{hdr, body, foot}

	total := len(hdr) + len(body) + len(foot)
	written := 0
	// writeAt writes 1 byte at a time to exercise the retry loop.
	writeAt := func(p []byte, off int64) (int, error) {
		n, werr := f.WriteAt(p[:1], off)
		written += n
		return n, werr
	}
	err = writeRemaining(writeAt, 0, iov, 0)
	require.NoError(t, err)
	assert.Equal(t, total, written, "all bytes written one at a time")

	buf := make([]byte, total)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, "HDRBODYDATAFOOT", string(buf), "full record on disk via short writes")
}
