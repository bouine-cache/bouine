package platform

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestErrPwritevUnsupported_Contract verifies the sentinel error is
// defined and distinguishable. The non-Linux stub returns this error
// so callers can fall back to sequential WriteAt without ambiguity.
func TestErrPwritevUnsupported_Contract(t *testing.T) {
	t.Parallel()
	assert.NotNil(t, ErrPwritevUnsupported, "sentinel must be defined")
	assert.True(t, errors.Is(ErrPwritevUnsupported, ErrPwritevUnsupported), "errors.Is self")
}

// TestPwritev_EmptyBuffers verifies that Pwritev with no buffers
// returns (0, nil) on Linux (the early-return guard). On non-Linux the
// stub always returns ErrPwritevUnsupported regardless of buffer count.
func TestPwritev_EmptyBuffers(t *testing.T) {
	t.Parallel()
	n, err := Pwritev(-1, nil, 0)
	if runtime.GOOS != "linux" {
		assert.Equal(t, 0, n, "non-linux empty buffers: n")
		assert.ErrorIs(t, err, ErrPwritevUnsupported, "non-linux stub always returns sentinel")
		return
	}
	assert.Equal(t, 0, n, "linux empty buffers: n")
	assert.NoError(t, err, "linux empty buffers: err")
}

// TestPwritev_NonLinuxStub verifies the non-Linux stub returns
// ErrPwritevUnsupported (not (0, nil)) when called with real buffers,
// so callers detect the fallback explicitly.
func TestPwritev_NonLinuxStub(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("test is for non-Linux stub only")
	}
	t.Parallel()
	buf := []byte("test")
	n, err := Pwritev(-1, [][]byte{buf}, 0)
	assert.Equal(t, 0, n, "non-linux stub: n")
	assert.ErrorIs(t, err, ErrPwritevUnsupported, "non-linux stub must return ErrPwritevUnsupported")
}

// TestPwritev_LinuxRealSyscall writes real buffers to a real file on
// Linux, verifying the pwritev syscall path writes all bytes in one
// call. On non-Linux this test is skipped (the stub is covered by
// TestPwritev_NonLinuxStub).
func TestPwritev_LinuxRealSyscall(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("test is for Linux pwritev syscall only")
	}
	t.Parallel()
	dir := t.TempDir()
	path := filepath.Join(dir, "pwritev.bin")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })

	hdr := []byte("HEADER")
	body := []byte("BODY-DATA")
	foot := []byte("FOOT")
	iov := [][]byte{hdr, body, foot}
	expected := len(hdr) + len(body) + len(foot)

	n, err := Pwritev(int(f.Fd()), iov, 0)
	require.NoError(t, err, "pwritev should succeed on Linux")
	assert.Equal(t, expected, n, "pwritev should write all bytes")

	buf := make([]byte, expected)
	_, err = f.ReadAt(buf, 0)
	require.NoError(t, err)
	assert.Equal(t, "HEADERBODY-DATAFOOT", string(buf), "buffer order preserved on disk")
}
