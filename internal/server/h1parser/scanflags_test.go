package h1parser

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bouine-cache/bouine/pkg/api"
)

// TestParseBuffer_ScanFlagsStaleState pins the fused-scan contract
// (W2): every ScanFlags/CacheControlRaw/ConnectionClose decision is
// re-derived per parse, so a keep-alive connection whose first request
// set flags cannot leak them into the second, and a many-header first
// request cannot leave stale headers visible past the second's
// NHeaders.
func TestParseBuffer_ScanFlagsStaleState(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("http"))

	// CL + TE together is a smuggling conflict: parseBuffer must report
	// fall-through before the fast path ever runs, and every derived
	// flag must be present for the blocking path's handlers.
	first := []byte("GET /one HTTP/1.1\r\nHost: a.example\r\nConnection: close\r\nIf-None-Match: x\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\nCache-Control: no-cache\r\n\r\n")
	second := []byte("GET /two HTTP/1.1\r\nHost: b.example\r\n\r\n")

	var scratch api.RawRequest
	req, ft, _, err := p.parseBuffer(first, len(first), &scratch)
	require.NoError(t, err)
	require.True(t, ft, "CL+TE smuggling must fall through")
	assert.True(t, req.ConnectionClose, "Connection: close derived")
	assert.NotZero(t, req.ScanFlags&api.DisqualifyFastPath, "conditional/TE disqualify")
	assert.NotZero(t, req.ScanFlags&api.FlagHasTE, "TE flag")
	assert.Equal(t, "no-cache", req.CacheControlRaw, "CC raw captured")

	// Second request on the same scratch: all flags must be re-derived
	// from its own header block, not inherited.
	req2, ft2, _, err := p.parseBuffer(second, len(second), &scratch)
	require.NoError(t, err)
	require.False(t, ft2, "plain GET qualifies")
	assert.False(t, req2.ConnectionClose, "ConnectionClose reset")
	assert.Zero(t, req2.ScanFlags&api.DisqualifyFastPath, "no disqualifying flags on a plain request")
	assert.Zero(t, req2.ScanFlags&api.FlagHasCL|req2.ScanFlags&api.FlagHasTE, "no CL/TE flags")
	assert.Empty(t, req2.CacheControlRaw, "CC raw reset")
	assert.Equal(t, "b.example", req2.Host, "host re-derived (first-Host wins within one parse)")
	require.Equal(t, 1, req2.NHeaders)
}

// TestParseBuffer_FirstHostDuplicateCacheControl pins the fused-scan
// first-vs-last semantics against regressions: first Host wins, last
// Cache-Control wins, duplicate CL saturates.
func TestParseBuffer_FirstHostDuplicateCacheControl(t *testing.T) {
	t.Parallel()
	p := New(nil, noopHandler, WithScheme("http"))
	buf := []byte("GET / HTTP/1.1\r\nHost: first.example\r\nHost: second.example\r\nCache-Control: max-age=1\r\nCache-Control: no-cache\r\n\r\n")
	var scratch api.RawRequest
	req, ft, _, err := p.parseBuffer(buf, len(buf), &scratch)
	require.NoError(t, err)
	require.False(t, ft)
	assert.Equal(t, "first.example", req.Host, "first Host wins")
	assert.Equal(t, "no-cache", req.CacheControlRaw, "last Cache-Control wins")
	// Duplicate CL saturates at 2: parse two CLs, expect the flag.
	buf2 := []byte("POST / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\nContent-Length: 2\r\n\r\n")
	var scratch2 api.RawRequest
	_, ft2, _, err2 := p.parseBuffer(buf2, len(buf2), &scratch2)
	require.NoError(t, err2)
	assert.True(t, ft2, "duplicate CL is smuggling")
	assert.NotZero(t, scratch2.ScanFlags&api.FlagDuplicateCL)
}

// TestReactor_AdvanceReadingScanResume exercises the multi-segment
// terminator scan (W6): a header block arriving in small chunks must
// parse exactly once, with the resume offset never missing a
// terminator that straddles a chunk boundary.
func TestReactor_AdvanceReadingScanResume(t *testing.T) {
	t.Parallel()
	full := "GET / HTTP/1.1\r\nHost: localhost\r\nX-Pad: " + string(bytes.Repeat([]byte{'a'}, 64)) + "\r\n\r\n"
	req := []byte(full)
	rc, fio := newTestReactorConn(t, req)

	// Deliver in 7-byte chunks; the machine alternates want-read with
	// progress until the terminator (which the chunking splits across
	// a boundary) completes the parse and the hit flushes inline.
	for i := 0; i < len(req); i += 7 {
		end := min(i+7, len(req))
		fio.readSrc = bytes.NewReader(req[i:end])
		for {
			switch rc.advance() {
			case actWaitRead:
				i = end - 7 // loop increments by 7 on continue
				goto nextChunk
			case actWaitWrite:
				continue // complete the flush with the same chunk source
			default:
				t.Fatalf("unexpected action mid-chunking: state=%d", rc.state)
			}
		}
	nextChunk:
	}
	// The hit was flushed somewhere in the loop; the machine must be
	// back to reading with the full response on the wire.
	assert.Equal(t, rcReading, rc.state, "machine returns to reading after the hit")
	assert.Contains(t, fio.written.String(), "hello", "hit body flushed")
	assert.Contains(t, fio.written.String(), "200 OK", "status line flushed")
}
