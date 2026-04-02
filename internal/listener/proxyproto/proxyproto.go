// Package proxyproto implements a minimal PROXY protocol v1 + v2
// parser for bouine's L1 listeners (ADR-0003). It is hand-rolled to
// stay zero-alloc on the v1 path and avoid an external dependency.
//
// The parser wraps a net.Listener so it can be inserted transparently
// below any *http.Server or http3.Server. When enabled, it peeks at
// the first bytes of every accepted connection:
//
//   - v1 ("PROXY ..."): ASCII line parsed from the peek buffer.
//   - v2 (0x0D0A0D0A...): binary header parsed from fixed-size read.
//   - Neither: connection used as-is (PROXY protocol not present).
//
// Security: unknown family/transport in v2 rejects the connection
// (strict mode). TLV extensions are skipped, not interpreted.
package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
)

var (
	// v1Prefix is the ASCII prefix for PROXY protocol v1.
	v1Prefix = []byte("PROXY ")
	// v2Signature is the 12-byte magic for PROXY protocol v2.
	v2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}
)

// Header is the parsed PROXY protocol header. Zero value means no
// header was present (pass-through).
type Header struct {
	Version    int // 1 or 2; 0 means absent
	SrcAddr    net.IP
	DstAddr    net.IP
	SrcPort    int
	DstPort    int
	Command    byte   // v2: 0=LOCAL, 1=PROXY
	TransProto string // "TCP4", "TCP6", "UDP4", "UDP6", "UNKNOWN"
}

// ErrInvalidHeader is returned when the PROXY header is malformed.
var ErrInvalidHeader = errors.New("proxyproto: invalid header")

// ReadHeader reads a PROXY protocol header from br. It peeks to
// determine the version without consuming bytes if no header is
// present.
func ReadHeader(br *bufio.Reader) (Header, error) {
	// Peek enough to detect v1 or v2.
	peek, err := br.Peek(len(v2Signature))
	if err != nil {
		return Header{}, nil //nolint:nilerr // short peek = no header present
	}

	if bytes.HasPrefix(peek, v1Prefix) {
		return readV1(br)
	}
	if bytes.Equal(peek[:len(v2Signature)], v2Signature) {
		return readV2(br)
	}
	return Header{}, nil
}

func readV1(br *bufio.Reader) (Header, error) {
	line, err := br.ReadString('\n')
	if err != nil {
		return Header{}, fmt.Errorf("%w: v1: %v", ErrInvalidHeader, err)
	}
	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	// "PROXY" proto src dst srcport dstport
	if len(parts) < 2 {
		return Header{}, fmt.Errorf("%w: v1: too few fields", ErrInvalidHeader)
	}

	h := Header{Version: 1, TransProto: parts[1]}
	if parts[1] == "UNKNOWN" {
		return h, nil
	}
	if len(parts) != 6 {
		return Header{}, fmt.Errorf("%w: v1: expected 6 fields, got %d", ErrInvalidHeader, len(parts))
	}

	h.SrcAddr = net.ParseIP(parts[2])
	h.DstAddr = net.ParseIP(parts[3])
	if h.SrcAddr == nil || h.DstAddr == nil {
		return Header{}, fmt.Errorf("%w: v1: bad IP", ErrInvalidHeader)
	}
	h.SrcPort, err = strconv.Atoi(parts[4])
	if err != nil {
		return Header{}, fmt.Errorf("%w: v1: bad src port: %v", ErrInvalidHeader, err)
	}
	h.DstPort, err = strconv.Atoi(parts[5])
	if err != nil {
		return Header{}, fmt.Errorf("%w: v1: bad dst port: %v", ErrInvalidHeader, err)
	}
	return h, nil
}

func readV2(br *bufio.Reader) (Header, error) {
	hdrBuf := make([]byte, 16)
	if err := readFull(br, hdrBuf); err != nil {
		return Header{}, fmt.Errorf("%w: v2: short header: %v", ErrInvalidHeader, err)
	}

	verCmd := hdrBuf[12]
	ver := (verCmd & 0xF0) >> 4
	if ver != 2 {
		return Header{}, fmt.Errorf("%w: v2: unsupported version %d", ErrInvalidHeader, ver)
	}
	cmd := verCmd & 0x0F
	if cmd > 1 {
		return Header{}, fmt.Errorf("%w: v2: unsupported command %d", ErrInvalidHeader, cmd)
	}

	famProto := hdrBuf[13]
	addrLen := binary.BigEndian.Uint16(hdrBuf[14:16])

	addrBuf := make([]byte, addrLen)
	if err := readFull(br, addrBuf); err != nil {
		return Header{}, fmt.Errorf("%w: v2: short addr: %v", ErrInvalidHeader, err)
	}

	h := Header{Version: 2, Command: cmd}
	return parseV2Addr(h, famProto, addrBuf)
}

func parseV2Addr(h Header, famProto byte, buf []byte) (Header, error) {
	switch famProto {
	case 0x11, 0x12: // AF_INET + STREAM/DGRAM
		if famProto == 0x11 {
			h.TransProto = "TCP4"
		} else {
			h.TransProto = "UDP4"
		}
		if len(buf) < 12 {
			return Header{}, fmt.Errorf("%w: v2: short IPv4 addr", ErrInvalidHeader)
		}
		h.SrcAddr = net.IP(buf[0:4])
		h.DstAddr = net.IP(buf[4:8])
		h.SrcPort = int(binary.BigEndian.Uint16(buf[8:10]))
		h.DstPort = int(binary.BigEndian.Uint16(buf[10:12]))
	case 0x21, 0x22: // AF_INET6 + STREAM/DGRAM
		if famProto == 0x21 {
			h.TransProto = "TCP6"
		} else {
			h.TransProto = "UDP6"
		}
		if len(buf) < 36 {
			return Header{}, fmt.Errorf("%w: v2: short IPv6 addr", ErrInvalidHeader)
		}
		h.SrcAddr = net.IP(buf[0:16])
		h.DstAddr = net.IP(buf[16:32])
		h.SrcPort = int(binary.BigEndian.Uint16(buf[32:34]))
		h.DstPort = int(binary.BigEndian.Uint16(buf[34:36]))
	case 0x00: // AF_UNSPEC
		h.TransProto = "UNKNOWN"
	default:
		return Header{}, fmt.Errorf("%w: v2: unsupported family/proto 0x%02x", ErrInvalidHeader, famProto)
	}
	return h, nil
}

func readFull(br *bufio.Reader, buf []byte) error {
	n := 0
	for n < len(buf) {
		nn, err := br.Read(buf[n:])
		n += nn
		if err != nil {
			return err
		}
	}
	return nil
}

// Conn wraps a net.Conn and transparently parses a PROXY protocol
// header from the first bytes of the stream. RemoteAddr returns the
// client address reported by the header when present; otherwise it
// falls through to the underlying connection.
//
// Stable.
type Conn struct {
	net.Conn
	br      *bufio.Reader
	src     net.Addr
	once    sync.Once
	initErr error
}

func newConn(c net.Conn) *Conn {
	return &Conn{
		Conn: c,
		br:   bufio.NewReaderSize(c, 128),
	}
}

func (c *Conn) doInit() {
	hdr, err := ReadHeader(c.br)
	if err != nil {
		c.initErr = err
		return
	}
	if hdr.Version > 0 && hdr.SrcAddr != nil {
		c.src = &net.TCPAddr{IP: hdr.SrcAddr, Port: hdr.SrcPort}
	}
}

// Read parses the PROXY header on the first call, then delegates to
// the buffered reader so no bytes consumed during header detection
// are lost.
func (c *Conn) Read(b []byte) (int, error) {
	c.once.Do(c.doInit)
	if c.initErr != nil {
		return 0, c.initErr
	}
	return c.br.Read(b)
}

// RemoteAddr returns the source address from the PROXY header when
// one is present; otherwise returns the underlying connection address.
// Note: the first call blocks briefly if bytes are not yet in the
// TCP buffer (the upstream load-balancer should send the header
// immediately on connect).
func (c *Conn) RemoteAddr() net.Addr {
	c.once.Do(c.doInit)
	if c.src != nil {
		return c.src
	}
	return c.Conn.RemoteAddr()
}

// Listener wraps a net.Listener so every accepted connection
// transparently parses a PROXY protocol v1/v2 header.
// Insert it before tls.NewListener so the header is consumed from
// the raw TCP bytes, not the TLS payload.
//
// Stable.
type Listener struct {
	net.Listener
}

// NewListener wraps ln with PROXY protocol parsing.
func NewListener(ln net.Listener) *Listener {
	return &Listener{Listener: ln}
}

// Accept returns a *Conn that parses the PROXY header on the first
// read or RemoteAddr call.
func (l *Listener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return newConn(c), nil
}
