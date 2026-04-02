package proxyproto

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"strings"
	"testing"
)

func TestV1_TCP4(t *testing.T) {
	line := "PROXY TCP4 192.168.1.1 10.0.0.1 56789 80\r\n"
	br := bufio.NewReader(strings.NewReader(line))
	h, err := ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Version != 1 || h.TransProto != "TCP4" {
		t.Fatalf("unexpected header: %+v", h)
	}
	if !h.SrcAddr.Equal(net.ParseIP("192.168.1.1")) {
		t.Fatalf("SrcAddr = %v", h.SrcAddr)
	}
	if !h.DstAddr.Equal(net.ParseIP("10.0.0.1")) {
		t.Fatalf("DstAddr = %v", h.DstAddr)
	}
	if h.SrcPort != 56789 || h.DstPort != 80 {
		t.Fatalf("ports = %d/%d", h.SrcPort, h.DstPort)
	}
}

func TestV1_TCP6(t *testing.T) {
	line := "PROXY TCP6 ::1 ::2 1234 5678\r\n"
	br := bufio.NewReader(strings.NewReader(line))
	h, err := ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Version != 1 || h.TransProto != "TCP6" {
		t.Fatalf("unexpected: %+v", h)
	}
}

func TestV1_Unknown(t *testing.T) {
	line := "PROXY UNKNOWN\r\n"
	br := bufio.NewReader(strings.NewReader(line))
	h, err := ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Version != 1 || h.TransProto != "UNKNOWN" {
		t.Fatalf("unexpected: %+v", h)
	}
}

func TestV1_MalformedPort(t *testing.T) {
	line := "PROXY TCP4 1.2.3.4 5.6.7.8 abc 80\r\n"
	br := bufio.NewReader(strings.NewReader(line))
	_, err := ReadHeader(br)
	if err == nil {
		t.Fatal("expected error on bad port")
	}
}

func TestV2_TCP4(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(v2Signature)
	buf.WriteByte(0x21) // version 2, PROXY command
	buf.WriteByte(0x11) // AF_INET + STREAM
	addrLen := make([]byte, 2)
	binary.BigEndian.PutUint16(addrLen, 12) // 4+4+2+2
	buf.Write(addrLen)
	buf.Write(net.ParseIP("192.168.0.1").To4())
	buf.Write(net.ParseIP("10.0.0.2").To4())
	portBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(portBuf, 9999)
	buf.Write(portBuf)
	binary.BigEndian.PutUint16(portBuf, 443)
	buf.Write(portBuf)

	br := bufio.NewReader(&buf)
	h, err := ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Version != 2 || h.TransProto != "TCP4" {
		t.Fatalf("unexpected: %+v", h)
	}
	if h.SrcPort != 9999 || h.DstPort != 443 {
		t.Fatalf("ports = %d/%d", h.SrcPort, h.DstPort)
	}
}

func TestV2_Local(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(v2Signature)
	buf.WriteByte(0x20) // version 2, LOCAL command
	buf.WriteByte(0x00) // AF_UNSPEC
	addrLen := make([]byte, 2)
	binary.BigEndian.PutUint16(addrLen, 0)
	buf.Write(addrLen)

	br := bufio.NewReader(&buf)
	h, err := ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Version != 2 || h.Command != 0 {
		t.Fatalf("unexpected: %+v", h)
	}
}

func TestV2_UnsupportedFamily(t *testing.T) {
	var buf bytes.Buffer
	buf.Write(v2Signature)
	buf.WriteByte(0x21) // version 2, PROXY
	buf.WriteByte(0xFF) // bad family
	addrLen := make([]byte, 2)
	binary.BigEndian.PutUint16(addrLen, 0)
	buf.Write(addrLen)

	br := bufio.NewReader(&buf)
	_, err := ReadHeader(br)
	if err == nil {
		t.Fatal("expected error on unsupported family")
	}
}

func TestNoHeader(t *testing.T) {
	br := bufio.NewReader(strings.NewReader("GET / HTTP/1.1\r\n"))
	h, err := ReadHeader(br)
	if err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
	if h.Version != 0 {
		t.Fatalf("expected no header, got %+v", h)
	}
}

// pipe returns two connected net.Conn pairs using net.Pipe.
func pipe(t *testing.T) (client, server net.Conn) {
	t.Helper()
	c, s := net.Pipe()
	t.Cleanup(func() { _ = c.Close(); _ = s.Close() })
	return c, s
}

func TestConn_V1RemoteAddr(t *testing.T) {
	client, raw := pipe(t)
	conn := newConn(raw)

	// Write PROXY header + payload from the client side.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = io.WriteString(client, "PROXY TCP4 1.2.3.4 5.6.7.8 1000 80\r\nHELLO")
	}()

	buf := make([]byte, 5)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(buf) != "HELLO" {
		t.Fatalf("payload = %q, want HELLO", buf)
	}

	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "1.2.3.4" {
		t.Fatalf("RemoteAddr IP = %s, want 1.2.3.4", addr.IP)
	}
	if addr.Port != 1000 {
		t.Fatalf("RemoteAddr Port = %d, want 1000", addr.Port)
	}
	<-done
}

func TestConn_NoHeader_FallsThrough(t *testing.T) {
	client, raw := pipe(t)
	conn := newConn(raw)

	go func() { _, _ = io.WriteString(client, "GET / HTTP/1.1\r\n") }()

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != "GET " {
		t.Fatalf("first bytes = %q, want GET ", buf)
	}
	// RemoteAddr should fall through to the raw connection.
	if conn.RemoteAddr() == nil {
		t.Fatal("RemoteAddr is nil")
	}
}

func TestListener_AcceptsProxyConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	pl := NewListener(ln)
	t.Cleanup(func() { _ = pl.Close() })

	dialDone := make(chan struct{})
	go func() {
		defer close(dialDone)
		c, dErr := net.Dial("tcp", pl.Addr().String())
		if dErr != nil {
			return
		}
		_, _ = io.WriteString(c, "PROXY TCP4 9.9.9.9 1.1.1.1 5555 80\r\nDATA")
		_ = c.Close()
	}()

	conn, err := pl.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	defer func() { _ = conn.Close() }()

	buf := make([]byte, 4)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatalf("read payload: %v", err)
	}
	if string(buf) != "DATA" {
		t.Fatalf("payload = %q, want DATA", buf)
	}
	addr := conn.RemoteAddr().(*net.TCPAddr)
	if addr.IP.String() != "9.9.9.9" {
		t.Fatalf("RemoteAddr = %s, want 9.9.9.9", addr.IP)
	}
	<-dialDone
}
