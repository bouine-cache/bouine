//go:build linux

package h1parser

// reactor_epoll_linux.go — the Linux transport for the reactor: one
// epoll instance multiplexing all of a listener's reactor connections.
// A single epoll_wait returns a batch of ready connections which the
// reactor goroutine parses and serves inline. This is the batching
// nginx's worker event loop gets — without forking the process (see
// docs/plans/hit-path-p99-optimization.md, "Rejected: fasthttp
// Prefork").
//
// All socket I/O inside the loop is raw-fd (read/write syscalls via
// unix.Read/unix.Write, mapped EAGAIN → errAgain): net.Conn.Read/Write
// would park the goroutine on the Go runtime poller, which is exactly
// the per-request park/unpark the reactor exists to avoid. net.Conn is
// still kept for Close, handoff to the blocking parser (which needs a
// net.Conn), and type-based deadline control.

import (
	"errors"
	"net"
	"runtime"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// reactorEpoll is the Linux reactor: one goroutine, one epoll fd, all
// hit-path connections of one listener.
type reactorEpoll struct {
	epfd   int
	events []unix.EpollEvent
	p      *Parser
	conns  map[int32]*reactorConn

	acceptLn net.Listener
	wakeR    int
	wakeW    int
	pending  chan *reactorConn
	// lastSweep paces the idle sweep (loop goroutine only).
	lastSweep time.Time
	// done is closed by Close (any goroutine) to stop the loop;
	// reading a closed channel is race-free from any goroutine.
	done chan struct{}
}

// newReactorLoop builds the Linux reactor. ok=false (and a nil loop)
// means epoll is unavailable and the listener uses the blocking path.
func newReactorLoop(p *Parser, ln net.Listener) (r *reactorEpoll, ok bool) {
	epfd, err := unix.EpollCreate1(unix.EPOLL_CLOEXEC)
	if err != nil {
		return nil, false
	}
	wakeFds := make([]int, 2)
	if err := unix.Pipe2(wakeFds, unix.O_CLOEXEC|unix.O_NONBLOCK); err != nil {
		_ = unix.Close(epfd)
		return nil, false
	}
	r = &reactorEpoll{
		epfd:      epfd,
		events:    make([]unix.EpollEvent, 1024),
		p:         p,
		conns:     make(map[int32]*reactorConn),
		acceptLn:  ln,
		wakeR:     wakeFds[0],
		wakeW:     wakeFds[1],
		pending:   make(chan *reactorConn, 1024),
		lastSweep: p.nowFunc(),
		done:      make(chan struct{}),
	}
	if err := r.register(r.wakeR, false, nil); err != nil {
		r.cleanup()
		return nil, false
	}
	return r, true
}
func (r *reactorEpoll) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	defer r.cleanup()
	go r.acceptLoop()
	for {
		select {
		case <-r.done:
			return
		default:
		}
		n, err := unix.EpollWait(r.epfd, r.events, 1000)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return
		}
		now := r.p.nowFunc()
		if now.Sub(r.lastSweep) >= reactorSweepInterval {
			r.lastSweep = now
			r.sweepIdle(now)
		}
		for i := int32(0); i < int32(n); i++ {
			ev := &r.events[i]
			rc, isConn := r.conns[ev.Fd]
			switch {
			case !isConn && ev.Fd == int32(r.wakeR):
				r.drainWake()
				r.drainPending()
			case isConn:
				r.dispatch(rc, ev.Events, now)
			}
		}
	}
}

// reactorSweepInterval paces the idle sweep. The epoll timeout is 1s,
// so the loop wakes at least this often even with zero traffic.
const reactorSweepInterval = time.Second

// sweepIdle closes connections whose idle budget expired without read
// progress. Idle connections generate no readiness events, so the
// per-dispatch idle check alone would never fire for them — this sweep
// is what bounds a reactor full of parked keep-alive clients, mirroring
// the blocking parser's 120s read deadline. Runs on the loop goroutine
// at most once per reactorSweepInterval; O(tracked conns).
func (r *reactorEpoll) sweepIdle(now time.Time) {
	for fd, rc := range r.conns {
		if rc.idleExpired(now) {
			// Close, not handoff: an idle conn has no in-flight
			// request to serve, and the blocking parser would itself
			// time it out — a handoff goroutine would be pure churn.
			_ = unix.EpollCtl(r.epfd, unix.EPOLL_CTL_DEL, int(fd), nil)
			delete(r.conns, fd)
			rc.release()
			_ = rc.conn.Close()
		}
	}
}

// dispatch advances one connection for the reported readiness events.
func (r *reactorEpoll) dispatch(rc *reactorConn, events uint32, now time.Time) {
	if events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
		r.drop(rc)
		return
	}
	if rc.idleExpired(now) {
		// The idle budget expired without read progress; the blocking
		// path would hit its read deadline and close, so drop here.
		r.drop(rc)
		return
	}
	switch rc.advance() {
	case actWaitRead:
		r.mod(rc, unix.EPOLLIN|unix.EPOLLRDHUP)
	case actWaitWrite:
		r.mod(rc, unix.EPOLLOUT)
	case actHandoff:
		r.handoff(rc)
	case actClose:
		r.drop(rc)
	}
}

// register adds an fd to the epoll set. conn nil means an internal
// fd (wakeup pipe) without a state machine.
func (r *reactorEpoll) register(fd int, wantWrite bool, rc *reactorConn) error {
	events := unix.EPOLLIN | unix.EPOLLRDHUP
	if wantWrite {
		events = unix.EPOLLOUT
	}
	err := unix.EpollCtl(r.epfd, unix.EPOLL_CTL_ADD, fd, &unix.EpollEvent{
		Events: uint32(events),
		Fd:     int32(fd),
	})
	if err != nil {
		return err
	}
	if rc != nil {
		r.conns[int32(fd)] = rc
	}
	return nil
}

// mod switches a connection's readiness interest. The epoll_ctl is
// skipped when the interest is unchanged — in the common full-flush
// case the interest stays EPOLLIN from registration, so this saves
// one syscall per hit. rc.epollInterest tracks the armed mask (0 is
// the registration default: read interest).
func (r *reactorEpoll) mod(rc *reactorConn, events uint32) {
	if rc.epollInterest == events {
		return
	}
	fd := r.connFd(rc)
	err := unix.EpollCtl(r.epfd, unix.EPOLL_CTL_MOD, fd, &unix.EpollEvent{
		Events: events,
		Fd:     int32(fd),
	})
	if err != nil {
		r.drop(rc)
		return
	}
	rc.epollInterest = events
}

// connFd returns the raw fd, preferring the transport-cached value
// (set at accept) over the SyscallConn dance — one closure call saved
// per mod/drop on the hot path.
func (r *reactorEpoll) connFd(rc *reactorConn) int {
	if rc.fd >= 0 {
		return rc.fd
	}
	return connFD(rc.conn)
}

// drop removes and closes a connection.
func (r *reactorEpoll) drop(rc *reactorConn) {
	fd := r.connFd(rc)
	_ = unix.EpollCtl(r.epfd, unix.EPOLL_CTL_DEL, fd, nil)
	delete(r.conns, int32(fd))
	rc.release()
	_ = rc.conn.Close()
}

// handoff releases a connection from the reactor and queues it for
// the blocking parser goroutine. The prefixConn replay is built here;
// ownership of the net.Conn transfers to that goroutine.
func (r *reactorEpoll) handoff(rc *reactorConn) {
	fd := r.connFd(rc)
	_ = unix.EpollCtl(r.epfd, unix.EPOLL_CTL_DEL, fd, nil)
	delete(r.conns, int32(fd))
	rc.release()
	rc.handoff()
}

// drainWake consumes the wakeup pipe bytes and processes accepted
// connections.
func (r *reactorEpoll) drainWake() {
	buf := [64]byte{}
	for {
		n, err := unix.Read(r.wakeR, buf[:])
		if n <= 0 || err != nil {
			return
		}
		if n < len(buf) {
			return
		}
	}
}

// wake writes a byte to the wakeup pipe so EpollWait returns.
func (r *reactorEpoll) wake() {
	_, _ = unix.Write(r.wakeW, wakeByte[:])
}

var wakeByte = [1]byte{1}

// acceptLoop runs on its own goroutine: accepts connections, sets
// them non-blocking, and parks them on the pending channel for the
// loop goroutine to register. All epoll/map state stays owned by the
// loop goroutine — this goroutine never touches it. Exits when Accept
// fails (listener closed).
func (r *reactorEpoll) acceptLoop() {
	for {
		conn, err := r.acceptLn.Accept()
		if err != nil {
			return
		}
		if tcp, ok := conn.(*net.TCPConn); ok {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetNoDelay(true)
		}
		fd := connFD(conn)
		if fd < 0 {
			_ = conn.Close()
			continue
		}
		if err := setNonblock(fd); err != nil {
			_ = conn.Close()
			continue
		}
		// TCP_QUICKACK parity with the blocking path (the accept-time
		// ACK heuristic matters for keep-alive request pipelining).
		_ = unix.SetsockoptInt(fd, unix.IPPROTO_TCP, unix.TCP_QUICKACK, 1)
		rc := newReactorConn(conn, r.p, rawRead(fd), rawWrite(fd))
		rc.fd = fd
		rc.writeVecFn = rawWritev(fd)
		select {
		case r.pending <- rc:
			r.wake()
		default:
			// Pending queue full: don't hold the connection hostage —
			// the blocking parser serves it.
			rc.handoff()
		}
	}
}

// drainPending registers connections parked by the accept goroutine.
// Runs on the loop goroutine only.
func (r *reactorEpoll) drainPending() {
	for {
		select {
		case rc := <-r.pending:
			if len(r.conns) >= reactorMaxConns {
				// Over cap: the blocking parser owns it.
				rc.handoff()
				continue
			}
			if err := r.register(connFD(rc.conn), false, rc); err != nil {
				rc.release()
				_ = rc.conn.Close()
			}
		default:
			return
		}
	}
}

// rawRead returns a non-blocking read func over fd. EAGAIN/EWOULDBLOCK
// maps to errAgain so the state machine never blocks.
func rawRead(fd int) func([]byte) (int, error) {
	return func(b []byte) (int, error) {
		n, err := unix.Read(fd, b)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				return 0, errAgain
			}
			return 0, err
		}
		return n, nil
	}
}

// rawWritev returns a non-blocking writev func over fd — the
// zero-copy flush for the retained fast-path response buffers.
func rawWritev(fd int) func([][]byte) (int, error) {
	// x/sys's exported Writev allocates an Iovec slice per call
	// (readv_unix.go builds `make([]Iovec, 0, minIovec)` every time) —
	// one heap allocation per hit. Build the iovec once per connection
	// and reuse the backing array across requests (the fast-path
	// response is always 3 slices: status line, header block, body),
	// then hit SYS_WRITEV directly — the same call x/sys makes
	// internally, minus the allocation.
	iovec := make([]unix.Iovec, 0, 4)
	return func(iovs [][]byte) (int, error) {
		iovec = iovec[:0]
		for _, b := range iovs {
			var v unix.Iovec
			v.SetLen(len(b))
			if len(b) > 0 {
				v.Base = &b[0]
			}
			iovec = append(iovec, v)
		}
		n, _, errno := unix.Syscall(unix.SYS_WRITEV, uintptr(fd),
			uintptr(unsafe.Pointer(&iovec[0])), uintptr(len(iovec)))
		if errno != 0 {
			err := error(errno)
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				return 0, errAgain
			}
			return 0, err
		}
		return int(n), nil
	}
}

// rawWrite returns a non-blocking write func over fd.
func rawWrite(fd int) func([]byte) (int, error) {
	return func(b []byte) (int, error) {
		n, err := unix.Write(fd, b)
		if err != nil {
			if errors.Is(err, syscall.EAGAIN) || errors.Is(err, syscall.EWOULDBLOCK) {
				return 0, errAgain
			}
			return 0, err
		}
		return n, nil
	}
}

// setNonblock flips O_NONBLOCK on fd.
func setNonblock(fd int) error {
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	_, err = unix.FcntlInt(uintptr(fd), unix.F_SETFL, flags|unix.O_NONBLOCK)
	return err
}

// connFD extracts the raw fd from a net.Conn without registering it
// with the Go runtime poller.
func connFD(conn net.Conn) int {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return -1
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return -1
	}
	fd := -1
	_ = raw.Control(func(f uintptr) { fd = int(f) })
	return fd
}

// cleanup drops all tracked connections at loop exit.
func (r *reactorEpoll) cleanup() {
	for _, rc := range r.conns {
		rc.release()
		_ = rc.conn.Close()
	}
	r.conns = nil
	_ = unix.Close(r.wakeR)
	_ = unix.Close(r.wakeW)
	_ = unix.Close(r.epfd)
}

// ensure the reactor runs with the caller's expectations documented:
// the listener wiring pins the loop goroutine to one OS thread
// (runtime.LockOSThread) so the hot maps and buffers stay core-local.
var _ = time.Second

// ReactorLoop is the platform reactor handle returned by
// NewReactorLoop.
type ReactorLoop = reactorEpoll

// NewReactorLoop is the exported constructor used by the listener. It
// returns ok=false when epoll is unavailable, in which case the caller
// falls back to the blocking parser path.
func NewReactorLoop(p *Parser, ln net.Listener) (r *ReactorLoop, ok bool) {
	return newReactorLoop(p, ln)
}

// Close stops the loop goroutine (idempotent). Called by the listener
// wiring when the serve context is cancelled, alongside closing the
// listener (Accept failure also terminates run()).
func (r *reactorEpoll) Close() {
	select {
	case <-r.done:
		// Already closed.
	default:
		close(r.done)
	}
	r.wake()
}

// Run drives the reactor until the listener is closed. Blocks.
func (r *reactorEpoll) Run() { r.run() }
