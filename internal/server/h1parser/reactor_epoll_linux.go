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
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

// epollFD is the integer epoll identifies registered descriptors by.
// Linux fds are kernel table indices — non-negative and far below
// math.MaxInt32 under any real RLIMIT_NOFILE — so the narrowing
// conversions funnel through one audited helper instead of scattered
// int32 casts (gosec G115).
type epollFD = int32

// asEpollFD narrows a descriptor for epoll registration and map keys.
// Returns -1 for out-of-range values, which callers treat as
// unregisterable (close).
func asEpollFD(fd int) epollFD {
	if fd < 0 || int64(fd) > int64(maxEPollFD) {
		return -1
	}
	return epollFD(fd)
}

// maxEPollFD bounds valid descriptors; real systems cap far below this
// via RLIMIT_NOFILE (Linux itself stores fds as int internally).
const maxEPollFD = int32(1) << 30

// reactorEpoll is the Linux reactor: one goroutine, one epoll fd, all
// hit-path connections of one listener.
//
// Field order keeps every field 8-byte aligned with zero padding in
// this order (verified: 24+24+16+8+8+8+8+8+8+8+8 = 128B). The pointer
// group comes first (lastSweep's time.Time carries a pointer and
// groups with the map/slice state); the scalar descriptors follow.
// Keeping the pointer group together mirrors the loop's ownership
// model (epoll/map state vs. descriptors).
//
// the verified layout above wins.
//
//nolint:govet // fieldalignment: the tool's "optimal" ordering is not padding-free;
type reactorEpoll struct {
	// Pointer-bearing fields first (fieldalignment): lastSweep's
	// time.Time carries a pointer, so it groups with the rest before
	// the scalar descriptors.
	events    []unix.EpollEvent
	p         *Parser
	connTable [reactorFDTableCap]*reactorConn // dense fd-indexed fast table
	overflow  map[epollFD]*reactorConn        // conns beyond the table cap
	acceptLn  net.Listener
	pending   chan *reactorConn
	lastSweep time.Time
	// done is closed by Close (any goroutine) to stop the loop;
	// reading a closed channel is race-free from any goroutine.
	done chan struct{}
	// handoffs owns the blocking-parser goroutines spawned at handoff
	// (fd-close on exit + bounded drain at shutdown — see handoffTracker).
	// handoffsDone is closed once the loop goroutine has fully exited, so
	// Close's drain cannot race handoffs spawned by a still-running loop.
	handoffs     handoffTracker
	handoffsDone chan struct{}
	// metricsDrainer, when the parser has a metrics hook, drains the
	// async hit-metrics ring (reactor_metrics.go). Started by run() and
	// joined by Close via metricsDone after a final full drain — no
	// record survives shutdown uncounted.
	metricsDrainer *metricsDrainer
	metricsDone    chan struct{}
	// acceptDone is closed by acceptLoop on exit. cleanup joins it
	// before draining the pending queue: a conn accepted just before
	// the listener closed can still be parked on pending after
	// cleanup's drain ran — joining first guarantees every accepted
	// conn has an owner (tracked, handed off, or closed by the drain).
	// acceptStarted gates the join: cleanup also runs on the
	// constructor's error path, where acceptLoop never started.
	acceptStarted atomic.Bool
	acceptDone    chan struct{}
	// ntracked is the number of tracked connections (table + overflow).
	// Kept as a scalar so the reactorMaxConns admission check is one
	// compare instead of a map length walk.
	ntracked int

	epfd  int
	wakeR int
	wakeW int
	// pendingWake coalesces wakeup-pipe writes (see wake).
	pendingWake atomic.Bool
}

// reactorFDTableCap is the dense connection-table capacity: fds are
// kernel table indices and real deployments run far below this under
// any RLIMIT_NOFILE, so the table turns every readiness dispatch into
// an array load instead of a map hash+probe. Power of two.
const reactorFDTableCap = 1 << 16

// connAt returns the connection tracked for fd, or nil. Loop-goroutine
// only.
func (r *reactorEpoll) connAt(fd epollFD) *reactorConn {
	if fd >= 0 && int(fd) < reactorFDTableCap {
		return r.connTable[fd]
	}
	return r.overflow[fd]
}

// connAdd tracks rc under fd. Loop-goroutine only.
func (r *reactorEpoll) connAdd(fd epollFD, rc *reactorConn) {
	if fd >= 0 && int(fd) < reactorFDTableCap {
		r.connTable[fd] = rc
	} else {
		if r.overflow == nil {
			r.overflow = make(map[epollFD]*reactorConn)
		}
		r.overflow[fd] = rc
	}
	r.ntracked++
}

// connDel untracks fd. Loop-goroutine only.
func (r *reactorEpoll) connDel(fd epollFD) {
	if fd >= 0 && int(fd) < reactorFDTableCap {
		r.connTable[fd] = nil
	} else {
		delete(r.overflow, fd)
	}
	r.ntracked--
}

// eachConn yields every tracked connection to fn once. Used by the idle
// sweep and shutdown cleanup. Loop-goroutine only. Ranges over the
// table as a slice — range-over-array would copy the whole 64 Ki-entry
// array (512 KB) on every call. The slice range reads elements lazily,
// so a connDel (nil store) at the current index inside the callback is
// safe; deleting other conns inside the callback is not (sweepIdle
// only deletes the yielded one).
func (r *reactorEpoll) eachConn(fn func(fd epollFD, rc *reactorConn)) {
	for fd, rc := range r.connTable[:] {
		if rc != nil {
			fn(epollFD(fd), rc)
		}
	}
	for fd, rc := range r.overflow {
		fn(fd, rc)
	}
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
		epfd:         epfd,
		events:       make([]unix.EpollEvent, 1024),
		p:            p,
		acceptLn:     ln,
		wakeR:        wakeFds[0],
		wakeW:        wakeFds[1],
		pending:      make(chan *reactorConn, 1024),
		lastSweep:    p.nowFunc(),
		done:         make(chan struct{}),
		handoffsDone: make(chan struct{}),
		acceptDone:   make(chan struct{}),
	}
	// connTable is a dense value field (~512 KB): zero-initialized by the
	// struct literal above at no measurable cost relative to the loop's
	// lifetime; overflow stays nil until an fd beyond the table arrives.
	r.overflow = nil
	// Async metrics (W3): the hook's CPU is serial loop time; move it
	// to a drainer goroutine via the SPSC ring. The ring is created
	// here, on the loop's Parser, before Run — the blocking-path share
	// of this Parser (handoffs) never sees it (handoff conns serve
	// misses, not hits).
	if p.metricsHook != nil {
		ring := &metricsRing{}
		p.metricsRing = ring
		r.metricsDrainer = &metricsDrainer{ring: ring, hook: p.metricsHook}
	}
	// The spawner goroutine starts here, not in run(): acceptLoop can
	// enqueue handoffs (pending-queue overflow) the moment Run is
	// called, and a handoff arriving before run()'s body executes
	// would dereference a nil spawn channel. Construction is the only
	// synchronization-free point where both accept and loop paths are
	// guaranteed to come after.
	r.handoffs.startSpawner()
	if err := r.register(asEpollFD(r.wakeR), nil); err != nil {
		r.cleanup()
		return nil, false
	}
	return r, true
}
func (r *reactorEpoll) run() {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	// Defer order is load-bearing: handoffsDone must close only after
	// cleanup has joined the spawner (LIFO — this defer runs last).
	// Closing it earlier would let Close pass its wait and race
	// wg.Wait against the spawner's in-flight wg.Add for queued jobs —
	// a data race the storm-shutdown test catches under -race.
	defer close(r.handoffsDone)
	defer r.cleanup()
	if r.metricsDrainer != nil {
		// metricsDone closes when the drainer's final drain completes;
		// Close joins it after the loop exits (the loop is the ring's
		// only producer, so no record can be pushed after handoffsDone).
		r.metricsDone = make(chan struct{})
		go r.metricsDrainer.run(r.done, r.metricsDone)
	}
	go r.acceptLoop()
	r.acceptStarted.Store(true)

	// Adaptive busy-poll: after serving a batch, poll readiness with a
	// zero timeout for up to reactorSpinBudget before parking in the
	// timed wait. Why: parking the locked OS thread and re-acquiring a
	// P through the scheduler costs tens of microseconds (futex wake,
	// findRunnable) — paid per *request* at low concurrency, where
	// batches are size one, and measured as an ~8x p50 RTT gap versus
	// the blocking path on a single client (see
	// BenchmarkSingle_Reactor_KeepAliveRTT and its controls in
	// reactor_bench_linux_test.go). Spinning for 80 µs after activity
	// serves back-to-back requests on the same connection without the
	// park/wake; the budget only spends while traffic keeps arriving,
	// so true idle parks after one spin window — the loop does not
	// become a busy loop. Sustained high load never parks regardless
	// (batches keep arriving), so the spin is free where it matters.
	spin := 0
	for {
		select {
		case <-r.done:
			return
		default:
		}
		var timeout int
		if spin < reactorSpinBudget {
			timeout = 0 // non-blocking poll while spin budget remains
			spin++
		} else {
			timeout = 1000
		}
		n, err := unix.EpollWait(r.epfd, r.events, timeout)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return
		}
		if n == 0 {
			// Nothing ready yet; the spin budget spends one unit per
			// poll (spin++ above), so an idle window simply runs out
			// and the next iteration parks. Sustained traffic keeps
			// returning batches, which reset the budget.
			continue
		}
		spin = 0
		now := r.p.nowFunc()
		if now.Sub(r.lastSweep) >= reactorSweepInterval {
			r.lastSweep = now
			r.sweepIdle(now)
		}
		for i := range n {
			ev := &r.events[i]
			rc := r.connAt(ev.Fd)
			switch {
			case rc == nil && ev.Fd == asEpollFD(r.wakeR):
				r.drainWake()
				r.drainPending()
			case rc != nil:
				r.dispatch(rc, ev.Events, now)
			}
		}
	}
}

// reactorSpinBudget bounds the zero-timeout epoll polls after a served
// batch (see run). Each poll costs ~1 µs, so the window is ~80 µs of
// spin; any event batch resets it. 80 µs covers a client's
// response-parse → next-request round-trip on loopback and typical
// intra-DC keep-alive reuse, without keeping a core hot when traffic
// has actually stopped.
//
// Overridable to 0 via BOUINE_REACTOR_SPIN_BUDGET for operators who
// would rather pay the park/wake (and for the spin-vs-no-spin A/B in
// the RTT benchmarks): 0 disables the busy-poll entirely.
var reactorSpinBudget = spinBudgetFromEnv()

func spinBudgetFromEnv() int {
	if v := os.Getenv("BOUINE_REACTOR_SPIN_BUDGET"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 && n <= 10_000 {
			return n
		}
	}
	return 80
}

// reactorSweepInterval paces the idle sweep. The epoll timeout is 1s,
// so the loop wakes at least this often even with zero traffic.
const reactorSweepInterval = time.Second

// sweepIdle closes connections whose per-request budgets expired:
// reading connections past the idle budget, and writing connections
// whose flush exceeded the write safety net. Idle connections generate
// no readiness events, so the per-dispatch idle check alone would never
// fire for them — this sweep is what bounds a reactor full of parked
// keep-alive clients, mirroring the blocking parser's 120s read
// deadline. Runs on the loop goroutine at most once per
// reactorSweepInterval; O(tracked conns).
//
// Writers are governed by the write safety net, not the idle budget:
// a connection flushing a hit to a slow client makes no read progress
// by definition. But raw-fd writes arm no OS deadline, so without this
// check a client that stops reading mid-response would park the
// connection in rcWriting forever, silently consuming the reactor
// connection budget. reactorWriteTimeout (5 minutes) mirrors the
// blocking path's SetWriteDeadline safety net; dropping mid-flush at
// that point is the same outcome the blocking path produces on its
// deadline — a cut response to a dead client, not a leaked one.
func (r *reactorEpoll) sweepIdle(now time.Time) {
	r.eachConn(func(fd epollFD, rc *reactorConn) {
		switch rc.state {
		case rcReading:
			if !rc.idleExpired(now) {
				return
			}
		case rcWriting:
			if !rc.writeStuckExpired(now) {
				return
			}
		default:
			return
		}
		// Close, not handoff: an expired connection has no in-flight
		// request worth serving, and the blocking parser would itself
		// time it out — a handoff goroutine would be pure churn.
		_ = unix.EpollCtl(r.epfd, unix.EPOLL_CTL_DEL, int(fd), nil)
		r.connDel(fd)
		rc.release()
		_ = rc.conn.Close()
	})
}

// dispatch advances one connection for the reported readiness events.
func (r *reactorEpoll) dispatch(rc *reactorConn, events uint32, now time.Time) {
	if events&(unix.EPOLLERR|unix.EPOLLHUP) != 0 {
		r.drop(rc)
		return
	}
	if rc.state == rcReading && rc.idleExpired(now) {
		// The per-request idle budget expired; the blocking path would
		// hit its read deadline and close, so drop here. Writers are
		// governed by the write safety net, not the idle budget.
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
	case actClose, actCloseAfterFlush:
		r.drop(rc)
	}
}

// register adds an fd to the epoll set with read interest (the state
// machine always starts reading). rc nil means an internal descriptor
// (wakeup pipe) without a state machine.
func (r *reactorEpoll) register(fd epollFD, rc *reactorConn) error {
	events := uint32(unix.EPOLLIN | unix.EPOLLRDHUP)
	err := unix.EpollCtl(r.epfd, unix.EPOLL_CTL_ADD, int(fd), &unix.EpollEvent{
		Events: events,
		Fd:     fd,
	})
	if err != nil {
		return err
	}
	if rc != nil {
		// Record the armed mask so the first hit's mod() to the same
		// read interest is recognized as unchanged and skipped — one
		// epoll_ctl per first hit saved.
		rc.epollInterest = events
		r.connAdd(fd, rc)
	}
	return nil
}

// mod switches a connection's readiness interest. The epoll_ctl is
// skipped when the interest is unchanged — registration arms read
// interest (EPOLLIN|EPOLLRDHUP) and rc.epollInterest records it, so
// the common full-flush case (back to read interest) issues zero
// syscalls. Only a partial write (EPOLLOUT re-arm, then back to read)
// pays two epoll_ctl calls per request.
func (r *reactorEpoll) mod(rc *reactorConn, events uint32) {
	if rc.epollInterest == events {
		return
	}
	fd := r.connFd(rc)
	err := unix.EpollCtl(r.epfd, unix.EPOLL_CTL_MOD, int(fd), &unix.EpollEvent{
		Events: events,
		Fd:     fd,
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
func (r *reactorEpoll) connFd(rc *reactorConn) epollFD {
	if rc.fd >= 0 {
		return asEpollFD(rc.fd)
	}
	return asEpollFD(connFD(rc.conn))
}

// drop removes and closes a connection.
func (r *reactorEpoll) drop(rc *reactorConn) {
	fd := r.connFd(rc)
	_ = unix.EpollCtl(r.epfd, unix.EPOLL_CTL_DEL, int(fd), nil)
	r.connDel(fd)
	rc.release()
	_ = rc.conn.Close()
}

// handoff releases a connection from the reactor and queues it for
// the blocking parser goroutine. The prefixConn replay is built here;
// ownership of the net.Conn transfers to that goroutine (which closes
// it on exit). Tracked by the handoff tracker so Close drains
// in-flight handed-off requests, then force-closes keep-alive parkers.
func (r *reactorEpoll) handoff(rc *reactorConn) {
	fd := r.connFd(rc)
	_ = unix.EpollCtl(r.epfd, unix.EPOLL_CTL_DEL, int(fd), nil)
	r.connDel(fd)
	rc.release()
	rc.handoff(&r.handoffs)
}

// drainWake consumes the wakeup pipe bytes and processes accepted
// connections. The pending flag is cleared after the drain so a
// racing wake() (flag set, byte in flight) re-arms: a stray extra
// byte is harmless, a suppressed wakeup would park the loop.
func (r *reactorEpoll) drainWake() {
	buf := [64]byte{}
	for {
		n, err := unix.Read(r.wakeR, buf[:])
		if n <= 0 || err != nil {
			r.pendingWake.Store(false)
			return
		}
		if n < len(buf) {
			r.pendingWake.Store(false)
			return
		}
	}
}

// wake writes a byte to the wakeup pipe so EpollWait returns. The
// write is coalesced: pendingWake tracks an undrained byte, so a burst
// of accepted connections costs one pipe write per drain cycle instead
// of one per accept. The flag is set before the write; drainWake clears
// it after draining, so the lose-side of the race is a redundant byte.
func (r *reactorEpoll) wake() {
	if r.pendingWake.Swap(true) {
		return
	}
	_, _ = unix.Write(r.wakeW, wakeByte[:])
}

var wakeByte = [1]byte{1}

// acceptLoop runs on its own goroutine: accepts connections, sets
// them non-blocking, and parks them on the pending channel for the
// loop goroutine to register. All epoll/map state stays owned by the
// loop goroutine — this goroutine never touches it. Exits only when
// Accept returns net.ErrClosed (listener closed for shutdown);
// transient errors (EMFILE, ENFILE, ECONNABORTED, ENOMEM) are skipped
// like the blocking path's accept loop — one failed accept must not
// permanently kill the listener.
func (r *reactorEpoll) acceptLoop() {
	defer close(r.acceptDone)
	for {
		conn, err := r.acceptLn.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
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
			rc.handoff(&r.handoffs)
		}
	}
}

// drainPending registers connections parked by the accept goroutine.
// Runs on the loop goroutine only.
func (r *reactorEpoll) drainPending() {
	for {
		select {
		case rc := <-r.pending:
			if r.ntracked >= reactorMaxConns {
				// Over cap: the blocking parser owns it.
				rc.handoff(&r.handoffs)
				continue
			}
			if err := r.register(asEpollFD(rc.fd), rc); err != nil {
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
			uintptr(unsafe.Pointer(&iovec[0])), uintptr(len(iovec))) //nolint:gosec // G103: the iovec aliases slices owned by this connection; they outlive the writev and are never mutated concurrently (loop-goroutine only)
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

// cleanup drops all tracked connections at loop exit. In-flight
// handed-off requests are not force-killed here: Close gives them
// the handoffDrainGrace window first (see Close).
func (r *reactorEpoll) cleanup() {
	r.eachConn(func(_ epollFD, rc *reactorConn) {
		rc.release()
		_ = rc.conn.Close()
	})
	r.overflow = nil
	// Join the accept loop before draining the pending queue: the
	// listener may already be closed (production wiring closes it
	// before Close), but a conn accepted just before that close can
	// still be parked on pending after this point — without the join
	// it would leak as a half-open socket past shutdown (the
	// storm-shutdown test catches exactly this). Closing the listener
	// here too makes Close safe regardless of caller ordering; the
	// double close is a no-op error.
	_ = r.acceptLn.Close()
	if r.acceptStarted.Load() {
		<-r.acceptDone
	}
	// Drain the pending queue: connections the accept goroutine
	// parked but the loop never registered are owned by nobody else —
	// without this drain they leak as half-open sockets past shutdown.
	for {
		select {
		case rc := <-r.pending:
			rc.release()
			_ = rc.conn.Close()
		default:
			goto drained
		}
	}
drained:
	// Stop the spawner: it drains any queued jobs (each wg.Add +
	// goroutine start) before exiting, and quit makes every late
	// spawnFromLoop a conn close instead of a send on a dead queue.
	// handoffsDone closes only after this join, so Close's WaitGroup
	// wait cannot race a still-pending Add.
	if r.handoffs.spawns != nil {
		r.handoffs.stopSpawner()
	}
	_ = unix.Close(r.wakeR)
	_ = unix.Close(r.wakeW)
	_ = unix.Close(r.epfd)
}

// ReactorLoop is the platform reactor handle returned by
// NewReactorLoop.
type ReactorLoop = reactorEpoll

// NewReactorLoop is the exported constructor used by the listener. It
// returns ok=false when epoll is unavailable, in which case the caller
// falls back to the blocking parser path.
func NewReactorLoop(p *Parser, ln net.Listener) (r *ReactorLoop, ok bool) {
	return newReactorLoop(p, ln)
}

// Close stops the loop goroutine (idempotent), then drains the
// handed-off blocking-parser goroutines with a bounded grace window:
// in-flight requests finish on their own, and after handoffDrainGrace
// any keep-alive-parked conns are force-closed so Serve's read loop
// exits instead of waiting out the 120s idle deadline. This is what
// lets the shutdown sequencer close the store without live handoffs
// underneath it — and what lets Serve return at all.
func (r *reactorEpoll) Close() {
	select {
	case <-r.done:
		// Already closed.
	default:
		close(r.done)
	}
	r.wake()

	// Wait for the loop to exit first: a still-running loop can spawn
	// new handoffs (misses in its final batch), which must drain too.
	<-r.handoffsDone

	// Join the metrics drainer: the loop has exited, so the ring can no
	// longer grow; the drainer applies every buffered record before
	// metricsDone closes (shutdown metrics stay complete, not "mostly
	// complete").
	if r.metricsDone != nil {
		<-r.metricsDone
	}

	drained := make(chan struct{})
	go func() {
		r.handoffs.wg.Wait()
		close(drained)
	}()
	select {
	case <-drained:
		// All handoffs finished on their own within the grace window.
	case <-time.After(handoffDrainGrace):
		// Keep-alive parkers remain: force-close their conns to unpark
		// Serve (it exits on the read error like a client reset).
		r.handoffs.drainForceClose()
		<-drained
	}
}

// Run drives the reactor until the listener is closed. Blocks.
func (r *reactorEpoll) Run() { r.run() }

// MetricsDropped returns how many hit-metric records the async ring
// dropped because the drainer could not keep up (see
// reactor_metrics.go). Zero unless the loop was saturated. Called by
// the listener wiring after Run returns, to surface the overflow at
// shutdown; steady-state operation should see zero.
func (r *reactorEpoll) MetricsDropped() uint64 {
	if r.metricsDrainer != nil {
		return r.metricsDrainer.ring.droppedTotal()
	}
	return 0
}

// handoff starts the blocking Parser on the spawner goroutine,
// replaying the buffered bytes. Called by the transport on actHandoff.
// The tracker owns the blocking goroutine: it tracks the conn, closes
// it when Serve returns, and force-closes it during shutdown drain
// after the grace window to unpark Serve's keep-alive read loop (Serve
// never closes the conn itself — the blocking path's caller does,
// which is the tracker). The prefixConn allocation stays on the loop
// goroutine: the readBuf it aliases is loop-owned and must not be
// freed before the handoff escapes the loop — the job's channel send
// is exactly the ownership boundary.
func (rc *reactorConn) handoff(tracker *handoffTracker) {
	conn := rc.handoffConn()
	rc.state = rcHandoff
	tracker.spawnFromLoop(rc.parser, conn)
}

// writeStuckExpired reports whether a write phase has exceeded the
// safety-net write budget without completing. Mirrors the blocking
// parser's SetWriteDeadline safety net (Parser.writeTime, 5 minutes)
// for connections the raw-fd transport never arms one on: without it,
// a client that stops reading mid-response parks the connection in
// rcWriting forever, silently consuming the reactor connection budget.
func (rc *reactorConn) writeStuckExpired(now time.Time) bool {
	return now.Sub(rc.reqStart) > reactorWriteTimeout
}

// reactorWriteTimeout is the stuck-writer safety net: a connection
// whose flush has not completed within this budget is dropped by the
// idle sweep. The blocking path enforces the same bound via
// SetWriteDeadline (fp_conn.go wires WithWriteTimeout(5 * time.Minute));
// the two constants must stay in sync.
const reactorWriteTimeout = 5 * time.Minute

// handoffTracker owns the blocking-parser goroutines spawned at handoff.
// Each goroutine closes its conn on exit (Serve never closes it), so an
// fd can never leak. The set is keyed by conn pointer: register before
// the goroutine starts, unregister in its defer, so drainForceClose
// always sees the exact set of live handoffs.
//
// The spawn work (mutex, map insert, `go` statement — ~1-2 µs) runs on
// the spawner goroutine, not the reactor loop goroutine: under missy
// traffic every one of those microseconds is serial latency added to
// every hit multiplexed on the same listener. The loop's part of a
// handoff is only the ownership-critical epoll/map work plus a
// non-blocking channel send; wg.Add happens on the spawner before the
// blocking-parser goroutine starts, which is the ordering the WaitGroup
// contract requires — the spawner serializes Add against the spawn
// queue, and Close only Waits after the loop has exited and the queue
// has been drained and joined, so no Add can race the Wait.
//
// drain waits for in-flight handed-off requests up to the grace window,
// then force-closes any conns still held by blocking parsers: after a
// request finishes, Serve's keep-alive loop parks on the read deadline
// (120s), and shutdown must not wait that out. A force-close unparks it
// — Serve treats the read error as connection termination, exactly like
// a client reset, and the goroutine's own conn close follows.
type handoffTracker struct {
	conns map[net.Conn]struct{}
	// spawns feeds the spawner goroutine. Buffered: the loop never
	// blocks on a full queue (a full queue means the spawner is
	// backed up, and the alternative — stalling the loop's hits —
	// costs more than the overflow policy below).
	spawns chan handoffJob
	// spawnerDone closes after the spawner has drained the queue and
	// exited. The WaitGroup wait must be ordered after this: every
	// wg.Add happens on the spawner, so joining the spawner first
	// guarantees no Add can race the Wait.
	spawnerDone chan struct{}
	// quit signals the spawner to stop accepting work. Closed exactly
	// once by stopSpawner (loop shutdown); spawnFromLoop's select drops
	// late jobs rather than sending on a closed channel — the channel
	// is never closed, only quit is.
	quit chan struct{}
	wg   sync.WaitGroup
	mu   sync.Mutex
}

// handoffJob is one queued blocking-parser start.
type handoffJob struct {
	p    *Parser
	conn net.Conn
}

// handoffSpawnQueue is the per-tracker spawn queue capacity.
const handoffSpawnQueue = 128

// startSpawner boots the tracker's spawner goroutine. Must be called
// before the first spawnFromLoop; the goroutine exits when spawns
// closes (at loop shutdown, in cleanup).
func (t *handoffTracker) startSpawner() {
	t.spawns = make(chan handoffJob, handoffSpawnQueue)
	t.spawnerDone = make(chan struct{})
	t.quit = make(chan struct{})
	go t.spawner()
}

// stopSpawner signals the spawner to finish and waits for it. Every
// job still in the queue is spawned (blocking-parser goroutines own
// their conns' close), so nothing parked in the queue can leak. Call
// from the loop goroutine's cleanup path only.
func (t *handoffTracker) stopSpawner() {
	close(t.quit)
	for {
		select {
		case job := <-t.spawns:
			t.spawn(job.p, job.conn)
		default:
			<-t.spawnerDone
			return
		}
	}
}

func (t *handoffTracker) spawner() {
	defer close(t.spawnerDone)
	for {
		select {
		case <-t.quit:
			// Drain the remainder here too — stopSpawner races its own
			// drain; whichever side pops a job, it is spawned exactly
			// once (channel receive is exclusive).
			for {
				select {
				case job := <-t.spawns:
					t.spawn(job.p, job.conn)
				default:
					return
				}
			}
		case job := <-t.spawns:
			t.spawn(job.p, job.conn)
		}
	}
}

// spawnFromLoop enqueues a blocking-parser start from the reactor loop
// goroutine. Non-blocking: when the queue is full the conn is closed
// instead of served — the client sees a reset, which for a miss under
// spawner saturation is the honest outcome, and infinitely better than
// parking every hit on the listener.
func (t *handoffTracker) spawnFromLoop(p *Parser, conn net.Conn) {
	job := handoffJob{p: p, conn: conn}
	select {
	case t.spawns <- job:
	case <-t.quit:
		// Shutdown already started: this conn arrived after the loop's
		// final batch (accept-side race). Nobody will serve it; close
		// it here — the client sees a reset, which under shutdown is
		// the honest outcome.
		_ = conn.Close()
	default:
		_ = conn.Close()
	}
}

func (t *handoffTracker) spawn(p *Parser, conn net.Conn) {
	t.wg.Add(1)
	t.mu.Lock()
	if t.conns == nil {
		t.conns = make(map[net.Conn]struct{})
	}
	t.conns[conn] = struct{}{}
	t.mu.Unlock()
	go func() {
		defer t.wg.Done()
		defer t.unregister(conn)
		defer func() { _ = conn.Close() }()
		_ = p.Serve(conn)
	}()
}

func (t *handoffTracker) unregister(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
}

// drainForceClose force-closes every live handed-off conn. Used after
// the grace window during shutdown; the goroutine's deferred close is
// idempotent at the syscall layer, so double-close is harmless.
func (t *handoffTracker) drainForceClose() {
	t.mu.Lock()
	for conn := range t.conns {
		_ = conn.Close()
	}
	t.mu.Unlock()
}

// handoffDrainGrace is how long Close waits for in-flight handed-off
// requests to finish on their own before force-closing. Generous for a
// full miss fetch (bounded by fetch_timeout), short enough that
// shutdown does not stall behind it.
const handoffDrainGrace = 5 * time.Second
