// Package wstest provides an in-memory implementation of websocket.Conn for
// testing the connection, manager, and gateway logic without a real network
// socket. It is a non-test package so multiple packages' tests can import it.
package wstest

import (
	"errors"
	"net"
	"sync"
	"time"

	"cpip/internal/websocket"
)

// timeoutError implements net.Error with Timeout() == true, to simulate a read
// deadline expiry (heartbeat loss).
type timeoutError struct{}

func (timeoutError) Error() string   { return "i/o timeout" }
func (timeoutError) Timeout() bool   { return true }
func (timeoutError) Temporary() bool { return true }

// ErrTimeout is a net.Error timeout usable with PushReadErr to simulate a dead
// connection.
var ErrTimeout net.Error = timeoutError{}

// fakeAddr is a stand-in net.Addr.
type fakeAddr struct{ s string }

func (a fakeAddr) Network() string { return "fake" }
func (a fakeAddr) String() string  { return a.s }

// frame is a queued inbound frame or error.
type frame struct {
	mt   websocket.MessageType
	data []byte
	err  error
}

// written captures an outbound frame.
type Written struct {
	Type websocket.MessageType
	Data []byte
	Ctrl bool // true if written via WriteControl
}

// FakeConn is an in-memory websocket.Conn.
type FakeConn struct {
	mu          sync.Mutex
	readCh      chan frame
	closedCh    chan struct{}
	closeOnce   sync.Once
	writes      []Written
	pongHandler func(string) error
	blockWrites bool          // when true, WriteMessage returns an error (broken pipe)
	stuckCh     chan struct{} // when non-nil, WriteMessage blocks until closed (slow peer)
	releaseOnce sync.Once
	readLimit   int64
	remote      net.Addr
	writeSignal chan struct{}
}

// NewFakeConn creates a FakeConn with a bounded inbound queue.
func NewFakeConn() *FakeConn {
	return &FakeConn{
		readCh:      make(chan frame, 64),
		closedCh:    make(chan struct{}),
		remote:      fakeAddr{s: "127.0.0.1:12345"},
		writeSignal: make(chan struct{}, 256),
	}
}

// --- test controls ---

// PushRead enqueues an inbound data frame to be returned by ReadMessage.
func (f *FakeConn) PushRead(mt websocket.MessageType, data []byte) {
	f.readCh <- frame{mt: mt, data: data}
}

// PushReadErr enqueues an error to be returned by the next ReadMessage.
func (f *FakeConn) PushReadErr(err error) {
	f.readCh <- frame{err: err}
}

// FirePong invokes the registered pong handler, simulating a client's pong.
func (f *FakeConn) FirePong() error {
	f.mu.Lock()
	h := f.pongHandler
	f.mu.Unlock()
	if h != nil {
		return h("")
	}
	return nil
}

// BlockWrites makes WriteMessage fail immediately, simulating a broken pipe.
func (f *FakeConn) BlockWrites() {
	f.mu.Lock()
	f.blockWrites = true
	f.mu.Unlock()
}

// StuckWrites makes WriteMessage block (as a slow but not broken peer would)
// until ReleaseWrites is called or the connection is closed. Used to fill the
// outbound queue and exercise backpressure.
func (f *FakeConn) StuckWrites() {
	f.mu.Lock()
	f.stuckCh = make(chan struct{})
	f.mu.Unlock()
}

// ReleaseWrites unblocks a prior StuckWrites.
func (f *FakeConn) ReleaseWrites() {
	f.mu.Lock()
	ch := f.stuckCh
	f.mu.Unlock()
	if ch != nil {
		f.releaseOnce.Do(func() { close(ch) })
	}
}

// Writes returns a snapshot of everything written so far.
func (f *FakeConn) Writes() []Written {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]Written, len(f.writes))
	copy(out, f.writes)
	return out
}

// WaitForWrite blocks until at least one write occurs or timeout elapses.
func (f *FakeConn) WaitForWrite(timeout time.Duration) bool {
	select {
	case <-f.writeSignal:
		return true
	case <-time.After(timeout):
		return false
	}
}

// ReadLimit returns the limit set via SetReadLimit.
func (f *FakeConn) ReadLimit() int64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.readLimit
}

// IsClosed reports whether Close has been called.
func (f *FakeConn) IsClosed() bool {
	select {
	case <-f.closedCh:
		return true
	default:
		return false
	}
}

// --- websocket.Conn implementation ---

func (f *FakeConn) ReadMessage() (websocket.MessageType, []byte, error) {
	select {
	case fr := <-f.readCh:
		if fr.err != nil {
			return 0, nil, fr.err
		}
		return fr.mt, fr.data, nil
	case <-f.closedCh:
		return 0, nil, errors.New("use of closed connection")
	}
}

func (f *FakeConn) WriteMessage(mt websocket.MessageType, data []byte) error {
	f.mu.Lock()
	blocked := f.blockWrites
	stuck := f.stuckCh
	f.mu.Unlock()
	if blocked {
		return errors.New("fake: write blocked (broken pipe)")
	}
	if stuck != nil {
		select {
		case <-stuck:
		case <-f.closedCh:
			return errors.New("fake: connection closed during write")
		}
	}
	f.mu.Lock()
	cp := append([]byte(nil), data...)
	f.writes = append(f.writes, Written{Type: mt, Data: cp})
	f.mu.Unlock()
	f.signal()
	return nil
}

func (f *FakeConn) WriteControl(mt websocket.MessageType, data []byte, _ time.Time) error {
	f.mu.Lock()
	cp := append([]byte(nil), data...)
	f.writes = append(f.writes, Written{Type: mt, Data: cp, Ctrl: true})
	f.mu.Unlock()
	f.signal()
	return nil
}

func (f *FakeConn) signal() {
	select {
	case f.writeSignal <- struct{}{}:
	default:
	}
}

func (f *FakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *FakeConn) SetWriteDeadline(time.Time) error { return nil }

func (f *FakeConn) SetReadLimit(limit int64) {
	f.mu.Lock()
	f.readLimit = limit
	f.mu.Unlock()
}

func (f *FakeConn) SetPongHandler(h func(string) error) {
	f.mu.Lock()
	f.pongHandler = h
	f.mu.Unlock()
}

func (f *FakeConn) SetPingHandler(func(string) error)       {}
func (f *FakeConn) SetCloseHandler(func(int, string) error) {}

func (f *FakeConn) RemoteAddr() net.Addr { return f.remote }

func (f *FakeConn) Close() error {
	f.closeOnce.Do(func() { close(f.closedCh) })
	return nil
}

// Compile-time assurance.
var _ websocket.Conn = (*FakeConn)(nil)
