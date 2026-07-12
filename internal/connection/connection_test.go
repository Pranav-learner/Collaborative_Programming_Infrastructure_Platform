package connection_test

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cpip/internal/auth"
	"cpip/internal/connection"
	"cpip/internal/logger"
	"cpip/internal/metrics"
	"cpip/internal/session"
	"cpip/internal/websocket"
	"cpip/internal/wstest"
)

// recordingHandler records lifecycle callbacks and optionally echoes messages.
type recordingHandler struct {
	echo         bool
	connects     atomic.Int32
	messages     atomic.Int32
	disconnected chan error
	once         sync.Once
}

func newHandler(echo bool) *recordingHandler {
	return &recordingHandler{echo: echo, disconnected: make(chan error, 1)}
}

func (h *recordingHandler) OnConnect(c *connection.Connection) { h.connects.Add(1) }

func (h *recordingHandler) OnMessage(c *connection.Connection, msg connection.Inbound) {
	h.messages.Add(1)
	if h.echo {
		_ = c.Send(msg.Type, msg.Payload)
	}
}

func (h *recordingHandler) OnDisconnect(c *connection.Connection, cause error) {
	h.once.Do(func() { h.disconnected <- cause })
}

func (h *recordingHandler) waitDisconnect(t *testing.T, d time.Duration) error {
	t.Helper()
	select {
	case err := <-h.disconnected:
		return err
	case <-time.After(d):
		t.Fatal("timed out waiting for disconnect")
		return nil
	}
}

func testConfig() connection.Config {
	return connection.Config{
		HeartbeatInterval: 20 * time.Millisecond,
		PongTimeout:       500 * time.Millisecond,
		WriteTimeout:      time.Second,
		MaxPayloadBytes:   1024,
		SendQueueSize:     4,
	}
}

func newConn(fc websocket.Conn, h connection.Handler) *connection.Connection {
	return connection.New(connection.Params{
		ID:       "c-test",
		Identity: auth.Identity{UserID: "u1"},
		Session:  session.New("u1", time.Now()),
		Conn:     fc,
		Config:   testConfig(),
		Logger:   logger.Nop(),
		Metrics:  metrics.NewNoop(),
		Handler:  h,
		Parent:   context.Background(),
	})
}

// firstDataWrite returns the payload of the first non-control frame, or nil.
func firstDataWrite(ws []wstest.Written) []byte {
	for _, w := range ws {
		if !w.Ctrl {
			return w.Data
		}
	}
	return nil
}

func hasControl(ws []wstest.Written, mt websocket.MessageType) bool {
	for _, w := range ws {
		if w.Ctrl && w.Type == mt {
			return true
		}
	}
	return false
}

func TestConnection_EchoAndClientClose(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(true)
	c := newConn(fc, h)

	go c.Serve()

	fc.PushRead(websocket.TextMessage, []byte("hello"))

	// Wait for the echo to be written.
	deadline := time.After(2 * time.Second)
	var echoed []byte
	for echoed == nil {
		select {
		case <-deadline:
			t.Fatal("did not observe echoed frame")
		default:
		}
		fc.WaitForWrite(50 * time.Millisecond)
		echoed = firstDataWrite(fc.Writes())
	}
	if string(echoed) != "hello" {
		t.Fatalf("echo mismatch: got %q want %q", echoed, "hello")
	}

	// Simulate client disconnect.
	fc.PushReadErr(io.EOF)

	cause := h.waitDisconnect(t, 2*time.Second)
	if cause == nil {
		t.Fatal("expected a non-nil close cause for client close")
	}
	if got := c.State(); got != connection.StateClosed {
		t.Fatalf("state = %v, want closed", got)
	}
	if h.connects.Load() != 1 {
		t.Fatalf("OnConnect called %d times, want 1", h.connects.Load())
	}
	// The write pump must emit a close frame on teardown.
	if !hasControl(fc.Writes(), websocket.CloseMessage) {
		t.Fatal("expected a close control frame on teardown")
	}
	if !fc.IsClosed() {
		t.Fatal("expected underlying conn to be closed")
	}
}

func TestConnection_HeartbeatSendsPing(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(false)
	c := newConn(fc, h)

	go c.Serve()
	defer func() { fc.PushReadErr(io.EOF); h.waitDisconnect(t, 2*time.Second) }()

	// HeartbeatInterval is 20ms; a ping should appear well within 1s.
	deadline := time.After(2 * time.Second)
	for {
		select {
		case <-deadline:
			t.Fatal("no ping frame observed")
		default:
		}
		fc.WaitForWrite(50 * time.Millisecond)
		if hasControl(fc.Writes(), websocket.PingMessage) {
			return
		}
	}
}

func TestConnection_PongUpdatesLastPong(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(false)
	c := newConn(fc, h)

	if !c.LastPong().IsZero() {
		t.Fatal("LastPong should be zero before any pong")
	}

	go c.Serve()
	defer func() { fc.PushReadErr(io.EOF); h.waitDisconnect(t, 2*time.Second) }()

	// Fire a pong and expect LastPong to advance.
	deadline := time.After(2 * time.Second)
	for c.LastPong().IsZero() {
		select {
		case <-deadline:
			t.Fatal("LastPong never updated after pong")
		default:
		}
		_ = fc.FirePong()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestConnection_SlowConsumerBackpressure(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(false)
	c := newConn(fc, h)

	fc.StuckWrites() // the write pump will block on its first write
	go c.Serve()

	// Give the write pump a moment to start and pull/stick on the first message.
	time.Sleep(20 * time.Millisecond)

	// Flood the outbound queue (size 4). With the pump stuck, some Send must
	// overflow and return ErrSendQueueFull, closing the connection.
	var overflow bool
	for i := 0; i < 50; i++ {
		if err := c.Send(websocket.TextMessage, []byte("x")); err != nil {
			overflow = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !overflow {
		t.Fatal("expected send-queue overflow (slow-consumer backpressure)")
	}

	// Release the stuck write so the pump can observe the close and exit.
	fc.ReleaseWrites()

	cause := h.waitDisconnect(t, 2*time.Second)
	if cause == nil {
		t.Fatal("expected a close cause after slow-consumer close")
	}
	if c.State() != connection.StateClosed {
		t.Fatalf("state = %v, want closed", c.State())
	}
}

func TestConnection_ServerShutdownCloses(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(false)
	c := newConn(fc, h)

	go c.Serve()
	time.Sleep(20 * time.Millisecond)

	c.Shutdown()

	cause := h.waitDisconnect(t, 2*time.Second)
	if cause == nil {
		t.Fatal("expected a close cause after shutdown")
	}
	if c.State() != connection.StateClosed {
		t.Fatalf("state = %v, want closed", c.State())
	}
	if !hasControl(fc.Writes(), websocket.CloseMessage) {
		t.Fatal("expected close frame after shutdown")
	}
}

func TestConnection_SetsReadLimit(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(false)
	c := newConn(fc, h)

	go c.Serve()
	defer func() { fc.PushReadErr(io.EOF); h.waitDisconnect(t, 2*time.Second) }()

	deadline := time.After(2 * time.Second)
	for fc.ReadLimit() == 0 {
		select {
		case <-deadline:
			t.Fatal("read limit was never applied")
		default:
		}
		time.Sleep(5 * time.Millisecond)
	}
	if fc.ReadLimit() != 1024 {
		t.Fatalf("read limit = %d, want 1024", fc.ReadLimit())
	}
}

func TestConnection_NoGoroutineLeakOnClose(t *testing.T) {
	fc := wstest.NewFakeConn()
	h := newHandler(false)
	c := newConn(fc, h)

	// Serve on a tracked goroutine; it must return after the client closes.
	done := make(chan struct{})
	go func() { c.Serve(); close(done) }()

	fc.PushReadErr(io.EOF)

	select {
	case <-done:
		// Serve returned => both pumps exited => no leak.
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return; goroutine leak")
	}
}
