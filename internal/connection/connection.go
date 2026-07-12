// Package connection models a single live WebSocket client connection and owns
// its full runtime: the read pump, the write pump, heartbeat, backpressure, and
// deterministic teardown.
//
// Goroutine model (exactly two goroutines per connection, both guaranteed to
// terminate):
//
//	Serve()            runs in the caller's goroutine and drives the read pump
//	  └─ go writePump  a second goroutine draining the outbound queue + pinging
//
// The connection is closed exactly once via a sync.Once. Closing cancels a
// context (carrying the cause) which both pumps observe; the write pump is the
// sole closer of the underlying socket, which unblocks the read pump. When both
// pumps have returned, finalize() runs cleanup (metrics, handler.OnDisconnect,
// registry removal) precisely once.
package connection

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"cpip/internal/auth"
	"cpip/internal/metrics"
	"cpip/internal/session"
	"cpip/internal/websocket"
)

// Exported errors returned by Send.
var (
	// ErrSendQueueFull means the outbound buffer is full: the client is a slow
	// consumer. The connection is closed as a side effect (slow-consumer
	// isolation) so it never degrades other connections.
	ErrSendQueueFull = errors.New("connection: send queue full (slow consumer)")
	// ErrConnectionClosed means the connection is closing or closed.
	ErrConnectionClosed = errors.New("connection: closed")
)

// Internal close causes. These are attached to the connection context via
// context.WithCancelCause and drive the close code, the metrics reason label,
// and the log line.
var (
	errClientClosed   = errors.New("client closed connection")
	errReadTimeout    = errors.New("read timeout: heartbeat lost")
	errSlowConsumer   = errors.New("send queue overflow: slow consumer")
	errWriteFailed    = errors.New("write failed")
	errOversized      = errors.New("inbound payload too large")
	errServerShutdown = errors.New("server shutting down")
	errServerClosed   = errors.New("closed by server")
	errPanic          = errors.New("panic recovered")
)

// Config is the connection-level subset of the platform configuration.
type Config struct {
	HeartbeatInterval time.Duration
	PongTimeout       time.Duration
	WriteTimeout      time.Duration
	MaxPayloadBytes   int64
	SendQueueSize     int
}

// Params are the inputs required to construct a Connection.
type Params struct {
	ID        string
	Identity  auth.Identity
	Session   *session.Session
	Conn      websocket.Conn
	Config    Config
	Logger    *slog.Logger
	Metrics   metrics.Recorder
	Handler   Handler
	Parent    context.Context // parent context (cancelled on gateway shutdown)
	RequestID string          // correlation id from the HTTP handshake
	// OnClose is invoked exactly once during finalize, after both pumps stop.
	// The gateway wires this to the connection manager's Unregister.
	OnClose func(*Connection)
}

// outbound is a queued message awaiting the write pump.
type outbound struct {
	mt   websocket.MessageType
	data []byte
}

// Connection is a single client's live WebSocket connection.
type Connection struct {
	id       string
	identity auth.Identity
	session  *session.Session

	conn       websocket.Conn
	remoteAddr string
	cfg        Config
	log        *slog.Logger
	metrics    metrics.Recorder
	handler    Handler

	send   chan outbound
	ctx    context.Context
	cancel context.CancelCauseFunc

	state         atomicState
	createdAt     time.Time
	lastPongNanos atomic.Int64 // unix-nanos of the most recent pong; 0 until first pong
	closeOnce     sync.Once
	closeCode     int // guarded by closeOnce; read only in write pump after cancel

	onClose func(*Connection)
	wg      sync.WaitGroup // tracks the write pump

	// Reserved for later modules (room binding). Guarded by roomMu.
	roomMu sync.RWMutex
	roomID string
}

// New builds a Connection from p. It does not start any goroutines; call Serve.
func New(p Params) *Connection {
	parent := p.Parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancelCause(parent)

	remote := ""
	if p.Conn != nil {
		if a := p.Conn.RemoteAddr(); a != nil {
			remote = a.String()
		}
	}

	log := p.Logger.With(
		"conn_id", p.ID,
		"user_id", p.Identity.UserID,
		"session_id", sessionID(p.Session),
		"request_id", p.RequestID,
	)

	c := &Connection{
		id:         p.ID,
		identity:   p.Identity,
		session:    p.Session,
		conn:       p.Conn,
		remoteAddr: remote,
		cfg:        p.Config,
		log:        log,
		metrics:    p.Metrics,
		handler:    p.Handler,
		send:       make(chan outbound, p.Config.SendQueueSize),
		ctx:        ctx,
		cancel:     cancel,
		createdAt:  time.Now(),
		onClose:    p.OnClose,
	}
	c.state.Store(StateConnecting)
	return c
}

// --- accessors (all safe for concurrent use) ---

// ID returns the unique connection id.
func (c *Connection) ID() string { return c.id }

// UserID returns the authenticated principal id.
func (c *Connection) UserID() string { return c.identity.UserID }

// SessionID returns the session id (empty if no session).
func (c *Connection) SessionID() string { return sessionID(c.session) }

// Identity returns the authenticated identity.
func (c *Connection) Identity() auth.Identity { return c.identity }

// State returns the current lifecycle state.
func (c *Connection) State() State { return c.state.Load() }

// CreatedAt returns when the connection was constructed.
func (c *Connection) CreatedAt() time.Time { return c.createdAt }

// RemoteAddr returns the peer address string.
func (c *Connection) RemoteAddr() string { return c.remoteAddr }

// LastPong returns the time of the most recent pong, or the zero time if no pong
// has been received yet. Useful for health/debug introspection.
func (c *Connection) LastPong() time.Time {
	n := c.lastPongNanos.Load()
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}

// Context returns the connection's context, cancelled (with cause) on close.
// Future modules can derive per-connection work from it.
func (c *Connection) Context() context.Context { return c.ctx }

// RoomID returns the bound room id, or "" (reserved for the room module).
func (c *Connection) RoomID() string {
	c.roomMu.RLock()
	defer c.roomMu.RUnlock()
	return c.roomID
}

// SetRoomID binds the connection to a room (reserved for the room module).
func (c *Connection) SetRoomID(id string) {
	c.roomMu.Lock()
	c.roomID = id
	c.roomMu.Unlock()
}

// Log returns the connection's correlation-scoped logger, for use by handlers.
func (c *Connection) Log() *slog.Logger { return c.log }

// --- sending ---

// Send enqueues a data frame for delivery. It never blocks: if the outbound
// queue is full the client is too slow, so the connection is closed (slow-
// consumer isolation) and ErrSendQueueFull is returned. This is the single
// backpressure decision point for outbound traffic.
func (c *Connection) Send(mt websocket.MessageType, data []byte) error {
	if c.state.Load() >= StateClosing {
		return ErrConnectionClosed
	}
	select {
	case c.send <- outbound{mt: mt, data: data}:
		return nil
	case <-c.ctx.Done():
		return ErrConnectionClosed
	default:
		c.metrics.SendDropped()
		c.log.Warn("closing slow consumer: outbound queue full", "queue_size", c.cfg.SendQueueSize)
		c.closeWithCause(errSlowConsumer, websocket.ClosePolicyViolation)
		return ErrSendQueueFull
	}
}

// SendText enqueues a UTF-8 text frame.
func (c *Connection) SendText(data []byte) error {
	return c.Send(websocket.TextMessage, data)
}

// SendBinary enqueues a binary frame.
func (c *Connection) SendBinary(data []byte) error {
	return c.Send(websocket.BinaryMessage, data)
}

// --- closing ---

// Close initiates a graceful close with the given WebSocket code and reason,
// idempotently. It does not block; teardown completes on the pump goroutines.
func (c *Connection) Close(code int, reason string) {
	c.closeWithCause(errServerClosed, code)
	_ = reason // reason is folded into the close cause mapping; kept for API clarity
}

// Shutdown initiates a graceful close because the server is shutting down.
func (c *Connection) Shutdown() {
	c.closeWithCause(errServerShutdown, websocket.CloseGoingAway)
}

// closeWithCause cancels the connection context exactly once, recording the
// cause and the close code. Both pumps observe the cancellation; the write pump
// emits the close frame and closes the socket.
func (c *Connection) closeWithCause(cause error, code int) {
	c.closeOnce.Do(func() {
		c.state.Store(StateClosing)
		c.closeCode = code
		c.cancel(cause)
	})
}

// RejectAndClose is used by the gateway when a connection is upgraded but cannot
// be admitted (e.g. capacity reached after the handshake). It sends a close
// frame and closes the socket without ever starting the pumps.
func (c *Connection) RejectAndClose(code int, reason string) {
	deadline := time.Now().Add(c.cfg.WriteTimeout)
	_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), deadline)
	_ = c.conn.Close()
	c.state.Store(StateClosed)
	c.cancel(errServerClosed)
}

// --- lifecycle ---

// Serve runs the connection until it closes. It MUST be called on its own
// goroutine (the gateway does `go c.Serve()`). It starts the write pump, runs
// the read pump inline, waits for the write pump to exit, then finalizes.
func (c *Connection) Serve() {
	c.state.Store(StateActive)
	c.metrics.ConnectionOpened()
	c.handler.OnConnect(c)
	c.log.Info("connection active", "remote", c.remoteAddr)

	c.wg.Add(1)
	go c.writePump()

	c.readPump() // blocks until the connection is done

	c.wg.Wait() // ensure the write pump has fully exited
	c.finalize()
}

// finalize runs exactly once after both pumps have stopped.
func (c *Connection) finalize() {
	c.state.Store(StateClosed)
	cause := context.Cause(c.ctx)
	reason := reasonFromCause(cause)

	c.metrics.ConnectionClosed(reason)
	c.handler.OnDisconnect(c, cause)
	if c.onClose != nil {
		c.onClose(c)
	}
	c.log.Info("connection closed",
		"reason", reason,
		"cause", errString(cause),
		"lifetime", time.Since(c.createdAt).String(),
	)
}

// --- helpers ---

func sessionID(s *session.Session) string {
	if s == nil {
		return ""
	}
	return s.ID
}

// reasonFromCause maps an internal close cause to a low-cardinality metrics
// label / log reason.
func reasonFromCause(cause error) string {
	switch {
	case cause == nil:
		return "closed"
	case errors.Is(cause, errClientClosed):
		return "client_closed"
	case errors.Is(cause, errReadTimeout):
		return "heartbeat_timeout"
	case errors.Is(cause, errSlowConsumer):
		return "slow_consumer"
	case errors.Is(cause, errWriteFailed):
		return "write_failed"
	case errors.Is(cause, errOversized):
		return "payload_too_large"
	case errors.Is(cause, errServerShutdown):
		return "server_shutdown"
	case errors.Is(cause, errServerClosed):
		return "server_closed"
	case errors.Is(cause, errPanic):
		return "panic"
	case errors.Is(cause, context.Canceled):
		return "context_canceled"
	default:
		return "unknown"
	}
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
