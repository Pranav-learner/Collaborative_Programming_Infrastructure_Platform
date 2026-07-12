package connection

import (
	"log/slog"
	"time"

	"cpip/internal/websocket"
)

// Inbound is a decoded data frame delivered to the Handler. It carries only what
// higher layers need; control frames (ping/pong/close) never reach the Handler.
type Inbound struct {
	Type       websocket.MessageType
	Payload    []byte
	ReceivedAt time.Time
}

// Handler is the seam between the transport layer (this module) and every future
// application module (rooms, presence, CRDT relay, execution result streaming).
//
// The gateway owns the socket; the Handler owns the meaning of the bytes. A
// future Room/Presence router will implement this interface and be injected into
// the gateway with no transport changes. All three methods are called from the
// connection's own goroutines:
//
//   - OnConnect is called once, before any message, when the connection becomes
//     active. Implementations may register the connection with a room, seed
//     presence, etc.
//   - OnMessage is called for every inbound data frame, in order, from the read
//     pump's goroutine. Implementations MUST NOT block for long; heavy work
//     should be handed to another goroutine/queue.
//   - OnDisconnect is called once, after both pumps have stopped, with the cause
//     of termination (may be nil for a clean close). Implementations release any
//     room/presence state here.
//
// Implementations MUST be safe for concurrent use across many connections.
type Handler interface {
	OnConnect(c *Connection)
	OnMessage(c *Connection, msg Inbound)
	OnDisconnect(c *Connection, cause error)
}

// NoopHandler is the default Handler for this module: it logs at debug level and
// does nothing else. It lets the gateway run end-to-end before the room/presence
// modules exist.
type NoopHandler struct {
	Log *slog.Logger
}

func (h NoopHandler) OnConnect(c *Connection) {
	if h.Log != nil {
		h.Log.Debug("handler: connect", "conn_id", c.ID(), "user_id", c.UserID())
	}
}

func (h NoopHandler) OnMessage(c *Connection, msg Inbound) {
	if h.Log != nil {
		h.Log.Debug("handler: message", "conn_id", c.ID(), "type", int(msg.Type), "bytes", len(msg.Payload))
	}
}

func (h NoopHandler) OnDisconnect(c *Connection, cause error) {
	if h.Log != nil {
		h.Log.Debug("handler: disconnect", "conn_id", c.ID(), "cause", errString(cause))
	}
}

// Compile-time assurance.
var _ Handler = NoopHandler{}
