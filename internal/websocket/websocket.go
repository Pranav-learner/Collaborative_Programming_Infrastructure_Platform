// Package websocket is the platform's abstraction over a concrete WebSocket
// library. Everything above the transport (connection, manager, gateway) depends
// only on the Conn and Upgrader interfaces defined here, never on the underlying
// library. This keeps the core testable with an in-memory fake and lets us swap
// the implementation (e.g. gorilla -> coder/websocket) without touching business
// logic.
//
// The gorilla-backed implementation lives in gorilla.go and is the only file in
// the codebase that imports the third-party library.
package websocket

import (
	"net"
	"net/http"
	"time"
)

// MessageType enumerates the WebSocket frame opcodes we care about. The numeric
// values match RFC 6455 (and gorilla's constants) so the adapter is a trivial
// cast.
type MessageType int

const (
	// TextMessage denotes a UTF-8 text data frame.
	TextMessage MessageType = 1
	// BinaryMessage denotes a binary data frame.
	BinaryMessage MessageType = 2
	// CloseMessage denotes a close control frame.
	CloseMessage MessageType = 8
	// PingMessage denotes a ping control frame.
	PingMessage MessageType = 9
	// PongMessage denotes a pong control frame.
	PongMessage MessageType = 10
)

// Close codes (RFC 6455 §7.4.1) used by the gateway when closing connections.
const (
	CloseNormalClosure     = 1000
	CloseGoingAway         = 1001
	CloseProtocolError     = 1002
	CloseAbnormalClosure   = 1006
	ClosePolicyViolation   = 1008
	CloseMessageTooBig     = 1009
	CloseInternalServerErr = 1011
	CloseTryAgainLater     = 1013
)

// Conn is the minimal WebSocket connection surface the platform relies on.
//
// Concurrency contract (inherited from the underlying library and enforced by
// the connection package's read-pump/write-pump split): at most one goroutine
// may call the read methods and at most one (a different one) may call the write
// methods concurrently. Deadline/handler setters follow the same discipline.
type Conn interface {
	// ReadMessage blocks until a data frame arrives or an error occurs. Control
	// frames (ping/pong/close) are handled by the registered handlers and are
	// not returned here.
	ReadMessage() (MessageType, []byte, error)

	// WriteMessage writes a single data frame.
	WriteMessage(mt MessageType, data []byte) error

	// WriteControl writes a control frame (ping/pong/close) with a hard deadline.
	WriteControl(mt MessageType, data []byte, deadline time.Time) error

	// SetReadDeadline sets the absolute deadline for future reads.
	SetReadDeadline(t time.Time) error
	// SetWriteDeadline sets the absolute deadline for future writes.
	SetWriteDeadline(t time.Time) error
	// SetReadLimit caps the size of an inbound message; exceeding it fails the read.
	SetReadLimit(limit int64)

	// SetPongHandler registers a handler invoked (from within ReadMessage) when a
	// pong arrives. Used to extend the read deadline for liveness.
	SetPongHandler(h func(appData string) error)
	// SetPingHandler registers a handler invoked when a ping arrives.
	SetPingHandler(h func(appData string) error)
	// SetCloseHandler registers a handler invoked when a close frame arrives.
	SetCloseHandler(h func(code int, text string) error)

	// RemoteAddr returns the peer address, for logging and rate-limit keying.
	RemoteAddr() net.Addr
	// Close closes the underlying network connection without a close handshake.
	Close() error
}

// Upgrader upgrades an HTTP request to a WebSocket Conn. The concrete
// implementation carries buffer sizes, handshake timeout, and the origin check.
type Upgrader interface {
	Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Conn, error)
}
