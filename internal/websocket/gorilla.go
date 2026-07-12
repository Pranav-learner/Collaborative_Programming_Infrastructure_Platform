package websocket

import (
	"net"
	"net/http"
	"time"

	gws "github.com/gorilla/websocket"
)

// This is the ONLY file that imports the third-party WebSocket library. It
// adapts gorilla/websocket to the platform's Conn and Upgrader interfaces.

// UpgraderConfig configures the gorilla-backed upgrader.
type UpgraderConfig struct {
	ReadBufferSize   int
	WriteBufferSize  int
	HandshakeTimeout time.Duration
	// CheckOrigin returns true if the request Origin is acceptable. It is
	// supplied by the security package; a nil value rejects all cross-origin
	// requests (gorilla's safe default).
	CheckOrigin func(r *http.Request) bool
}

// GorillaUpgrader implements Upgrader using gorilla/websocket.
type GorillaUpgrader struct {
	up gws.Upgrader
}

// NewGorillaUpgrader builds a GorillaUpgrader from cfg.
func NewGorillaUpgrader(cfg UpgraderConfig) *GorillaUpgrader {
	return &GorillaUpgrader{
		up: gws.Upgrader{
			ReadBufferSize:   cfg.ReadBufferSize,
			WriteBufferSize:  cfg.WriteBufferSize,
			HandshakeTimeout: cfg.HandshakeTimeout,
			CheckOrigin:      cfg.CheckOrigin,
			// The upgrader writes its own HTTP error responses on failure so the
			// caller can distinguish upgrade failures from other errors.
			Error: nil,
		},
	}
}

// Upgrade implements Upgrader.
func (g *GorillaUpgrader) Upgrade(w http.ResponseWriter, r *http.Request, responseHeader http.Header) (Conn, error) {
	c, err := g.up.Upgrade(w, r, responseHeader)
	if err != nil {
		return nil, err
	}
	return &gorillaConn{c: c}, nil
}

// gorillaConn adapts *gws.Conn to Conn.
type gorillaConn struct {
	c *gws.Conn
}

// Compile-time assurance the adapter satisfies the interface.
var _ Conn = (*gorillaConn)(nil)

func (g *gorillaConn) ReadMessage() (MessageType, []byte, error) {
	mt, p, err := g.c.ReadMessage()
	return MessageType(mt), p, err
}

func (g *gorillaConn) WriteMessage(mt MessageType, data []byte) error {
	return g.c.WriteMessage(int(mt), data)
}

func (g *gorillaConn) WriteControl(mt MessageType, data []byte, deadline time.Time) error {
	return g.c.WriteControl(int(mt), data, deadline)
}

func (g *gorillaConn) SetReadDeadline(t time.Time) error  { return g.c.SetReadDeadline(t) }
func (g *gorillaConn) SetWriteDeadline(t time.Time) error { return g.c.SetWriteDeadline(t) }
func (g *gorillaConn) SetReadLimit(limit int64)           { g.c.SetReadLimit(limit) }

func (g *gorillaConn) SetPongHandler(h func(string) error) { g.c.SetPongHandler(h) }
func (g *gorillaConn) SetPingHandler(h func(string) error) { g.c.SetPingHandler(h) }
func (g *gorillaConn) SetCloseHandler(h func(int, string) error) {
	g.c.SetCloseHandler(h)
}

func (g *gorillaConn) RemoteAddr() net.Addr { return g.c.RemoteAddr() }
func (g *gorillaConn) Close() error         { return g.c.Close() }

// FormatCloseMessage builds a close-frame payload for the given code and text.
func FormatCloseMessage(code int, text string) []byte {
	return gws.FormatCloseMessage(code, text)
}

// IsUnexpectedClose reports whether err is a close error with a code other than
// the "clean" ones (normal, going-away, no-status). Used to distinguish a
// graceful client close from an abnormal one for logging/metrics.
func IsUnexpectedClose(err error) bool {
	return gws.IsUnexpectedCloseError(err,
		gws.CloseNormalClosure, gws.CloseGoingAway, gws.CloseNoStatusReceived)
}

// IsCloseError reports whether err is a *CloseError with one of the given codes.
func IsCloseError(err error, codes ...int) bool {
	return gws.IsCloseError(err, codes...)
}
