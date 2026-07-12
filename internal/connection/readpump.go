package connection

import (
	"errors"
	"net"
	"runtime/debug"
	"strings"
	"time"

	"cpip/internal/websocket"
)

// readPump is the sole reader of the underlying socket. It runs in the Serve
// goroutine. It configures the read limit, the initial read deadline, and the
// pong handler (which extends the deadline — this is how dead connections are
// detected: if no pong/data arrives within PongTimeout, ReadMessage fails and the
// pump exits). Every inbound data frame is handed to the Handler in order.
//
// On any exit path the pump records a close cause; the deferred cancel ensures
// the write pump is signalled to stop. The read pump never closes the socket
// itself — that is the write pump's job — so the two never race on Close.
func (c *Connection) readPump() {
	defer func() {
		if r := recover(); r != nil {
			c.log.Error("read pump panic recovered", "panic", r, "stack", string(debug.Stack()))
			c.closeWithCause(errPanic, websocket.CloseInternalServerErr)
		}
		// Guarantee the write pump is signalled even if we exited via an
		// unclassified path. Context cancel is idempotent and first-cause-wins,
		// so this never overrides a cause already set by handleReadError.
		c.cancel(errClientClosed)
	}()

	c.conn.SetReadLimit(c.cfg.MaxPayloadBytes)
	_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongTimeout))
	c.conn.SetPongHandler(func(string) error {
		now := time.Now()
		c.lastPongNanos.Store(now.UnixNano())
		c.metrics.PongReceived()
		return c.conn.SetReadDeadline(now.Add(c.cfg.PongTimeout))
	})

	for {
		mt, payload, err := c.conn.ReadMessage()
		if err != nil {
			c.handleReadError(err)
			return
		}

		// Any inbound frame is liveness evidence; extend the read deadline.
		_ = c.conn.SetReadDeadline(time.Now().Add(c.cfg.PongTimeout))
		c.metrics.MessageReceived(len(payload))

		// Deliver to the application layer. The Handler is expected not to block.
		c.handler.OnMessage(c, Inbound{Type: mt, Payload: payload, ReceivedAt: time.Now()})

		// Break promptly if a close was initiated concurrently (e.g. shutdown or
		// a slow-consumer close triggered by an outbound Send).
		select {
		case <-c.ctx.Done():
			return
		default:
		}
	}
}

// handleReadError classifies a read error and records the appropriate close
// cause and WebSocket close code.
func (c *Connection) handleReadError(err error) {
	switch {
	case isTimeout(err):
		c.metrics.HeartbeatTimeout()
		c.log.Info("read deadline exceeded: heartbeat lost", "err", err.Error())
		c.closeWithCause(errReadTimeout, websocket.CloseGoingAway)

	case isReadLimit(err):
		c.log.Warn("inbound payload exceeded maximum", "max_bytes", c.cfg.MaxPayloadBytes)
		c.closeWithCause(errOversized, websocket.CloseMessageTooBig)

	case websocket.IsUnexpectedClose(err):
		c.log.Info("connection closed abnormally by peer", "err", err.Error())
		c.closeWithCause(errClientClosed, websocket.CloseAbnormalClosure)

	default:
		// Clean close, EOF, going-away, or a broken pipe after our own close.
		c.log.Debug("connection closed by peer", "err", err.Error())
		c.closeWithCause(errClientClosed, websocket.CloseNormalClosure)
	}
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

// isReadLimit best-effort detects gorilla's "read limit exceeded" error, which
// is not exported as a sentinel. If the library changes, the connection still
// closes correctly — only the metrics label differs.
func isReadLimit(err error) bool {
	return err != nil && strings.Contains(err.Error(), "read limit")
}
