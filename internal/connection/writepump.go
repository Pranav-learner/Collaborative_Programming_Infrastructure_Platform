package connection

import (
	"runtime/debug"
	"time"

	"cpip/internal/websocket"
)

// writePump is the sole writer of the underlying socket. It runs on its own
// goroutine and multiplexes three sources onto the single write path:
//
//   - queued outbound data frames (from Send),
//   - periodic pings (heartbeat), and
//   - the close signal (context cancellation).
//
// Because gorilla/websocket forbids concurrent writes, funnelling everything
// through this one goroutine is what makes the connection race-free. The write
// pump is also the sole closer of the socket: on exit it writes a best-effort
// close frame and closes the connection, which unblocks the read pump.
func (c *Connection) writePump() {
	hb := newHeartbeat(c.cfg.HeartbeatInterval)

	defer func() {
		if r := recover(); r != nil {
			c.log.Error("write pump panic recovered", "panic", r, "stack", string(debug.Stack()))
			c.closeWithCause(errPanic, websocket.CloseInternalServerErr)
		}
		hb.stop()
		c.writeClose()     // emit a courteous close frame (best-effort)
		_ = c.conn.Close() // unblock the read pump
		c.wg.Done()
	}()

	for {
		select {
		case msg := <-c.send:
			if err := c.writeFrame(msg); err != nil {
				c.log.Debug("write failed, closing", "err", err.Error())
				c.closeWithCause(errWriteFailed, websocket.CloseAbnormalClosure)
				return
			}

		case <-hb.C():
			if err := c.writePing(); err != nil {
				c.log.Debug("ping failed, closing", "err", err.Error())
				c.closeWithCause(errWriteFailed, websocket.CloseAbnormalClosure)
				return
			}
			c.metrics.PingSent()

		case <-c.ctx.Done():
			// A close was initiated (by us, the peer via the read pump, or
			// shutdown). Stop writing; the deferred close handshake runs next.
			return
		}
	}
}

// writeFrame writes a single data frame with the configured write deadline.
func (c *Connection) writeFrame(msg outbound) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.cfg.WriteTimeout)); err != nil {
		return err
	}
	if err := c.conn.WriteMessage(msg.mt, msg.data); err != nil {
		return err
	}
	c.metrics.MessageSent(len(msg.data))
	return nil
}

// writePing sends a ping control frame with the write deadline.
func (c *Connection) writePing() error {
	return c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(c.cfg.WriteTimeout))
}

// writeClose sends a best-effort close control frame carrying the recorded close
// code. Errors are ignored: the socket is about to be closed regardless.
func (c *Connection) writeClose() {
	code := c.closeCode
	if code == 0 {
		code = websocket.CloseNormalClosure
	}
	payload := websocket.FormatCloseMessage(code, "")
	_ = c.conn.WriteControl(websocket.CloseMessage, payload, time.Now().Add(c.cfg.WriteTimeout))
}
