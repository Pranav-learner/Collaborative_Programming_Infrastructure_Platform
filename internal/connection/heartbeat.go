package connection

import "time"

// heartbeat encapsulates the server-side ping cadence. It is a small, cohesive
// unit owned by the write pump (the only goroutine allowed to write), so pings
// are emitted without violating the single-writer rule.
//
// The liveness model is split across the two pumps, which is the standard robust
// pattern:
//
//   - write pump: every HeartbeatInterval, send a ping (this type's ticker).
//   - read pump:  each read (including the pong that answers our ping) extends
//     the read deadline by PongTimeout. If PongTimeout elapses with no pong or
//     data, ReadMessage fails and the connection is torn down.
//
// Config.Validate guarantees PongTimeout > HeartbeatInterval, so a healthy client
// always answers a ping well before the deadline.
type heartbeat struct {
	ticker *time.Ticker
}

func newHeartbeat(interval time.Duration) *heartbeat {
	if interval <= 0 {
		interval = 30 * time.Second // defensive; Config.Validate forbids <= 0
	}
	return &heartbeat{ticker: time.NewTicker(interval)}
}

// C returns the tick channel; a tick means "send a ping now".
func (h *heartbeat) C() <-chan time.Time { return h.ticker.C }

// stop halts the ticker and releases its resources.
func (h *heartbeat) stop() { h.ticker.Stop() }
