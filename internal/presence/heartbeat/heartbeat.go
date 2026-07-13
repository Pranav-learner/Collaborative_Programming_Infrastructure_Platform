// Package heartbeat validates heartbeats, timeout windows, and session recoveries.
package heartbeat

import (
	"errors"
	"time"

	"cpip/internal/presence/types"
)

var (
	ErrTokenMismatch  = errors.New("heartbeat: reconnect token mismatch")
	ErrSessionExpired = errors.New("heartbeat: session recovery window expired")
)

// Monitor validates heartbeat events and handles recovery tokens.
type Monitor struct {
	heartbeatTimeout time.Duration
	recoveryTimeout  time.Duration
}

// NewMonitor constructs a heartbeat Monitor.
func NewMonitor(hbTimeout, recTimeout time.Duration) *Monitor {
	return &Monitor{
		heartbeatTimeout: hbTimeout,
		recoveryTimeout:  recTimeout,
	}
}

// IsDead checks if the participant has missed heartbeats past the timeout duration.
func (m *Monitor) IsDead(lastHb time.Time, now time.Time) bool {
	return !lastHb.IsZero() && now.Sub(lastHb) > m.heartbeatTimeout
}

// ValidateRecovery checks if the reconnect token matches and the recovery window is still open.
func (m *Monitor) ValidateRecovery(p types.Presence, token string, now time.Time) error {
	if p.ReconnectToken == "" || p.ReconnectToken != token {
		return ErrTokenMismatch
	}
	if p.State != types.StateDisconnected {
		return errors.New("heartbeat: presence session is not in disconnected state")
	}
	// We check the recovery window based on LastActivity or LastHeartbeat (when they went disconnected).
	lastSeen := p.LastActivity
	if p.LastHeartbeat.After(lastSeen) {
		lastSeen = p.LastHeartbeat
	}
	if !lastSeen.IsZero() && now.Sub(lastSeen) > m.recoveryTimeout {
		return ErrSessionExpired
	}
	return nil
}
