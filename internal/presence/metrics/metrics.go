// Package metrics provides interfaces and hooks for recording presence metrics.
package metrics

// Recorder abstracts the collection of presence-related telemetries.
type Recorder interface {
	// ActiveConnections tracks the total count of live presence connections.
	ActiveConnections(count int)
	// HeartbeatReceived increments the counter for received heartbeat signals.
	HeartbeatReceived()
	// HeartbeatTimeout increments the counter for dead heartbeats.
	HeartbeatTimeout()
	// EventPublished increments the count of presence events dispatched.
	EventPublished()
	// EventDropped counts events dropped due to slow subscribers.
	EventDropped()
	// TypingFloodPrevented tracks rate-limiting activations on typing messages.
	TypingFloodPrevented()
	// PresenceConflict counts duplicate session registrations.
	PresenceConflict()
	// ReconnectExpired counts reconnect grace period expirations.
	ReconnectExpired()
}

// Noop implements Recorder by doing nothing (useful for tests or default configurations).
type Noop struct{}

// NewNoop returns a no-op metrics recorder.
func NewNoop() Recorder {
	return Noop{}
}

func (Noop) ActiveConnections(count int) {}
func (Noop) HeartbeatReceived()          {}
func (Noop) HeartbeatTimeout()           {}
func (Noop) EventPublished()             {}
func (Noop) EventDropped()               {}
func (Noop) TypingFloodPrevented()       {}
func (Noop) PresenceConflict()           {}
func (Noop) ReconnectExpired()           {}
