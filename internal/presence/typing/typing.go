// Package typing manages user typing indicators, heartbeat refreshes, and spam rate-limiting.
package typing

import (
	"time"
)

// Manager tracks typing actions and determines if an event should be dispatched.
type Manager struct {
	typingHeartbeat time.Duration
	typingTimeout   time.Duration
}

// New constructs a typing Manager.
func New(heartbeat, timeout time.Duration) *Manager {
	return &Manager{
		typingHeartbeat: heartbeat,
		typingTimeout:   timeout,
	}
}

// ShouldPublish determines whether a transition or heartbeat needs to be broadcast
// to prevent spamming redundant typing states to clients.
func (m *Manager) ShouldPublish(currentlyTyping, previouslyTyping bool, lastTypingUpdate time.Time, now time.Time) bool {
	// If starting or stopping typing, immediately publish
	if currentlyTyping != previouslyTyping {
		return true
	}
	// If already typing, only publish a heartbeat/keepalive if enough time has passed
	if currentlyTyping && now.Sub(lastTypingUpdate) >= m.typingHeartbeat {
		return true
	}
	return false
}

// IsExpired checks if a typing indicator has timed out.
func (m *Manager) IsExpired(lastTypingUpdate time.Time, now time.Time) bool {
	return !lastTypingUpdate.IsZero() && now.Sub(lastTypingUpdate) >= m.typingTimeout
}
