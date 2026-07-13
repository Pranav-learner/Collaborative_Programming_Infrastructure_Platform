// Package recovery assists in managing session recovery states and timeout tracking.
package recovery

import (
	"time"
)

// Tracker assists in monitoring and validating session recovery status.
// In our architecture, the recovery status is tracked on the Room's Participant model.
// This package contains helper types/methods for recovery window evaluations.
type Tracker struct{}

// NewTracker returns a new session recovery Tracker.
func NewTracker() *Tracker {
	return &Tracker{}
}

// IsExpired checks if a disconnected participant has exceeded their recovery timeout.
func (t *Tracker) IsExpired(lastSeen time.Time, timeout time.Duration, now time.Time) bool {
	return !lastSeen.IsZero() && now.Sub(lastSeen) > timeout
}
