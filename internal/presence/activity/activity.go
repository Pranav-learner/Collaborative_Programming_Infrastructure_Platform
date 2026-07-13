// Package activity tracks user action metrics and last-active stats.
package activity

import (
	"sync/atomic"
	"time"
)

// Action represents categories of user interaction.
type Action uint8

const (
	ActionCursorMove Action = iota
	ActionSelectionChange
	ActionTyping
	ActionHeartbeat
	ActionJoin
	ActionLeave
)

// Stats holds metrics on user actions.
type Stats struct {
	CursorMoves      uint64 `json:"cursor_moves"`
	SelectionChanges uint64 `json:"selection_changes"`
	TypingSessions   uint64 `json:"typing_sessions"`
	Heartbeats       uint64 `json:"heartbeats"`
	LastActive       int64  `json:"last_active_nanos"`
}

// Tracker tracks user interactions and maintains atomics-guarded statistics.
type Tracker struct {
	cursorMoves      uint64
	selectionChanges uint64
	typingSessions   uint64
	heartbeats       uint64
	lastActive       int64
}

// NewTracker builds a Tracker.
func NewTracker() *Tracker {
	return &Tracker{}
}

// Record registers an activity event, updating the last active timestamp and incrementing stats.
func (t *Tracker) Record(action Action, now time.Time) {
	atomic.StoreInt64(&t.lastActive, now.UnixNano())
	switch action {
	case ActionCursorMove:
		atomic.AddUint64(&t.cursorMoves, 1)
	case ActionSelectionChange:
		atomic.AddUint64(&t.selectionChanges, 1)
	case ActionTyping:
		atomic.AddUint64(&t.typingSessions, 1)
	case ActionHeartbeat:
		atomic.AddUint64(&t.heartbeats, 1)
	}
}

// Stats returns a snapshot copy of the activity statistics.
func (t *Tracker) Stats() Stats {
	return Stats{
		CursorMoves:      atomic.LoadUint64(&t.cursorMoves),
		SelectionChanges: atomic.LoadUint64(&t.selectionChanges),
		TypingSessions:   atomic.LoadUint64(&t.typingSessions),
		Heartbeats:       atomic.LoadUint64(&t.heartbeats),
		LastActive:       atomic.LoadInt64(&t.lastActive),
	}
}

// LastActive returns the time of the last recorded action, or a zero time.
func (t *Tracker) LastActive() time.Time {
	nanos := atomic.LoadInt64(&t.lastActive)
	if nanos == 0 {
		return time.Time{}
	}
	return time.Unix(0, nanos)
}
