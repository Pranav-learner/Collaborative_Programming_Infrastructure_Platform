// Package types defines the leaf models and constants for the Presence & Awareness System.
package types

import (
	"errors"
	"time"
)

// State defines the current connection and activity status of a participant's presence.
type State uint8

const (
	StateConnecting State = iota
	StateOnline
	StateIdle
	StateAway
	StateDisconnected
	StateRecovered
	StateOffline
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "Connecting"
	case StateOnline:
		return "Online"
	case StateIdle:
		return "Idle"
	case StateAway:
		return "Away"
	case StateDisconnected:
		return "Disconnected"
	case StateRecovered:
		return "Recovered"
	case StateOffline:
		return "Offline"
	default:
		return "Unknown"
	}
}

// CanTransition validates presence state machine transitions.
func CanTransition(from, to State) bool {
	switch from {
	case StateConnecting:
		return to == StateOnline || to == StateDisconnected || to == StateOffline
	case StateOnline:
		return to == StateIdle || to == StateAway || to == StateDisconnected || to == StateOffline
	case StateIdle:
		return to == StateOnline || to == StateAway || to == StateDisconnected || to == StateOffline
	case StateAway:
		return to == StateOnline || to == StateDisconnected || to == StateOffline
	case StateDisconnected:
		return to == StateRecovered || to == StateOffline
	case StateRecovered:
		return to == StateOnline || to == StateOffline
	case StateOffline:
		return false
	default:
		return false
	}
}

// Cursor models the coordinates and attributes of a user's cursor.
type Cursor struct {
	Line     int    `json:"line"`
	Ch       int    `json:"ch"`
	Color    string `json:"color,omitempty"`
	Visible  bool   `json:"visible"`
	FilePath string `json:"file_path,omitempty"` // For future multi-file workspace support
}

// Selection models the text selection range.
type Selection struct {
	AnchorLine int `json:"anchor_line"`
	AnchorCh   int `json:"anchor_ch"`
	FocusLine  int `json:"focus_line"`
	FocusCh    int `json:"focus_ch"`
	Direction  int `json:"direction"` // 1 forward, -1 backward, 0 none
}

// Presence defines the presence model for a connected participant's live session.
type Presence struct {
	UserID         string         `json:"user_id"`
	ConnID         string         `json:"conn_id"`
	SessionID      string         `json:"session_id"`
	RoomID         string         `json:"room_id"`
	State          State          `json:"state"`
	Cursor         Cursor         `json:"cursor"`
	Selection      Selection      `json:"selection"`
	IsTyping       bool           `json:"is_typing"`
	LastHeartbeat  time.Time      `json:"last_heartbeat"`
	LastActivity   time.Time      `json:"last_activity"`
	JoinTime       time.Time      `json:"join_time"`
	ReconnectToken string         `json:"-"`
	Metadata       map[string]any `json:"metadata,omitempty"`
}

// Clone performs a deep copy of the presence entity to maintain boundaries.
func (p Presence) Clone() Presence {
	cloned := p
	if p.Metadata != nil {
		cloned.Metadata = make(map[string]any, len(p.Metadata))
		for k, v := range p.Metadata {
			cloned.Metadata[k] = v
		}
	}
	return cloned
}

var (
	ErrInvalidStateTransition = errors.New("presence: invalid state transition")
	ErrMetadataTooLarge       = errors.New("presence: metadata size exceeds limit")
)
