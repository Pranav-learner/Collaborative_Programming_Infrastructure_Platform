// Package presence implements the Presence & Awareness System.
package presence

import (
	"cpip/internal/presence/types"
)

// Expose type aliases so external packages continue using cpip/internal/presence types.
type State = types.State
type Cursor = types.Cursor
type Selection = types.Selection
type Presence = types.Presence

const (
	StateConnecting   = types.StateConnecting
	StateOnline       = types.StateOnline
	StateIdle         = types.StateIdle
	StateAway         = types.StateAway
	StateDisconnected = types.StateDisconnected
	StateRecovered    = types.StateRecovered
	StateOffline      = types.StateOffline
)

var (
	ErrInvalidStateTransition = types.ErrInvalidStateTransition
	ErrMetadataTooLarge       = types.ErrMetadataTooLarge
)

// CanTransition validates presence state machine transitions.
func CanTransition(from, to State) bool {
	return types.CanTransition(from, to)
}
