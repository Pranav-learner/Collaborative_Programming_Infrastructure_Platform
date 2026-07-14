package plugins

import (
	"fmt"
)

// State defines the plugin lifecycle states.
type State string

const (
	StateRegistered  State = "registered"
	StateValidated   State = "validated"
	StateLoaded      State = "loaded"
	StateInitialized State = "initialized"
	StateReady       State = "ready"
	StateExecuting   State = "executing"
	StateIdle        State = "idle"
	StateUnloaded    State = "unloaded"
	StateRemoved     State = "removed"
)

// ValidateTransition returns nil if a state transition is legal, or an error if illegal.
func ValidateTransition(current, next State) error {
	if current == next {
		return nil
	}

	allowed := false
	switch current {
	case StateRegistered:
		allowed = (next == StateValidated || next == StateRemoved)
	case StateValidated:
		allowed = (next == StateLoaded || next == StateRemoved)
	case StateLoaded:
		allowed = (next == StateInitialized || next == StateUnloaded || next == StateRemoved)
	case StateInitialized:
		allowed = (next == StateReady || next == StateUnloaded || next == StateRemoved)
	case StateReady:
		allowed = (next == StateExecuting || next == StateUnloaded || next == StateRemoved)
	case StateExecuting:
		allowed = (next == StateIdle || next == StateReady || next == StateUnloaded || next == StateRemoved)
	case StateIdle:
		allowed = (next == StateExecuting || next == StateReady || next == StateUnloaded || next == StateRemoved)
	case StateUnloaded:
		allowed = (next == StateLoaded || next == StateRemoved)
	case StateRemoved:
		allowed = false // Terminal state
	}

	if !allowed {
		return fmt.Errorf("invalid state transition: %s -> %s", current, next)
	}
	return nil
}
