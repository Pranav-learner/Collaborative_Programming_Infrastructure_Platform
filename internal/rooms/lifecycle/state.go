// Package lifecycle defines the room state machine: the set of lifecycle states,
// the legal transitions between them, and a Machine that validates transitions.
//
// The package is a pure leaf — it depends on nothing else in the room subtree —
// so every other component (the Room entity, the janitor, the event system) can
// import it without risking an import cycle. It knows the shape of the lifecycle
// but drives nothing itself; the runtime (Room + the manager's janitor) applies
// transitions and reacts to timeouts.
//
// State graph:
//
//	Created ─▶ Waiting ─▶ Active ─▶ Idle ─▶ Expiring ─▶ Closed ─▶ Destroyed
//	              │          ▲        │          │
//	              │          └────────┴──────────┘   (activity/recovery resumes Active)
//	              └────────────────────────────────▶ Closed (early close)
//
// Every non-terminal state may also transition directly to Closed (an explicit
// close or a hard failure can happen at any time), and Closed is the only state
// that may transition to the terminal Destroyed.
package lifecycle

import "fmt"

// State is a room's position in its lifecycle. It is stored as an int32 so a
// Room may hold it atomically for lock-free reads.
type State int32

const (
	// StateCreated is the transient initial state immediately after construction,
	// before the room is registered or anyone has joined.
	StateCreated State = iota
	// StateWaiting means the room exists and is registered but has no connected
	// participants yet (e.g. the owner created it and has not joined, or everyone
	// has temporarily disconnected).
	StateWaiting
	// StateActive means at least one participant is connected and the room has
	// seen recent activity. This is the normal working state.
	StateActive
	// StateIdle means the room still exists but has seen no activity for the
	// configured idle timeout. It is fully functional; activity returns it to
	// Active.
	StateIdle
	// StateExpiring means the room has been idle beyond the expiry timeout and is
	// in its grace period before automatic closure. Activity or a recovery can
	// still rescue it back to Active.
	StateExpiring
	// StateClosed means the room has been terminated: it accepts no new members
	// and its participants have been released. It is retained briefly for
	// observability and reconnection grace before destruction.
	StateClosed
	// StateDestroyed is terminal: the room has been removed from the registry and
	// its resources released. No transitions leave this state.
	StateDestroyed
)

// String renders the state as a stable, low-cardinality label suitable for logs
// and metrics.
func (s State) String() string {
	switch s {
	case StateCreated:
		return "created"
	case StateWaiting:
		return "waiting"
	case StateActive:
		return "active"
	case StateIdle:
		return "idle"
	case StateExpiring:
		return "expiring"
	case StateClosed:
		return "closed"
	case StateDestroyed:
		return "destroyed"
	default:
		return fmt.Sprintf("state(%d)", int32(s))
	}
}

// IsTerminal reports whether s is an end state from which no transition exists.
func (s State) IsTerminal() bool { return s == StateDestroyed }

// Valid reports whether s is a known state.
func (s State) Valid() bool { return s >= StateCreated && s <= StateDestroyed }
