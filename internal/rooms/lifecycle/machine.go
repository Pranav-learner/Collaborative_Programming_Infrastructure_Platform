package lifecycle

import (
	"errors"
	"fmt"
)

// ErrInvalidTransition is returned when a transition that the machine forbids is
// attempted. Callers compare with errors.Is.
var ErrInvalidTransition = errors.New("lifecycle: invalid transition")

// transitions is the adjacency set of the state graph: transitions[from]
// contains every state reachable from `from` in one step. A transition is legal
// iff the target appears in the source's set.
//
// Note that every non-terminal, non-closed state includes StateClosed so an
// explicit close or hard failure is always permitted, and only StateClosed
// reaches StateDestroyed.
var transitions = map[State]map[State]struct{}{
	StateCreated: setOf(StateWaiting, StateActive, StateClosed),
	StateWaiting: setOf(StateActive, StateExpiring, StateClosed),
	StateActive:  setOf(StateWaiting, StateIdle, StateClosed),
	StateIdle:    setOf(StateActive, StateWaiting, StateExpiring, StateClosed),
	// Expiring is the grace period: activity or recovery rescues it to Active;
	// otherwise it proceeds to Closed.
	StateExpiring:  setOf(StateActive, StateClosed),
	StateClosed:    setOf(StateDestroyed),
	StateDestroyed: setOf(),
}

func setOf(states ...State) map[State]struct{} {
	m := make(map[State]struct{}, len(states))
	for _, s := range states {
		m[s] = struct{}{}
	}
	return m
}

// Machine validates room state transitions against the lifecycle graph. It is
// stateless and immutable, therefore safe for concurrent use and shareable by
// value across all rooms; the current state lives on each Room, not here.
type Machine struct{}

// NewMachine returns the room lifecycle machine.
func NewMachine() Machine { return Machine{} }

// CanTransition reports whether from → to is a legal one-step transition. A
// self-transition (from == to) is always legal and is treated as a no-op by
// callers.
func (Machine) CanTransition(from, to State) bool {
	if from == to {
		return true
	}
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// Validate returns nil if from → to is legal, else an error wrapping
// ErrInvalidTransition.
func (m Machine) Validate(from, to State) error {
	if !from.Valid() || !to.Valid() {
		return fmt.Errorf("%w: %s -> %s (unknown state)", ErrInvalidTransition, from, to)
	}
	if !m.CanTransition(from, to) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
	}
	return nil
}

// Next returns the sorted-free set of states reachable from `from` in one step.
// It is primarily useful for tests and introspection/debug tooling.
func (Machine) Next(from State) []State {
	next := transitions[from]
	out := make([]State, 0, len(next))
	for s := range next {
		out = append(out, s)
	}
	return out
}
