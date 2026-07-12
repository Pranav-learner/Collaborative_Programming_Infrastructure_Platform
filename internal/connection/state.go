package connection

import "sync/atomic"

// State is the lifecycle state of a connection. It is stored atomically so it can
// be read from any goroutine (e.g. Send checks it before enqueuing).
type State int32

const (
	// StateConnecting is the initial state before the pumps start.
	StateConnecting State = iota
	// StateActive means the read/write pumps are running and traffic may flow.
	StateActive
	// StateClosing means a close has been initiated; no new sends are accepted.
	StateClosing
	// StateClosed is terminal; all goroutines have exited and cleanup ran.
	StateClosed
)

func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateActive:
		return "active"
	case StateClosing:
		return "closing"
	case StateClosed:
		return "closed"
	default:
		return "unknown"
	}
}

// atomicState wraps atomic access to a State value.
type atomicState struct{ v atomic.Int32 }

func (a *atomicState) Load() State   { return State(a.v.Load()) }
func (a *atomicState) Store(s State) { a.v.Store(int32(s)) }
