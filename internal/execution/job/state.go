// Package job defines the dependency-free domain model for the execution
// orchestrator: the execution Request, the Job entity, its formal lifecycle
// state machine, priority/outcome/resource types, per-job statistics, and the
// canonical error set.
//
// This package imports only the standard library so that every other execution
// package may depend on it without risking an import cycle. It contains data and
// pure functions only — no goroutines, no I/O, no locks.
package job

// State is a position in the job lifecycle state machine.
//
// The nominal happy path is:
//
//	Pending → Validated → Queued → Dispatched → Running → Streaming → Completed
//
// with Failed, TimedOut, and Cancelled as alternative outcomes reachable from
// the in-flight states, Retrying as the re-entry path for recoverable failures,
// and Archived as the single terminal sink.
type State uint8

const (
	// StatePending is a freshly accepted request that has not yet been validated.
	StatePending State = iota
	// StateValidated is a request that passed the validation pipeline.
	StateValidated
	// StateQueued is a job handed to the scheduler and awaiting dispatch.
	StateQueued
	// StateDispatched is a job claimed by a worker but not yet executing.
	StateDispatched
	// StateRunning is a job whose code is executing.
	StateRunning
	// StateStreaming is a running job actively streaming output back to clients.
	StateStreaming
	// StateCompleted is a job that finished successfully.
	StateCompleted
	// StateFailed is a job that finished with an execution or system error.
	StateFailed
	// StateTimedOut is a job terminated for exceeding its wall-clock deadline.
	StateTimedOut
	// StateCancelled is a job cancelled by a user or the system before finishing.
	StateCancelled
	// StateRetrying is a recoverable failure being re-entered into the pipeline.
	StateRetrying
	// StateArchived is the terminal sink for finished jobs past retention.
	StateArchived
)

// String returns the lowercase snake_case name of the state.
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateValidated:
		return "validated"
	case StateQueued:
		return "queued"
	case StateDispatched:
		return "dispatched"
	case StateRunning:
		return "running"
	case StateStreaming:
		return "streaming"
	case StateCompleted:
		return "completed"
	case StateFailed:
		return "failed"
	case StateTimedOut:
		return "timed_out"
	case StateCancelled:
		return "cancelled"
	case StateRetrying:
		return "retrying"
	case StateArchived:
		return "archived"
	default:
		return "unknown"
	}
}

// transitions is the adjacency set of the lifecycle graph: transitions[from]
// contains every state reachable from `from` in one step. A transition is legal
// iff the target appears in the source's set. This is the single source of truth
// for the state machine, consulted by both the Job entity and the registry.
var transitions = map[State]map[State]struct{}{
	StatePending:    setOf(StateValidated, StateFailed, StateCancelled),
	StateValidated:  setOf(StateQueued, StateFailed, StateCancelled),
	StateQueued:     setOf(StateDispatched, StateFailed, StateTimedOut, StateCancelled),
	StateDispatched: setOf(StateRunning, StateFailed, StateTimedOut, StateCancelled),
	StateRunning:    setOf(StateStreaming, StateCompleted, StateFailed, StateTimedOut, StateCancelled),
	StateStreaming:  setOf(StateCompleted, StateFailed, StateTimedOut, StateCancelled),
	// Recoverable failures may be retried (→ Retrying) or aged out (→ Archived).
	StateCompleted: setOf(StateArchived),
	StateFailed:    setOf(StateRetrying, StateArchived),
	StateTimedOut:  setOf(StateRetrying, StateArchived),
	StateCancelled: setOf(StateArchived),
	// Retrying re-enters the queue for another attempt.
	StateRetrying: setOf(StateQueued, StateFailed, StateCancelled),
	StateArchived: setOf(),
}

func setOf(states ...State) map[State]struct{} {
	m := make(map[State]struct{}, len(states))
	for _, s := range states {
		m[s] = struct{}{}
	}
	return m
}

// CanTransition reports whether from → to is a legal one-step transition. A
// self-transition (from == to) is rejected: the caller should treat "no change"
// explicitly rather than through the state machine, so that spurious re-entry is
// surfaced as an illegal transition.
func CanTransition(from, to State) bool {
	next, ok := transitions[from]
	if !ok {
		return false
	}
	_, ok = next[to]
	return ok
}

// IsTerminal reports whether the state admits no further transitions.
func (s State) IsTerminal() bool { return s == StateArchived }

// IsFinished reports whether the job has stopped executing — it reached a final
// outcome (or was archived). Finished jobs are eligible for archival.
func (s State) IsFinished() bool {
	switch s {
	case StateCompleted, StateFailed, StateTimedOut, StateCancelled, StateArchived:
		return true
	default:
		return false
	}
}

// IsActive reports whether the job is somewhere in the in-flight pipeline
// (accepted but not yet finished).
func (s State) IsActive() bool { return !s.IsFinished() }

// CanRetry reports whether the state is a recoverable failure eligible for retry.
func (s State) CanRetry() bool { return s == StateFailed || s == StateTimedOut }

// Outcome classifies the terminal result of a job, independent of its fine-
// grained state, for reporting to callers and metrics.
type Outcome uint8

const (
	// OutcomeNone indicates the job has not reached a final outcome yet.
	OutcomeNone Outcome = iota
	// OutcomeSuccess indicates the job completed successfully.
	OutcomeSuccess
	// OutcomeFailure indicates the job failed.
	OutcomeFailure
	// OutcomeTimeout indicates the job exceeded its deadline.
	OutcomeTimeout
	// OutcomeCancelled indicates the job was cancelled.
	OutcomeCancelled
)

// String returns the lowercase name of the outcome.
func (o Outcome) String() string {
	switch o {
	case OutcomeNone:
		return "none"
	case OutcomeSuccess:
		return "success"
	case OutcomeFailure:
		return "failure"
	case OutcomeTimeout:
		return "timeout"
	case OutcomeCancelled:
		return "cancelled"
	default:
		return "unknown"
	}
}

// OutcomeFor maps a terminal state to its Outcome, or OutcomeNone if the state
// is not a final outcome.
func OutcomeFor(s State) Outcome {
	switch s {
	case StateCompleted:
		return OutcomeSuccess
	case StateFailed:
		return OutcomeFailure
	case StateTimedOut:
		return OutcomeTimeout
	case StateCancelled:
		return OutcomeCancelled
	default:
		return OutcomeNone
	}
}
