// Package events provides the execution orchestrator's internal event bus and
// the typed events published across the job lifecycle. It is a thread-safe,
// non-blocking local bus: a slow subscriber is dropped for a given event rather
// than being allowed to stall the orchestrator. Future modules (queue, workers,
// monitoring) subscribe to react to lifecycle changes.
package events

import (
	"sync"
	"time"

	"cpip/internal/execution/job"
)

// Type enumerates the execution lifecycle events.
type Type uint8

const (
	// ExecutionRequested fires when a submission is accepted for processing.
	ExecutionRequested Type = iota
	// ExecutionValidated fires when a request passes the validation pipeline.
	ExecutionValidated
	// ExecutionRejected fires when a request fails validation (no job created).
	ExecutionRejected
	// JobCreated fires when a Job entity is created and registered.
	JobCreated
	// JobQueued fires when a job is handed to the scheduler.
	JobQueued
	// JobDispatched fires when a worker claims a job.
	JobDispatched
	// JobStarted fires when a job begins executing.
	JobStarted
	// JobCancelled fires when a job is cancelled.
	JobCancelled
	// JobRetried fires when a job is re-entered for another attempt.
	JobRetried
	// JobCompleted fires when a job finishes successfully.
	JobCompleted
	// JobFailed fires when a job finishes with an error.
	JobFailed
	// JobTimedOut fires when a job exceeds its deadline.
	JobTimedOut
	// ExecutionArchived fires when a finished job is archived.
	ExecutionArchived
)

// String returns the lowercase snake_case name of the event type.
func (t Type) String() string {
	switch t {
	case ExecutionRequested:
		return "execution_requested"
	case ExecutionValidated:
		return "execution_validated"
	case ExecutionRejected:
		return "execution_rejected"
	case JobCreated:
		return "job_created"
	case JobQueued:
		return "job_queued"
	case JobDispatched:
		return "job_dispatched"
	case JobStarted:
		return "job_started"
	case JobCancelled:
		return "job_cancelled"
	case JobRetried:
		return "job_retried"
	case JobCompleted:
		return "job_completed"
	case JobFailed:
		return "job_failed"
	case JobTimedOut:
		return "job_timed_out"
	case ExecutionArchived:
		return "execution_archived"
	default:
		return "unknown"
	}
}

// Event carries information about an execution lifecycle change.
type Event struct {
	Type          Type
	JobID         string
	RequestID     string
	CorrelationID string
	UserID        string
	SessionID     string
	RoomID        string
	Language      string
	State         job.State
	Timestamp     time.Time
	// Reason carries a human-readable cause for rejection/failure events.
	Reason string
	// Payload carries an optional event-specific value (e.g. a job snapshot).
	Payload any
}

// Options configure the behavior of the event Bus.
type Options struct {
	OnPublish func()
	OnDrop    func()
}

// Bus is a thread-safe, non-blocking local event bus.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
	opts        Options
}

// New constructs an event Bus.
func New(opts Options) *Bus {
	return &Bus{subscribers: make(map[chan Event]struct{}), opts: opts}
}

// Subscribe registers a new subscriber and returns a buffered channel.
func (b *Bus) Subscribe(bufSize int) chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, bufSize)
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe unregisters a subscriber channel and closes it.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.subscribers[ch]; exists {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// Publish broadcasts an event to all subscribers without blocking. If a
// subscriber's buffer is full the event is dropped for that subscriber and the
// OnDrop hook is invoked.
func (b *Bus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	if b.opts.OnPublish != nil {
		b.opts.OnPublish()
	}
	for ch := range b.subscribers {
		select {
		case ch <- e:
		default:
			if b.opts.OnDrop != nil {
				b.opts.OnDrop()
			}
		}
	}
}

// Close unsubscribes and closes all active subscriber channels.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = make(map[chan Event]struct{})
}
