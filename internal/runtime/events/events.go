package events

import (
	"sync"
	"time"
)

// Type enumerates the events that can occur in the runtime session lifecycle.
type Type uint8

const (
	RuntimeCreated Type = iota
	CompilationStarted
	CompilationFinished
	ExecutionStarted
	ExecutionProgress
	StdoutChunk
	StderrChunk
	ExecutionCompleted
	ExecutionFailed
	ExecutionCancelled
	ExecutionTimedOut
	CleanupCompleted
)

// String returns the lowercase string representation of the event Type.
func (t Type) String() string {
	switch t {
	case RuntimeCreated:
		return "runtime_created"
	case CompilationStarted:
		return "compilation_started"
	case CompilationFinished:
		return "compilation_finished"
	case ExecutionStarted:
		return "execution_started"
	case ExecutionProgress:
		return "execution_progress"
	case StdoutChunk:
		return "stdout_chunk"
	case StderrChunk:
		return "stderr_chunk"
	case ExecutionCompleted:
		return "execution_completed"
	case ExecutionFailed:
		return "execution_failed"
	case ExecutionCancelled:
		return "execution_cancelled"
	case ExecutionTimedOut:
		return "execution_timed_out"
	case CleanupCompleted:
		return "cleanup_completed"
	default:
		return "unknown"
	}
}

// Event represents a single typed event occurring during runtime execution.
type Event struct {
	Type          Type      `json:"type"`
	SessionID     string    `json:"session_id"`
	JobID         string    `json:"job_id"`
	CorrelationID string    `json:"correlation_id"`
	Language      string    `json:"language"`
	Timestamp     time.Time `json:"timestamp"`
	Payload       any       `json:"payload,omitempty"`
}

// Options configure event Bus hooks.
type Options struct {
	OnPublish func()
	OnDrop    func()
}

// Bus is a thread-safe, non-blocking in-memory event bus.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
	opts        Options
}

// NewBus constructs a new local event Bus.
func NewBus(opts Options) *Bus {
	return &Bus{
		subscribers: make(map[chan Event]struct{}),
		opts:        opts,
	}
}

// Subscribe registers a new subscriber with a buffered channel of the given size.
func (b *Bus) Subscribe(bufSize int) chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, bufSize)
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe unregisters and closes a subscriber's channel.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.subscribers[ch]; exists {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// Publish broadcasts an event to all subscribers in a non-blocking manner.
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

// Close closes all subscriber channels.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = make(map[chan Event]struct{})
}
