// Package events provides the queue subsystem's internal event bus and the typed
// events published across the message and worker lifecycles. It is a thread-safe,
// non-blocking local bus: a slow subscriber is dropped for a given event rather
// than being allowed to stall the queue. Future modules (monitoring, autoscaler)
// subscribe to react.
package events

import (
	"sync"
	"time"

	"cpip/internal/queue/types"
)

// Type enumerates the queue lifecycle events.
type Type uint8

const (
	MessageQueued Type = iota
	MessageClaimed
	MessageAcknowledged
	JobDispatched
	RetryScheduled
	RetryFailed
	MovedToDeadLetter
	WorkerRegistered
	WorkerHeartbeat
	WorkerOffline
	WorkerRecovered
	ConsumerStarted
	ConsumerStopped
)

// String returns the snake_case name of the event type.
func (t Type) String() string {
	switch t {
	case MessageQueued:
		return "message_queued"
	case MessageClaimed:
		return "message_claimed"
	case MessageAcknowledged:
		return "message_acknowledged"
	case JobDispatched:
		return "job_dispatched"
	case RetryScheduled:
		return "retry_scheduled"
	case RetryFailed:
		return "retry_failed"
	case MovedToDeadLetter:
		return "moved_to_dead_letter"
	case WorkerRegistered:
		return "worker_registered"
	case WorkerHeartbeat:
		return "worker_heartbeat"
	case WorkerOffline:
		return "worker_offline"
	case WorkerRecovered:
		return "worker_recovered"
	case ConsumerStarted:
		return "consumer_started"
	case ConsumerStopped:
		return "consumer_stopped"
	default:
		return "unknown"
	}
}

// Event carries information about a queue lifecycle change.
type Event struct {
	Type      Type
	MessageID string
	JobID     string
	WorkerID  string
	Stream    string
	Group     string
	Consumer  string
	State     types.MessageState
	Attempt   int
	Reason    string
	Timestamp time.Time
	Payload   any
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

// Publish broadcasts an event to all subscribers without blocking.
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
