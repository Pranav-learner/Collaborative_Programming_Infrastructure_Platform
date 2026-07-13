package events

import (
	"sync"
	"time"
)

// Type enumerates all types of collaboration events.
type Type uint8

const (
	// DocumentCreated fires when a document is first registered.
	DocumentCreated Type = iota
	// DocumentInitialized fires when the Yjs Doc is allocated.
	DocumentInitialized
	// DocumentSaved fires when incremental updates are persisted.
	DocumentSaved
	// DocumentArchived fires when a document is unloaded from active memory.
	DocumentArchived
	// DocumentRecovered fires when a document is restored from storage.
	DocumentRecovered
	// DocumentDestroyed fires when a document is permanently deleted.
	DocumentDestroyed
	// SyncStepCompleted fires when a sync handshake step completes.
	SyncStepCompleted
	// SnapshotCreated fires when a new snapshot is successfully stored.
	SnapshotCreated
)

// String returns the string representation of the event Type.
func (t Type) String() string {
	switch t {
	case DocumentCreated:
		return "document_created"
	case DocumentInitialized:
		return "document_initialized"
	case DocumentSaved:
		return "document_saved"
	case DocumentArchived:
		return "document_archived"
	case DocumentRecovered:
		return "document_recovered"
	case DocumentDestroyed:
		return "document_destroyed"
	case SyncStepCompleted:
		return "sync_step_completed"
	case SnapshotCreated:
		return "snapshot_created"
	default:
		return "unknown"
	}
}

// Event carries information about a collaboration state change.
type Event struct {
	Type      Type
	DocID     string
	RoomID    string
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
	return &Bus{
		subscribers: make(map[chan Event]struct{}),
		opts:        opts,
	}
}

// Subscribe registers a new subscriber and returns a channel of the given buffer size.
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

// Publish broadcasts an event to all subscribers in a non-blocking fashion.
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
