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
	// DocumentLoaded fires when an existing document is loaded into active memory.
	DocumentLoaded
	// DocumentInitialized fires when the Yjs Doc is allocated and ready for edits.
	DocumentInitialized
	// DocumentSaved fires when incremental updates or a snapshot are persisted.
	DocumentSaved
	// DocumentPersisted fires when a document's full state is durably persisted.
	DocumentPersisted
	// DocumentArchived fires when a document is unloaded from active memory.
	DocumentArchived
	// DocumentRecovered fires when a document is restored from storage.
	DocumentRecovered
	// DocumentDestroyed fires when a document is permanently deleted.
	DocumentDestroyed
	// DocumentSnapshotCreated fires when a new snapshot is successfully stored.
	DocumentSnapshotCreated

	// UpdateApplied fires when a remote update is merged into a document.
	UpdateApplied
	// UpdateGenerated fires when the engine generates an update to send to a peer.
	UpdateGenerated

	// SynchronizationStarted fires when a sync handshake begins for a participant.
	SynchronizationStarted
	// SynchronizationCompleted fires when a sync handshake completes successfully.
	SynchronizationCompleted
	// SynchronizationFailed fires when a sync handshake fails.
	SynchronizationFailed
	// SyncStepCompleted fires when an individual sync handshake step completes.
	SyncStepCompleted

	// ConflictResolved fires when the CRDT engine reconciles concurrent edits.
	ConflictResolved
	// ParticipantSynchronized fires when a participant reaches the synced state.
	ParticipantSynchronized
	// ParticipantJoined fires when a participant joins a document.
	ParticipantJoined
	// ParticipantLeft fires when a participant leaves a document.
	ParticipantLeft
)

// Retained aliases for backwards compatibility with earlier revisions.
const (
	// SnapshotCreated is retained as an alias of DocumentSnapshotCreated.
	SnapshotCreated = DocumentSnapshotCreated
)

// String returns the string representation of the event Type.
func (t Type) String() string {
	switch t {
	case DocumentCreated:
		return "document_created"
	case DocumentLoaded:
		return "document_loaded"
	case DocumentInitialized:
		return "document_initialized"
	case DocumentSaved:
		return "document_saved"
	case DocumentPersisted:
		return "document_persisted"
	case DocumentArchived:
		return "document_archived"
	case DocumentRecovered:
		return "document_recovered"
	case DocumentDestroyed:
		return "document_destroyed"
	case DocumentSnapshotCreated:
		return "document_snapshot_created"
	case UpdateApplied:
		return "update_applied"
	case UpdateGenerated:
		return "update_generated"
	case SynchronizationStarted:
		return "synchronization_started"
	case SynchronizationCompleted:
		return "synchronization_completed"
	case SynchronizationFailed:
		return "synchronization_failed"
	case SyncStepCompleted:
		return "sync_step_completed"
	case ConflictResolved:
		return "conflict_resolved"
	case ParticipantSynchronized:
		return "participant_synchronized"
	case ParticipantJoined:
		return "participant_joined"
	case ParticipantLeft:
		return "participant_left"
	default:
		return "unknown"
	}
}

// Event carries information about a collaboration state change. It is the unit
// broadcast on the Bus; downstream subsystems (gateway, presence, execution,
// future replication) subscribe and react.
type Event struct {
	Type          Type
	DocID         string
	RoomID        string
	ParticipantID string
	Version       uint64
	Timestamp     time.Time
	Payload       any
}

// UpdatePayload is carried in the Payload field of UpdateGenerated and
// UpdateApplied events. It gives subscribers (notably the WebSocket gateway,
// which fans updates out to peers) the raw binary CRDT update together with the
// information needed to route it without re-echoing it to its author.
type UpdatePayload struct {
	// Data is the binary V1 CRDT update.
	Data []byte
	// Remote reports whether the update was applied from a peer (true) or
	// produced by a local mutation of the authoritative server document (false).
	Remote bool
	// FilePath identifies the logical file (named shared type) the update targets,
	// enabling multi-file routing. Empty for single-file documents.
	FilePath string
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
