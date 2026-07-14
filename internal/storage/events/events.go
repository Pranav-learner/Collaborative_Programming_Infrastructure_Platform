// Package events is the module's internal event bus. Every storage subsystem
// publishes typed lifecycle events here; future modules (distributed
// coordination, REST/gRPC APIs, backup system, CDN warmers, analytics) subscribe
// without any coupling to the emitting subsystem.
//
// The bus is deliberately in-process and best-effort: it fans out to synchronous
// handlers and to buffered subscriber channels, dropping for slow subscribers so
// a stalled consumer can never block an upload. Cross-node propagation is the job
// of the distributed coordination module, not this bus.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Type classifies an artifact/storage lifecycle event.
type Type string

const (
	ArtifactCreated        Type = "artifact_created"
	ArtifactUploaded       Type = "artifact_uploaded"
	ArtifactDownloaded     Type = "artifact_downloaded"
	ArtifactDeleted        Type = "artifact_deleted"
	ArtifactRestored       Type = "artifact_restored"
	ArtifactVersionCreated Type = "artifact_version_created"
	ArtifactRolledBack     Type = "artifact_rolled_back"
	RetentionApplied       Type = "retention_applied"
	CleanupStarted         Type = "cleanup_started"
	CleanupCompleted       Type = "cleanup_completed"
	CompressionCompleted   Type = "compression_completed"
	IntegrityValidated     Type = "integrity_validated"
	IntegrityFailed        Type = "integrity_failed"
	BucketCreated          Type = "bucket_created"
	BackendDegraded        Type = "backend_degraded"
	BackendRecovered       Type = "backend_recovered"
)

// Event carries structured details for observability and cross-subsystem
// coordination.
type Event struct {
	EventID   string    `json:"event_id"`
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	// Subsystem names the emitter (e.g. "upload", "manager", "cleanup").
	Subsystem string `json:"subsystem"`
	// ArtifactID / LineageID / Bucket / Key identify the affected entity.
	ArtifactID string `json:"artifact_id,omitempty"`
	LineageID  string `json:"lineage_id,omitempty"`
	Bucket     string `json:"bucket,omitempty"`
	Key        string `json:"key,omitempty"`
	// Owner / JobID / RoomID enable subscribers to route events by tenant/context.
	Owner  string `json:"owner,omitempty"`
	JobID  string `json:"job_id,omitempty"`
	RoomID string `json:"room_id,omitempty"`
	// Payload holds event-specific detail.
	Payload any `json:"payload,omitempty"`
}

// New creates a populated Event with a random ID and current timestamp.
func New(t Type, subsystem string) Event {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return Event{
		EventID:   hex.EncodeToString(b),
		Type:      t,
		Timestamp: time.Now().UTC(),
		Subsystem: subsystem,
	}
}

// Handler is a synchronous callback invoked on every Publish.
type Handler func(Event)

// Bus is a thread-safe best-effort event broker.
type Bus struct {
	mu          sync.RWMutex
	handlers    []Handler
	subscribers []chan Event
	closed      bool
}

// NewBus creates an empty event bus.
func NewBus() *Bus { return &Bus{} }

// Subscribe registers a buffered channel that receives all future events until
// the bus is closed. A slow subscriber loses events rather than blocking others.
func (b *Bus) Subscribe(bufSize int) <-chan Event {
	if b == nil {
		ch := make(chan Event)
		close(ch)
		return ch
	}
	if bufSize <= 0 {
		bufSize = 256
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, bufSize)
	b.subscribers = append(b.subscribers, ch)
	return ch
}

// OnEvent registers a synchronous handler that fires on every Publish. Handlers
// must not block; heavy work should be dispatched to a goroutine by the handler.
func (b *Bus) OnEvent(h Handler) {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, h)
}

// OnType registers a handler that fires only for a specific event type.
func (b *Bus) OnType(t Type, h Handler) {
	b.OnEvent(func(e Event) {
		if e.Type == t {
			h(e)
		}
	})
}

// Publish emits an event to all handlers and subscribers.
func (b *Bus) Publish(e Event) {
	if b == nil {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return
	}
	for _, h := range b.handlers {
		h(e)
	}
	for _, ch := range b.subscribers {
		select {
		case ch <- e:
		default:
		}
	}
}

// Emit builds and publishes an event in one call, applying mutate to populate
// context fields.
func (b *Bus) Emit(t Type, subsystem string, mutate func(*Event)) {
	if b == nil {
		return
	}
	e := New(t, subsystem)
	if mutate != nil {
		mutate(&e)
	}
	b.Publish(e)
}

// Close drains and closes all subscriber channels. Idempotent.
func (b *Bus) Close() {
	if b == nil {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for _, ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = nil
	b.handlers = nil
}
