// Package events is the module's internal event bus. Every subsystem publishes
// typed lifecycle events here; future modules (coordination layer, REST/gRPC
// APIs, analytics) subscribe without any coupling to the emitting subsystem.
//
// The bus is deliberately in-process and best-effort: it fans out to synchronous
// handlers and to buffered subscriber channels, dropping for slow subscribers so
// a stalled consumer can never block a cache write. Cross-node event propagation
// is the job of the pub/sub and replication packages, not this bus.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Type classifies a cache/state lifecycle event.
type Type string

const (
	CacheHit           Type = "cache_hit"
	CacheMiss          Type = "cache_miss"
	CacheSet           Type = "cache_set"
	CacheDeleted       Type = "cache_deleted"
	CacheInvalidated   Type = "cache_invalidated"
	TTLExpired         Type = "ttl_expired"
	SessionCreated     Type = "session_created"
	SessionRenewed     Type = "session_renewed"
	SessionExpired     Type = "session_expired"
	SessionInvalidated Type = "session_invalidated"
	PresenceReplicated Type = "presence_replicated"
	PresenceApplied    Type = "presence_applied"
	LockAcquired       Type = "lock_acquired"
	LockReleased       Type = "lock_released"
	LockRenewed        Type = "lock_renewed"
	LockExpired        Type = "lock_expired"
	MessagePublished   Type = "message_published"
	MessageReceived    Type = "message_received"
	StateUpdated       Type = "state_updated"
	StateSynced        Type = "state_synced"
	RedisDegraded      Type = "redis_degraded"
	RedisRecovered     Type = "redis_recovered"
)

// Event carries structured details for observability and cross-subsystem
// coordination.
type Event struct {
	EventID   string    `json:"event_id"`
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	// Subsystem names the emitter (e.g. "manager", "sessions", "locks").
	Subsystem string `json:"subsystem"`
	// Cache/Key/Resource identify the affected entity where applicable.
	Cache    string `json:"cache,omitempty"`
	Key      string `json:"key,omitempty"`
	Resource string `json:"resource,omitempty"`
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
		Timestamp: time.Now(),
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

// Emit is a convenience that builds and publishes an event in one call.
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
