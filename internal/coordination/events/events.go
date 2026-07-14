// Package events is the module's Cluster Event Bus. Every coordination subsystem
// publishes typed lifecycle events here; future modules (distributed scheduler,
// autoscaler, Kubernetes bridge, observability) subscribe without any coupling to
// the emitting subsystem.
//
// The bus is in-process and best-effort: it fans out to synchronous handlers and
// buffered subscriber channels, dropping for slow subscribers so a stalled
// consumer can never block a heartbeat or an election. CROSS-NODE propagation is
// handled separately by the replication framework (which bridges selected events
// onto the backend's pub/sub); this bus is the local fan-out.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Type classifies a cluster coordination event.
type Type string

const (
	NodeJoined        Type = "node_joined"
	NodeLeft          Type = "node_left"
	NodeUpdated       Type = "node_updated"
	LeaderElected     Type = "leader_elected"
	LeaderLost        Type = "leader_lost"
	LeaderStepped     Type = "leader_stepped_down"
	HeartbeatReceived Type = "heartbeat_received"
	HeartbeatExpired  Type = "heartbeat_expired"
	NodeSuspected     Type = "node_suspected"
	StateReplicated   Type = "state_replicated"
	LockAcquired      Type = "lock_acquired"
	LockReleased      Type = "lock_released"
	LockExpired       Type = "lock_expired"
	ServiceDiscovered Type = "service_discovered"
	MembershipChanged Type = "membership_changed"
	HealthChanged     Type = "health_changed"
	BackendDegraded   Type = "backend_degraded"
	BackendRecovered  Type = "backend_recovered"
)

// Event carries structured details for observability and cross-subsystem
// coordination.
type Event struct {
	EventID   string    `json:"event_id"`
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	// Subsystem names the emitter (e.g. "membership", "leader", "heartbeat").
	Subsystem string `json:"subsystem"`
	// NodeID / Resource / LeaderID identify the affected entity where applicable.
	NodeID   string `json:"node_id,omitempty"`
	Resource string `json:"resource,omitempty"`
	LeaderID string `json:"leader_id,omitempty"`
	// Origin is the node that emitted the event (for cross-node dedup).
	Origin string `json:"origin,omitempty"`
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
