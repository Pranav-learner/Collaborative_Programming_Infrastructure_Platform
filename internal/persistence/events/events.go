package events

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Type classifies persistence lifecycle events.
type Type string

const (
	TransactionStarted     Type = "tx_started"
	TransactionCommitted   Type = "tx_committed"
	TransactionRolledBack  Type = "tx_rolledback"
	MigrationStarted       Type = "migration_started"
	MigrationCompleted     Type = "migration_completed"
	RepositoryCreated      Type = "repository_created"
	EntityPersisted        Type = "entity_persisted"
	EntityUpdated          Type = "entity_updated"
	EntityDeleted          Type = "entity_deleted"
	OptimisticLockConflict Type = "optimistic_lock_conflict"
	AuditRecorded          Type = "audit_recorded"
)

// PersistenceEvent wraps structured details for audit, coordination, and observability.
type PersistenceEvent struct {
	EventID       string    `json:"event_id"`
	Type          Type      `json:"type"`
	Timestamp     time.Time `json:"timestamp"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	EntityName    string    `json:"entity_name,omitempty"`
	EntityID      string    `json:"entity_id,omitempty"`
	Payload       any       `json:"payload,omitempty"`
}

// NewPersistenceEvent creates a populated PersistenceEvent.
func NewPersistenceEvent(evtType Type, correlationID, entityName, entityID string, payload any) PersistenceEvent {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return PersistenceEvent{
		EventID:       hex.EncodeToString(b),
		Type:          evtType,
		Timestamp:     time.Now(),
		CorrelationID: correlationID,
		EntityName:    entityName,
		EntityID:      entityID,
		Payload:       payload,
	}
}

// Handler is a callback invoked when a persistence event fires.
type Handler func(PersistenceEvent)

// Bus is a thread-safe broker for persistence events. Subscribers receive
// events asynchronously on buffered channels; a slow subscriber will not
// block other subscribers or the publisher.
type Bus struct {
	mu          sync.RWMutex
	handlers    []Handler
	subscribers []chan PersistenceEvent
}

// NewBus creates an empty event bus.
func NewBus() *Bus {
	return &Bus{}
}

// Subscribe registers a buffered channel that will receive all future events.
// Returns the channel so the caller can range over it.
func (b *Bus) Subscribe(bufSize int) <-chan PersistenceEvent {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan PersistenceEvent, bufSize)
	b.subscribers = append(b.subscribers, ch)
	return ch
}

// OnEvent registers a synchronous handler that fires on every Publish.
func (b *Bus) OnEvent(h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, h)
}

// Publish emits a persistence event to all subscribers and handlers.
func (b *Bus) Publish(e PersistenceEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	for _, h := range b.handlers {
		h(e)
	}
	for _, ch := range b.subscribers {
		select {
		case ch <- e:
		default:
			// Drop events for slow consumers rather than blocking.
		}
	}
}

// Close drains and closes all subscriber channels.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, ch := range b.subscribers {
		close(ch)
	}
	b.subscribers = nil
	b.handlers = nil
}

// --- Global convenience (for TransactionManager which has no injected Bus) ---

var globalBus = NewBus()

// GlobalBus returns the process-wide persistence event bus.
func GlobalBus() *Bus { return globalBus }

// Publish is a convenience that publishes to the global bus.
func Publish(e PersistenceEvent) {
	globalBus.Publish(e)
}
