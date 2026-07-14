package events

import (
	"sync"
	"time"
)

// EventType defines reliability and failure mitigation state changes.
type EventType string

const (
	CircuitOpened         EventType = "CircuitOpened"
	CircuitClosed         EventType = "CircuitClosed"
	CircuitHalfOpened     EventType = "CircuitHalfOpened"
	RetryExecuted         EventType = "RetryExecuted"
	BulkheadRejected      EventType = "BulkheadRejected"
	RateLimitExceeded     EventType = "RateLimitExceeded"
	BackpressureActivated EventType = "BackpressureActivated"
	ShutdownStarted       EventType = "ShutdownStarted"
	ShutdownCompleted     EventType = "ShutdownCompleted"
	BackupCompleted       EventType = "BackupCompleted"
	RestoreCompleted      EventType = "RestoreCompleted"
	RecoveryStarted       EventType = "RecoveryStarted"
	RecoveryCompleted     EventType = "RecoveryCompleted"
)

// Event defines the payload distributed across the platform.
type Event struct {
	Type      EventType `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Policy    string    `json:"policy,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	Error     error     `json:"-"`
}

// Handler handles event callbacks.
type Handler func(Event)

// Bus implements a thread-safe message broker.
type Bus struct {
	mu        sync.RWMutex
	listeners []Handler
	isClosed  bool
}

// NewBus creates a new Bus.
func NewBus() *Bus {
	return &Bus{
		listeners: make([]Handler, 0),
	}
}

// Subscribe appends a new listener.
func (b *Bus) Subscribe(h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isClosed {
		return
	}
	b.listeners = append(b.listeners, h)
}

// Publish distributes the event asynchronously.
func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.isClosed {
		return
	}
	for _, l := range b.listeners {
		go l(ev)
	}
}

// Close unsubscribes all listeners.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.listeners = nil
	b.isClosed = true
}
