package events

import (
	"sync"
	"time"
)

// EventType represents different deployment lifecycle phases.
type EventType string

const (
	DeploymentStarted   EventType = "DeploymentStarted"
	DeploymentSucceeded EventType = "DeploymentSucceeded"
	DeploymentFailed    EventType = "DeploymentFailed"
	RollbackStarted     EventType = "RollbackStarted"
	RollbackCompleted   EventType = "RollbackCompleted"
	ValidationSucceeded EventType = "ValidationSucceeded"
	ValidationFailed    EventType = "ValidationFailed"
	ProfileSelected     EventType = "ProfileSelected"
	ServiceDeployed     EventType = "ServiceDeployed"
)

// Event defines a deployment status event payload.
type Event struct {
	Type      EventType `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Profile   string    `json:"profile"`
	Provider  string    `json:"provider"`
	Version   int       `json:"version,omitempty"`
	Detail    string    `json:"detail,omitempty"`
	Error     error     `json:"-"`
}

// Handler defines a callback interface for listening to events.
type Handler func(Event)

// Bus implements a thread-safe publish-subscribe message broker.
type Bus struct {
	mu          sync.RWMutex
	listeners   []Handler
	isClosed    bool
}

// NewBus creates a new Bus.
func NewBus() *Bus {
	return &Bus{
		listeners: make([]Handler, 0),
	}
}

// Subscribe appends an event subscriber.
func (b *Bus) Subscribe(h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.isClosed {
		return
	}
	b.listeners = append(b.listeners, h)
}

// Publish distributes an event to all subscribers asynchronously.
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
