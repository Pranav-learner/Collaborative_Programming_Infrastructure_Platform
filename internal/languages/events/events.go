package events

import (
	"sync"
	"time"
)

type Type string

const (
	PluginRegistered         Type = "plugin_registered"
	PluginValidated          Type = "plugin_validated"
	PluginLoaded             Type = "plugin_loaded"
	PluginInitialized        Type = "plugin_initialized"
	PluginReady              Type = "plugin_ready"
	PluginExecutionStarted   Type = "plugin_execution_started"
	PluginExecutionCompleted Type = "plugin_execution_completed"
	PluginReloaded           Type = "plugin_reloaded"
	PluginUnloaded           Type = "plugin_unloaded"
	PluginRemoved            Type = "plugin_removed"
)

// Event is a typed plugin lifecycle event.
type Event struct {
	Type          Type      `json:"type"`
	PluginID      string    `json:"plugin_id"`
	Version       string    `json:"version"`
	Timestamp     time.Time `json:"timestamp"`
	CorrelationID string    `json:"correlation_id,omitempty"`
	Payload       any       `json:"payload,omitempty"`
}

// Bus is a local thread-safe event bus for plugin events.
type Bus struct {
	mu          sync.RWMutex
	subscribers map[chan Event]struct{}
}

// NewBus creates a new local plugin event bus.
func NewBus() *Bus {
	return &Bus{
		subscribers: make(map[chan Event]struct{}),
	}
}

// Subscribe returns a channel to receive plugin events.
func (b *Bus) Subscribe(bufSize int) chan Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	ch := make(chan Event, bufSize)
	b.subscribers[ch] = struct{}{}
	return ch
}

// Unsubscribe removes a channel subscriber.
func (b *Bus) Unsubscribe(ch chan Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.subscribers[ch]; ok {
		delete(b.subscribers, ch)
		close(ch)
	}
}

// Publish broadcasts an event non-blocking.
func (b *Bus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	for ch := range b.subscribers {
		select {
		case ch <- e:
		default:
			// drop if full to avoid blocking
		}
	}
}
