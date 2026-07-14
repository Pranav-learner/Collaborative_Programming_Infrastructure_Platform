// Package events defines the persistence-lifecycle event bus for the
// configuration platform. All subsystems publish here; future modules subscribe.
package events

import (
	"sync"
	"time"
)

// Type classifies configuration lifecycle events.
type Type string

const (
	ConfigurationLoaded        Type = "configuration.loaded"
	ConfigurationReloaded      Type = "configuration.reloaded"
	ConfigurationValidated     Type = "configuration.validated"
	ConfigurationRolledBack    Type = "configuration.rolledback"
	SecretLoaded               Type = "secret.loaded"
	SecretRotated              Type = "secret.rotated"
	FeatureFlagChanged         Type = "featureflag.changed"
	RuntimeConfigUpdated       Type = "runtime.config.updated"
	ProviderRegistered         Type = "provider.registered"
	ValidationFailed           Type = "validation.failed"
)

// Event carries structured details about a configuration lifecycle event.
type Event struct {
	Type      Type      `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Key       string    `json:"key,omitempty"`
	Provider  string    `json:"provider,omitempty"`
	Profile   string    `json:"profile,omitempty"`
	Version   int       `json:"version,omitempty"`
	Detail    string    `json:"detail,omitempty"`
}

// Handler is a synchronous callback for events.
type Handler func(Event)

// Bus is a thread-safe event fanout broker.
type Bus struct {
	mu       sync.RWMutex
	handlers []Handler
}

// NewBus creates a new event bus.
func NewBus() *Bus { return &Bus{} }

// OnEvent registers a handler.
func (b *Bus) OnEvent(h Handler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.handlers = append(b.handlers, h)
}

// Publish fires an event to all registered handlers.
func (b *Bus) Publish(e Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, h := range b.handlers {
		h(e)
	}
}
